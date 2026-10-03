/**
 * This file defines the life of one delivery as pure transitions over a mailbox's delivery slot.
 *
 * Every store runs these inside its own atomic update: memory and the Durable Object over a mailbox
 * kept as one value, Postgres over the rows it locked in one transaction, and Redis over one read of
 * the mailbox, written only if nothing changed it since. Keeping them here means the stores cannot
 * drift apart.
 *
 * A transition that can be refused is an Effect that fails with the refusal, such as `ClaimNotOwned`
 * or `DeliveryClosed`. It needs no services and does no I/O, so a store can run it inside its atomic
 * section; a refused change writes nothing.
 *
 * ```text
 * Local ──handoff──▶ ExternalCleaning ──callback returns──▶ ExternalWaiting ──result──▶ Finishing ──output settled──▶ Retired
 *   │                     │ result: finish once the callback returns                          ▲
 *   │ retryable failure   └───────────────────────────────────────────────────────────────────┘
 *   ▼
 * Retry ──due──▶ Local (new claim, same batch)
 * ```
 *
 * Output runs only while no callback code does: in `ExternalWaiting` and `Finishing`. Operations run
 * one at a time, in the order they were saved. A delivery retires once its callback has returned and
 * every operation is delivered or has failed.
 */
import { Array as Arr, Data, Effect, Match, Predicate, Schema, Struct } from 'effect'

import { DeliveryActivity, SetActivity, sameDeliveryActivity } from './DeliveryActivity'
import { DeliveryOperationKind, PreparedDeliveryInvocation, type DeliveryStage } from './DeliveryContext'
import {
	DeliveryClosed,
	DeliveryMessageConflict,
	DeliveryMessageDeleted,
	DeliveryMessageNotFound,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryOperationUnsupported,
	DeliveryReactionTargetUnavailable,
	DeliveryStatus,
	DeliveryTerminalConflict,
	sameDeliveryTerminal,
	terminalFromMutation,
	type DeliveryMessageMutation,
	type DeliveryMutation,
	type PutDeliveryPlan,
	type SetDeliveryActivity,
	type SetDeliveryReaction,
} from './DeliveryControl'
import { AddExternalLink, ExternalLink } from './DeliveryLink'
import {
	CreateMessage,
	DeleteMessage,
	UpdateMessage,
	type MessageId,
	type ProviderMessageReference,
} from './DeliveryMessage'
import {
	DeliveryOperation,
	DeliveryOperationId,
	DeliveryOperationState,
	DeliveryOutputStatus,
	deliveryOutputStatus,
	isUnsettledOperation,
	operationMessageId,
	type DeliveryOutputOperation,
	type DeliveryOutputSettlement,
} from './DeliveryOperation'
import { DeliveryOutcome, DeliveryTerminal, PresentOutcome } from './DeliveryOutcome'
import {
	DeliveryPlanRevision,
	DeliveryPlanState,
	DeliveryPlanStatus,
	RenderPlan,
	RenderedDeliveryPlan,
	deliveryPlanStatus,
	sameDeliveryPlan,
} from './DeliveryPlan'
import {
	DeliveryReactionTarget,
	SetMessageReaction,
	deliveryReactionTargetKind,
	sameDeliveryReactionTarget,
	type PortableReaction,
} from './DeliveryReaction'
import {
	BatchId,
	DeliveryAccessToken,
	deliveryAccessTokenMatches,
	type DeliveryReference,
} from './DeliveryReference'
import { Timestamp } from './MailboxPolicy'
import { DeliveryAdmissionBatch } from './ProviderEventProcessing'

/** How long a finished delivery stays readable, so a remote worker can retry its last request. */
export const DELIVERY_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000

/** The most finished deliveries one mailbox keeps. The oldest go first. */
export const MAX_RETAINED_DELIVERIES = 20

/** The final message of a delivery whose remote worker sent nothing before its handoff's time limit. */
export const DELIVERY_TIMED_OUT_MARKDOWN = 'The remote worker stopped responding, so this was ended.'

/** The default wait before a retryable failure runs again. */
export const DEFAULT_RETRY_AFTER_MS = 1_000

/**
 * Where the active delivery is. `Finishing` means its callback has returned and it is sending the
 * output it still owes before it retires.
 */
export const ActiveDeliveryStage = Schema.Literals(['Local', 'Retry', 'ExternalCleaning', 'ExternalWaiting', 'Finishing'])
export type ActiveDeliveryStage = typeof ActiveDeliveryStage.Type

/**
 * The batch a mailbox is working on.
 *
 * @property claimId - the attempt that owns the batch; kept through `ExternalCleaning` as ownership of callback cleanup
 * @property terminal - the remote worker's result, when it has sent one
 * @property plan - the plan the remote worker last sent, and the plan a provider last showed
 * @property operations - provider output the delivery owes or has sent, in the order it was saved
 */
export const ActiveDelivery = Schema.Struct({
	batchId: BatchId,
	accessToken: DeliveryAccessToken,
	admissions: DeliveryAdmissionBatch,
	attempt: Schema.Int.check(Schema.isGreaterThan(0)),
	claimId: Schema.NullOr(Schema.NonEmptyString),
	stage: ActiveDeliveryStage,
	prepared: Schema.optionalKey(PreparedDeliveryInvocation),
	terminal: Schema.optionalKey(DeliveryTerminal),
	interruptRequestedAt: Schema.optionalKey(Timestamp),
	handedOffAt: Schema.optionalKey(Timestamp),
	/** How long the remote worker may go without a request before the delivery fails on its own. Absent means no limit. */
	failAfterMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
	/** When the delivery fails on its own, unless the remote worker sends a request first. */
	failAt: Schema.optionalKey(Timestamp),
	links: Schema.Array(ExternalLink),
	plan: Schema.optionalKey(DeliveryPlanState),
	/** Stores written before output existed read it as empty. */
	operations: Schema.Array(DeliveryOperation).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
})
export type ActiveDelivery = typeof ActiveDelivery.Type

/** A finished delivery, kept for status reads and repeated requests. */
export const RetainedDelivery = Schema.Struct({
	batchId: BatchId,
	accessToken: DeliveryAccessToken,
	supportedOperations: Schema.Array(DeliveryOperationKind),
	terminal: Schema.optionalKey(DeliveryTerminal),
	interruptRequested: Schema.Boolean,
	retainUntil: Timestamp,
	/** Stores written before output existed read these as empty. */
	links: Schema.Array(ExternalLink).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
	output: Schema.Array(DeliveryOutputStatus).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
	plan: Schema.optionalKey(DeliveryPlanStatus),
})
export type RetainedDelivery = typeof RetainedDelivery.Type

/**
 * One mailbox's deliveries.
 *
 * @property readyAt - when the mailbox next needs processing: a lease deadline, a retry, due output, or waiting events
 */
export const DeliverySlot = Schema.Struct({
	active: Schema.NullOr(ActiveDelivery),
	readyAt: Schema.NullOr(Timestamp),
	retained: Schema.Array(RetainedDelivery),
})
export type DeliverySlot = typeof DeliverySlot.Type

export const emptyDeliverySlot = DeliverySlot.make({ active: null, readyAt: null, retained: [] })

/**
 * The part of an `ActiveDelivery` that Postgres and Redis keep as one JSON value. They keep the batch
 * ID, token, admissions, callback choice, and stage in columns or fields of their own.
 */
export const StoredActiveDelivery = ActiveDelivery.mapFields(
	Struct.omit(['batchId', 'accessToken', 'admissions', 'prepared', 'stage']),
)
export interface StoredActiveDelivery extends Schema.Schema.Type<typeof StoredActiveDelivery> {}

/**
 * How a polling store's scheduler sees a mailbox: no active delivery, one waiting to retry, or one in
 * any other stage. Postgres and Redis keep it beside the slot.
 */
export const MailboxSchedulerStatus = Schema.Literals(['idle', 'active', 'retry'])
export type MailboxSchedulerStatus = typeof MailboxSchedulerStatus.Type

export const mailboxSchedulerStatus = (slot: DeliverySlot): MailboxSchedulerStatus => {
	if (Predicate.isNull(slot.active)) return 'idle'
	return slot.active.stage === 'Retry' ? 'retry' : 'active'
}

/** What a transition needs to know about the rest of the mailbox. */
export type MailboxFacts = {
	readonly now: number
	readonly hasWaiting: boolean
}

/** A slot after a claim, and the delivery the claim now owns, if any. */
export type DeliverySlotClaim = { readonly slot: DeliverySlot; readonly claimed: ActiveDelivery | null }

/** The claim does not own the batch, or the batch is not in a stage the operation allows. */
export class ClaimNotOwned extends Data.TaggedError('ClaimNotOwned')<{}> {}

/** A different preparation is already saved. */
export class PreparationMismatch extends Data.TaggedError('PreparationMismatch')<{ readonly batchId: BatchId }> {}

/**
 * What due work the active delivery has: its callback to run, or output to send. Takes only the
 * stage, so a store that keeps the stage in its own column can ask without loading the delivery.
 */
export const activeDeliveryWork = (active: { readonly stage: ActiveDeliveryStage }): 'Callback' | 'Output' =>
	active.stage === 'ExternalWaiting' || active.stage === 'Finishing' ? 'Output' : 'Callback'

/** The operation that runs next. Operations run in order, so a later one waits for this one to settle. */
const nextOperation = (active: ActiveDelivery) => active.operations.find(isUnsettledOperation)

/** When the next operation is due: when it is ready, or when its attempt's lease runs out. */
const nextOutputAt = (active: ActiveDelivery): number | null => {
	const next = nextOperation(active)
	if (next === undefined) return null
	return DeliveryOperationState.match(next.state, {
		Pending: ({ readyAt }) => readyAt,
		Delivering: ({ leaseUntil }) => leaseUntil,
		Delivered: () => null,
		Failed: () => null,
	})
}

/**
 * When a handed-off delivery fails on its own: its time limit, while it waits for its remote worker with
 * no result. None otherwise.
 */
const deadline = (active: ActiveDelivery): number | null =>
	active.stage === 'ExternalWaiting' && active.terminal === undefined ? (active.failAt ?? null) : null

/** When the delivery next needs processing: its next output, or its time limit, whichever is first. */
const nextDueAt = (active: ActiveDelivery): number | null =>
	Arr.match([nextOutputAt(active), deadline(active)].filter(Predicate.isNotNull), {
		onEmpty: () => null,
		onNonEmpty: (times) => Math.min(...times),
	})

/**
 * Fail a handed-off delivery whose remote worker sent nothing before its time limit: save the `Failed`
 * result, with reason `TimedOut`, and the `PresentOutcome` that shows it. Any other delivery is unchanged.
 */
const failIfOverdue = (active: ActiveDelivery, now: number): ActiveDelivery => {
	const due = deadline(active)
	if (due === null || due > now) return active
	const terminal = DeliveryTerminal.make({
		outcome: DeliveryOutcome.cases.Failed.make({ reason: 'TimedOut' }),
		markdown: DELIVERY_TIMED_OUT_MARKDOWN,
	})
	const failed = withOperations(
		ActiveDelivery.make({ ...active, terminal }),
		[PresentOutcome.make({ outcome: terminal.outcome, markdown: DELIVERY_TIMED_OUT_MARKDOWN })],
		now,
	)
	return ActiveDelivery.make({ ...failed, stage: 'Finishing' })
}

/** Start a handed-off delivery's time limit again from `now`, after a request from its remote worker. */
const withDeadlineFrom = (active: ActiveDelivery, now: number): ActiveDelivery =>
	active.failAfterMs === undefined || active.handedOffAt === undefined || active.terminal !== undefined
		? active
		: ActiveDelivery.make({ ...active, failAt: Timestamp.make(now + active.failAfterMs) })

const retire = (slot: DeliverySlot, active: ActiveDelivery, facts: MailboxFacts): DeliverySlot => {
	const entry = {
		batchId: active.batchId,
		accessToken: active.accessToken,
		supportedOperations: active.prepared?.supportedOperations ?? [],
		interruptRequested: active.interruptRequestedAt !== undefined,
		retainUntil: Timestamp.make(facts.now + DELIVERY_RETENTION_MS),
		links: active.links,
		output: active.operations.map(deliveryOutputStatus),
	}
	const withTerminal = Predicate.isUndefined(active.terminal) ? entry : { ...entry, terminal: active.terminal }
	const retiredEntry = RetainedDelivery.make(
		Predicate.isUndefined(active.plan) ? withTerminal : { ...withTerminal, plan: deliveryPlanStatus(active.plan) },
	)
	const retained = [...slot.retained.filter(({ retainUntil }) => retainUntil > facts.now), retiredEntry].slice(
		-MAX_RETAINED_DELIVERIES,
	)
	return DeliverySlot.make({
		active: null,
		readyAt: facts.hasWaiting ? Timestamp.make(facts.now) : null,
		retained,
	})
}

const withActive = (slot: DeliverySlot, active: ActiveDelivery, readyAt: number | null): DeliverySlot =>
	DeliverySlot.make({ ...slot, active, readyAt: readyAt === null ? null : Timestamp.make(readyAt) })

/**
 * Settle a delivery whose callback code has stopped. A handed-off delivery without a result waits for
 * its remote worker. Any other sends the output it still owes, then retires.
 */
const afterCallback = (slot: DeliverySlot, active: ActiveDelivery, facts: MailboxFacts): DeliverySlot => {
	if (active.handedOffAt !== undefined && active.terminal === undefined) {
		const waiting = ActiveDelivery.make({ ...active, stage: 'ExternalWaiting', claimId: null })
		return withActive(slot, waiting, nextDueAt(waiting))
	}
	if (nextOperation(active) === undefined) return retire(slot, active, facts)
	const finishing = ActiveDelivery.make({ ...active, stage: 'Finishing', claimId: null })
	return withActive(slot, finishing, nextDueAt(finishing))
}

/** Add operations, each with the next free ID of its kind. Output is due at `now`. */
const withOperations = (
	active: ActiveDelivery,
	operations: ReadonlyArray<DeliveryOutputOperation>,
	now: number,
): ActiveDelivery => {
	const added = operations.reduce<ReadonlyArray<DeliveryOperation>>((saved, operation) => {
		const links = () => saved.filter(({ operation }) => Predicate.isTagged(operation, 'AddExternalLink')).length
		const messages = () => saved.filter(({ operation }) => isMessageOperation(operation)).length
		const activities = () => saved.filter(({ operation }) => Predicate.isTagged(operation, 'SetActivity')).length
		const reactions = () =>
			saved.filter(({ operation }) => Predicate.isTagged(operation, 'SetMessageReaction')).length
		const plans = () => saved.filter(({ operation }) => Predicate.isTagged(operation, 'RenderPlan')).length
		const operationId = Match.value(operation).pipe(
			Match.tagsExhaustive({
				PresentOutcome: () => 'outcome',
				AddExternalLink: () => `link-${links() + 1}`,
				CreateMessage: () => `message-${messages() + 1}`,
				UpdateMessage: () => `message-${messages() + 1}`,
				DeleteMessage: () => `message-${messages() + 1}`,
				SetActivity: () => `activity-${activities() + 1}`,
				SetMessageReaction: () => `reaction-${reactions() + 1}`,
				RenderPlan: () => `plan-${plans() + 1}`,
			}),
		)
		return [
			...saved,
			DeliveryOperation.make({
				operationId: DeliveryOperationId.make(operationId),
				operation,
				state: DeliveryOperationState.cases.Pending.make({ readyAt: Timestamp.make(now) }),
				attempt: 0,
				hadAmbiguousAttempt: false,
			}),
		]
	}, active.operations)
	return ActiveDelivery.make({ ...active, operations: added })
}

const isMessageOperation = (operation: DeliveryOutputOperation) =>
	Predicate.isTagged(operation, 'CreateMessage') ||
	Predicate.isTagged(operation, 'UpdateMessage') ||
	Predicate.isTagged(operation, 'DeleteMessage')

/** The saved `CreateMessage` of a message, if the delivery has created it. */
const messageCreation = (active: ActiveDelivery, messageId: MessageId) =>
	active.operations.find(
		({ operation }) => Predicate.isTagged(operation, 'CreateMessage') && operation.messageId === messageId,
	)

/**
 * The provider's reference to a message the delivery posted: the receipt of its `CreateMessage`.
 * None while the create has not been applied, or when it failed.
 */
export const sentMessageReference = (
	active: ActiveDelivery,
	messageId: MessageId,
): ProviderMessageReference | undefined => {
	const state = messageCreation(active, messageId)?.state
	return state !== undefined && Predicate.isTagged(state, 'Delivered') ? state.receipt : undefined
}

/** The last saved `SetActivity`, which holds the activity the remote worker last asked for. */
const lastActivityOperation = (active: ActiveDelivery) =>
	active.operations.findLast(({ operation }) => Predicate.isTagged(operation, 'SetActivity'))

/** The activity the remote worker last asked for. `Idle` when it never asked. */
const desiredActivity = (active: ActiveDelivery): DeliveryActivity => {
	const last = lastActivityOperation(active)?.operation
	return last !== undefined && Predicate.isTagged(last, 'SetActivity') ? last.activity : DeliveryActivity.cases.Idle.make({})
}

/** Whether the delivery's last activity request was `Working`, so ending it must clear the provider's activity. */
export const activityToClear = (active: ActiveDelivery) =>
	Predicate.isTagged(desiredActivity(active), 'Working')

/** The last saved `RenderPlan`, which holds the latest plan sent to the provider or waiting to be. */
const lastPlanOperation = (active: ActiveDelivery) =>
	active.operations.findLast(({ operation }) => Predicate.isTagged(operation, 'RenderPlan'))

/** The plan the provider last showed, with its presentation. None until a `RenderPlan` has been applied. */
export const renderedDeliveryPlan = (active: ActiveDelivery): RenderedDeliveryPlan | undefined => active.plan?.rendered

/**
 * Record what an applied `RenderPlan` showed: exactly the plan it sent, never a newer desired plan that
 * arrived while it ran, and the provider's presentation from its receipt.
 */
const withRenderedPlan = (active: ActiveDelivery, request: RenderPlan, presentation: Schema.Json | undefined) => {
	const { plan } = active
	if (plan === undefined || (plan.rendered !== undefined && plan.rendered.revision >= request.revision)) return active
	const rendered = RenderedDeliveryPlan.make(
		Predicate.isUndefined(presentation)
			? { revision: request.revision, plan: request.plan }
			: { revision: request.revision, plan: request.plan, presentation },
	)
	return ActiveDelivery.make({ ...active, plan: DeliveryPlanState.make({ ...plan, rendered }) })
}

/** The saved `SetMessageReaction` operations for one reaction on one target, oldest first. */
const reactionsOn = (
	operations: ReadonlyArray<DeliveryOperation>,
	key: { readonly target: DeliveryReactionTarget; readonly reaction: PortableReaction },
) =>
	operations.flatMap((saved) => {
		const { operation } = saved
		return Predicate.isTagged(operation, 'SetMessageReaction') &&
			operation.reaction === key.reaction &&
			sameDeliveryReactionTarget(operation.target, key.target)
			? [{ saved, request: operation }]
			: []
	})

/**
 * For a removal, the provider's receipt for the add it undoes: the last add of the same reaction on the
 * same target that the provider applied before this operation, unless an applied removal came after it.
 */
export const reactionAddedReference = (
	active: ActiveDelivery,
	operation: DeliveryOperation,
): ProviderMessageReference | undefined => {
	const request = operation.operation
	if (!Predicate.isTagged(request, 'SetMessageReaction') || request.active) return undefined
	const earlier = active.operations.slice(
		0,
		active.operations.findIndex(({ operationId }) => operationId === operation.operationId),
	)
	return reactionsOn(earlier, request).reduce<ProviderMessageReference | undefined>(
		(added, { saved, request: change }) =>
			Predicate.isTagged(saved.state, 'Delivered') ? (change.active ? saved.state.receipt : undefined) : added,
		undefined,
	)
}

/** Links not already saved, each once. A repeat of a saved URL is a replay. */
const newLinks = (saved: ReadonlyArray<ExternalLink>, links: ReadonlyArray<ExternalLink>) =>
	links.reduce<ReadonlyArray<ExternalLink>>(
		(fresh, link) => ([...saved, ...fresh].some(({ url }) => url === link.url) ? fresh : [...fresh, link]),
		[],
	)

/** Save links and an `AddExternalLink` operation for each. */
const withLinks = (active: ActiveDelivery, links: ReadonlyArray<ExternalLink>, now: number) =>
	withOperations(
		ActiveDelivery.make({ ...active, links: [...active.links, ...links] }),
		links.map((link) => AddExternalLink.make({ link })),
		now,
	)

/** The owned active delivery, when `claimId` owns it in a stage where callback code may be running. */
const owned = (slot: DeliverySlot, claimId: string) =>
	slot.active !== null &&
	slot.active.claimId === claimId &&
	(slot.active.stage === 'Local' || slot.active.stage === 'ExternalCleaning')
		? slot.active
		: null

/**
 * Start a new batch in an idle slot. The store has already chosen its admissions.
 * Returns null when the slot already has an active delivery.
 */
export const startDeliveryBatch = (
	slot: DeliverySlot,
	input: {
		readonly batchId: BatchId
		readonly accessToken: DeliveryAccessToken
		readonly admissions: DeliveryAdmissionBatch
		readonly claimId: string
		readonly leaseMs: number
		readonly now: number
	},
): { readonly slot: DeliverySlot; readonly claimed: ActiveDelivery } | null => {
	if (slot.active !== null) return null
	const claimed = ActiveDelivery.make({
		batchId: input.batchId,
		accessToken: input.accessToken,
		admissions: input.admissions,
		attempt: 1,
		claimId: input.claimId,
		stage: 'Local',
		links: [],
		operations: [],
	})
	return { slot: withActive(slot, claimed, input.now + input.leaseMs), claimed }
}

/**
 * Take the frozen batch again after a retry comes due or a lease runs out.
 *
 * A lease that ran out after a handoff does not run the callback again: the handoff stands, so the
 * delivery waits for its remote worker, or finishes if the remote worker already sent a result.
 */
export const claimFrozenBatch = (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly leaseMs: number } & MailboxFacts,
): DeliverySlotClaim => {
	const active = slot.active
	if (active === null || slot.readyAt === null || slot.readyAt > input.now) return { slot, claimed: null }
	if (activeDeliveryWork(active) === 'Output') return { slot, claimed: null }
	if (active.terminal !== undefined || active.stage === 'ExternalCleaning') {
		return { slot: afterCallback(slot, active, input), claimed: null }
	}
	const claimed = ActiveDelivery.make({
		...active,
		stage: 'Local',
		claimId: input.claimId,
		attempt: active.attempt + 1,
	})
	return { slot: withActive(slot, claimed, input.now + input.leaseMs), claimed }
}

/** Move a live claim's lease forward. */
export const renewDeliveryClaim = Effect.fn('delivery.lifecycle.renew_claim')(function* (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly leaseMs: number; readonly now: number },
) {
	const active = owned(slot, input.claimId)
	if (active === null) return yield* new ClaimNotOwned()
	return withActive(slot, active, input.now + input.leaseMs)
})

/** Save the callback choice and destination, once per batch. */
export const prepareDeliverySlot = Effect.fn('delivery.lifecycle.prepare')(function* (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly prepared: PreparedDeliveryInvocation },
): Effect.fn.Return<
	{ readonly slot: DeliverySlot; readonly prepared: PreparedDeliveryInvocation },
	ClaimNotOwned | PreparationMismatch
> {
	const active = owned(slot, input.claimId)
	if (active === null) return yield* new ClaimNotOwned()
	if (active.prepared !== undefined) {
		if (!Schema.toEquivalence(PreparedDeliveryInvocation)(active.prepared, input.prepared)) {
			return yield* new PreparationMismatch({ batchId: active.batchId })
		}
		return { slot, prepared: active.prepared }
	}
	const next = ActiveDelivery.make({ ...active, prepared: input.prepared })
	return { slot: withActive(slot, next, slot.readyAt), prepared: input.prepared }
})

/**
 * Hand the batch off, saving any new links with an `AddExternalLink` each, and its time limit, when it
 * has one. Repeating it is harmless. The claim stays as ownership of callback cleanup; the links are
 * sent once the callback returns.
 */
export const handOffDeliverySlot = Effect.fn('delivery.lifecycle.hand_off')(function* (
	slot: DeliverySlot,
	input: {
		readonly claimId: string
		readonly handedOffAt: number
		readonly links: ReadonlyArray<ExternalLink>
		readonly failAfterMs?: number
	},
) {
	const active = owned(slot, input.claimId)
	if (active === null) return yield* new ClaimNotOwned()
	if (active.stage === 'ExternalCleaning') return slot
	const handedOff = ActiveDelivery.make({ ...active, stage: 'ExternalCleaning', handedOffAt: Timestamp.make(input.handedOffAt) })
	const limited = Predicate.isUndefined(input.failAfterMs)
		? handedOff
		: withDeadlineFrom(ActiveDelivery.make({ ...handedOff, failAfterMs: input.failAfterMs }), input.handedOffAt)
	const next = withLinks(
		limited,
		newLinks(active.links, input.links),
		input.handedOffAt,
	)
	return withActive(slot, next, slot.readyAt)
})

/**
 * Record how an attempt ended. The stage decides, not only the result: a handed-off delivery waits
 * for its remote worker whatever the callback returned, and one with a remote result finishes.
 */
export const recordDeliveryAttempt = Effect.fn('delivery.lifecycle.record_attempt')(function* (
	slot: DeliverySlot,
	input: {
		readonly claimId: string
		readonly retryAfterMs: number | null
	} & MailboxFacts,
) {
	const active = owned(slot, input.claimId)
	if (active === null) return yield* new ClaimNotOwned()
	if (active.terminal === undefined && active.stage === 'Local' && input.retryAfterMs !== null) {
		const next = ActiveDelivery.make({ ...active, stage: 'Retry', claimId: null })
		return withActive(slot, next, input.now + input.retryAfterMs)
	}
	return afterCallback(slot, active, input)
})

/** An event asked the current delivery to stop. Marks it once; the event itself still waits its turn. */
export const requestDeliveryInterrupt = (slot: DeliverySlot, now: number): DeliverySlot =>
	slot.active === null || slot.active.interruptRequestedAt !== undefined
		? slot
		: DeliverySlot.make({
				...slot,
				active: ActiveDelivery.make({ ...slot.active, interruptRequestedAt: Timestamp.make(now) }),
			})

/**
 * An output operation claimed by one attempt, with the delivery it belongs to.
 *
 * @property idempotencyKey - the operation's key, the same on every attempt at it
 */
export type DeliveryOutputClaim = {
	readonly slot: DeliverySlot
	readonly claimed: {
		readonly active: ActiveDelivery
		readonly operation: DeliveryOperation
		readonly idempotencyKey: string
	} | null
}

/**
 * Claim the next operation when it is due. An operation whose earlier attempt's lease ran out is
 * claimed again and marked ambiguous: the provider may already have applied it. A handed-off delivery
 * past its time limit first fails on its own, and its `PresentOutcome` is claimed.
 *
 * The first claim gives the operation `idempotencyKey`, a random UUID the caller made; later claims
 * keep the one it has, so every attempt sends the same key.
 */
export const claimDeliveryOutput = (
	slot: DeliverySlot,
	input: { readonly claimId: string; readonly leaseMs: number; readonly now: number; readonly idempotencyKey: string },
): DeliveryOutputClaim => {
	const active = slot.active
	if (active === null || activeDeliveryWork(active) !== 'Output') return { slot, claimed: null }
	const current = failIfOverdue(active, input.now)
	const next = nextOperation(current)
	const dueAt = nextOutputAt(current)
	if (next === undefined || dueAt === null || dueAt > input.now) return { slot, claimed: null }
	const idempotencyKey = next.idempotencyKey ?? input.idempotencyKey
	const operation = DeliveryOperation.make({
		...next,
		state: DeliveryOperationState.cases.Delivering.make({
			claimId: input.claimId,
			leaseUntil: Timestamp.make(input.now + input.leaseMs),
		}),
		attempt: next.attempt + 1,
		hadAmbiguousAttempt: next.hadAmbiguousAttempt || Predicate.isTagged(next.state, 'Delivering'),
		idempotencyKey,
	})
	const claimed = replaceOperation(current, operation)
	return {
		slot: withActive(slot, claimed, nextDueAt(claimed)),
		claimed: { active: claimed, operation, idempotencyKey },
	}
}

/** The operation `claimId` is delivering, if it still is. */
const ownedOperation = (slot: DeliverySlot, input: { readonly operationId: string; readonly claimId: string }) => {
	const active = slot.active
	if (active === null) return null
	const operation = active.operations.find(({ operationId }) => operationId === input.operationId)
	if (operation === undefined) return null
	const { state } = operation
	return Predicate.isTagged(state, 'Delivering') && state.claimId === input.claimId ? { active, operation } : null
}

const replaceOperation = (active: ActiveDelivery, operation: DeliveryOperation) =>
	ActiveDelivery.make({
		...active,
		operations: active.operations.map((saved) => (saved.operationId === operation.operationId ? operation : saved)),
	})

/** Move a live output attempt's lease forward. */
export const renewDeliveryOutput = Effect.fn('delivery.lifecycle.renew_output')(function* (
	slot: DeliverySlot,
	input: { readonly operationId: string; readonly claimId: string; readonly leaseMs: number; readonly now: number },
) {
	const owner = ownedOperation(slot, input)
	if (owner === null) return yield* new ClaimNotOwned()
	const active = replaceOperation(
		owner.active,
		DeliveryOperation.make({
			...owner.operation,
			state: DeliveryOperationState.cases.Delivering.make({
				claimId: input.claimId,
				leaseUntil: Timestamp.make(input.now + input.leaseMs),
			}),
		}),
	)
	return withActive(slot, active, nextDueAt(active))
})

/**
 * Record how an output attempt ended. A finishing delivery whose output is all settled retires and
 * wakes the events waiting behind it.
 */
export const settleDeliveryOutput = Effect.fn('delivery.lifecycle.settle_output')(function* (
	slot: DeliverySlot,
	input: {
		readonly operationId: string
		readonly claimId: string
		readonly settlement: DeliveryOutputSettlement
	} & MailboxFacts,
) {
	const owner = ownedOperation(slot, input)
	if (owner === null) return yield* new ClaimNotOwned()
	const state = Match.value(input.settlement).pipe(
		Match.tagsExhaustive({
			Applied: ({ receipt }) =>
				DeliveryOperationState.cases.Delivered.make(Predicate.isUndefined(receipt) ? {} : { receipt }),
			Retry: ({ readyAt }) => DeliveryOperationState.cases.Pending.make({ readyAt }),
			Failed: ({ safeCode }) => DeliveryOperationState.cases.Failed.make({ safeCode }),
		}),
	)
	const settled = replaceOperation(owner.active, DeliveryOperation.make({ ...owner.operation, state }))
	const request = owner.operation.operation
	const active =
		Predicate.isTagged(request, 'RenderPlan') && Predicate.isTagged(state, 'Delivered')
			? withRenderedPlan(settled, request, state.receipt)
			: settled
	if (active.stage === 'Finishing' && nextOperation(active) === undefined) return retire(slot, active, input)
	return withActive(slot, active, nextDueAt(active))
})

/** The stage a remote worker sees. A delivery with a result whose callback is still returning is finishing. */
const publicStage = (active: ActiveDelivery): DeliveryStage =>
	active.terminal !== undefined ? 'Finishing' : active.stage

type Located = Data.TaggedEnum<{
	Active: { readonly active: ActiveDelivery }
	Retained: { readonly retained: RetainedDelivery }
}>
const Located = Data.taggedEnum<Located>()

/** Find the delivery and check the token. A missing delivery and a wrong token look the same. */
const locate = (
	slot: DeliverySlot,
	input: { readonly reference: DeliveryReference; readonly accessToken: string; readonly now: number },
): Located | null => {
	const matches = (saved: string) => deliveryAccessTokenMatches({ saved, presented: input.accessToken })
	if (slot.active !== null && slot.active.batchId === input.reference.batchId) {
		return matches(slot.active.accessToken) ? Located.Active({ active: slot.active }) : null
	}
	const retained = slot.retained.find(
		({ batchId, retainUntil }) => batchId === input.reference.batchId && retainUntil > input.now,
	)
	return retained !== undefined && matches(retained.accessToken) ? Located.Retained({ retained }) : null
}

/** A status with the outcome and plan the delivery has, when it has them. */
const statusWith = (
	status: Omit<DeliveryStatus, '_tag' | 'outcome' | 'plan'>,
	terminal: DeliveryTerminal | undefined,
	plan: DeliveryPlanStatus | undefined,
) => {
	const withOutcome = Predicate.isUndefined(terminal) ? status : { ...status, outcome: terminal.outcome }
	return DeliveryStatus.make(Predicate.isUndefined(plan) ? withOutcome : { ...withOutcome, plan })
}

/** What a remote worker may read about its delivery. */
export const readDeliverySlotStatus = Effect.fn('delivery.lifecycle.read_status')(function* (
	slot: DeliverySlot,
	input: { readonly reference: DeliveryReference; readonly accessToken: string; readonly now: number },
): Effect.fn.Return<DeliveryStatus, DeliveryNotFound> {
	const located = locate(slot, input)
	if (located === null) return yield* new DeliveryNotFound()
	return Located.$match(located, {
		Active: ({ active }) =>
			statusWith(
				{
					deliveryId: input.reference.deliveryId,
					stage: publicStage(active),
					/** A result clears the activity, so a finishing delivery shows `Idle`. */
					activity:
						active.terminal === undefined ? desiredActivity(active) : DeliveryActivity.cases.Idle.make({}),
					interruptRequested: active.interruptRequestedAt !== undefined,
					supportedOperations: active.prepared?.supportedOperations ?? [],
					reactionTargets: active.prepared?.reactionTargets ?? [],
					...(active.terminal === undefined && active.failAt !== undefined ? { failAt: active.failAt } : undefined),
					output: active.operations.map(deliveryOutputStatus),
				},
				active.terminal,
				Predicate.isUndefined(active.plan) ? undefined : deliveryPlanStatus(active.plan),
			),
		Retained: ({ retained }) =>
			statusWith(
				{
					deliveryId: input.reference.deliveryId,
					stage: 'Retired',
					interruptRequested: retained.interruptRequested,
					supportedOperations: retained.supportedOperations,
					reactionTargets: [],
					output: retained.output,
				},
				retained.terminal,
				retained.plan,
			),
	})
})

type MutationChange = Effect.Effect<
	{ readonly slot: DeliverySlot; readonly receipt: DeliveryMutationReceipt },
	| DeliveryNotFound
	| DeliveryTerminalConflict
	| DeliveryClosed
	| DeliveryOperationUnsupported
	| DeliveryMessageNotFound
	| DeliveryMessageDeleted
	| DeliveryMessageConflict
	| DeliveryReactionTargetUnavailable
>

/**
 * Record a remote worker's final result, and the `PresentOutcome` that shows it, in one change.
 *
 * - The same result again is `already_recorded`; a different one is a conflict.
 * - A delivery with no callback running starts finishing at once.
 * - A delivery whose callback is still running keeps the result and finishes when the callback returns.
 * - A delivery that ended without a remote result is closed.
 */
const recordTerminal = (
	slot: DeliverySlot,
	located: Located,
	input: { readonly terminal: DeliveryTerminal; readonly receipt: (status: DeliveryMutationReceipt['status']) => DeliveryMutationReceipt } & MailboxFacts,
): MutationChange => {
	const replay = (saved: DeliveryTerminal | undefined) => {
		if (saved === undefined) return Effect.fail(new DeliveryClosed())
		return sameDeliveryTerminal(saved, input.terminal)
			? Effect.succeed({ slot, receipt: input.receipt('already_recorded') })
			: Effect.fail(new DeliveryTerminalConflict())
	}
	return Located.$match(located, {
		Retained: ({ retained }) => replay(retained.terminal),
		Active: ({ active }) => {
			if (active.terminal !== undefined || active.stage === 'Finishing') return replay(active.terminal)
			const present = PresentOutcome.make(
				Predicate.isUndefined(input.terminal.markdown)
					? { outcome: input.terminal.outcome }
					: { outcome: input.terminal.outcome, markdown: input.terminal.markdown },
			)
			const next = withOperations(ActiveDelivery.make({ ...active, terminal: input.terminal }), [present], input.now)
			/** No callback code is running in these stages, so the output can start now. */
			const nextSlot =
				active.stage === 'ExternalWaiting' || active.stage === 'Retry'
					? afterCallback(slot, next, input)
					: withActive(slot, next, slot.readyAt)
			return Effect.succeed({ slot: nextSlot, receipt: input.receipt('accepted') })
		},
	})
}

/**
 * Add a link, and the `AddExternalLink` that shows it, in one change. A URL already added is a replay,
 * even after the delivery ended. A new link after the delivery ended is refused.
 */
const addLink = (
	slot: DeliverySlot,
	located: Located,
	input: { readonly link: ExternalLink; readonly receipt: (status: DeliveryMutationReceipt['status']) => DeliveryMutationReceipt } & MailboxFacts,
): MutationChange => {
	const saved = Located.$match(located, {
		Active: ({ active }) => active.links,
		Retained: ({ retained }) => retained.links,
	})
	if (saved.some(({ url }) => url === input.link.url)) {
		return Effect.succeed({ slot, receipt: input.receipt('already_recorded') })
	}
	return Located.$match(located, {
		Retained: () => Effect.fail(new DeliveryClosed()),
		Active: ({ active }) => {
			if (active.terminal !== undefined || active.stage === 'Finishing') return Effect.fail(new DeliveryClosed())
			const next = withLinks(active, [input.link], input.now)
			/** A delivery waiting for its remote worker has no callback running, so the output can start now. */
			const readyAt = active.stage === 'ExternalWaiting' ? nextDueAt(next) : slot.readyAt
			return Effect.succeed({ slot: withActive(slot, next, readyAt), receipt: input.receipt('accepted') })
		},
	})
}

/** What a message change asks of the delivery: nothing new, or one more operation. */
type MessageDecision = Effect.Effect<
	'Replay' | DeliveryOutputOperation,
	DeliveryClosed | DeliveryMessageNotFound | DeliveryMessageDeleted | DeliveryMessageConflict
>

/**
 * Decide a message change against the operations already saved.
 *
 * - A repeat of a change already saved is a replay, even after the delivery ended.
 * - Any other change after the delivery ended is refused.
 * - A change to a message never created, or whose create failed, names no message.
 * - An update or deletion of a message whose create has not run yet is saved behind it.
 */
const decideMessageChange = (active: ActiveDelivery, mutation: DeliveryMessageMutation): MessageDecision => {
	const closed = active.terminal !== undefined || active.stage === 'Finishing'
	const { messageId } = mutation
	const operations = active.operations
		.map(({ operation }) => operation)
		.filter((operation) => operationMessageId(operation) === messageId)
	const creation = messageCreation(active, messageId)
	const deleted = operations.some((operation) => Predicate.isTagged(operation, 'DeleteMessage'))
	const refuse = <E>(error: E) => Effect.fail(closed ? new DeliveryClosed() : error)
	const accept = (operation: DeliveryOutputOperation): MessageDecision =>
		closed ? Effect.fail(new DeliveryClosed()) : Effect.succeed(operation)
	const replay: MessageDecision = Effect.succeed('Replay')

	return Match.value(mutation).pipe(
		Match.tagsExhaustive({
			CreateDeliveryMessage: ({ markdown }): MessageDecision => {
				if (creation === undefined) return accept(CreateMessage.make({ messageId, markdown }))
				return Predicate.isTagged(creation.operation, 'CreateMessage') && creation.operation.markdown === markdown
					? replay
					: Effect.fail(new DeliveryMessageConflict({ messageId }))
			},
			UpdateDeliveryMessage: ({ markdown }): MessageDecision => {
				if (creation === undefined || Predicate.isTagged(creation.state, 'Failed')) {
					return refuse(new DeliveryMessageNotFound({ messageId }))
				}
				if (deleted) return refuse(new DeliveryMessageDeleted({ messageId }))
				const latestMarkdown = operations.reduce<string | undefined>(
					(text, operation) =>
						Match.value(operation).pipe(
							Match.tag('CreateMessage', 'UpdateMessage', (change) => change.markdown),
							Match.orElse(() => text),
						),
					undefined,
				)
				if (latestMarkdown === markdown) return replay
				return accept(UpdateMessage.make({ messageId, markdown }))
			},
			DeleteDeliveryMessage: (): MessageDecision => {
				if (creation === undefined || Predicate.isTagged(creation.state, 'Failed')) {
					return refuse(new DeliveryMessageNotFound({ messageId }))
				}
				return deleted ? replay : accept(DeleteMessage.make({ messageId }))
			},
		}),
	)
}

/**
 * The output operation a change needs its destination to support. A plan needs none: a destination
 * that cannot show it still keeps it.
 */
const requiredOperation = (mutation: DeliveryMutation): DeliveryOperationKind | undefined =>
	Match.value(mutation).pipe(
		Match.tagsExhaustive({
			CompleteDelivery: () => 'PresentOutcome' as const,
			FailDelivery: () => 'PresentOutcome' as const,
			AddDeliveryLink: () => 'AddExternalLink' as const,
			CreateDeliveryMessage: () => 'CreateMessage' as const,
			UpdateDeliveryMessage: () => 'UpdateMessage' as const,
			DeleteDeliveryMessage: () => 'DeleteMessage' as const,
			SetDeliveryActivity: () => 'SetActivity' as const,
			SetDeliveryReaction: () => 'SetMessageReaction' as const,
			PutDeliveryPlan: () => undefined,
		}),
	)

/**
 * A waiting desired-state operation, such as `SetActivity`, given a new request. It drops its
 * idempotency key: an earlier attempt may have applied the old request under that key, so the new one
 * needs its own.
 */
const replacedRequest = (saved: DeliveryOperation, operation: DeliveryOutputOperation) =>
	DeliveryOperation.make({
		operationId: saved.operationId,
		operation,
		state: saved.state,
		attempt: saved.attempt,
		hadAmbiguousAttempt: saved.hadAmbiguousAttempt,
	})

/**
 * Save the activity the remote worker wants shown. The activity already desired is a replay. A new
 * one replaces a `SetActivity` still waiting to be sent, so only the latest is sent; one already being
 * sent is followed by a new operation. A delivery with a result takes no new activity.
 */
const changeActivity = (
	slot: DeliverySlot,
	located: Located,
	input: {
		readonly mutation: SetDeliveryActivity
		readonly receipt: (status: DeliveryMutationReceipt['status']) => DeliveryMutationReceipt
	} & MailboxFacts,
): MutationChange =>
	Located.$match(located, {
		Retained: () => Effect.fail(new DeliveryClosed()),
		Active: ({ active }) => {
			const { activity } = input.mutation
			if (sameDeliveryActivity(desiredActivity(active), activity)) {
				return Effect.succeed({ slot, receipt: input.receipt('already_recorded') })
			}
			if (active.terminal !== undefined || active.stage === 'Finishing') return Effect.fail(new DeliveryClosed())
			const last = lastActivityOperation(active)
			const next =
				last !== undefined && Predicate.isTagged(last.state, 'Pending')
					? replaceOperation(active, replacedRequest(last, SetActivity.make({ activity })))
					: withOperations(active, [SetActivity.make({ activity })], input.now)
			/** A delivery waiting for its remote worker has no callback running, so the output can start now. */
			const readyAt = active.stage === 'ExternalWaiting' ? nextDueAt(next) : slot.readyAt
			return Effect.succeed({ slot: withActive(slot, next, readyAt), receipt: input.receipt('accepted') })
		},
	})

type ReactionTargetCheck = Effect.Effect<
	void,
	DeliveryReactionTargetUnavailable | DeliveryMessageNotFound | DeliveryMessageDeleted
>

/**
 * Check that a reaction's target is one the destination can react on, and that it exists: the delivery
 * has an activation target, or the message was created and not removed. A message whose create has not
 * run yet is fine; the reaction runs after it. No provider reacts on its plan yet.
 */
const checkReactionTarget = (active: ActiveDelivery, target: DeliveryReactionTarget): ReactionTargetCheck => {
	const kind = deliveryReactionTargetKind(target)
	const unavailable: ReactionTargetCheck = Effect.fail(new DeliveryReactionTargetUnavailable({ target: kind }))
	if (!(active.prepared?.reactionTargets ?? []).includes(kind)) return unavailable
	return DeliveryReactionTarget.match(target, {
		ActivationTarget: (): ReactionTargetCheck => (active.prepared?.activationTarget === undefined ? unavailable : Effect.void),
		PlanTarget: (): ReactionTargetCheck => unavailable,
		MessageTarget: ({ messageId }): ReactionTargetCheck => {
			const creation = messageCreation(active, messageId)
			if (creation === undefined || Predicate.isTagged(creation.state, 'Failed')) {
				return Effect.fail(new DeliveryMessageNotFound({ messageId }))
			}
			const deleted = active.operations.some(
				({ operation }) => Predicate.isTagged(operation, 'DeleteMessage') && operation.messageId === messageId,
			)
			return deleted ? Effect.fail(new DeliveryMessageDeleted({ messageId })) : Effect.void
		},
	})
}

/**
 * Save whether the bot's reaction should be on a target. The state already asked for is a replay, even
 * after a result. A change replaces a `SetMessageReaction` for the same reaction and target still
 * waiting to be sent; one already being sent is followed by a new operation. A delivery with a result
 * takes no new change.
 */
const changeReaction = (
	slot: DeliverySlot,
	located: Located,
	input: {
		readonly mutation: SetDeliveryReaction
		readonly receipt: (status: DeliveryMutationReceipt['status']) => DeliveryMutationReceipt
	} & MailboxFacts,
): MutationChange =>
	Located.$match(located, {
		Retained: () => Effect.fail(new DeliveryClosed()),
		Active: ({ active }) =>
			Effect.gen(function* () {
				const { target, reaction } = input.mutation
				const last = reactionsOn(active.operations, input.mutation).at(-1)
				if (last !== undefined && last.request.active === input.mutation.active) {
					return { slot, receipt: input.receipt('already_recorded') }
				}
				if (active.terminal !== undefined || active.stage === 'Finishing') return yield* new DeliveryClosed()
				yield* checkReactionTarget(active, target)
				const request = SetMessageReaction.make({ target, reaction, active: input.mutation.active })
				const next =
					last !== undefined && Predicate.isTagged(last.saved.state, 'Pending')
						? replaceOperation(active, replacedRequest(last.saved, request))
						: withOperations(active, [request], input.now)
				/** A delivery waiting for its remote worker has no callback running, so the output can start now. */
				const readyAt = active.stage === 'ExternalWaiting' ? nextDueAt(next) : slot.readyAt
				return { slot: withActive(slot, next, readyAt), receipt: input.receipt('accepted') }
			}),
	})

/**
 * Save the remote worker's whole plan as the desired plan, advancing its revision. The plan already
 * desired is a replay, even after the delivery ended. A new plan replaces a `RenderPlan` still waiting
 * to be sent, so only the latest is sent; one already being sent is followed by a new operation. A
 * destination that cannot show plans keeps the plan with no operation. A delivery with a result takes no
 * new plan.
 */
const changePlan = (
	slot: DeliverySlot,
	located: Located,
	input: {
		readonly mutation: PutDeliveryPlan
		readonly receipt: (status: DeliveryMutationReceipt['status']) => DeliveryMutationReceipt
	} & MailboxFacts,
): MutationChange => {
	const { plan } = input.mutation
	const replay = Effect.succeed({ slot, receipt: input.receipt('already_recorded') })
	return Located.$match(located, {
		Retained: ({ retained }) =>
			retained.plan !== undefined && sameDeliveryPlan(retained.plan.plan, plan) ? replay : Effect.fail(new DeliveryClosed()),
		Active: ({ active }) => {
			if (active.plan !== undefined && sameDeliveryPlan(active.plan.desired.plan, plan)) return replay
			if (active.terminal !== undefined || active.stage === 'Finishing') return Effect.fail(new DeliveryClosed())
			const revision = DeliveryPlanRevision.make((active.plan?.desired.revision ?? 0) + 1)
			const desired = { revision, plan }
			const planned = ActiveDelivery.make({
				...active,
				plan: DeliveryPlanState.make(
					active.plan?.rendered === undefined ? { desired } : { desired, rendered: active.plan.rendered },
				),
			})
			const accepted = (next: ActiveDelivery, readyAt: number | null) =>
				Effect.succeed({ slot: withActive(slot, next, readyAt), receipt: input.receipt('accepted') })
			if (!(active.prepared?.supportedOperations ?? []).includes('RenderPlan')) return accepted(planned, slot.readyAt)
			const request = RenderPlan.make({ revision, plan })
			const last = lastPlanOperation(active)
			const next =
				last !== undefined && Predicate.isTagged(last.state, 'Pending')
					? replaceOperation(planned, replacedRequest(last, request))
					: withOperations(planned, [request], input.now)
			/** A delivery waiting for its remote worker has no callback running, so the output can start now. */
			return accepted(next, active.stage === 'ExternalWaiting' ? nextDueAt(next) : slot.readyAt)
		},
	})
}

/** Save a message change and the operation that shows it, in one change. A retired delivery takes none. */
const changeMessage = (
	slot: DeliverySlot,
	located: Located,
	input: {
		readonly mutation: DeliveryMessageMutation
		readonly receipt: (status: DeliveryMutationReceipt['status']) => DeliveryMutationReceipt
	} & MailboxFacts,
): MutationChange => {
	return Located.$match(located, {
		Retained: () => Effect.fail(new DeliveryClosed()),
		Active: ({ active }) =>
			Effect.map(decideMessageChange(active, input.mutation), (decision) => {
				if (decision === 'Replay') return { slot, receipt: input.receipt('already_recorded') }
				const next = withOperations(active, [decision], input.now)
				/** A delivery waiting for its remote worker has no callback running, so the output can start now. */
				const readyAt = active.stage === 'ExternalWaiting' ? nextDueAt(next) : slot.readyAt
				return { slot: withActive(slot, next, readyAt), receipt: input.receipt('accepted') }
			}),
	})
}

/** Apply a remote worker's change: check its token, then change the delivery and save the output it needs. */
export const applyDeliverySlotMutation = Effect.fn('delivery.lifecycle.apply_mutation')(function* (
	slot: DeliverySlot,
	input: {
		readonly reference: DeliveryReference
		readonly accessToken: string
		readonly mutation: DeliveryMutation
	} & MailboxFacts,
) {
	const located = locate(slot, input)
	if (located === null) return yield* new DeliveryNotFound()
	/** A destination that cannot show the change refuses it before anything is saved. */
	const operation = requiredOperation(input.mutation)
	const supported = Located.$match(located, {
		Active: ({ active }) => active.prepared?.supportedOperations ?? [],
		Retained: ({ retained }) => retained.supportedOperations,
	})
	if (operation !== undefined && !supported.includes(operation)) {
		return yield* new DeliveryOperationUnsupported({ operation })
	}
	const receipt = (status: DeliveryMutationReceipt['status']) =>
		DeliveryMutationReceipt.make({ deliveryId: input.reference.deliveryId, status })
	const facts = { now: input.now, hasWaiting: input.hasWaiting, receipt }
	const changed = yield* Match.value(input.mutation).pipe(
		Match.tagsExhaustive({
			CompleteDelivery: (mutation) => recordTerminal(slot, located, { ...facts, terminal: terminalFromMutation(mutation) }),
			FailDelivery: (mutation) => recordTerminal(slot, located, { ...facts, terminal: terminalFromMutation(mutation) }),
			AddDeliveryLink: ({ link }) => addLink(slot, located, { ...facts, link }),
			CreateDeliveryMessage: (mutation) => changeMessage(slot, located, { ...facts, mutation }),
			UpdateDeliveryMessage: (mutation) => changeMessage(slot, located, { ...facts, mutation }),
			DeleteDeliveryMessage: (mutation) => changeMessage(slot, located, { ...facts, mutation }),
			SetDeliveryActivity: (mutation) => changeActivity(slot, located, { ...facts, mutation }),
			SetDeliveryReaction: (mutation) => changeReaction(slot, located, { ...facts, mutation }),
			PutDeliveryPlan: (mutation) => changePlan(slot, located, { ...facts, mutation }),
		}),
	)
	return { ...changed, slot: restartDeadline(changed.slot, input.reference, input.now) }
})

/**
 * Any request the store took from a remote worker, a repeat included, shows it is still working, so the
 * time limit of its delivery starts again. A delivery with a result has no time limit left to restart.
 */
const restartDeadline = (slot: DeliverySlot, reference: DeliveryReference, now: number): DeliverySlot => {
	const active = slot.active
	if (active === null || active.batchId !== reference.batchId) return slot
	const restarted = withDeadlineFrom(active, now)
	if (restarted === active) return slot
	return withActive(slot, restarted, restarted.stage === 'ExternalWaiting' ? nextDueAt(restarted) : slot.readyAt)
}
