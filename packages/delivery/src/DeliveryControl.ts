import { Clock, Context, Effect, Layer, Predicate, Schema } from 'effect'

import {
	deliveryOperationStatus,
	FINAL_MESSAGE_MARKDOWN_MAX_BYTES,
	FINAL_MESSAGE_MARKDOWN_MAX_LENGTH,
	FinalMessageMarkdown,
	finalMessageOperationId,
	type DeliveryOperation,
	FinalMessageOperation,
	PendingDeliveryOperation,
} from './DeliveryOperation.js'
import { decodeDeliveryReference, DeliveryId, DeliveryReference } from './DeliveryReference.js'
import {
	type ActiveBatch,
	activeBatches,
	currentMailbox,
	type Envelope,
	type Outcome,
	mailboxCapacityUsage,
	parseMailboxAddress,
	retainedOutcomes,
} from './Mailbox.js'
import { DeliveryLocatorStore, MailboxStore } from './MailboxStore.js'
import {
	deliveryControlUnavailable,
	deliveryNotFound,
	deliveryOutcomeConflict,
	deliveryTerminalRequestConflict,
	deliveryTerminalCapacityExceeded,
	DeliveryControlUnavailable,
	DeliveryNotFound,
	DeliveryOutcomeConflict,
	DeliveryTerminalRequestConflict,
	DeliveryTerminalRequestInvalid,
	DeliveryTerminalCapacityExceeded,
	DeliveryTerminalOutcome,
	DeliveryTerminalReceipt,
} from './protocol.js'

export {
	DeliveryControlUnavailable,
	DeliveryNotFound,
	DeliveryOutcomeConflict,
	DeliveryTerminalRequestConflict,
	DeliveryTerminalRequestInvalid,
	DeliveryTerminalCapacityExceeded,
	DeliveryTerminalOutcome,
	DeliveryTerminalReceipt,
} from './protocol.js'
export type { DeliveryTerminalOutcome as DeliveryTerminalOutcomeType } from './protocol.js'

export const FinishDelivery = Schema.Struct({
	deliveryId: DeliveryId,
	outcome: DeliveryTerminalOutcome,
	markdown: Schema.optionalKey(FinalMessageMarkdown),
})
export interface FinishDelivery extends Schema.Schema.Type<typeof FinishDelivery> {}

export const ResolvedDelivery = Schema.Struct({
	deliveryId: DeliveryId,
	provider: Schema.NonEmptyString,
	installation: Schema.NonEmptyString,
	organizationId: Schema.NonEmptyString,
	definition: Schema.NonEmptyString,
	version: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	resource: Schema.String,
	payload: Schema.String,
})
export interface ResolvedDelivery extends Schema.Schema.Type<typeof ResolvedDelivery> {}

const ResolvedMetadata = Schema.Struct({
	organizationId: Schema.NonEmptyString,
	definition: Schema.NonEmptyString,
	version: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	resource: Schema.String,
	payload: Schema.String,
})

const resolutionMetadata = (envelope: Envelope | undefined, outcome: Outcome | undefined) => ({
	organizationId: envelope?.organizationId ?? outcome?.organizationId,
	definition: envelope?.definition ?? outcome?.definition,
	version: envelope?.version ?? outcome?.version,
	eventId: envelope?.eventId ?? outcome?.eventId,
	resource: envelope?.resource ?? outcome?.resource,
	payload: envelope?.payload ?? outcome?.payload,
})

export type DeliveryControlError =
	| DeliveryNotFound
	| DeliveryOutcomeConflict
	| DeliveryTerminalRequestConflict
	| DeliveryTerminalRequestInvalid
	| DeliveryTerminalCapacityExceeded
	| DeliveryControlUnavailable

const terminalReceipt = (
	deliveryId: DeliveryId,
	outcome: DeliveryTerminalOutcome,
	status: 'accepted' | 'already_recorded',
	operation: DeliveryOperation | undefined,
) =>
	operation === undefined
		? DeliveryTerminalReceipt.make({ deliveryId, outcome, status })
		: DeliveryTerminalReceipt.make({
				deliveryId,
				outcome,
				status,
				finalMessage: {
					operationId: operation.operationId,
					status: deliveryOperationStatus(operation.state),
				},
			})

const replayTerminal = Effect.fn('delivery.control.replay_terminal')(function* (input: {
	readonly request: FinishDelivery
	readonly recorded: 'completed' | 'failed'
	readonly operation: DeliveryOperation | undefined
}) {
	if (input.recorded !== input.request.outcome)
		return yield* deliveryOutcomeConflict({
			deliveryId: input.request.deliveryId,
			recordedOutcome: input.recorded,
			requestedOutcome: input.request.outcome,
		})
	if ((input.operation?.markdown ?? undefined) !== input.request.markdown)
		return yield* deliveryTerminalRequestConflict(input.request.deliveryId)
	return terminalReceipt(input.request.deliveryId, input.recorded, 'already_recorded', input.operation)
})

const retainedTerminalOutcome = (outcomes: ReadonlyArray<Outcome>, deliveryId: DeliveryId, now: number) => {
	const outcome = outcomes.findLast((entry) => entry.deliveryId === deliveryId && entry.expiresAt > now)
	return outcome?.kind === 'completed' || outcome?.kind === 'failed' ? outcome.kind : undefined
}

const finalMessageFor = (input: {
	readonly request: FinishDelivery
	readonly envelope: Envelope
	readonly address: NonNullable<ReturnType<typeof parseMailboxAddress>>
	readonly now: number
}) => {
	if (input.request.markdown === undefined) return undefined
	return FinalMessageOperation.make({
		operationId: finalMessageOperationId(input.request.deliveryId),
		deliveryId: input.request.deliveryId,
		outcome: input.request.outcome,
		markdown: input.request.markdown,
		provider: input.address.provider,
		installation: input.address.installation,
		destination: input.envelope.resource,
		presentation: input.envelope.definition,
		presentationVersion: input.envelope.version,
		state: PendingDeliveryOperation.make({ attempt: 0, readyAt: input.now, hadAmbiguousAttempt: false }),
	})
}

const terminalReadyAt = (batch: ActiveBatch, current: number | null, now: number) =>
	Predicate.isTagged('External')(batch.stage) && batch.stage.cleanupOwner === null
		? Math.min(current ?? now, now)
		: current

export interface DeliveryControlService {
	readonly resolve: (input: {
		readonly deliveryId: DeliveryId
	}) => Effect.Effect<ResolvedDelivery, DeliveryControlError>
	readonly finish: (input: FinishDelivery) => Effect.Effect<DeliveryTerminalReceipt, DeliveryControlError>
}

export class DeliveryControl extends Context.Service<DeliveryControl, DeliveryControlService>()(
	'delivery/DeliveryControl',
) {
	static readonly layer = Layer.effect(
		DeliveryControl,
		Effect.gen(function* () {
			const store = yield* MailboxStore
			const locator = yield* DeliveryLocatorStore
			const referenceFor = Effect.fn('delivery.control.reference')(function* (input: {
				readonly deliveryId: DeliveryId
			}) {
				if (input.deliveryId.startsWith('delivery:v1:'))
					return yield* decodeDeliveryReference(input).pipe(
						Effect.mapError(() => deliveryNotFound(input.deliveryId)),
					)
				const mailboxKey = yield* locator
					.locateDelivery(input)
					.pipe(Effect.mapError(() => deliveryControlUnavailable('lookup')))
				if (mailboxKey === undefined) return yield* deliveryNotFound(input.deliveryId)
				return DeliveryReference.make({ deliveryId: input.deliveryId, mailboxKey })
			})
			const resolve = Effect.fn('delivery.control.resolve')(function* (input: {
				readonly deliveryId: DeliveryId
			}) {
				const parsed = yield* referenceFor(input)
				const snapshot = yield* store
					.loadMailbox({ key: parsed.mailboxKey })
					.pipe(Effect.mapError(() => deliveryControlUnavailable('lookup')))
				const batch =
					snapshot === undefined
						? undefined
						: activeBatches(snapshot.state).find((entry) => entry.deliveryId === parsed.deliveryId)
				const address = parseMailboxAddress(parsed.mailboxKey)
				const envelope = batch?.envelopes.at(-1)
				const now = yield* Clock.currentTimeMillis
				const outcome = snapshot?.state.outcomes.findLast(
					(entry) => entry.deliveryId === parsed.deliveryId && entry.expiresAt > now,
				)
				const metadata = resolutionMetadata(envelope, outcome)
				if (address === undefined || !Schema.is(ResolvedMetadata)(metadata))
					return yield* deliveryNotFound(parsed.deliveryId)
				return ResolvedDelivery.make({
					deliveryId: parsed.deliveryId,
					provider: address.provider,
					installation: address.installation,
					...metadata,
				})
			})
			const finish = Effect.fn('delivery.control.finish')(function* (input: FinishDelivery) {
				const request = yield* FinishDelivery.makeEffect(input).pipe(
					Effect.mapError(() =>
						DeliveryTerminalRequestInvalid.make({
							reason:
								input.markdown !== undefined &&
								(input.markdown.length > FINAL_MESSAGE_MARKDOWN_MAX_LENGTH ||
									new TextEncoder().encode(input.markdown).byteLength >
										FINAL_MESSAGE_MARKDOWN_MAX_BYTES)
									? 'markdown_too_large'
									: 'invalid_markdown',
						}),
					),
				)
				const parsed = yield* referenceFor(request)
				for (let attempt = 0; attempt <= 8; attempt++) {
					const snapshot = yield* store
						.loadMailbox({ key: parsed.mailboxKey })
						.pipe(Effect.mapError(() => deliveryControlUnavailable('lookup')))
					if (snapshot === undefined) return yield* deliveryNotFound(parsed.deliveryId)
					const current = currentMailbox(snapshot.state)
					const now = yield* Clock.currentTimeMillis
					const state = { ...current, outcomes: retainedOutcomes(current, now) }
					const batch = activeBatches(state).find((entry) => entry.deliveryId === parsed.deliveryId)
					const operation = state.operations?.find((entry) => entry.deliveryId === parsed.deliveryId)
					if (batch === undefined) {
						const recorded = retainedTerminalOutcome(state.outcomes, parsed.deliveryId, now)
						if (recorded === undefined) return yield* deliveryNotFound(parsed.deliveryId)
						return yield* replayTerminal({ request, recorded, operation })
					}
					const recorded = batch.stage?.terminalOutcome
					if (recorded !== undefined) {
						return yield* replayTerminal({ request, recorded, operation })
					}
					if (batch.stage === undefined) return yield* deliveryNotFound(parsed.deliveryId)
					const envelope = batch.envelopes.at(-1)
					const address = parseMailboxAddress(parsed.mailboxKey)
					if (envelope === undefined || address === undefined)
						return yield* deliveryNotFound(parsed.deliveryId)
					const nextOperation = finalMessageFor({ request, envelope, address, now })
					if (
						nextOperation !== undefined &&
						(state.maxOutcomes === undefined || mailboxCapacityUsage(state) >= state.maxOutcomes)
					)
						return yield* deliveryTerminalCapacityExceeded(parsed.deliveryId)
					const nextBatch = {
						...batch,
						stage: { ...batch.stage, terminalOutcome: request.outcome },
					}
					const batches = activeBatches(state).map((entry) => (entry === batch ? nextBatch : entry))
					const readyAt = terminalReadyAt(batch, state.readyAt, now)
					const committed = yield* store
						.commitMailbox({
							key: parsed.mailboxKey,
							expectedRevision: snapshot.revision,
							nextState: {
								...state,
								active: batches[0] ?? null,
								additionalActive: batches.slice(1),
								readyAt,
								operations:
									nextOperation === undefined
										? (state.operations ?? [])
										: [...(state.operations ?? []), nextOperation],
							},
						})
						.pipe(Effect.mapError(() => deliveryControlUnavailable('finish')))
					if (committed === 'committed')
						return terminalReceipt(parsed.deliveryId, request.outcome, 'accepted', nextOperation)
				}
				return yield* deliveryControlUnavailable('finish')
			})
			return DeliveryControl.of({ resolve, finish })
		}),
	)
}
