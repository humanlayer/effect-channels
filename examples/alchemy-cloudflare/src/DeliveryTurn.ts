/**
 * One handed-off delivery's agent turn. The GitHub callback hands its delivery off once `accept` returns; the
 * turn then runs in the background and reports how it ended to the delivery, which DeliveryMailbox holds open
 * until then.
 *
 * The delivery waits in the Durable Object's key-value storage, not in Fold's log: it carries the delivery's
 * access token, which the model must never see. Keeping it there makes a retried callback a no-op, and lets the
 * object finish the right delivery after a deploy or crash. DeliveryMailbox hands off one delivery per mailbox
 * at a time, so there is at most one.
 */
import {
	DELIVERY_MARKDOWN_MAX_LENGTH,
	DeliveryActivity,
	DeliveryId,
	type CompleteDeliveryPayload,
	type FailDeliveryPayload,
} from '@humanlayer/channels-delivery'
import { GitHubIssue, GitHubPullRequest } from '@humanlayer/channels-github'
import { EventLog, type AgentFinishedLogEntry, type FoldSession, type UserMessageLogEntry } from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import {
	Cause,
	Context,
	Data,
	Effect,
	Layer,
	Match,
	Option,
	Predicate,
	Schedule,
	Schema,
	Semaphore,
	Stream,
} from 'effect'

import { DeliveryApi } from './DeliveryApi'
import { RunRecovery } from './SessionRecovery'
import type { RepoCloneError } from './Workspace'

export const RESTART_NUDGE =
	'<system-information>A restart cut you off before you finished. Continue where you left off.</system-information>'
const MAX_RESTART_NUDGES = 3
const ACTIVE_DELIVERY_KEY = 'active_delivery'
const TRUNCATED_SUFFIX = '\n\n_(Output truncated.)_'
const WORKING = DeliveryActivity.cases.Working.make({ message: 'Working on it' })

/** A message for the AgentSession: the prompt, its GitHub discussion, and the handed-off delivery to finish. */
export const AgentSessionMessage = Schema.Struct({
	prompt: Schema.String,
	githubDiscussion: Schema.Union([GitHubIssue, GitHubPullRequest]),
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.String),
})
export type AgentSessionMessage = typeof AgentSessionMessage.Type

/** The delivery being worked on, and how long Fold's log was when it arrived. */
export const ActiveDeliveryRecord = Schema.Struct({ message: AgentSessionMessage, fromSeq: Schema.Int })
export type ActiveDeliveryRecord = typeof ActiveDeliveryRecord.Type

/** The AgentSession's one {@link ActiveDeliveryRecord}. */
export class ActiveDelivery extends Context.Service<
	ActiveDelivery,
	{
		readonly current: Effect.Effect<Option.Option<ActiveDeliveryRecord>>
		readonly save: (record: ActiveDeliveryRecord) => Effect.Effect<void>
		/** Forget the record if it is still the one for `deliveryId`. */
		readonly clear: (input: { readonly deliveryId: DeliveryId }) => Effect.Effect<void>
	}
>()('alchemy-cloudflare/ActiveDelivery') {
	/** Over the Durable Object's key-value storage. */
	static readonly layer = Layer.effect(
		ActiveDelivery,
		Effect.gen(function* () {
			const { storage } = (yield* Cloudflare.DurableObjectState).raw
			const remove = Effect.promise(() => storage.delete(ACTIVE_DELIVERY_KEY)).pipe(Effect.asVoid)
			const current = Effect.promise(() => storage.get(ACTIVE_DELIVERY_KEY)).pipe(
				Effect.flatMap((value) =>
					Predicate.isUndefined(value)
						? Effect.succeedNone
						: Effect.asSome(Schema.decodeUnknownEffect(ActiveDeliveryRecord)(value)),
				),
				Effect.catchTag('SchemaError', (error) =>
					Effect.logError('active_delivery: the saved delivery is unreadable; forgetting it', error).pipe(
						Effect.andThen(remove),
						Effect.as(Option.none<ActiveDeliveryRecord>()),
					),
				),
			)

			return ActiveDelivery.of({
				current,
				save: (record) =>
					Schema.encodeEffect(ActiveDeliveryRecord)(record).pipe(
						Effect.orDie,
						Effect.flatMap((encoded) => Effect.promise(() => storage.put(ACTIVE_DELIVERY_KEY, encoded))),
					),
				clear: ({ deliveryId }) =>
					Effect.flatMap(current, (saved) =>
						Option.exists(saved, (record) => record.message.deliveryId === deliveryId)
							? remove
							: Effect.void,
					),
			})
		}),
	)
}

/** What a turn needs from the Fold session. */
export type TurnSession = Pick<FoldSession, 'rootAgentId' | 'entries' | 'send'>

/** The AgentSession's Fold conversation. */
export class AgentConversation extends Context.Service<
	AgentConversation,
	{
		/** The Fold session, started with the message's discussion when the log is empty. */
		readonly open: (message: AgentSessionMessage) => Effect.Effect<TurnSession, RepoCloneError>
	}
>()('alchemy-cloudflare/AgentConversation') {}

/** The turn restarted {@link MAX_RESTART_NUDGES} times without finishing. */
export class TurnRestartLimit extends Data.TaggedError('TurnRestartLimit')<{}> {}

const isRestartNudge = ({ message }: UserMessageLogEntry) =>
	Predicate.isString(message.content)
		? message.content === RESTART_NUDGE
		: message.content.some((part) => part.type === 'text' && part.text === RESTART_NUDGE)

/** Send a message into the session and resolve when the run that consumed it finishes. */
const send = (session: TurnSession, prompt: string) =>
	session.send(prompt).pipe(Effect.catchTag('SubagentNotFoundError', (error) => Effect.die(error)))

/**
 * Run the delivery's turn from wherever Fold's log says it stands: send the prompt if it never reached Fold,
 * return the result if the turn already finished, or nudge the agent on if a restart cut it off.
 */
const continueTurn = (session: TurnSession, record: ActiveDeliveryRecord) =>
	Effect.gen(function* () {
		const since = (yield* session.entries).filter(
			(entry) => entry.seq >= record.fromSeq && entry.agentId === session.rootAgentId,
		)
		const messages = since.filter((entry) => Predicate.isTagged(entry, 'user-message'))
		if (messages.length === 0) return yield* send(session, record.message.prompt)

		const finished = since.findLast((entry) => Predicate.isTagged(entry, 'agent-finished'))
		const open = messages.filter((entry) => Predicate.isUndefined(finished) || entry.seq > finished.seq)
		if (open.length === 0 && Predicate.isNotUndefined(finished)) return finished
		if (open.filter(isRestartNudge).length >= MAX_RESTART_NUDGES) return yield* new TurnRestartLimit()
		return yield* send(session, RESTART_NUDGE)
	})

const truncate = (markdown: string) =>
	markdown.length <= DELIVERY_MARKDOWN_MAX_LENGTH
		? markdown
		: markdown.slice(0, DELIVERY_MARKDOWN_MAX_LENGTH - TRUNCATED_SUFFIX.length) + TRUNCATED_SUFFIX

/** How a delivery ends: completed with Fold's answer, or failed with a short note. */
type DeliveryEnding = Data.TaggedEnum<{
	Complete: { readonly payload: CompleteDeliveryPayload }
	Fail: { readonly payload: FailDeliveryPayload }
}>
const DeliveryEnding = Data.taggedEnum<DeliveryEnding>()

const FAILED = DeliveryEnding.Fail({ payload: { markdown: 'Something went wrong, and I could not finish.' } })

/**
 * How the delivery ends, from how Fold's run ended. A question for the user is an ordinary completion: the
 * user answers by mentioning the bot again, which starts the next delivery.
 */
const deliveryEnding = (finished: AgentFinishedLogEntry): DeliveryEnding =>
	Match.value(finished.outcome).pipe(
		Match.when('completed', () =>
			DeliveryEnding.Complete({
				payload: Predicate.isNull(finished.resultText) ? {} : { markdown: truncate(finished.resultText) },
			}),
		),
		Match.when('error', () =>
			DeliveryEnding.Fail({ payload: { markdown: 'I ran into an error and could not finish.' } }),
		),
		Match.orElse(() => DeliveryEnding.Fail({ payload: { markdown: 'I stopped before finishing.' } })),
	)

const targetOf = (message: AgentSessionMessage) => ({
	deliveryId: message.deliveryId,
	accessToken: message.accessToken,
})

/** Show `Working`, which GitHub renders as `eyes`. Only cosmetic, so a failure is logged, not raised. */
const markWorking = (message: AgentSessionMessage) =>
	Effect.gen(function* () {
		const api = yield* DeliveryApi
		yield* api.activity.set({ ...targetOf(message), activity: WORKING })
	}).pipe(
		Effect.tapError((error) => Effect.logWarning('delivery.set_activity failed', error)),
		Effect.ignore,
		Effect.withSpan('agent_session.mark_working'),
	)

/**
 * End the delivery, retrying while the delivery API or its mailbox is unreachable. A delivery that already
 * ended counts as done.
 */
const finishDelivery = (message: AgentSessionMessage, ending: DeliveryEnding) =>
	Effect.gen(function* () {
		const api = yield* DeliveryApi
		yield* DeliveryEnding.$match(ending, {
			Complete: ({ payload }) => api.complete({ ...targetOf(message), payload }),
			Fail: ({ payload }) => api.fail({ ...targetOf(message), payload }),
		})
	}).pipe(
		Effect.retry({
			schedule: Schedule.spaced('10 seconds'),
			times: 30,
			while: (error) =>
				Predicate.isTagged(error, 'DeliveryControlUnavailable') || Predicate.isTagged(error, 'HttpClientError'),
		}),
		Effect.tapError((error) => Effect.logError('delivery.finish failed', error)),
		Effect.catchTags({
			DeliveryClosed: (error) => Effect.logWarning('delivery.finish: the delivery already ended', error),
			DeliveryTerminalConflict: (error) => Effect.logWarning('delivery.finish: it ended differently', error),
			DeliveryNotFound: (error) => Effect.logWarning('delivery.finish: the delivery is gone', error),
		}),
		Effect.withSpan('agent_session.finish_delivery', { attributes: { 'delivery.ending': ending._tag } }),
	)

/**
 * Run a saved delivery's turn to the end and report it. Any failure but an interruption fails the delivery: an
 * interruption means the object is going away, and recovery will continue the turn.
 */
export const runDeliveryTurn = (record: ActiveDeliveryRecord) =>
	Effect.gen(function* () {
		const conversation = yield* AgentConversation
		yield* markWorking(record.message)
		const session = yield* conversation.open(record.message)
		const finished = yield* continueTurn(session, record)
		yield* finishDelivery(record.message, deliveryEnding(finished))
	}).pipe(
		Effect.catchCauseIf(
			(cause) => !Cause.hasInterruptsOnly(cause),
			(cause) =>
				Effect.logError('agent turn failed', cause).pipe(
					Effect.andThen(finishDelivery(record.message, FAILED)),
				),
		),
		Effect.andThen(
			Effect.gen(function* () {
				const active = yield* ActiveDelivery
				yield* active.clear({ deliveryId: record.message.deliveryId })
			}),
		),
		Effect.withSpan('agent_session.run_delivery_turn', {
			attributes: { 'delivery.id': record.message.deliveryId },
		}),
		Effect.annotateLogs({ 'delivery.id': record.message.deliveryId }),
	)

/** Start and recover handed-off turns. */
export class DeliveryTurns extends Context.Service<
	DeliveryTurns,
	{
		/**
		 * Save the delivery and start its turn in the background. A delivery already saved is a retried callback:
		 * its turn is running, so nothing starts.
		 */
		readonly accept: (message: AgentSessionMessage) => Effect.Effect<void>
		/** Continue the saved delivery's turn, if a deploy or crash cut it off. */
		readonly recover: Effect.Effect<void>
	}
>()('alchemy-cloudflare/DeliveryTurns') {
	static readonly layer = Layer.effect(
		DeliveryTurns,
		Effect.gen(function* () {
			const active = yield* ActiveDelivery
			const conversation = yield* AgentConversation
			const api = yield* DeliveryApi
			const recovery = yield* RunRecovery
			const eventLog = yield* EventLog
			const lock = yield* Semaphore.make(1)

			/** Leave the record saved for the next activation when the turn could not report its result. */
			const start = (record: ActiveDeliveryRecord) =>
				recovery.fork(
					runDeliveryTurn(record).pipe(
						Effect.catchCause((cause) =>
							Cause.hasInterruptsOnly(cause)
								? Effect.void
								: Effect.logError('agent turn could not finish its delivery; it stays saved', cause),
						),
						Effect.provideService(ActiveDelivery, active),
						Effect.provideService(AgentConversation, conversation),
						Effect.provideService(DeliveryApi, api),
					),
				)

			return DeliveryTurns.of({
				accept: (message) =>
					lock
						.withPermit(
							Effect.gen(function* () {
								const current = yield* active.current
								if (Option.isSome(current)) {
									if (current.value.message.deliveryId === message.deliveryId) return
									yield* Effect.logWarning('agent session replaced an unfinished delivery').pipe(
										Effect.annotateLogs({
											'delivery.previous_id': current.value.message.deliveryId,
										}),
									)
								}
								const fromSeq = yield* Stream.runCount(eventLog.entries()).pipe(Effect.orDie)
								const record = ActiveDeliveryRecord.make({ message, fromSeq })
								yield* active.save(record)
								yield* start(record)
							}),
						)
						.pipe(
							Effect.withSpan('agent_session.accept_delivery', {
								attributes: { 'delivery.id': message.deliveryId },
							}),
						),
				recover: active.current.pipe(
					Effect.flatMap(Option.match({ onNone: () => Effect.void, onSome: start })),
					Effect.withSpan('agent_session.recover_delivery'),
				),
			})
		}),
	)
}
