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
import { type AgentFinishedLogEntry, type FoldSession, type UserMessageLogEntry } from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
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
	Scope,
	Semaphore,
} from 'effect'

import { DeliveryApi } from './DeliveryApi'
import { MentionedIn } from './DiscussionContext'
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
	/** The comment that mentioned the bot, if a comment did. */
	mentionedIn: Schema.optional(MentionedIn),
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.String),
})
export type AgentSessionMessage = typeof AgentSessionMessage.Type

/** The delivery being worked on. */
export const ActiveDeliveryRecord = Schema.Struct({
	message: AgentSessionMessage,
	/** Where the delivery's entries begin in Fold's log; saved just before its prompt is sent. */
	fromSeq: Schema.optionalKey(Schema.Int),
})
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

/** What pulling the discussion's repository did before a new turn. */
export type RepositoryUpdate = Data.TaggedEnum<{
	Unchanged: {}
	Updated: { readonly before: string; readonly after: string }
	Failed: { readonly reason: string }
}>
export const RepositoryUpdate = Data.taggedEnum<RepositoryUpdate>()

/** The AgentSession's Fold conversation. */
export class AgentConversation extends Context.Service<
	AgentConversation,
	{
		/**
		 * Open the Fold session for one turn, starting it with the message's discussion when the log is empty.
		 * It stays open until the scope closes.
		 */
		readonly open: (
			message: AgentSessionMessage,
		) => Effect.Effect<TurnSession, RepoCloneError | PullRequestBranchUnavailable, Scope.Scope | RuntimeContext>
		/** Pull commits pushed since the repository was cloned or last pulled. A failure is reported, not raised. */
		readonly pullRepository: (message: AgentSessionMessage) => Effect.Effect<RepositoryUpdate>
		/**
		 * What the agent has not seen of the discussion, to put before the prompt; `markSeen` records it as seen
		 * once the agent has it. A failure is reported in the text, not raised.
		 */
		readonly readDiscussion: (
			message: AgentSessionMessage,
		) => Effect.Effect<{ readonly text: string; readonly markSeen: Effect.Effect<void> }>
	}
>()('alchemy-cloudflare/AgentConversation') {}

/** A pull request's branch cannot be checked out: GitHub could not be read, or the branch is in a fork or gone. */
export class PullRequestBranchUnavailable extends Data.TaggedError('PullRequestBranchUnavailable')<{
	readonly reason: string
}> {
	override get message() {
		return `I can't work on this pull request's branch: ${this.reason}`
	}
}

/** The turn restarted {@link MAX_RESTART_NUDGES} times without finishing. */
export class TurnRestartLimit extends Data.TaggedError('TurnRestartLimit')<{}> {
	override readonly message = `Restarts cut the turn off ${MAX_RESTART_NUDGES} times, so I gave up.`
}

const isRestartNudge = ({ message }: UserMessageLogEntry) =>
	Predicate.isString(message.content)
		? message.content === RESTART_NUDGE
		: message.content.some((part) => part.type === 'text' && part.text === RESTART_NUDGE)

/** Send a message into the session and resolve when the run that consumed it finishes. */
const send = (session: TurnSession, prompt: string) =>
	session.send(prompt).pipe(Effect.catchTag('SubagentNotFoundError', (error) => Effect.die(error)))

const shortCommit = (commit: string) => commit.slice(0, 7)

/** What the agent is told about the pull: nothing, the new commits, or that the repository may be behind. */
const repositoryNote = (update: RepositoryUpdate) =>
	RepositoryUpdate.$match(update, {
		Unchanged: () => '',
		Updated: ({ before, after }) =>
			`\n\n<system-information>New commits were pulled into the repository since your last turn (${shortCommit(before)} to ${shortCommit(after)}).</system-information>`,
		Failed: ({ reason }) =>
			`\n\n<system-information>Pulling the latest commits failed, so the repository may be behind: ${reason}</system-information>`,
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

/** A note that says what went wrong, with the error's own words in a code block. */
const failureNote = (summary: string, details: string | null) =>
	DeliveryEnding.Fail({
		payload: {
			markdown: truncate(
				Predicate.isNull(details) || details.length === 0
					? summary
					: `${summary}\n\n\`\`\`\n${details}\n\`\`\``,
			),
		},
	})

/** The turn itself failed, such as when the repository could not be cloned. */
const turnFailed = (cause: Cause.Cause<unknown>) =>
	failureNote(
		'Something went wrong, and I could not finish.',
		Cause.prettyErrors(cause)
			.map((error) => error.message)
			.join('\n'),
	)

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
		Match.when('error', () => failureNote('I ran into an error and could not finish.', finished.reason)),
		Match.orElse(() => failureNote(`I stopped before finishing (${finished.outcome}).`, finished.reason)),
	)

/** The delivery API's target for the message's delivery: its ID and access token. */
export const targetOf = (message: AgentSessionMessage) => ({
	deliveryId: message.deliveryId,
	accessToken: message.accessToken,
})

/**
 * Start and recover handed-off turns. They run in the background through the live object's `waitUntil`, so
 * both methods need its runtime context.
 *
 * @effect-expect-leaking RuntimeContext
 */
export class DeliveryTurns extends Context.Service<
	DeliveryTurns,
	{
		/**
		 * Save the delivery and start its turn in the background. A delivery already saved is a retried callback:
		 * its turn is running, so nothing starts.
		 */
		readonly accept: (message: AgentSessionMessage) => Effect.Effect<void, never, RuntimeContext>
		/** Continue the saved delivery's turn, if a deploy or crash cut it off. */
		readonly recover: Effect.Effect<void, never, RuntimeContext>
	}
>()('alchemy-cloudflare/DeliveryTurns') {
	static readonly layer = Layer.effect(
		DeliveryTurns,
		Effect.gen(function* () {
			const active = yield* ActiveDelivery
			const conversation = yield* AgentConversation
			const api = yield* DeliveryApi
			const recovery = yield* RunRecovery
			const lock = yield* Semaphore.make(1)

			/** Show `Working`, which GitHub renders as `eyes`. Only cosmetic, so a failure is logged, not raised. */
			const markWorking = (message: AgentSessionMessage) =>
				api.activity.set({ ...targetOf(message), activity: WORKING }).pipe(
					Effect.tapError((error) => Effect.logWarning('delivery.set_activity failed', error)),
					Effect.ignore,
					Effect.withSpan('agent_session.mark_working'),
				)

			/**
			 * End the delivery, retrying while the delivery API or its mailbox is unreachable. A delivery that
			 * already ended counts as done.
			 */
			const finishDelivery = (message: AgentSessionMessage, ending: DeliveryEnding) =>
				DeliveryEnding.$match(ending, {
					Complete: ({ payload }) => api.complete({ ...targetOf(message), payload }),
					Fail: ({ payload }) => api.fail({ ...targetOf(message), payload }),
				}).pipe(
					Effect.retry({
						schedule: Schedule.spaced('10 seconds'),
						times: 30,
						while: (error) =>
							Predicate.isTagged(error, 'DeliveryControlUnavailable') ||
							Predicate.isTagged(error, 'HttpClientError'),
					}),
					Effect.tapError((error) => Effect.logError('delivery.finish failed', error)),
					Effect.catchTags({
						DeliveryClosed: (error) =>
							Effect.logWarning('delivery.finish: the delivery already ended', error),
						DeliveryTerminalConflict: (error) =>
							Effect.logWarning('delivery.finish: it ended differently', error),
						DeliveryNotFound: (error) => Effect.logWarning('delivery.finish: the delivery is gone', error),
					}),
					Effect.withSpan('agent_session.finish_delivery', {
						attributes: { 'delivery.ending': ending._tag },
					}),
				)

			/**
			 * Pull the commits pushed since the last turn, then send the delivery's prompt after what is new in the
			 * discussion, which counts as seen once the turn ends.
			 */
			const sendPrompt = (session: TurnSession, message: AgentSessionMessage) =>
				Effect.gen(function* () {
					const update = yield* conversation.pullRepository(message)
					const discussion = yield* conversation.readDiscussion(message)
					const finished = yield* send(session, discussion.text + message.prompt + repositoryNote(update))
					yield* discussion.markSeen
					return finished
				})

			/**
			 * Run the delivery's turn from wherever Fold's log says it stands. A new delivery first saves where its
			 * entries begin in the log, then sends the prompt. A saved one sends the prompt if it never reached
			 * Fold, returns the result if the turn already finished, or nudges the agent on if a restart cut it
			 * off, without pulling, since the agent may be partway through its edits.
			 */
			const continueTurn = (session: TurnSession, record: ActiveDeliveryRecord) =>
				Effect.gen(function* () {
					const entries = yield* session.entries
					if (Predicate.isUndefined(record.fromSeq)) {
						yield* active.save(ActiveDeliveryRecord.make({ ...record, fromSeq: entries.length }))
						yield* Effect.logInfo('agent_turn.prompt_sent')
						return yield* sendPrompt(session, record.message)
					}

					const fromSeq = record.fromSeq
					const since = entries.filter(
						(entry) => entry.seq >= fromSeq && entry.agentId === session.rootAgentId,
					)
					const messages = since.filter((entry) => Predicate.isTagged(entry, 'user-message'))
					if (messages.length === 0) {
						yield* Effect.logInfo('agent_turn.recovered: the prompt never reached Fold; sending it')
						return yield* sendPrompt(session, record.message)
					}

					const finished = since.findLast((entry) => Predicate.isTagged(entry, 'agent-finished'))
					const open = messages.filter((entry) => Predicate.isUndefined(finished) || entry.seq > finished.seq)
					if (open.length === 0 && Predicate.isNotUndefined(finished)) {
						yield* Effect.logInfo('agent_turn.recovered: the turn had finished; reporting it')
						return finished
					}
					if (open.filter(isRestartNudge).length >= MAX_RESTART_NUDGES) return yield* new TurnRestartLimit()
					yield* Effect.logInfo('agent_turn.recovered: a restart cut the turn off; nudging it on')
					return yield* send(session, RESTART_NUDGE)
				})

			/**
			 * Run a saved delivery's turn to the end and report it. Any failure but an interruption fails the
			 * delivery: an interruption means the object is going away, and recovery will continue the turn.
			 */
			const runTurn = (record: ActiveDeliveryRecord) =>
				Effect.gen(function* () {
					yield* Effect.logInfo('agent_turn.started')
					yield* markWorking(record.message)
					const session = yield* conversation.open(record.message)
					const finished = yield* continueTurn(session, record)
					const ending = deliveryEnding(finished)
					yield* Effect.logInfo('agent_turn.finished').pipe(
						Effect.annotateLogs({ 'fold.outcome': finished.outcome, 'delivery.ending': ending._tag }),
					)
					yield* finishDelivery(record.message, ending)
				}).pipe(
					Effect.scoped,
					Effect.catchCauseIf(
						(cause) => !Cause.hasInterruptsOnly(cause),
						(cause) =>
							Effect.logError('agent turn failed', cause).pipe(
								Effect.andThen(finishDelivery(record.message, turnFailed(cause))),
							),
					),
					Effect.andThen(active.clear({ deliveryId: record.message.deliveryId })),
					Effect.withSpan('agent_session.run_delivery_turn', {
						attributes: { 'delivery.id': record.message.deliveryId },
					}),
					Effect.annotateLogs({ 'delivery.id': record.message.deliveryId }),
				)

			/** Leave the record saved for the next activation when the turn could not report its result. */
			const start = (record: ActiveDeliveryRecord) =>
				recovery.fork(
					runTurn(record).pipe(
						Effect.catchCause((cause) =>
							Cause.hasInterruptsOnly(cause)
								? Effect.void
								: Effect.logError('agent turn could not finish its delivery; it stays saved', cause),
						),
					),
				)

			return DeliveryTurns.of({
				accept: (message) =>
					lock
						.withPermit(
							Effect.gen(function* () {
								const current = yield* active.current
								if (Option.isSome(current)) {
									if (current.value.message.deliveryId === message.deliveryId) {
										return yield* Effect.logInfo(
											'agent_session.accept: already running this delivery',
										)
									}
									yield* Effect.logWarning('agent session replaced an unfinished delivery').pipe(
										Effect.annotateLogs({
											'delivery.previous_id': current.value.message.deliveryId,
										}),
									)
								}
								const record = ActiveDeliveryRecord.make({ message })
								yield* active.save(record)
								yield* start(record)
								yield* Effect.logInfo('agent_session.accepted')
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
