/**
 * `FakeRemoteAgent`: a Durable Object that stands in for a remote agent host, such as a VM or a sandbox.
 *
 * It does no agent work. One object per delivery ID saves the job, wakes on its alarm, and reports back
 * over the public delivery API, as a real remote agent would. It works the same for a Slack thread, a
 * Linear Agent Session, a Linear issue, or a GitHub issue or pull request, because it asks the delivery
 * what it supports:
 *
 * - While it waits it shows what it is doing as the delivery's activity (Slack's thread status line, an
 *   ephemeral thought in a Linear session, or the bot's `eyes` reaction on GitHub), changing the text
 *   halfway through. GitHub does not show the text.
 * - Just before it finishes it posts one lasting summary message (a Slack message, a lasting thought in
 *   a Linear session, or an issue or pull request comment).
 * - With `react`, it adds the bot's `rocket` reaction to what started the delivery when it starts (asking
 *   twice, to show the second request is a replay), removes it halfway through (also twice), and adds
 *   `heart` to its summary message. Each step is skipped where the delivery cannot react on that target,
 *   such as a Linear session's messages.
 * - With `plan`, it keeps a three-step plan up to date: one step in progress when it starts, the next one
 *   halfway through, all done just before it finishes. It sends the whole plan each time, and the second
 *   request of each is a replay. Slack shows it as one plan message, a Linear session as its Agent Plan, and
 *   GitHub and a Linear issue as one comment the agent's plan edits.
 * - It then completes the delivery with a final message, or with a question and choices for `ask`.
 * - It reads the delivery's status at least every few seconds. When someone asked it to stop, such as
 *   Stop in a Linear session, it fails the delivery with `Stopped as requested.` and ends.
 *
 * It is a Durable Object only because the example already deploys to Cloudflare: its alarm outlives the
 * callback, the mailbox claim, and a redeploy.
 */
import {
	DeliveryActivity,
	DeliveryId,
	DeliveryPlan,
	DeliveryPlanItem,
	DeliveryPlanItemId,
	DeliveryPlanItemState,
	DeliveryReactionTarget,
	MessageId,
	deliveryReactionTargetKind,
	makeDeliveryClient,
	type DeliveryClient,
	type DeliveryStatus,
	type PortableReaction,
} from '@humanlayer/channels-delivery'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Context, Effect, Option, Schema } from 'effect'
import { HttpClient } from 'effect/http'

import { withFlakyOutputMarker } from './FlakySlackApi'

const JOB_KEY = 'fake-remote-agent-job'

/** The longest the agent goes without reading its delivery's status, so a stop request is seen soon. */
const STATUS_POLL_MS = 5_000

/** The question and choices an `ask` job ends its turn with. */
const ASK_MARKDOWN = 'Which environment should I deploy to?'
const ASK_OPTIONS = ['staging', 'production']

/** The summary message's name. One job posts one. */
const SUMMARY_MESSAGE = MessageId.make('summary')

/** How long after starting a job shows its first activity. */
const FIRST_ACTIVITY_DELAY_MS = 1_000

/** How long Slack refuses a `flaky` job's final message, counted from when the job completes. */
const FLAKY_OUTPUT_MS = 20_000

/** Every way a delivery API call can fail. */
type DeliveryApiError =
	| Effect.Error<ReturnType<DeliveryClient['messages']['create']>>
	| Effect.Error<ReturnType<DeliveryClient['status']>>

/** The delivery API, as the fake remote agent calls it: through the Worker's public URL. */
class DeliveryApi extends Context.Service<DeliveryApi, DeliveryClient>()('alchemy-cloudflare-example/DeliveryApi') {}

/**
 * What `start` receives over RPC. The token arrives as a plain string and is redacted on arrival.
 *
 * @property flakyOutput - make Slack refuse the final message for a while
 * @property askForInput - end the turn with a question and choices instead of an answer
 * @property react - add and remove portable reactions while the job runs
 * @property plan - keep a plan up to date while the job runs
 */
export const StartRemoteAgentJob = Schema.Struct({
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.NonEmptyString),
	delaySeconds: Schema.Int.check(Schema.isGreaterThan(0)),
	flakyOutput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	askForInput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	react: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	plan: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
})
export type StartRemoteAgentJob = typeof StartRemoteAgentJob.Type

/**
 * How far a job has got. The alarm moves it one step at a time: show `Working`, change the text
 * halfway, then post the summary and complete.
 */
const RemoteAgentStep = Schema.Literals(['NotStarted', 'Working', 'Halfway'])

/**
 * The job one object saves. Stored in its encoded form, so the token is kept as its plain value.
 * Jobs saved before a field existed read it as its default.
 */
const RemoteAgentJob = Schema.Struct({
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.NonEmptyString),
	finishAt: Schema.Int,
	delaySeconds: Schema.Int.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
	step: RemoteAgentStep.pipe(Schema.withDecodingDefaultKey(Effect.succeed('NotStarted' as const))),
	flakyOutput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	askForInput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	react: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	plan: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
})
type RemoteAgentJob = typeof RemoteAgentJob.Type

/**
 * What `start` answers: the saved job, without its token.
 *
 * @property runLogUrl - a page on the Worker that shows how far the job has got
 */
export const RemoteAgentJobStarted = Schema.Struct({
	deliveryId: DeliveryId,
	finishAt: Schema.Int,
	runLogUrl: Schema.String,
})
export type RemoteAgentJobStarted = typeof RemoteAgentJobStarted.Type

/** A start request or a stored job did not decode. Carries no values, so it cannot leak the token. */
class RemoteAgentJobInvalid extends Schema.TaggedError<RemoteAgentJobInvalid>()('RemoteAgentJobInvalid', {
	source: Schema.Literals(['start', 'storage']),
}) {}

/** The delivery API could not be reached; the alarm dies so Cloudflare retries it. */
class RemoteAgentRetryable extends Schema.TaggedError<RemoteAgentRetryable>()('RemoteAgentRetryable', {
	deliveryId: DeliveryId,
	reason: Schema.String,
}) {}

const decodeStart = (input: typeof StartRemoteAgentJob.Encoded) =>
	Schema.decodeEffect(StartRemoteAgentJob)(input).pipe(
		Effect.catchTag('SchemaError', () => Effect.die(new RemoteAgentJobInvalid({ source: 'start' }))),
	)

const decodeJob = (stored: Schema.Json) =>
	Schema.decodeUnknownEffect(RemoteAgentJob)(stored).pipe(
		Effect.catchTag('SchemaError', () => Effect.die(new RemoteAgentJobInvalid({ source: 'storage' }))),
	)

const encodeJob = (job: RemoteAgentJob) => Schema.encodeEffect(RemoteAgentJob)(job).pipe(Effect.orDie)

/** The run-log page of a delivery's job. The Worker serves it at `/fake-agent/runs/:deliveryId`. */
const runLogPath = (deliveryId: string) => `/fake-agent/runs/${encodeURIComponent(deliveryId)}`

const toStarted = (job: RemoteAgentJob, workerUrl: string) =>
	RemoteAgentJobStarted.make({
		deliveryId: job.deliveryId,
		finishAt: job.finishAt,
		runLogUrl: new URL(runLogPath(job.deliveryId), workerUrl).toString(),
	})

/** The delivery API refused the job's request, so the job ends. */
class RemoteAgentRefused extends Schema.TaggedError<RemoteAgentRefused>()('RemoteAgentRefused', {
	reason: Schema.String,
}) {}

/**
 * How the fake remote agent treats a failed delivery API call: a refusal ends the job; an unreachable
 * API dies so Cloudflare retries the alarm. Only the error's tag is logged.
 */
const handleDeliveryApiFailure =
	(job: RemoteAgentJob) =>
	<A, R>(effect: Effect.Effect<A, DeliveryApiError, R>) => {
		const refused = (reason: string) =>
			Effect.logWarning('Fake remote agent refused; dropping the job').pipe(
				Effect.annotateLogs({ delivery_id: job.deliveryId, reason }),
				Effect.andThen(Effect.fail(new RemoteAgentRefused({ reason }))),
			)
		const retry = (reason: string) =>
			Effect.logWarning('Fake remote agent could not reach the delivery API; the alarm will retry').pipe(
				Effect.annotateLogs({ delivery_id: job.deliveryId, reason }),
				Effect.andThen(Effect.die(new RemoteAgentRetryable({ deliveryId: job.deliveryId, reason }))),
			)
		return effect.pipe(
			Effect.catchTags({
				DeliveryCredentialMissing: ({ _tag }) => refused(_tag),
				DeliveryNotFound: ({ _tag }) => refused(_tag),
				DeliveryTerminalConflict: ({ _tag }) => refused(_tag),
				DeliveryClosed: ({ _tag }) => refused(_tag),
				DeliveryOperationUnsupported: ({ _tag }) => refused(_tag),
				DeliveryMessageNotFound: ({ _tag }) => refused(_tag),
				DeliveryMessageDeleted: ({ _tag }) => refused(_tag),
				DeliveryMessageConflict: ({ _tag }) => refused(_tag),
				DeliveryReactionTargetUnavailable: ({ _tag }) => refused(_tag),
				SchemaError: ({ _tag }) => refused(_tag),
				DeliveryControlUnavailable: ({ _tag }) => retry(_tag),
				HttpClientError: ({ _tag }) => retry(_tag),
			}),
		)
	}

const secondsLeft = (job: RemoteAgentJob, now: number) => Math.max(1, Math.round((job.finishAt - now) / 1_000))

/** When the job changes its activity text: halfway through the wait. */
const halfwayAt = (job: RemoteAgentJob) => job.finishAt - (job.delaySeconds * 1_000) / 2

/**
 * Show what the job is doing as the delivery's activity: `Working`, with the text for its step. A
 * delivery that cannot show activity, such as a Linear issue, is skipped.
 */
const showActivity = Effect.fn('fake_remote_agent.show_activity')(function* (
	job: RemoteAgentJob,
	status: DeliveryStatus,
	message: string,
) {
	if (!status.supportedOperations.includes('SetActivity')) return
	const deliveryApi = yield* DeliveryApi
	const receipt = yield* deliveryApi.activity.set({
		deliveryId: job.deliveryId,
		accessToken: job.accessToken,
		activity: DeliveryActivity.cases.Working.make({ message }),
	})
	yield* Effect.logInfo('Fake remote agent set its activity').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, step: job.step, receipt_status: receipt.status }),
	)
})

/** The reaction a `react` job adds to what started the delivery, then removes. */
const ACTIVATION_REACTION: PortableReaction = 'rocket'

/** The reaction a `react` job adds to its summary message. */
const SUMMARY_REACTION: PortableReaction = 'heart'

/**
 * For a `react` job, make the bot's `reaction` present or absent on `target`, asking twice: the second
 * request answers `already_recorded`. Skipped where the delivery cannot react on the target.
 */
const setReactionTwice = Effect.fn('fake_remote_agent.set_reaction')(function* (
	job: RemoteAgentJob,
	status: DeliveryStatus,
	request: { readonly target: DeliveryReactionTarget; readonly reaction: PortableReaction; readonly active: boolean },
) {
	const kind = deliveryReactionTargetKind(request.target)
	if (!job.react || !status.reactionTargets.includes(kind)) return
	const deliveryApi = yield* DeliveryApi
	const set = deliveryApi.reactions.set({ deliveryId: job.deliveryId, accessToken: job.accessToken, ...request })
	const first = yield* set
	const second = yield* set
	yield* Effect.logInfo('Fake remote agent set a reaction').pipe(
		Effect.annotateLogs({
			delivery_id: job.deliveryId,
			target: kind,
			reaction: request.reaction,
			active: request.active,
			receipt_status: `${first.status},${second.status}`,
		}),
	)
})

/** Where a `plan` job is: starting, halfway, or done. */
type PlanStage = 'Started' | 'Halfway' | 'Done'

const planStep = (id: string, title: string, state: DeliveryPlanItemState) =>
	DeliveryPlanItem.make({ id: DeliveryPlanItemId.make(id), title, state })

/**
 * The plan at each stage. Halfway, `look` gains a result and `change` its details; at the end `change`
 * loses its details, which shows that each provider replaces the whole plan.
 */
const fakePlan = (stage: PlanStage) => {
	const { Pending, InProgress, Completed } = DeliveryPlanItemState.cases
	type Steps = readonly [DeliveryPlanItemState, DeliveryPlanItemState, DeliveryPlanItemState]
	const stages = {
		Started: [InProgress.make({}), Pending.make({}), Pending.make({})],
		Halfway: [Completed.make({ result: 'Nothing unusual' }), InProgress.make({ details: 'Waiting it out' }), Pending.make({})],
		Done: [Completed.make({ result: 'Nothing unusual' }), Completed.make({}), Completed.make({ result: 'All quiet' })],
	} satisfies Record<PlanStage, Steps>
	const [look, change, check] = stages[stage]
	return DeliveryPlan.make({
		title: 'Fake remote agent plan',
		items: [
			planStep('look', 'Look into it', look),
			planStep('change', 'Make no change', change),
			planStep('check', 'Check the result', check),
		],
	})
}

/** For a `plan` job, send the whole plan for `stage`, twice: the second request answers `already_recorded`. */
const putPlanTwice = Effect.fn('fake_remote_agent.put_plan')(function* (job: RemoteAgentJob, stage: PlanStage) {
	if (!job.plan) return
	const deliveryApi = yield* DeliveryApi
	const put = deliveryApi.plan.put({ deliveryId: job.deliveryId, accessToken: job.accessToken, plan: fakePlan(stage) })
	const first = yield* put
	const second = yield* put
	yield* Effect.logInfo('Fake remote agent set its plan').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, stage, receipt_status: `${first.status},${second.status}` }),
	)
})

/** Someone asked the job to stop, such as Stop in a Linear session: fail the delivery, which ends the turn. */
const stopDelivery = Effect.fn('fake_remote_agent.stop_delivery')(function* (job: RemoteAgentJob) {
	const deliveryApi = yield* DeliveryApi
	const receipt = yield* deliveryApi.fail({
		deliveryId: job.deliveryId,
		accessToken: job.accessToken,
		payload: { markdown: 'Stopped as requested.' },
	})
	yield* Effect.logInfo('Fake remote agent stopped because the delivery asked it to').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, receipt_status: receipt.status }),
	)
})

/**
 * Report the job's delivery as complete: post a lasting summary message when the delivery supports
 * messages, read the delivery's status, complete it with a final message (or a question for `ask`),
 * read its status again, and log only safe fields. The second read shows the delivery still finishing:
 * the request returned before the provider was called. The result clears the activity.
 */
const completeDelivery = Effect.fn('fake_remote_agent.complete_delivery')(function* (
	job: RemoteAgentJob,
	before: DeliveryStatus,
) {
	const deliveryApi = yield* DeliveryApi
	const target = { deliveryId: job.deliveryId, accessToken: job.accessToken }
	yield* putPlanTwice(job, 'Done')
	if (before.supportedOperations.includes('CreateMessage')) {
		const summary = yield* deliveryApi.messages.create({
			...target,
			message: {
				messageId: SUMMARY_MESSAGE,
				markdown: `Summary: the fake remote agent waited ${job.delaySeconds}s and did no real work.`,
			},
		})
		yield* Effect.logInfo('Fake remote agent posted its summary message').pipe(
			Effect.annotateLogs({ delivery_id: job.deliveryId, receipt_status: summary.status }),
		)
		yield* setReactionTwice(job, before, {
			target: DeliveryReactionTarget.cases.MessageTarget.make({ messageId: SUMMARY_MESSAGE }),
			reaction: SUMMARY_REACTION,
			active: true,
		})
	}
	const status = yield* deliveryApi.status(target)
	yield* Effect.logInfo('Fake remote agent read delivery status').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, stage: status.stage }),
	)
	const finished = `Fake remote agent finished after ${job.delaySeconds}s.`
	const markdown = job.flakyOutput
		? withFlakyOutputMarker(finished, (yield* Clock.currentTimeMillis) + FLAKY_OUTPUT_MS)
		: finished
	const receipt = yield* deliveryApi.complete({
		...target,
		payload: job.askForInput
			? { markdown: ASK_MARKDOWN, awaitingInput: { options: ASK_OPTIONS } }
			: { markdown },
	})
	yield* Effect.logInfo('Fake remote agent completed delivery').pipe(
		Effect.annotateLogs({
			delivery_id: job.deliveryId,
			receipt_status: receipt.status,
			awaiting_input: job.askForInput,
		}),
	)
	const after = yield* deliveryApi.status(target)
	yield* Effect.logInfo('Fake remote agent read delivery status after completing').pipe(
		Effect.annotateLogs({
			delivery_id: job.deliveryId,
			stage: after.stage,
			output: after.output.map(({ operationId, state }) => `${operationId}:${state}`).join(','),
		}),
	)
})

/** One object per delivery ID. Address it with `getByName(deliveryId)`. */
export class FakeRemoteAgent extends Cloudflare.DurableObject<
	FakeRemoteAgent,
	{
		readonly start: (
			input: typeof StartRemoteAgentJob.Encoded,
		) => Effect.Effect<RemoteAgentJobStarted, never, RuntimeContext>
		/** A plain-text line on how far the job has got, for its run-log page. Never includes the token. */
		readonly describe: () => Effect.Effect<string, never, RuntimeContext>
		readonly alarm: () => Effect.Effect<void, never, RuntimeContext>
	}
>()('FakeRemoteAgent') {}

/**
 * The fake remote agent's implementation, in Alchemy's two phases. Its layer requires an `HttpClient`;
 * the host Worker provides it. The delivery API lives at the host Worker's own URL, which Alchemy binds
 * at deploy and which can only be read per instance. `start` saves the job once and sets the alarm; the
 * alarm completes the delivery and deletes the job.
 */
export const FakeRemoteAgentLive = FakeRemoteAgent.make(
	Effect.gen(function* () {
		const state = yield* Cloudflare.DurableObjectState
		const workerUrl = yield* Cloudflare.Worker.URL
		const httpClient = yield* HttpClient.HttpClient

		return Effect.gen(function* () {
			const baseUrl = yield* workerUrl
			const deliveryApi = yield* makeDeliveryClient({ baseUrl }).pipe(
				Effect.provideService(HttpClient.HttpClient, httpClient),
			)
			const { storage } = state
			const readJob = storage.get<Schema.Json>(JOB_KEY).pipe(
				Effect.map(Option.fromNullishOr),
				Effect.flatMap(
					Option.match({
						onNone: () => Effect.succeedNone,
						onSome: (stored) => decodeJob(stored).pipe(Effect.asSome),
					}),
				),
			)

			const start = Effect.fn('fake_remote_agent.start')(function* (input: typeof StartRemoteAgentJob.Encoded) {
				const request = yield* decodeStart(input)
				const existing = yield* readJob
				if (Option.isSome(existing)) return toStarted(existing.value, baseUrl)
				const now = yield* Clock.currentTimeMillis
				const job = RemoteAgentJob.make({
					deliveryId: request.deliveryId,
					accessToken: request.accessToken,
					finishAt: now + request.delaySeconds * 1_000,
					delaySeconds: request.delaySeconds,
					step: 'NotStarted',
					flakyOutput: request.flakyOutput,
					askForInput: request.askForInput,
					react: request.react,
					plan: request.plan,
				})
				yield* storage.put(JOB_KEY, yield* encodeJob(job))
				yield* storage.setAlarm(Math.min(now + FIRST_ACTIVITY_DELAY_MS, job.finishAt))
				yield* Effect.logInfo('Fake remote agent job started').pipe(
					Effect.annotateLogs({
						delivery_id: job.deliveryId,
						finish_at: job.finishAt,
						flaky_output: job.flakyOutput,
						ask_for_input: job.askForInput,
						react: job.react,
						plan: job.plan,
					}),
				)
				return toStarted(job, baseUrl)
			})

			const describe = Effect.fn('fake_remote_agent.describe')(function* () {
				const now = yield* Clock.currentTimeMillis
				return Option.match(yield* readJob, {
					onNone: () => 'No job is running here. It finished, stopped, or never started.',
					onSome: (job) =>
						`Fake remote agent job for ${job.deliveryId}: step ${job.step}, about ${secondsLeft(job, now)}s to go.`,
				})
			})

			/** Save the job at `step`, and wake at `dueAt`, or sooner to read the delivery's status again. */
			const waitUntil = (job: RemoteAgentJob, step: RemoteAgentJob['step'], dueAt: number, now: number) =>
				Effect.gen(function* () {
					yield* storage.put(JOB_KEY, yield* encodeJob(RemoteAgentJob.make({ ...job, step })))
					yield* storage.setAlarm(Math.max(now + 1, Math.min(dueAt, now + STATUS_POLL_MS)))
					return false
				})

			/**
			 * Take the job's next step. Every wake reads the delivery's status first, and stops the job
			 * when a stop was asked for; otherwise it shows its first activity, changes the text halfway,
			 * or finishes, whichever is due. A refused request ends the job.
			 */
			const runJob = Effect.fn('fake_remote_agent.run_job')(function* (job: RemoteAgentJob) {
				const ended = yield* Effect.gen(function* () {
					const status = yield* deliveryApi.status({ deliveryId: job.deliveryId, accessToken: job.accessToken })
					if (status.interruptRequested) {
						yield* stopDelivery(job)
						return true
					}
					const now = yield* Clock.currentTimeMillis
					if (job.step === 'NotStarted') {
						yield* showActivity(job, status, `Looking into it, about ${secondsLeft(job, now)}s to go`)
						yield* setReactionTwice(job, status, {
							target: DeliveryReactionTarget.cases.ActivationTarget.make({}),
							reaction: ACTIVATION_REACTION,
							active: true,
						})
						yield* putPlanTwice(job, 'Started')
						return yield* waitUntil(job, 'Working', halfwayAt(job), now)
					}
					if (job.step === 'Working') {
						if (now < halfwayAt(job)) return yield* waitUntil(job, 'Working', halfwayAt(job), now)
						yield* showActivity(job, status, `Halfway there, about ${secondsLeft(job, now)}s to go`)
						yield* setReactionTwice(job, status, {
							target: DeliveryReactionTarget.cases.ActivationTarget.make({}),
							reaction: ACTIVATION_REACTION,
							active: false,
						})
						yield* putPlanTwice(job, 'Halfway')
						return yield* waitUntil(job, 'Halfway', job.finishAt, now)
					}
					if (now < job.finishAt) return yield* waitUntil(job, 'Halfway', job.finishAt, now)
					yield* completeDelivery(job, status)
					return true
				}).pipe(
					handleDeliveryApiFailure(job),
					Effect.catchTag('RemoteAgentRefused', () => Effect.succeed(true)),
					Effect.provideService(DeliveryApi, deliveryApi),
				)
				if (ended) yield* storage.delete(JOB_KEY)
			})

			const alarm = Effect.fn('fake_remote_agent.alarm')(function* () {
				yield* Option.match(yield* readJob, {
					onNone: () => Effect.logWarning('Fake remote agent alarm fired without a job'),
					onSome: runJob,
				})
			})

			return { start, describe, alarm }
		})
	}),
)
