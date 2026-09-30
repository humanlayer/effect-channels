/**
 * `FakeRemoteAgent`: a Durable Object that stands in for a remote agent host, such as a VM or a sandbox.
 *
 * It does no agent work. One object per delivery ID saves the job, waits for its alarm, then reports
 * back over the public delivery API, as a real remote agent would: it completes the delivery with a
 * final message, which the bot then posts to Slack. While it waits it shows what it is doing as the
 * delivery's activity (Slack's thread status line), changing the text halfway through, and just before
 * it finishes it posts one lasting summary message. It is a Durable Object only because the example
 * already deploys to Cloudflare: its alarm outlives the callback, the mailbox claim, and a redeploy.
 */
import {
	DeliveryActivity,
	DeliveryId,
	MessageId,
	makeDeliveryClient,
	type DeliveryClient,
} from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Context, Effect, Option, Schema } from 'effect'
import { HttpClient } from 'effect/unstable/http'

import { withFlakyOutputMarker } from './FlakySlackApi'

const JOB_KEY = 'fake-remote-agent-job'

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
 */
export const StartRemoteAgentJob = Schema.Struct({
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.NonEmptyString),
	delaySeconds: Schema.Int.check(Schema.isGreaterThan(0)),
	flakyOutput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
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
})
type RemoteAgentJob = typeof RemoteAgentJob.Type

/** What `start` answers: the saved job, without its token. */
export const RemoteAgentJobStarted = Schema.Struct({
	deliveryId: DeliveryId,
	finishAt: Schema.Int,
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

const toStarted = (job: RemoteAgentJob) =>
	RemoteAgentJobStarted.make({ deliveryId: job.deliveryId, finishAt: job.finishAt })

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
				SchemaError: ({ _tag }) => refused(_tag),
				DeliveryControlUnavailable: ({ _tag }) => retry(_tag),
				HttpClientError: ({ _tag }) => retry(_tag),
			}),
		)
	}

const secondsLeft = (job: RemoteAgentJob, now: number) => Math.max(1, Math.round((job.finishAt - now) / 1_000))

/**
 * Show what the job is doing as the delivery's activity: `Working` at first, with new text halfway
 * through the wait. Answers the step the job is at afterwards and when its alarm should next fire.
 */
const showActivity = Effect.fn('fake_remote_agent.show_activity')(function* (job: RemoteAgentJob) {
	const deliveryApi = yield* DeliveryApi
	const target = { deliveryId: job.deliveryId, accessToken: job.accessToken }
	const now = yield* Clock.currentTimeMillis
	const starting = job.step === 'NotStarted'
	const message = starting
		? `Looking into it, about ${secondsLeft(job, now)}s to go`
		: `Halfway there, about ${secondsLeft(job, now)}s to go`
	const receipt = yield* deliveryApi.activity.set({
		...target,
		activity: DeliveryActivity.cases.Working.make({ message }),
	})
	yield* Effect.logInfo('Fake remote agent set its activity').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, step: job.step, receipt_status: receipt.status }),
	)
	if (!starting) return { step: 'Halfway' as const, nextAlarmAt: job.finishAt }
	const halfway = job.finishAt - (job.delaySeconds * 1_000) / 2
	return { step: 'Working' as const, nextAlarmAt: Math.min(Math.max(halfway, now + 1_000), job.finishAt) }
})

/**
 * Report the job's delivery as complete: post a lasting summary message, read the delivery's status,
 * complete it with a final message, read its status again, and log only safe fields. The second read
 * shows the delivery still finishing: the request returned before Slack was called. The result clears
 * the activity.
 */
const completeDelivery = Effect.fn('fake_remote_agent.complete_delivery')(function* (job: RemoteAgentJob) {
	const deliveryApi = yield* DeliveryApi
	const target = { deliveryId: job.deliveryId, accessToken: job.accessToken }
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
	const status = yield* deliveryApi.status(target)
	yield* Effect.logInfo('Fake remote agent read delivery status').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, stage: status.stage }),
	)
	const finished = `Fake remote agent finished after ${job.delaySeconds}s.`
	const markdown = job.flakyOutput
		? withFlakyOutputMarker(finished, (yield* Clock.currentTimeMillis) + FLAKY_OUTPUT_MS)
		: finished
	const receipt = yield* deliveryApi.complete({ ...target, payload: { markdown } })
	yield* Effect.logInfo('Fake remote agent completed delivery').pipe(
		Effect.annotateLogs({ delivery_id: job.deliveryId, receipt_status: receipt.status }),
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
		readonly alarm: () => Effect.Effect<void, never, RuntimeContext>
	}
>()('FakeRemoteAgent') {}

/**
 * The fake remote agent's implementation, in Alchemy's two phases. Its layer requires an `HttpClient`;
 * the host Worker provides it. The delivery API lives at the host Worker's own URL, which Alchemy binds
 * at deploy and which can only be read per instance. `start` saves the job once and sets the alarm; the
 * alarm completes the delivery and deletes the job.
 */
export const FakeRemoteAgentLive = FakeRemoteAgent.make<HttpClient.HttpClient>(
	Effect.gen(function* () {
		const state = yield* Cloudflare.DurableObjectState
		const workerUrl = yield* Cloudflare.Worker.URL
		const httpClient = yield* HttpClient.HttpClient

		return Effect.gen(function* () {
			const deliveryApi = yield* makeDeliveryClient({ baseUrl: yield* workerUrl }).pipe(
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
				if (Option.isSome(existing)) return toStarted(existing.value)
				const now = yield* Clock.currentTimeMillis
				const job = RemoteAgentJob.make({
					deliveryId: request.deliveryId,
					accessToken: request.accessToken,
					finishAt: now + request.delaySeconds * 1_000,
					delaySeconds: request.delaySeconds,
					step: 'NotStarted',
					flakyOutput: request.flakyOutput,
				})
				yield* storage.put(JOB_KEY, yield* encodeJob(job))
				yield* storage.setAlarm(Math.min(now + FIRST_ACTIVITY_DELAY_MS, job.finishAt))
				yield* Effect.logInfo('Fake remote agent job started').pipe(
					Effect.annotateLogs({
						delivery_id: job.deliveryId,
						finish_at: job.finishAt,
						flaky_output: job.flakyOutput,
					}),
				)
				return toStarted(job)
			})

			/** Take the job's next step: show its activity, or finish. A refused request ends the job. */
			const runJob = Effect.fn('fake_remote_agent.run_job')(function* (job: RemoteAgentJob) {
				const ended = yield* Effect.gen(function* () {
					if (job.step !== 'Halfway') {
						const { step, nextAlarmAt } = yield* showActivity(job)
						yield* storage.put(JOB_KEY, yield* encodeJob(RemoteAgentJob.make({ ...job, step })))
						yield* storage.setAlarm(nextAlarmAt)
						return false
					}
					yield* completeDelivery(job)
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

			return { start, alarm }
		})
	}),
)
