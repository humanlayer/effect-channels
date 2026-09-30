/**
 * `FakeRemoteAgent`: a Durable Object that stands in for a remote agent host, such as a VM or a sandbox.
 *
 * It does no agent work. One object per delivery ID saves the job, waits for its alarm, then reports
 * back over the public delivery API, as a real remote agent would: it completes the delivery with a
 * final message, which the bot then posts to Slack. It is a Durable Object only because the example
 * already deploys to Cloudflare: its alarm outlives the callback, the mailbox claim, and a redeploy.
 */
import { DeliveryId, makeDeliveryClient, type DeliveryClient } from '@humanlayer/channels-delivery-next'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Context, Effect, Option, Schema } from 'effect'
import { HttpClient } from 'effect/unstable/http'

import { withFlakyOutputMarker } from './FlakySlackApi'

const JOB_KEY = 'fake-remote-agent-job'

/** How long Slack refuses a `flaky` job's final message, counted from when the job completes. */
const FLAKY_OUTPUT_MS = 20_000

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
 * The job one object saves. Stored in its encoded form, so the token is kept as its plain value.
 * Jobs saved before `delaySeconds` and `flakyOutput` existed read them as 0 and false.
 */
const RemoteAgentJob = Schema.Struct({
	deliveryId: DeliveryId,
	accessToken: Schema.RedactedFromValue(Schema.NonEmptyString),
	finishAt: Schema.Int,
	delaySeconds: Schema.Int.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
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

/**
 * Report the job's delivery as complete: read its status, complete it with a final message, read its
 * status again, and log only safe fields. The second read shows the delivery still finishing: the
 * request returned before Slack was called. A refused request ends the job; an unreachable API dies
 * so Cloudflare retries the alarm.
 */
const completeDelivery = Effect.fn('fake_remote_agent.complete_delivery')(
	function* (job: RemoteAgentJob) {
		const deliveryApi = yield* DeliveryApi
		const target = { deliveryId: job.deliveryId, accessToken: job.accessToken }
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
	},
	(effect, job) => {
		const refused = (reason: string) =>
			Effect.logWarning('Fake remote agent refused; dropping the job').pipe(
				Effect.annotateLogs({ delivery_id: job.deliveryId, reason }),
			)
		const retry = (reason: string) =>
			Effect.logWarning('Fake remote agent could not reach the delivery API; the alarm will retry').pipe(
				Effect.annotateLogs({ delivery_id: job.deliveryId, reason }),
				Effect.andThen(Effect.die(new RemoteAgentRetryable({ deliveryId: job.deliveryId, reason }))),
			)
		return effect.pipe(
			Effect.catchTags({
				DeliveryCredentialMissing: () => refused('DeliveryCredentialMissing'),
				DeliveryNotFound: () => refused('DeliveryNotFound'),
				DeliveryTerminalConflict: () => refused('DeliveryTerminalConflict'),
				DeliveryClosed: () => refused('DeliveryClosed'),
				SchemaError: () => refused('SchemaError'),
				DeliveryControlUnavailable: () => retry('DeliveryControlUnavailable'),
				HttpClientError: () => retry('HttpClientError'),
			}),
		)
	},
)

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
				const finishAt = (yield* Clock.currentTimeMillis) + request.delaySeconds * 1_000
				const job = RemoteAgentJob.make({
					deliveryId: request.deliveryId,
					accessToken: request.accessToken,
					finishAt,
					delaySeconds: request.delaySeconds,
					flakyOutput: request.flakyOutput,
				})
				yield* storage.put(JOB_KEY, yield* encodeJob(job))
				yield* storage.setAlarm(job.finishAt)
				yield* Effect.logInfo('Fake remote agent job started').pipe(
					Effect.annotateLogs({
						delivery_id: job.deliveryId,
						finish_at: job.finishAt,
						flaky_output: job.flakyOutput,
					}),
				)
				return toStarted(job)
			})

			const finishJob = Effect.fn('fake_remote_agent.finish_job')(function* (job: RemoteAgentJob) {
				yield* completeDelivery(job).pipe(Effect.provideService(DeliveryApi, deliveryApi))
				yield* storage.delete(JOB_KEY)
			})

			const alarm = Effect.fn('fake_remote_agent.alarm')(function* () {
				yield* Option.match(yield* readJob, {
					onNone: () => Effect.logWarning('Fake remote agent alarm fired without a job'),
					onSome: finishJob,
				})
			})

			return { start, alarm }
		})
	}),
)
