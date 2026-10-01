/**
 * Slack's plan: the diff from the plan Slack shows to the next one, the output processor over a
 * recording `SlackApi` (start, append, replace, a closed stream, and stopping it with the result), and
 * the `chat.startStream` request at the HTTP seam.
 */
import { describe, it } from '@effect/vitest'
import {
	BatchId,
	DeliveryOperationId,
	DeliveryOutcome,
	DeliveryPlan,
	DeliveryPlanItem,
	DeliveryPlanItemId,
	DeliveryPlanItemState,
	PreparedDeliveryInvocation,
	ProviderOutputAttempt,
	ProviderPresentOutcome,
	ProviderRenderPlan,
	RenderedDeliveryPlan,
	makeDeliveryId,
	type ProviderOutputOperation,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Option, Queue, Ref, Result, Schema } from 'effect'

import {
	PlanUpdateChunk,
	SlackApi,
	SlackApiError,
	SlackChannelId,
	SlackDeliveryDestination,
	SlackDeliveryDestinationJson,
	SlackMessage,
	SlackMessageRef,
	SlackMessageTs,
	SlackParticipant,
	SlackPlainTextContent,
	SlackPlanChange,
	SlackPlanPresentation,
	SlackPlanPresentationJson,
	SlackTeamId,
	SlackThreadRef,
	SlackUserId,
	TaskUpdateChunk,
	makeSlackOutputProcessor,
	slackPlanChange,
	slackPresentationVersion,
	slackThreadSupportedOperations,
	type SlackAppendStreamRequest,
	type SlackStartPlanStreamRequest,
} from '../src'
import { makeRecordingSlackHttp, slackApiLayer, type RecordedSlackRequest } from './slack-file-fixtures'

const teamId = SlackTeamId.make('T_PLAN')
const channelId = SlackChannelId.make('C_PLAN')
const thread = SlackThreadRef.make({ teamId, channelId, threadTs: SlackMessageTs.make('1700000000.000001'), isDm: true })
const streamRef = (ts: string) => SlackMessageRef.make({ teamId, channelId, messageTs: SlackMessageTs.make(ts) })
const firstStream = streamRef('1700000001.000001')

const item = (id: string, title: string, state: DeliveryPlanItemState) =>
	DeliveryPlanItem.make({ id: DeliveryPlanItemId.make(id), title, state })
const pending = DeliveryPlanItemState.cases.Pending.make({})

/** Artifact 22's example: revision 7, then revision 8. */
const inspect = item('inspect', 'inspect logs', DeliveryPlanItemState.cases.Completed.make({ result: 'Found expired credentials' }))
const rotate = item('rotate', 'rotate secret', DeliveryPlanItemState.cases.InProgress.make({}))
const verify = item('verify', 'verify deployment', pending)
const revision7 = DeliveryPlan.make({ title: 'Ship the fix', items: [inspect, rotate, verify] })
const revision8 = DeliveryPlan.make({
	title: 'Ship the fix',
	items: [
		inspect,
		item('rotate', 'rotate secret', DeliveryPlanItemState.cases.Completed.make({})),
		verify,
		item('notify', 'notify owner', pending),
	],
})

const presentation = (message: SlackMessageRef) =>
	Schema.encodeSync(SlackPlanPresentationJson)(SlackPlanPresentation.make({ message }))
const shown = (plan: DeliveryPlan, message = firstStream) =>
	RenderedDeliveryPlan.make({ revision: 7, plan, presentation: presentation(message) })

const attempt = (operation: ProviderOutputOperation) =>
	ProviderOutputAttempt.make({
		deliveryId: makeDeliveryId({ mailboxKey: 'slack:v1:mailbox', batchId: BatchId.make('batch-1') }),
		operationId: DeliveryOperationId.make('plan-1'),
		attempt: 1,
		hadAmbiguousAttempt: false,
		idempotencyKey: '00000000-0000-4000-8000-000000000001',
		prepared: PreparedDeliveryInvocation.make({
			callback: 'onNewMention',
			presentationVersion: slackPresentationVersion,
			destination: Schema.encodeSync(SlackDeliveryDestinationJson)(SlackDeliveryDestination.make({ thread })),
			supportedOperations: slackThreadSupportedOperations,
		}),
		operation,
	})

type StreamCalls = {
	readonly starts: ReadonlyArray<SlackStartPlanStreamRequest>
	readonly appends: ReadonlyArray<SlackAppendStreamRequest>
	readonly stops: ReadonlyArray<SlackMessageRef>
	readonly deletes: ReadonlyArray<SlackMessageRef>
	readonly posts: number
}

/** Slack's answer for each call; absent means it succeeds. */
type StreamFaults = {
	readonly start?: string
	readonly append?: string
	readonly stop?: string
}

/** A `SlackApi` that records stream calls. The first start makes stream message `1700000002.000001`, the next `…003`. */
const recordingSlackApi = (calls: Ref.Ref<StreamCalls>, faults: StreamFaults) =>
	Layer.mock(SlackApi, {
		startPlanStream: (request) =>
			Effect.gen(function* () {
				const { starts } = yield* Ref.updateAndGet(calls, (all) => ({ ...all, starts: [...all.starts, request] }))
				if (faults.start !== undefined) {
					return yield* SlackApiError.make({ operation: 'start_plan_stream', message: faults.start })
				}
				return streamRef(`170000000${starts.length + 1}.000001`)
			}),
		appendStream: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, appends: [...all.appends, request] }))
				if (faults.append !== undefined) {
					return yield* SlackApiError.make({ operation: 'append_stream', message: faults.append })
				}
			}),
		stopStream: ({ message }) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, stops: [...all.stops, message] }))
				if (faults.stop !== undefined) return yield* SlackApiError.make({ operation: 'stop_stream', message: faults.stop })
			}),
		deleteMessage: ({ message }) => Ref.update(calls, (all) => ({ ...all, deletes: [...all.deletes, message] })),
		postToThread: () =>
			Ref.update(calls, (all) => ({ ...all, posts: all.posts + 1 })).pipe(
				Effect.as({
					ref: streamRef('1700000099.000001'),
					message: SlackMessage.make({
						ref: streamRef('1700000099.000001'),
						thread,
						author: SlackParticipant.make({
							userId: SlackUserId.make('U_BOT'),
							userName: 'bot',
							fullName: 'Bot',
							isBot: true,
							isMe: true,
						}),
						content: SlackPlainTextContent.make({ text: 'posted' }),
						files: [],
						metadata: {},
					}),
				}),
			),
	})

const run = (operation: ProviderOutputOperation, faults: StreamFaults = {}) =>
	Effect.gen(function* () {
		const calls = yield* Ref.make<StreamCalls>({ starts: [], appends: [], stops: [], deletes: [], posts: 0 })
		const processor = yield* makeSlackOutputProcessor({ namespace: 'slack-plan-test' }).pipe(
			Effect.provide(recordingSlackApi(calls, faults)),
		)
		const result = yield* processor.process(attempt(operation)).pipe(Effect.result)
		const receipt = Result.isSuccess(result)
			? Option.getOrUndefined(Schema.decodeUnknownOption(SlackPlanPresentationJson)(result.success.receipt))?.message
			: undefined
		return { result, receipt, ...(yield* Ref.get(calls)) }
	})

const task = (id: string, title: string, status: TaskUpdateChunk['status'], fields: { details?: string; output?: string } = {}) =>
	TaskUpdateChunk.make({ id, title, status, ...fields })

describe('Slack plan diff', () => {
	it('appends exactly the changed and new tasks', ({ expect }) => {
		expect(slackPlanChange(revision7, revision8)).toEqual(
			SlackPlanChange.Append({
				chunks: [task('rotate', 'rotate secret', 'complete'), task('notify', 'notify owner', 'pending')],
			}),
		)
		expect(slackPlanChange(revision8, revision8)).toEqual(SlackPlanChange.Append({ chunks: [] }))
	})

	it('sends a new title, and the default title when the plan loses its own', ({ expect }) => {
		const { title: _, ...untitled } = revision7
		expect(slackPlanChange(revision7, DeliveryPlan.make({ ...revision7, title: 'Ship it now' }))).toEqual(
			SlackPlanChange.Append({ chunks: [PlanUpdateChunk.make({ title: 'Ship it now' })] }),
		)
		expect(slackPlanChange(revision7, DeliveryPlan.make(untitled))).toEqual(
			SlackPlanChange.Append({ chunks: [PlanUpdateChunk.make({ title: 'Plan' })] }),
		)
	})

	it('replaces the stream for a removal, a reorder, an insertion before a shown task, or a cleared field', ({
		expect,
	}) => {
		const withItems = (...items: ReadonlyArray<DeliveryPlanItem>) => DeliveryPlan.make({ ...revision7, items })
		const replace = SlackPlanChange.Replace()
		expect(slackPlanChange(revision7, withItems(inspect, rotate))).toEqual(replace)
		expect(slackPlanChange(revision7, withItems(rotate, inspect, verify))).toEqual(replace)
		expect(slackPlanChange(revision7, withItems(item('first', 'first', pending), inspect, rotate, verify))).toEqual(
			replace,
		)
		const inspectAgain = item('inspect', 'inspect logs', DeliveryPlanItemState.cases.InProgress.make({}))
		expect(slackPlanChange(revision7, withItems(inspectAgain, rotate, verify))).toEqual(replace)
	})
})

describe('Slack plan output', () => {
	it.effect('starts a plan stream with the title and every task, and keeps the stream as the presentation', ({ expect }) =>
		Effect.gen(function* () {
			const { starts, receipt } = yield* run(ProviderRenderPlan.make({ revision: 1, plan: revision7 }))
			expect(starts).toEqual([
				{
					thread,
					chunks: [
						PlanUpdateChunk.make({ title: 'Ship the fix' }),
						task('inspect', 'inspect logs', 'complete', { output: 'Found expired credentials' }),
						task('rotate', 'rotate secret', 'in_progress'),
						task('verify', 'verify deployment', 'pending'),
					],
				},
			])
			expect(receipt).toEqual(streamRef('1700000002.000001'))
		}),
	)

	it.effect('appends only what changed to the shown stream, and sends nothing when nothing did', ({ expect }) =>
		Effect.gen(function* () {
			const changed = yield* run(ProviderRenderPlan.make({ revision: 8, plan: revision8, rendered: shown(revision7) }))
			expect(changed.starts).toEqual([])
			expect(changed.appends).toEqual([
				{
					message: firstStream,
					chunks: [task('rotate', 'rotate secret', 'complete'), task('notify', 'notify owner', 'pending')],
				},
			])
			expect(changed.receipt).toEqual(firstStream)

			const same = yield* run(ProviderRenderPlan.make({ revision: 8, plan: revision7, rendered: shown(revision7) }))
			expect(same.appends).toEqual([])
			expect(same.starts).toEqual([])
			expect(same.receipt).toEqual(firstStream)
		}),
	)

	it.effect('replaces a stream it cannot change, then stops and deletes the old one', ({ expect }) =>
		Effect.gen(function* () {
			const reordered = DeliveryPlan.make({ ...revision7, items: revision7.items.toReversed() })
			const { starts, appends, stops, deletes, receipt } = yield* run(
				ProviderRenderPlan.make({ revision: 8, plan: reordered, rendered: shown(revision7) }),
			)
			expect(appends).toEqual([])
			expect(starts.map(({ chunks }) => chunks.length)).toEqual([4])
			expect(stops).toEqual([firstStream])
			expect(deletes).toEqual([firstStream])
			expect(receipt).toEqual(streamRef('1700000002.000001'))
		}),
	)

	it.effect('starts a new stream when Slack has closed the shown one', ({ expect }) =>
		Effect.gen(function* () {
			const { appends, starts, deletes, receipt } = yield* run(
				ProviderRenderPlan.make({ revision: 8, plan: revision8, rendered: shown(revision7, streamRef('1700000000.500000')) }),
				{ append: 'message_not_in_streaming_state', stop: 'message_not_in_streaming_state' },
			)
			expect(appends).toHaveLength(1)
			expect(starts.map(({ chunks }) => chunks.length)).toEqual([5])
			expect(deletes).toEqual([streamRef('1700000000.500000')])
			expect(receipt).toEqual(streamRef('1700000002.000001'))
		}),
	)

	it.effect('keeps the new stream when the old one cannot be removed', ({ expect }) =>
		Effect.gen(function* () {
			const reordered = DeliveryPlan.make({ ...revision7, items: revision7.items.toReversed() })
			const { result, deletes, receipt } = yield* run(
				ProviderRenderPlan.make({ revision: 8, plan: reordered, rendered: shown(revision7) }),
				{ stop: 'invalid_auth' },
			)
			expect(Result.isSuccess(result)).toEqual(true)
			expect(deletes).toEqual([])
			expect(receipt).toEqual(streamRef('1700000002.000001'))
		}),
	)

	it.effect('reports a start Slack may get over as retryable, and one it will not as final', ({ expect }) =>
		Effect.gen(function* () {
			const render = ProviderRenderPlan.make({ revision: 1, plan: revision7 })
			const unreachable = yield* run(render, { start: 'Could not reach Slack' })
			const archived = yield* run(render, { start: 'is_archived' })
			if (!Result.isFailure(unreachable.result) || !Result.isFailure(archived.result)) return expect.unreachable()
			expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_plan_failed' })
			expect(archived.result.failure).toMatchObject({ retryable: false, safeCode: 'slack_plan_failed' })
		}),
	)

	it.effect('stops the plan stream before it posts the result; a stream already closed counts as stopped', ({
		expect,
	}) =>
		Effect.gen(function* () {
			const present = ProviderPresentOutcome.make({
				outcome: DeliveryOutcome.cases.Completed.make({}),
				markdown: 'Shipped.',
				clearActivity: false,
				planPresentation: presentation(firstStream),
			})
			const open = yield* run(present)
			expect(open.stops).toEqual([firstStream])
			expect(open.posts).toEqual(1)

			const closed = yield* run(present, { stop: 'message_not_in_streaming_state' })
			expect(Result.isSuccess(closed.result)).toEqual(true)
			expect(closed.posts).toEqual(1)

			const unreachable = yield* run(present, { stop: 'Could not reach Slack' })
			if (!Result.isFailure(unreachable.result)) return expect.unreachable()
			expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_plan_failed' })
			expect(unreachable.posts).toEqual(0)
		}),
	)
})

describe('Slack plan stream API', () => {
	it.effect('starts a stream in plan mode with pending tasks, appends, and stops it', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<RecordedSlackRequest>()
			const http = makeRecordingSlackHttp(requests, () => Response.json({ ok: true, ts: '1700000005.000001' }))
			const body = (request: RecordedSlackRequest) =>
				Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json))(new TextDecoder().decode(request.body))
			const message = yield* Effect.gen(function* () {
				const slackApi = yield* SlackApi
				const started = yield* slackApi.startPlanStream({
					thread,
					chunks: [PlanUpdateChunk.make({ title: 'Ship it' }), task('verify', 'verify', 'pending')],
				})
				yield* slackApi.appendStream({ message: started, chunks: [task('verify', 'verify', 'complete', { output: 'ok' })] })
				yield* slackApi.stopStream({ message: started })
				return started
			}).pipe(Effect.provide(slackApiLayer(http)))
			expect(message).toEqual(streamRef('1700000005.000001'))
			const [start, append, stop] = yield* Queue.takeAll(requests)
			if (start === undefined || append === undefined || stop === undefined) return expect.unreachable()
			expect(start.url).toEqual('https://slack.com/api/chat.startStream')
			expect(body(start)).toEqual({
				channel: channelId,
				thread_ts: thread.threadTs,
				task_display_mode: 'plan',
				chunks: [
					{ type: 'plan_update', title: 'Ship it' },
					{ type: 'task_update', id: 'verify', title: 'verify', status: 'pending' },
				],
			})
			expect(append.url).toEqual('https://slack.com/api/chat.appendStream')
			expect(body(append)).toEqual({
				channel: channelId,
				ts: '1700000005.000001',
				chunks: [{ type: 'task_update', id: 'verify', title: 'verify', status: 'complete', output: 'ok' }],
			})
			expect(stop.url).toEqual('https://slack.com/api/chat.stopStream')
			expect(body(stop)).toEqual({ channel: channelId, ts: '1700000005.000001', chunks: [] })
		}),
	)
})
