import { assert, it } from '@effect/vitest'
import { Effect, Layer, Queue, Schema } from 'effect'

import { SlackApiError } from '../src/Errors'
import { EphemeralFallbackToDm, EphemeralNoFallback, MarkdownContent, ThreadId, UserId } from '../src/index'
import { SlackChannelId, SlackMessageTs, SlackPostEphemeralInput, SlackSentMessage } from '../src/Schema'
import { Slack } from '../src/Slack'
import { SlackClient } from '../src/SlackClient'
import { expectTaggedFailure, testAuthor, testThreadRef } from './nativeSupport'
import { testConnectionStoreLayer } from './support'
import { makeSlackClientHarness, makeStubSlackClient, slackJsonResponse, testChannelId, testTeamId } from './support'

const EphemeralBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	user: Schema.String,
	text: Schema.String,
})

it.effect('encodes chat.postEphemeral and returns the ephemeral message reference', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(() => slackJsonResponse('{"ok":true,"message_ts":"100.2"}'))
		const sent = yield* Effect.flatMap(SlackClient, (client) =>
			client.postEphemeral(
				SlackPostEphemeralInput.make({
					teamId: testTeamId,
					channelId: testChannelId,
					threadTs: SlackMessageTs.make('100.1'),
					userId: UserId.make('U_TEST'),
					text: 'private',
				}),
			),
		).pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		const body = yield* Schema.decodeEffect(Schema.fromJsonString(EphemeralBody))(request.body)
		assert.strictEqual(request.url.pathname, '/api/chat.postEphemeral')
		assert.deepStrictEqual(body, {
			channel: 'C_TEST',
			thread_ts: '100.1',
			user: 'U_TEST',
			text: 'private',
		})
		assert.strictEqual(sent.channelId, 'C_TEST')
		assert.strictEqual(sent.ts, '100.2')
	}),
)

it.effect('finalizes native typing after an ephemeral post and skips typing for proactive DM conversations', () =>
	Effect.gen(function* () {
		const statuses = yield* Queue.unbounded<string>()
		const layer = Slack.layer.pipe(
			Layer.provide(testConnectionStoreLayer),
			Layer.provide(
				Layer.succeed(
					SlackClient,
					makeStubSlackClient({
						setSessionStatus: (input) => Queue.offer(statuses, input.status).pipe(Effect.asVoid),
						postEphemeral: () =>
							Effect.succeed(
								SlackSentMessage.make({ channelId: testChannelId, ts: SlackMessageTs.make('100.2') }),
							),
					}),
				),
			),
		)
		const rooted = ThreadId.make('slack:v1:T_TEST:im:C_TEST:100.1')
		const proactive = ThreadId.make('slack:v1:T_TEST:im:C_TEST')
		yield* Effect.gen(function* () {
			const provider = yield* Slack
			yield* provider.startThreadTyping({ threadId: rooted })
			yield* provider.postEphemeral({
				threadId: rooted,
				user: testAuthor,
				content: MarkdownContent.make({ markdown: 'private' }),
				fallback: EphemeralNoFallback.make({}),
			})
			yield* provider.startThreadTyping({ threadId: proactive })
		}).pipe(Effect.provide(layer))
		assert.deepStrictEqual(yield* Queue.takeAll(statuses), ['processing', 'active'])
	}),
)

it.effect('uses a persistent DM only when the explicit fallback policy requests it', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const dm = SlackChannelId.make('D_OPENED')
		const client = makeStubSlackClient({
			postEphemeral: () =>
				Queue.offer(calls, 'ephemeral').pipe(
					Effect.andThen(
						Effect.fail(
							SlackApiError.make({ operation: 'chat.postEphemeral', code: 'channel_type_not_supported' }),
						),
					),
				),
			openDM: (input) => {
				assert.strictEqual(input.userId, testAuthor.userId)
				return Queue.offer(calls, 'openDM').pipe(Effect.as(dm))
			},
			postMessage: (input) => {
				assert.strictEqual(input.channelId, dm)
				assert.strictEqual(input.threadTs, undefined)
				assert.strictEqual(input.text, 'private')
				return Queue.offer(calls, 'post').pipe(
					Effect.as(SlackSentMessage.make({ channelId: dm, ts: SlackMessageTs.make('200.1') })),
				)
			},
		})
		const result = yield* Effect.flatMap(Slack, (slack) =>
			slack.postEphemeral({
				threadId: testThreadRef.id,
				user: testAuthor,
				content: MarkdownContent.make({ markdown: 'private' }),
				fallback: EphemeralFallbackToDm.make({}),
			}),
		).pipe(
			Effect.provide(
				Slack.layer.pipe(
					Layer.provide(testConnectionStoreLayer),
					Layer.provide(Layer.succeed(SlackClient, client)),
				),
			),
		)
		assert.strictEqual(result.usedFallback, true)
		assert.strictEqual(result.sent?.ref.threadId, 'slack:v1:T_TEST:im:D_OPENED')
		assert.deepStrictEqual(yield* Queue.takeAll(calls), ['ephemeral', 'openDM', 'post'])
	}),
)

it.effect('does not open a DM or persist content when no fallback is explicitly selected', () =>
	Effect.gen(function* () {
		const calls = yield* Queue.unbounded<string>()
		const client = makeStubSlackClient({
			postEphemeral: () =>
				Queue.offer(calls, 'ephemeral').pipe(
					Effect.andThen(
						Effect.fail(
							SlackApiError.make({ operation: 'chat.postEphemeral', code: 'channel_type_not_supported' }),
						),
					),
				),
			openDM: () => Queue.offer(calls, 'openDM').pipe(Effect.andThen(Effect.die('unexpected DM fallback'))),
			postMessage: () =>
				Queue.offer(calls, 'post').pipe(Effect.andThen(Effect.die('unexpected persistent post'))),
		})
		const error = yield* expectTaggedFailure('PostFailed')(
			Effect.flatMap(Slack, (slack) =>
				slack.postEphemeral({
					threadId: testThreadRef.id,
					user: testAuthor,
					content: MarkdownContent.make({ markdown: 'private' }),
					fallback: EphemeralNoFallback.make({}),
				}),
			),
		).pipe(
			Effect.provide(
				Slack.layer.pipe(
					Layer.provide(testConnectionStoreLayer),
					Layer.provide(Layer.succeed(SlackClient, client)),
				),
			),
		)
		assert.strictEqual(error.threadId, testThreadRef.id)
		assert.strictEqual(error.retryability, 'non_retryable')
		assert.deepStrictEqual(yield* Queue.takeAll(calls), ['ephemeral'])
	}),
)
