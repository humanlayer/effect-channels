import { assert, it } from '@effect/vitest'
import { finalMessageOperationId, FinalMessageOperation, PendingDeliveryOperation } from '@humanlayer/channels-delivery'
import { Effect, Layer, Queue, Redacted, Schema } from 'effect'

import { SlackTransportError } from '../src/Errors'
import { ThreadId } from '../src/Model'
import { SlackMessageTs } from '../src/Schema'
import { Slack } from '../src/Slack'
import { SlackClient } from '../src/SlackClient'
import { SlackConnectionStore } from '../src/SlackConnectionStore'
import { deliverSlackFinalMessage } from '../src/SlackDeliveryOutput'
import { SlackDeliveryResource } from '../src/SlackIngressBindings'
import { makeStubSlackClient, testBotToken, testChannelId, testConnectionStoreLayer, testRootThreadId } from './support'

it.effect('decodes a saved Slack destination and posts final Markdown through the native service', () =>
	Effect.gen(function* () {
		const posts = yield* Queue.unbounded<{ readonly text: string; readonly threadTs?: string }>()
		const client = makeStubSlackClient({
			postMessage: (input) => {
				const post =
					input.threadTs === undefined ? { text: input.text } : { text: input.text, threadTs: input.threadTs }
				return Queue.offer(posts, post).pipe(
					Effect.as({ channelId: testChannelId, ts: SlackMessageTs.make('100.9') }),
				)
			},
		})
		const provider = Slack.layer.pipe(
			Layer.provide(testConnectionStoreLayer),
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		const deliveryId = 'delivery:v2:slack-final'
		const destination = yield* Schema.encodeEffect(Schema.fromJsonString(SlackDeliveryResource))({
			threadId: ThreadId.make(testRootThreadId),
		})
		const receipt = yield* deliverSlackFinalMessage(
			FinalMessageOperation.make({
				operationId: finalMessageOperationId(deliveryId),
				deliveryId,
				outcome: 'completed',
				markdown: 'Final **Slack** answer',
				provider: 'slack',
				installation: 'T_TEST',
				destination,
				presentation: 'slack.message',
				presentationVersion: '1',
				state: PendingDeliveryOperation.make({ attempt: 0, readyAt: 0, hadAmbiguousAttempt: false }),
			}),
		).pipe(Effect.provide(provider))
		assert.deepStrictEqual(yield* Queue.take(posts), { text: 'Final **Slack** answer', threadTs: '100.1' })
		assert.match(receipt.providerReceipt, /100\.9/)
	}),
)

it.effect('preserves Slack retry-after metadata in the delivery output error', () =>
	Effect.gen(function* () {
		const client = makeStubSlackClient({
			postMessage: () =>
				Effect.fail(
					SlackTransportError.make({ operation: 'chat.postMessage', status: 429, retryAfterMs: 7000 }),
				),
		})
		const provider = Slack.layer.pipe(
			Layer.provide(testConnectionStoreLayer),
			Layer.provide(Layer.succeed(SlackClient, client)),
		)
		const deliveryId = 'delivery:v2:slack-rate-limit'
		const destination = yield* Schema.encodeEffect(Schema.fromJsonString(SlackDeliveryResource))({
			threadId: ThreadId.make(testRootThreadId),
		})
		const error = yield* Effect.flip(
			deliverSlackFinalMessage(
				FinalMessageOperation.make({
					operationId: finalMessageOperationId(deliveryId),
					deliveryId,
					outcome: 'completed',
					markdown: 'Retry later.',
					provider: 'slack',
					installation: 'T_TEST',
					destination,
					presentation: 'slack.message',
					presentationVersion: '1',
					state: PendingDeliveryOperation.make({ attempt: 0, readyAt: 0, hadAmbiguousAttempt: false }),
				}),
			).pipe(Effect.provide(provider)),
		)
		assert.strictEqual(error.retryable, true)
		assert.strictEqual(error.retryAfterMs, 7000)
	}),
)

it.effect('rejects a saved destination from another Slack installation before credential lookup or posting', () =>
	Effect.gen(function* () {
		const credentialReads = yield* Queue.unbounded<void>()
		const posts = yield* Queue.unbounded<void>()
		const client = makeStubSlackClient({
			postMessage: () => Queue.offer(posts, undefined).pipe(Effect.andThen(Effect.die('unexpected Slack post'))),
		})
		const connections = Layer.succeed(
			SlackConnectionStore,
			SlackConnectionStore.of({
				get: () =>
					Queue.offer(credentialReads, undefined).pipe(
						Effect.as({
							credentials: {
								botToken: Redacted.make(testBotToken),
								botUserId: 'U_BOT',
								botId: 'B_OURS',
							},
						}),
					),
				upsert: () => Effect.die('unexpected connection upsert'),
				remove: () => Effect.die('unexpected connection removal'),
			}),
		)
		const provider = Slack.layer.pipe(Layer.provide(connections), Layer.provide(Layer.succeed(SlackClient, client)))
		const deliveryId = 'delivery:v2:slack-installation-mismatch'
		const destination = yield* Schema.encodeEffect(Schema.fromJsonString(SlackDeliveryResource))({
			threadId: ThreadId.make(testRootThreadId),
		})
		const error = yield* Effect.flip(
			deliverSlackFinalMessage(
				FinalMessageOperation.make({
					operationId: finalMessageOperationId(deliveryId),
					deliveryId,
					outcome: 'completed',
					markdown: 'Never post this.',
					provider: 'slack',
					installation: 'T_OTHER',
					destination,
					presentation: 'slack.message',
					presentationVersion: '1',
					state: PendingDeliveryOperation.make({ attempt: 0, readyAt: 0, hadAmbiguousAttempt: false }),
				}),
			).pipe(Effect.provide(provider)),
		)
		assert.strictEqual(error.retryable, false)
		assert.strictEqual(error.safeCode, 'installation_mismatch')
		assert.strictEqual(yield* Queue.size(credentialReads), 0)
		assert.strictEqual(yield* Queue.size(posts), 0)
	}),
)
