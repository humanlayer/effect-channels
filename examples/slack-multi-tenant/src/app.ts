import { NodeCrypto } from '@effect/platform-node'
import { DeliveryPolicy } from '@humanlayer/channels-delivery'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import {
	SlackClient,
	SlackIngress,
	SlackRoutes,
	SlackSubscriptions,
	SlackTenantCredentials,
	SlackUserDirectory,
	Slack,
	Emoji,
	MarkdownContent,
	MarkdownTextChunk,
	type MessageEvent,
	type MessageUpdatedEvent,
	type MessageDeletedEvent,
	type ReactionEvent,
	type ConversationStoppedEvent,
} from '@humanlayer/channels-slack'
import { Effect, Layer, Stream } from 'effect'
import { FetchHttpClient, HttpRouter } from 'effect/unstable/http'

import { loadSlackConnection } from './store.ts'

const reactionRequest = /\breact(?:ion)?\b/i
const streamRequest = /\bstream\b/i

const handlers = {
	onNewMention: ({ thread, message }: MessageEvent) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a mention from ${message.author.fullName}`).pipe(
				Effect.annotateLogs({ tenant: thread.ref.channel.tenant }),
			)
			yield* thread.subscribe()
			if (streamRequest.test(message.text)) {
				yield* thread.stream(
					Stream.make(
						MarkdownTextChunk.make({ text: 'Workspace-aware streaming echo: ' }),
						MarkdownTextChunk.make({ text: message.text }),
					).pipe(Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk)))),
				)
			} else {
				yield* thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
			}
			if (reactionRequest.test(message.text)) {
				const slack = yield* Slack
				yield* slack.addReaction({
					threadId: thread.ref.id,
					messageRef: message.ref,
					emoji: Emoji.Check,
				})
			}
		}),
	onSubscribedMessage: ({ thread, message }: MessageEvent) =>
		(streamRequest.test(message.text)
			? thread.stream(
					Stream.make(MarkdownTextChunk.make({ text: `Workspace-aware stream: ${message.text}` })).pipe(
						Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk))),
					),
				)
			: thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
		).pipe(Effect.asVoid),
	onMessageUpdated: (event: MessageUpdatedEvent) =>
		Effect.logInfo(`Workspace message ${event.message.ref} was edited`).pipe(
			Effect.annotateLogs({ tenant: event.tenant }),
		),
	onMessageDeleted: (event: MessageDeletedEvent) =>
		Effect.logInfo(`Workspace message ${event.messageRef} was deleted`).pipe(
			Effect.annotateLogs({ tenant: event.tenant }),
		),
	onAnyReaction: (event: ReactionEvent) =>
		Effect.logInfo(`Workspace reaction ${event.rawEmoji}`).pipe(Effect.annotateLogs({ tenant: event.tenant })),
	onConversationStopped: (event: ConversationStoppedEvent) =>
		Effect.logInfo(`Workspace stream stopped in ${event.threadRef.id}`).pipe(
			Effect.annotateLogs({ tenant: event.tenant }),
		),
}

const policy = DeliveryPolicy.make({
	mode: 'queue',
	maxPayloadBytes: 256_000,
	maxEnvelopes: 1_000,
	maxOutcomes: 10_000,
	retentionMs: 86_400_000,
	maxAttempts: 5,
	retryBaseMs: 100,
	retryMaxMs: 30_000,
	leaseMs: 30_000,
	heartbeatMs: 5_000,
	conflictRetries: 10,
})

export const transport = SlackClient.layer.pipe(
	Layer.provideMerge(SlackTenantCredentials.layerWithLookup({ loadConnection: loadSlackConnection })),
	Layer.provide(FetchHttpClient.layer),
)
const native = Slack.layer
const subscriptions = SlackSubscriptions.layerMemory()
const directory = SlackUserDirectory.layer.pipe(Layer.provide(native))
export const services = SlackIngress.layer({
	namespace: 'slack-multi-tenant',
	policy,
	handlers: {
		onNewMention: [{ id: 'mention', handler: handlers.onNewMention }],
		onSubscribedMessage: [{ id: 'subscribed', handler: handlers.onSubscribedMessage }],
		onDirectMessage: [{ id: 'dm', handler: handlers.onSubscribedMessage }],
		onMessageUpdated: [{ id: 'edited', handler: handlers.onMessageUpdated }],
		onMessageDeleted: [{ id: 'deleted', handler: handlers.onMessageDeleted }],
		onConversationStopped: [{ id: 'stopped', handler: handlers.onConversationStopped }],
		onReaction: [{ id: 'reaction', handler: handlers.onAnyReaction }],
	},
}).pipe(Layer.provideMerge(Layer.mergeAll(native, subscriptions, directory, deliveryMemory({ maxMailboxes: 10_000 }))))
export const run = Effect.flatMap(SlackIngress, (ingress) =>
	ingress.run({ scanLimit: 100, concurrency: 8, pollMs: 25 }),
)
export const worker = Layer.effectDiscard(run.pipe(Effect.forkScoped)).pipe(Layer.provide(services))
export const routes = SlackRoutes.layer.pipe(
	HttpRouter.provideRequest(Layer.merge(NodeCrypto.layer, services)),
	Layer.provide(services),
)
export const application = Layer.merge(routes, worker)
