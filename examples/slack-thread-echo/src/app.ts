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
	FileUpload,
	MarkdownContent,
	MarkdownTextChunk,
	PlanUpdateChunk,
	TaskUpdateChunk,
	type Message,
	type SentMessage,
	type Thread,
	type MessageEvent,
	type MessageUpdatedEvent,
	type MessageDeletedEvent,
	type ReactionEvent,
	type ConversationStoppedEvent,
} from '@humanlayer/channels-slack'
import { Effect, Layer, Stream } from 'effect'
import { FetchHttpClient, HttpRouter } from 'effect/unstable/http'

const imageRequest = /\bimage\b/i
const reactionRequest = /\breact(?:ion)?\b/i
const editRequest = /\bedit\b/i
const deleteRequest = /\bdelete\b/i
const streamRequest = /\bstream\b/i
const encoder = new TextEncoder()

const exampleImage = () =>
	FileUpload.make({
		filename: 'channels-example.svg',
		mimeType: 'image/svg+xml',
		data: encoder.encode(
			'<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360" viewBox="0 0 640 360"><rect width="640" height="360" rx="32" fill="#4a154b"/><text x="320" y="180" fill="white" font-family="sans-serif" font-size="42" text-anchor="middle" dominant-baseline="middle">Hello from Slack</text></svg>',
		),
	})

const echoContent = (prefix: string, text: string) => {
	const markdown = `${prefix}: ${text}`
	return imageRequest.test(text)
		? MarkdownContent.make({ markdown, files: [exampleImage()] })
		: MarkdownContent.make({ markdown })
}

const demonstrateLifecycle = (thread: Thread, message: Message, sent: SentMessage) =>
	Effect.gen(function* () {
		if (reactionRequest.test(message.text)) {
			const slack = yield* Slack
			yield* slack.addReaction({ threadId: thread.ref.id, messageRef: message.ref, emoji: Emoji.Check })
		}
		if (editRequest.test(message.text)) {
			yield* sent.edit(MarkdownContent.make({ markdown: `Edited echo: ${message.text}` }))
		}
		if (deleteRequest.test(message.text)) yield* sent.delete()
	})

const respond = (thread: Thread, prefix: string, text: string) =>
	streamRequest.test(text)
		? thread.stream(
				Stream.make(
					PlanUpdateChunk.make({ title: 'Streaming an Effect response' }),
					TaskUpdateChunk.make({ id: 'compose', title: 'Compose reply', status: 'in_progress' }),
					MarkdownTextChunk.make({ text: `${prefix}: ` }),
					MarkdownTextChunk.make({ text }),
					TaskUpdateChunk.make({ id: 'compose', title: 'Compose reply', status: 'complete' }),
				).pipe(Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk)))),
			)
		: thread.post(echoContent(prefix, text))

const handlers = {
	onNewMention: ({ thread, message }: MessageEvent) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a mention from ${message.author.fullName}`)
			const slack = yield* Slack
			const previousChannelMessages = yield* slack.containerMessages({
				channel: thread.ref.channel,
				before: message.ref,
				options: { limit: 20, direction: 'backward' },
			})
			yield* Effect.logInfo(`Loaded ${previousChannelMessages.messages.length} previous channel messages`)
			yield* thread.subscribe()
			yield* thread.startTyping()
			const sent = yield* respond(thread, 'Echo', message.text)
			yield* demonstrateLifecycle(thread, message, sent)
		}),
	onSubscribedMessage: ({ thread, message }: MessageEvent) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a subscribed message from ${message.author.fullName}`)
			const threadMessages = yield* thread.messages.pipe(Stream.take(100), Stream.runCollect)
			yield* Effect.logInfo(`Loaded ${threadMessages.length} recent messages from the thread`)
			yield* thread.startTyping()
			const sent = yield* respond(thread, 'Echo 2', message.text)
			yield* demonstrateLifecycle(thread, message, sent)
		}),
	onDirectMessage: ({ thread, message }: MessageEvent) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a direct message from ${message.author.fullName}`)
			const sent = yield* respond(thread, 'Direct echo', message.text)
			yield* demonstrateLifecycle(thread, message, sent)
		}),
	onMessageUpdated: (event: MessageUpdatedEvent) =>
		Effect.logInfo(`Message ${event.message.ref} was edited in ${event.thread.ref.id}`),
	onMessageDeleted: (event: MessageDeletedEvent) =>
		Effect.logInfo(`Message ${event.messageRef} was deleted from ${event.threadRef.id}`),
	onReaction: [
		{
			id: 'approval-reaction',
			emojis: [Emoji.ThumbsUp],
			handler: (event: ReactionEvent) =>
				Effect.logInfo(`${event.actor.fullName} approved with ${event.rawEmoji}`),
		},
		{
			id: 'heart-or-check-reaction',
			emojis: [Emoji.Heart, Emoji.Check],
			handler: (event: ReactionEvent) => Effect.logInfo(`${event.actor.fullName} reacted with ${event.rawEmoji}`),
		},
	],
	onConversationStopped: (event: ConversationStoppedEvent) =>
		Effect.logInfo(`Slack stopped the active response in ${event.threadRef.id}`),
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
	Layer.provideMerge(SlackTenantCredentials.layerFromConfig),
	Layer.provide(FetchHttpClient.layer),
)
const native = Slack.layer
const subscriptions = SlackSubscriptions.layerMemory()
const directory = SlackUserDirectory.layer.pipe(Layer.provide(native))
export const services = SlackIngress.layer({
	namespace: 'slack-thread-echo',
	policy,
	handlers: {
		onNewMention: [{ id: 'mention', handler: handlers.onNewMention }],
		onSubscribedMessage: [{ id: 'subscribed', handler: handlers.onSubscribedMessage }],
		onMessageUpdated: [{ id: 'edited', handler: handlers.onMessageUpdated }],
		onMessageDeleted: [{ id: 'deleted', handler: handlers.onMessageDeleted }],
		onConversationStopped: [{ id: 'stopped', handler: handlers.onConversationStopped }],
		onDirectMessage: [{ id: 'dm', handler: handlers.onDirectMessage }],
		onReaction: handlers.onReaction.map((registration) => ({
			id: registration.id,
			handler: (event: ReactionEvent) =>
				registration.emojis.some((emoji) => emoji.name === event.emoji.name)
					? registration.handler(event)
					: Effect.void,
		})),
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
