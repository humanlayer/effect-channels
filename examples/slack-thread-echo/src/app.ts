import {
	Channels,
	Emoji,
	FileUpload,
	MarkdownContent,
	MarkdownTextChunk,
	PlanUpdateChunk,
	TaskUpdateChunk,
	type Message,
	type SentMessage,
	type Thread,
} from '@humanlayer/channels'
import { createChannelsApp, postgres, slack } from '@humanlayer/channels-app'
import { Effect, Stream } from 'effect'

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
			'<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360" viewBox="0 0 640 360"><rect width="640" height="360" rx="32" fill="#4a154b"/><text x="320" y="180" fill="white" font-family="sans-serif" font-size="42" text-anchor="middle" dominant-baseline="middle">Hello from Channels</text></svg>',
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
			const channels = yield* Channels
			yield* channels.addReaction({ threadId: thread.ref.id, messageRef: message.ref, emoji: Emoji.Check })
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

export const app = createChannelsApp({
	providers: [slack()],
	storage: postgres(),
	onNewMention: (thread, message) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a mention from ${message.author.fullName}`)
			const channels = yield* Channels
			const previousChannelMessages = yield* channels.containerMessages({
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
	onSubscribedMessage: (thread, message) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a subscribed message from ${message.author.fullName}`)
			const threadMessages = yield* thread.allMessages.pipe(Stream.runCollect)
			yield* Effect.logInfo(`Loaded ${threadMessages.length} messages from the thread`)
			yield* thread.startTyping()
			const sent = yield* respond(thread, 'Echo 2', message.text)
			yield* demonstrateLifecycle(thread, message, sent)
		}),
	onMessageUpdated: (event) => Effect.logInfo(`Message ${event.message.ref} was edited in ${event.thread.ref.id}`),
	onMessageDeleted: (event) => Effect.logInfo(`Message ${event.messageRef} was deleted from ${event.threadRef.id}`),
	onReaction: [
		{
			emojis: [Emoji.ThumbsUp],
			handler: (event) => Effect.logInfo(`${event.actor.fullName} approved with ${event.rawEmoji}`),
		},
		{
			emojis: [Emoji.Heart, Emoji.Check],
			handler: (event) => Effect.logInfo(`${event.actor.fullName} reacted with ${event.rawEmoji}`),
		},
	],
	onConversationStopped: (event) => Effect.logInfo(`Slack stopped the active response in ${event.threadRef.id}`),
})

export const handle = app.handle
export const routes = app.routes
