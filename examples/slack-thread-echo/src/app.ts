import { Channels, MarkdownContent } from '@humanlayer/channels'
import { createChannelsApp, postgres, slack } from '@humanlayer/channels-app'
import { Effect, Stream } from 'effect'

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
			yield* thread.post(MarkdownContent.make({ markdown: `Echo: ${message.text}` }))
		}),
	onSubscribedMessage: (thread, message) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a subscribed message from ${message.author.fullName}`)
			const threadMessages = yield* thread.allMessages.pipe(Stream.runCollect)
			yield* Effect.logInfo(`Loaded ${threadMessages.length} messages from the thread`)
			yield* thread.startTyping()
			yield* thread.post(MarkdownContent.make({ markdown: `Echo 2: ${message.text}` }))
		}),
})

export const handle = app.handle
export const routes = app.routes
