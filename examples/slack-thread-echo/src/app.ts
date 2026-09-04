import { Channels, FileUpload, MarkdownContent } from '@humanlayer/channels'
import { createChannelsApp, postgres, slack } from '@humanlayer/channels-app'
import { Effect, Stream } from 'effect'

const imageRequest = /\bimage\b/i
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
			yield* thread.post(echoContent('Echo', message.text))
		}),
	onSubscribedMessage: (thread, message) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`Received a subscribed message from ${message.author.fullName}`)
			const threadMessages = yield* thread.allMessages.pipe(Stream.runCollect)
			yield* Effect.logInfo(`Loaded ${threadMessages.length} messages from the thread`)
			yield* thread.startTyping()
			yield* thread.post(echoContent('Echo 2', message.text))
		}),
})

export const handle = app.handle
export const routes = app.routes
