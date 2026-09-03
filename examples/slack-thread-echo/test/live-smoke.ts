import { createServer } from 'node:http'

import { NodeFileSystem, NodeHttpClient, NodeHttpServer, NodeRuntime } from '@effect/platform-node'
import { MarkdownContent, type Message, type Thread } from '@humanlayer/channels'
import { createChannelsApp, postgres, slack } from '@humanlayer/channels-app'
import { Config, Effect, Layer, Stream } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { recordInbound, RecordingHttpClient, SlackRecording } from './record.ts'

const assertSurface = (thread: Thread) =>
	Effect.gen(function* () {
		const page = yield* thread.listMessages({ limit: 10, direction: 'backward' })
		const newest = yield* thread.messages.pipe(Stream.take(10), Stream.runCollect)
		const all = yield* thread.allMessages.pipe(Stream.take(100), Stream.runCollect)
		const participants = yield* thread.getParticipants()
		if (page.messages.length === 0 || newest.length === 0 || all.length === 0 || participants.length === 0) {
			return yield* Effect.die(new Error('Slack history or participants acceptance failed'))
		}
		yield* thread.fetchMetadata()
		yield* thread.channel.listMessages({ limit: 10, direction: 'backward' })
		yield* thread.channel.messages.pipe(Stream.take(10), Stream.runDrain)
		yield* thread.channel.listThreads({ limit: 10 })
		yield* thread.channel.threads.pipe(Stream.take(10), Stream.runDrain)
		yield* thread.channel.fetchMetadata()
	})

const onNewMention = (thread: Thread, message: Message) =>
	Effect.gen(function* () {
		if (yield* thread.isSubscribed()) {
			return yield* Effect.die(new Error('Slack mention was subscribed before application code opted in'))
		}
		yield* thread.subscribe()
		yield* thread.startTyping()
		yield* thread.post(MarkdownContent.make({ markdown: `Live echo: ${message.text}` }))
		yield* thread.channel.post(MarkdownContent.make({ markdown: 'Live top-level channel acceptance.' }))
		yield* assertSurface(thread)
		if (!thread.ref.isNew) {
			const history = yield* thread.allMessages.pipe(Stream.take(100), Stream.runCollect)
			if (!history.some((item) => item.author.isBot === true && !item.author.isMe)) {
				return yield* Effect.die(new Error('Integration-authored root was absent from Slack history'))
			}
		}
	})

const onSubscribedMessage = (thread: Thread, message: Message) =>
	Effect.gen(function* () {
		if (message.author.isMe) {
			return yield* Effect.die(new Error('Our Slack echo reached onSubscribedMessage'))
		}
		yield* thread.startTyping()
		yield* thread.post(MarkdownContent.make({ markdown: `Subscribed echo: ${message.text}` }))
		yield* assertSurface(thread)
	})

const recordMode = Bun.argv.at(2) === 'record'
const recording = SlackRecording.layer(recordMode)
const recordingHttp = RecordingHttpClient.pipe(
	Layer.provide(NodeHttpClient.layerFetch),
	Layer.provide(recording),
	Layer.provide(NodeFileSystem.layer),
)
const app = createChannelsApp({
	providers: [slack()],
	storage: postgres(),
	onNewMention,
	onSubscribedMessage,
	advanced: { httpClient: recordingHttp },
})
const HttpLive = HttpRouter.serve(app.routes, { middleware: recordInbound }).pipe(
	Layer.provide(
		NodeHttpServer.layerConfig(createServer, {
			port: Config.number('PORT').pipe(Config.withDefault(3000)),
			gracefulShutdownTimeout: Config.succeed('10 seconds'),
		}),
	),
)
const RuntimeLive = HttpLive.pipe(Layer.provideMerge(recording), Layer.provide(NodeFileSystem.layer))

const program = Effect.gen(function* () {
	const recorder = yield* SlackRecording
	yield* Effect.logInfo('Run the public, private, and integration-authored Slack scenarios from README.md')
	return yield* Effect.never.pipe(
		Effect.ensuring(
			recorder.enabled
				? recorder.write.pipe(Effect.tap((path) => Effect.logInfo(`Sanitized recording written to ${path}`)))
				: Effect.void,
		),
	)
}).pipe(Effect.provide(RuntimeLive), Effect.scoped)

NodeRuntime.runMain(program)
