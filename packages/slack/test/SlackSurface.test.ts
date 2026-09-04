import { assert, it } from '@effect/vitest'
import {
	ChannelId,
	Emoji,
	EphemeralNoFallback,
	MarkdownContent,
	MessageRef,
	TenantId,
	ThreadId,
	UserId,
	unimplemented,
} from '@humanlayer/channels'
import { Cause, ConfigProvider, Effect, Exit, Layer, Option, Redacted, Stream } from 'effect'
import { HttpClient } from 'effect/unstable/http'

import { testAuthor, testMessage } from '../../channels/test/support.ts'
import { SlackChannelId, SlackMessageTs, SlackSentMessage, SlackTeamId } from '../src/Schema.ts'
import { Slack } from '../src/Slack.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { SlackProvider } from '../src/SlackProvider.ts'
import { SlackTenantCredentials } from '../src/SlackTenantCredentials.ts'

const teamId = SlackTeamId.make('T_TEST')
const channelId = SlackChannelId.make('C_TEST')
const messageTs = SlackMessageTs.make('100.1')
const streamRef = { channelId, messageTs, threadTs: messageTs }
const threadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')
const tenant = TenantId.make('T_TEST')
const userId = UserId.make('U_TEST')
const channel = { id: ChannelId.make('slack:v1:T_TEST:C_TEST'), provider: 'slack' as const, tenant, isDm: false }
const content = MarkdownContent.make({ markdown: 'hello' })

const expectDefect = <A, E>(operation: string, effect: Effect.Effect<A, E>) =>
	Effect.gen(function* () {
		const exit = yield* Effect.exit(effect)
		assert.strictEqual(Exit.isFailure(exit), true)
		if (Exit.isFailure(exit)) {
			assert.ok(
				Cause.pretty(exit.cause).includes(`${operation} is intentionally unimplemented`),
				`expected defect naming ${operation}, got: ${Cause.pretty(exit.cause)}`,
			)
		}
	})

const deadHttpClient = HttpClient.make(() => Effect.die(new Error('unexpected HTTP request')))
const credentialsLayer = SlackTenantCredentials.make({
	load: () => Effect.succeed(Option.some({ botToken: Redacted.make('xoxb-test-token') })),
	save: () => Effect.void,
})
const clientLayer = SlackClient.layer.pipe(
	Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, deadHttpClient), credentialsLayer)),
)

it.effect('names every Phase 2 SlackClient placeholder', () =>
	Effect.gen(function* () {
		const client = yield* SlackClient
		yield* expectDefect(
			'SlackClient.startStream',
			client.startStream({ teamId, channelId, threadTs: messageTs, chunks: [] }),
		)
		yield* expectDefect('SlackClient.appendStream', client.appendStream({ teamId, stream: streamRef, chunks: [] }))
		yield* expectDefect('SlackClient.stopStream', client.stopStream({ teamId, stream: streamRef, chunks: [] }))
		yield* expectDefect(
			'SlackClient.updateMessage',
			client.updateMessage({ teamId, channelId, ts: messageTs, text: 'edited' }),
		)
		yield* expectDefect('SlackClient.deleteMessage', client.deleteMessage({ teamId, channelId, ts: messageTs }))
		yield* expectDefect(
			'SlackClient.addReaction',
			client.addReaction({ teamId, channelId, ts: messageTs, emoji: 'thumbsup' }),
		)
		yield* expectDefect(
			'SlackClient.removeReaction',
			client.removeReaction({ teamId, channelId, ts: messageTs, emoji: 'thumbsup' }),
		)
		yield* expectDefect('SlackClient.openDM', client.openDM({ teamId, userId }))
		yield* expectDefect(
			'SlackClient.postEphemeral',
			client.postEphemeral({ teamId, channelId, userId, text: 'private' }),
		)
		yield* expectDefect('SlackClient.api', client.api({ teamId, method: 'chat.scheduleMessage', payload: {} }))
	}).pipe(Effect.provide(clientLayer)),
)

it.effect('names every Phase 2 native Slack placeholder', () =>
	Effect.gen(function* () {
		const slack = yield* Slack
		yield* expectDefect('Slack.createThread', slack.createThread({ teamId, channelId, content }))
		yield* expectDefect('Slack.post', slack.post({ threadId, payload: {} }))
		yield* expectDefect('Slack.postEphemeral', slack.postEphemeral({ threadId, userId, payload: {} }))
		yield* expectDefect('Slack.api', slack.api({ teamId, method: 'chat.scheduleMessage', payload: {} }))
	}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(clientLayer)))),
)

it.effect('names every Phase 2 Slack provider placeholder', () =>
	Effect.gen(function* () {
		const provider = yield* SlackProvider
		yield* expectDefect(
			'SlackProvider.edit',
			provider.edit({ threadId, messageRef: MessageRef.make('100.2'), content }),
		)
		yield* expectDefect('SlackProvider.delete', provider.delete({ threadId, messageRef: MessageRef.make('100.2') }))
		yield* expectDefect('SlackProvider.stream', provider.stream({ threadId }, Stream.empty))
		yield* expectDefect('SlackProvider.startChannelTyping', provider.startChannelTyping({ channel }))
		yield* expectDefect(
			'SlackProvider.addReaction',
			provider.addReaction({ threadId, messageRef: MessageRef.make('100.2'), emoji: Emoji.ThumbsUp }),
		)
		yield* expectDefect(
			'SlackProvider.removeReaction',
			provider.removeReaction({ threadId, messageRef: MessageRef.make('100.2'), emoji: Emoji.ThumbsUp }),
		)
		yield* expectDefect('SlackProvider.subject', provider.subject({ message: testMessage }))
		yield* expectDefect('SlackProvider.openDM', provider.openDM({ provider: 'slack', tenant, user: testAuthor }))
		yield* expectDefect(
			'SlackProvider.postEphemeral',
			provider.postEphemeral({ threadId, user: testAuthor, content, fallback: EphemeralNoFallback.make({}) }),
		)
	}).pipe(Effect.provide(SlackProvider.layer.pipe(Layer.provide(clientLayer)))),
)

it.effect('names the fromConfig credential save placeholder', () =>
	Effect.gen(function* () {
		const credentials = yield* SlackTenantCredentials
		yield* expectDefect(
			'SlackTenantCredentials.save',
			credentials.save({ teamId, credentials: { botToken: Redacted.make('xoxb-test-token') } }),
		)
	}).pipe(
		Effect.provide(
			SlackTenantCredentials.layerFromConfig.pipe(
				Layer.provide(
					ConfigProvider.layer(ConfigProvider.fromUnknown({ SLACK_BOT_TOKEN: 'xoxb-config-token' })),
				),
			),
		),
	),
)

const stubClientLayer = Layer.succeed(
	SlackClient,
	SlackClient.of({
		postMessage: () => Effect.succeed(SlackSentMessage.make({ channelId, ts: SlackMessageTs.make('100.9') })),
		setSessionStatus: () => Effect.void,
		startStream: () => unimplemented('test.SlackClient.startStream'),
		appendStream: () => unimplemented('test.SlackClient.appendStream'),
		stopStream: () => unimplemented('test.SlackClient.stopStream'),
		updateMessage: () => unimplemented('test.SlackClient.updateMessage'),
		deleteMessage: () => unimplemented('test.SlackClient.deleteMessage'),
		addReaction: () => unimplemented('test.SlackClient.addReaction'),
		removeReaction: () => unimplemented('test.SlackClient.removeReaction'),
		replies: () => unimplemented('test.SlackClient.replies'),
		history: () => unimplemented('test.SlackClient.history'),
		channelInfo: () => unimplemented('test.SlackClient.channelInfo'),
		listThreads: () => unimplemented('test.SlackClient.listThreads'),
		getUser: () => unimplemented('test.SlackClient.getUser'),
		uploadFiles: () => unimplemented('test.SlackClient.uploadFiles'),
		downloadFile: () => unimplemented('test.SlackClient.downloadFile'),
		openDM: () => unimplemented('test.SlackClient.openDM'),
		postEphemeral: () => unimplemented('test.SlackClient.postEphemeral'),
		api: () => unimplemented('test.SlackClient.api'),
	}),
)

it.effect('advertises support only for implemented operations', () =>
	Effect.gen(function* () {
		const provider = yield* SlackProvider
		assert.deepStrictEqual(provider.capabilities, {
			threadPost: true,
			channelPost: true,
			edit: false,
			delete: false,
			streaming: 'unsupported',
			typing: { thread: true, channel: false },
			history: { thread: true, channelMessages: true, channelThreads: true },
			reactions: { add: false, remove: false, events: false },
			files: { read: true, upload: true },
			actions: false,
			threadInfo: true,
			channelInfo: true,
			createThread: false,
			directMessages: { ingress: false, open: false },
			ephemeral: { native: false, dmFallback: false },
			subject: false,
		})
		const sent = yield* provider.post({ threadId, content: MarkdownContent.make({ markdown: 'capability check' }) })
		assert.strictEqual(sent.ref.messageRef, '100.9')
		assert.deepStrictEqual(sent.ref.degraded, [])
	}).pipe(Effect.provide(SlackProvider.layer.pipe(Layer.provide(stubClientLayer)))),
)
