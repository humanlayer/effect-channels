import { NodeCrypto } from '@effect/platform-node'
import { Clock, ConfigProvider, Effect, Layer, Queue, Redacted, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient, HttpClientRequest, HttpClientResponse, HttpRouter } from 'effect/unstable/http'

import { ChannelInfo, MessagePage, SlackIngress as Ingress } from '../src/index'
import { SlackChannelId, SlackEventCallback, SlackMessageTs, SlackTeamId } from '../src/Schema'
import { SlackClient } from '../src/SlackClient'
import { SlackConnectionStore } from '../src/SlackConnectionStore'
import { SlackRoutes } from '../src/SlackRoutes'
import { hmacSha256 } from '../src/SlackSignature'
import { SlackTenantCredentials } from '../src/SlackTenantCredentials'
import { slackChannelRef } from '../src/SlackThreadId'

export const signingSecret = Redacted.make('test-signing-secret')

export const testTeamId = SlackTeamId.make('T_TEST')
export const testChannelId = SlackChannelId.make('C_TEST')
export const testRootTs = SlackMessageTs.make('100.1')
export const testChannelRef = slackChannelRef(testTeamId, testChannelId)
export const testRootThreadId = 'slack:v1:T_TEST:C_TEST:100.1'

export const testIdentityConfigLayer = ConfigProvider.layer(
	ConfigProvider.fromUnknown({ SLACK_BOT_USER_ID: 'U_BOT', SLACK_BOT_ID: 'B_OURS' }),
)

export const webhookUrl = 'http://channels.test/integrations/slack/webhook'

export const testBotToken = 'xoxb-test-token'

export const testConnectionStoreLayer = Layer.succeed(
	SlackConnectionStore,
	SlackConnectionStore.of({
		get: () =>
			Effect.succeed({
				credentials: { botToken: Redacted.make(testBotToken), botUserId: 'U_BOT', botId: 'B_OURS' },
			}),
		upsert: () => Effect.die(new Error('unexpected test connection upsert')),
		remove: () => Effect.die(new Error('unexpected test connection remove')),
	}),
)

export const unimplemented = (operation: string): Effect.Effect<never> =>
	Effect.die(new Error(operation + ' is intentionally unimplemented'))

export const testCredentialsLayer = SlackTenantCredentials.make({
	load: () => Effect.succeedSome({ botToken: Redacted.make(testBotToken) }),
	save: () => Effect.void,
})

export const unknownTenantCredentialsLayer = SlackTenantCredentials.make({
	load: () => Effect.succeedNone,
	save: () => Effect.void,
})

export const signedSlackRequest = (callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const body = yield* Schema.encodeEffect(Schema.fromJsonString(SlackEventCallback))(callback)
		const currentTime = yield* Clock.currentTimeMillis.pipe(TestClock.withLive)
		const timestamp = Math.floor(currentTime / 1000).toString()
		const signature = yield* signSlackBody(body, timestamp)
		return new Request(webhookUrl, {
			method: 'POST',
			headers: {
				'content-type': 'application/json',
				'x-slack-request-timestamp': timestamp,
				'x-slack-signature': signature,
			},
			body,
		})
	})

export const makeTestIngress = (overrides: Partial<Ingress['Service']>) =>
	Ingress.of({
		acceptMessage: () => unimplemented('test.Ingress.acceptMessage'),
		acceptMessageUpdated: () => unimplemented('test.Ingress.acceptMessageUpdated'),
		acceptMessageDeleted: () => unimplemented('test.Ingress.acceptMessageDeleted'),
		acceptReaction: () => unimplemented('test.Ingress.acceptReaction'),
		acceptConversationStopped: () => unimplemented('test.Ingress.acceptConversationStopped'),
		run: () => unimplemented('test.Ingress.run'),
		processMailbox: () => unimplemented('test.Ingress.processMailbox'),
		...overrides,
	})

export interface RecordedSlackRequest {
	readonly method: string
	readonly url: URL
	readonly authorization: string | null
	readonly body: string
}

export const slackJsonResponse = (body: string, status = 200) =>
	new Response(body, { status, headers: { 'content-type': 'application/json' } })

export const makeSlackClientHarness = (
	respond: (request: RecordedSlackRequest) => Response,
	credentials: Layer.Layer<SlackTenantCredentials> = testCredentialsLayer,
	identityConfig: Layer.Layer<never> = testIdentityConfigLayer,
) =>
	Effect.gen(function* () {
		const requests = yield* Queue.unbounded<RecordedSlackRequest>()
		const httpClient = HttpClient.make((request) =>
			Effect.gen(function* () {
				const webRequest = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
				const body = yield* Effect.promise(() => webRequest.text())
				const url = new URL(webRequest.url)
				if (webRequest.headers.get('content-type')?.startsWith('application/x-www-form-urlencoded')) {
					for (const [key, value] of new URLSearchParams(body)) {
						url.searchParams.set(key, value)
					}
				}
				const recorded: RecordedSlackRequest = {
					method: webRequest.method,
					url,
					authorization: webRequest.headers.get('authorization'),
					body,
				}
				yield* Queue.offer(requests, recorded)
				return HttpClientResponse.fromWeb(request, respond(recorded))
			}),
		)
		const layer = SlackClient.layer.pipe(
			Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), credentials)),
			Layer.provide(identityConfig),
			Layer.provideMerge(testConnectionStoreLayer),
		)
		return { requests, layer }
	})

export const makeStubSlackClient = (overrides: Partial<SlackClient['Service']>) =>
	SlackClient.of({
		postMessage: () => unimplemented('test.SlackClient.postMessage'),
		setSessionStatus: () => unimplemented('test.SlackClient.setSessionStatus'),
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
		...overrides,
	})

export const stubSlackClientLayer = (overrides: Partial<SlackClient['Service']>) =>
	Layer.merge(Layer.succeed(SlackClient, makeStubSlackClient(overrides)), testConnectionStoreLayer)

export const testRouteSlackClientLayer = stubSlackClientLayer({
	replies: () => Effect.succeed(MessagePage.make({ messages: [] })),
	channelInfo: (input) =>
		Effect.succeed(
			ChannelInfo.make({
				channel: slackChannelRef(
					input.teamId,
					input.channelId,
					input.channelId.startsWith('G') ? 'mpim' : undefined,
				),
			}),
		),
})

export const testRouteLayer = SlackRoutes.layer.pipe(
	HttpRouter.provideRequest(NodeCrypto.layer),
	Layer.provide(
		ConfigProvider.layer(
			ConfigProvider.fromUnknown({
				SLACK_SIGNING_SECRET: 'test-signing-secret',
				SLACK_BOT_USER_ID: 'U_BOT',
				SLACK_BOT_ID: 'B_OURS',
			}),
		),
	),
	Layer.provide(testCredentialsLayer),
	Layer.provide(testRouteSlackClientLayer),
)

export const signSlackBody = (body: string, timestamp: string, secret = 'test-signing-secret') => {
	return hmacSha256({
		secret: Redacted.make(secret),
		data: new TextEncoder().encode(`v0:${timestamp}:${body}`),
	}).pipe(Effect.map((signature) => `v0=${bytesToHex(signature)}`))
}

export const bytesToHex = (bytes: Uint8Array) =>
	Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')

export const appMentionCallback = {
	type: 'event_callback' as const,
	team_id: 'T_TEST',
	event_id: 'Ev_TEST_1',
	event_time: 1_788_000_000,
	event: {
		type: 'app_mention' as const,
		user: 'U_HUMAN',
		text: '<@U_BOT> hello from Slack',
		ts: '100.1',
		channel: 'C_TEST',
	},
}

export const reactionAddedCallback = {
	type: 'event_callback' as const,
	team_id: 'T_TEST',
	event_id: 'Ev_TEST_2',
	event_time: 1_788_000_000,
	event: {
		type: 'reaction_added' as const,
		user: 'U_HUMAN',
		reaction: 'thumbsup',
		item: { type: 'message' as const, channel: 'C_TEST', ts: '100.1' },
		event_ts: '101.1',
	},
}
