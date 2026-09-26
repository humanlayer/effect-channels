import { NodeCrypto, NodeSocketServer } from '@effect/platform-node'
import { Clock, Context, Crypto, Effect, Layer, Match, Schema } from 'effect'
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'
import { createEmulator, type Emulator } from 'emulate'

import { SlackEventCallback } from '../../../src/Schema'
import { signSlackBody } from '../../support'

const TeamResponse = Schema.Struct({ ok: Schema.Literal(true), team_id: Schema.String })
const User = Schema.Struct({
	id: Schema.String,
	name: Schema.String,
	real_name: Schema.optionalKey(Schema.String),
	is_bot: Schema.optionalKey(Schema.Boolean),
})
const UsersResponse = Schema.Struct({ ok: Schema.Literal(true), members: Schema.Array(User) })
const Conversation = Schema.Struct({
	id: Schema.String,
	name: Schema.String,
	is_private: Schema.optionalKey(Schema.Boolean),
	num_members: Schema.optionalKey(Schema.Finite),
})
const ConversationsResponse = Schema.Struct({ ok: Schema.Literal(true), channels: Schema.Array(Conversation) })
const PostedMessageResponse = Schema.Struct({
	ok: Schema.Literal(true),
	channel: Schema.String,
	ts: Schema.String,
	message: Schema.Struct({
		user: Schema.optionalKey(Schema.String),
		text: Schema.String,
		thread_ts: Schema.optionalKey(Schema.String),
	}),
})
const OpenConversationResponse = Schema.Struct({
	ok: Schema.Literal(true),
	channel: Schema.Struct({ id: Schema.String }),
})
const HistoryResponse = Schema.Struct({
	ok: Schema.Literal(true),
	messages: Schema.Array(
		Schema.Struct({
			ts: Schema.String,
			text: Schema.String,
			user: Schema.optionalKey(Schema.String),
			thread_ts: Schema.optionalKey(Schema.String),
			reply_count: Schema.optionalKey(Schema.Finite),
			files: Schema.optionalKey(
				Schema.Array(
					Schema.Struct({
						id: Schema.String,
						name: Schema.optionalKey(Schema.String),
						mimetype: Schema.optionalKey(Schema.String),
						size: Schema.optionalKey(Schema.Finite),
						url_private: Schema.optionalKey(Schema.String),
						url_private_download: Schema.optionalKey(Schema.String),
					}),
				),
			),
		}),
	),
	has_more: Schema.optionalKey(Schema.Boolean),
	response_metadata: Schema.optionalKey(Schema.Struct({ next_cursor: Schema.optionalKey(Schema.String) })),
})
const OkResponse = Schema.Struct({ ok: Schema.Literal(true) })

const FileInfoResponse = Schema.Struct({
	ok: Schema.Literal(true),
	file: Schema.Struct({
		id: Schema.String,
		name: Schema.String,
		mimetype: Schema.String,
		size: Schema.Finite,
		url_private: Schema.String,
		url_private_download: Schema.String,
	}),
})

const ReactionsResponse = Schema.Struct({
	ok: Schema.Literal(true),
	message: Schema.Struct({
		reactions: Schema.optionalKey(
			Schema.Array(Schema.Struct({ name: Schema.String, users: Schema.Array(Schema.String) })),
		),
	}),
})

const ChannelRequest = Schema.Struct({ channel: Schema.String })
const ThreadRequest = Schema.Struct({ channel: Schema.String, ts: Schema.String })

/** Request and response schemas for each Slack Web API method the emulator tests call directly. */
const slackEmulatorMethods = {
	'auth.test': { request: Schema.Struct({}), response: TeamResponse },
	'users.list': { request: Schema.Struct({}), response: UsersResponse },
	'conversations.list': { request: Schema.Struct({ types: Schema.String }), response: ConversationsResponse },
	'conversations.join': { request: ChannelRequest, response: OkResponse },
	'conversations.invite': {
		request: Schema.Struct({ channel: Schema.String, users: Schema.String }),
		response: OkResponse,
	},
	'conversations.open': { request: Schema.Struct({ users: Schema.String }), response: OpenConversationResponse },
	'conversations.history': { request: ChannelRequest, response: HistoryResponse },
	'conversations.replies': { request: ThreadRequest, response: HistoryResponse },
	'chat.postMessage': {
		request: Schema.Struct({
			channel: Schema.String,
			text: Schema.String,
			thread_ts: Schema.optionalKey(Schema.String),
		}),
		response: PostedMessageResponse,
	},
	'files.info': { request: Schema.Struct({ file: Schema.String }), response: FileInfoResponse },
	'reactions.get': {
		request: Schema.Struct({ channel: Schema.String, timestamp: Schema.String }),
		response: ReactionsResponse,
	},
}

type SlackEmulatorMethods = typeof slackEmulatorMethods
type SlackEmulatorMethod = keyof SlackEmulatorMethods
type SlackEmulatorRequest<M extends SlackEmulatorMethod> = SlackEmulatorMethods[M]['request']['Type']
type SlackEmulatorResponse<M extends SlackEmulatorMethod> = SlackEmulatorMethods[M]['response']['Type']

export class SlackEmulatorError extends Schema.TaggedError<SlackEmulatorError>()('SlackEmulatorError', {
	operation: Schema.String,
	cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const slackEmulatorBotToken = 'xoxb-channels-emulator'
export const slackEmulatorAliceToken = 'xoxp-alice-emulator'
export const slackEmulatorAdminToken = 'xoxp-admin-emulator'
export const slackEmulatorBotUserId = 'U_CHANNELS_BOT'
export const slackEmulatorBotId = 'B_CHANNELS_BOT'
export const slackEmulatorIntegrationToken = 'xoxb-integration-emulator'
export const slackEmulatorIntegrationUserId = 'U_INTEGRATION_BOT'
export const slackEmulatorIntegrationBotId = 'B_INTEGRATION_BOT'
export const slackEmulatorSigningSecret = 'channels-emulator-signing-secret'

/** The host verifies signatures against its own runtime clock, so signing ignores any TestClock in scope. */
const wallClock = Clock.Clock.defaultValue()

const SlackEventCallbackJson = Schema.fromJsonString(Schema.toEncoded(SlackEventCallback))

const availablePort = Effect.scoped(
	Effect.gen(function* () {
		const server = yield* NodeSocketServer.make({ port: 0, host: '127.0.0.1' }).pipe(
			Effect.mapError((cause) => SlackEmulatorError.make({ operation: 'allocate_port', cause })),
		)
		return yield* Match.value(server.address).pipe(
			Match.tagsExhaustive({
				TcpAddress: (address) => Effect.succeed(address.port),
				UnixAddress: () => Effect.fail(SlackEmulatorError.make({ operation: 'allocate_port' })),
			}),
		)
	}),
)

const callSlackMethod =
	(client: HttpClient.HttpClient, emulator: Emulator) =>
	<Req extends Schema.Codec<unknown, unknown>, Res extends Schema.Codec<unknown, unknown>>(
		token: string,
		method: string,
		endpoint: { readonly request: Req; readonly response: Res },
		body: Req['Type'],
	): Effect.Effect<Res['Type'], SlackEmulatorError> =>
		HttpClientRequest.post(`${emulator.url}/api/${method}`).pipe(
			HttpClientRequest.bearerToken(token),
			HttpClientRequest.setHeader('connection', 'close'),
			HttpClientRequest.schemaBodyJson(endpoint.request)(body),
			Effect.flatMap(client.execute),
			Effect.flatMap(HttpClientResponse.filterStatusOk),
			Effect.flatMap(HttpClientResponse.schemaBodyJson(endpoint.response)),
			Effect.mapError((cause) => SlackEmulatorError.make({ operation: method, cause })),
		)

export class SlackEmulator extends Context.Service<
	SlackEmulator,
	{
		readonly emulator: Emulator
		readonly teamId: string
		readonly aliceUserId: string
		readonly adminUserId: string
		readonly publicChannelId: string
		readonly privateChannelId: string
		readonly call: <M extends SlackEmulatorMethod>(
			token: string,
			method: M,
			body: SlackEmulatorRequest<M>,
		) => Effect.Effect<SlackEmulatorResponse<M>, SlackEmulatorError>
		/** Signs an Events API callback with the emulator signing secret at the live wall-clock time. */
		readonly signedWebhook: (event: typeof SlackEventCallback.Encoded) => Effect.Effect<Request, SlackEmulatorError>
	}
>()('test/SlackEmulator') {
	static readonly layer = Layer.effect(
		SlackEmulator,
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient
			const crypto = yield* Crypto.Crypto
			const acquireEmulator = Effect.gen(function* () {
				const port = yield* availablePort
				return yield* Effect.tryPromise({
					try: () =>
						createEmulator({
							service: 'slack',
							port,
							seed: {
								slack: {
									team: { name: 'Channels Integration', domain: 'channels-integration' },
									users: [
										{
											name: 'alice',
											real_name: 'Alice Example',
											email: 'alice@example.test',
										},
									],
									channels: [
										{ name: 'channels-public', topic: 'Public integration tests' },
										{
											name: 'channels-private',
											topic: 'Private integration tests',
											is_private: true,
										},
									],
									oauth_apps: [
										{
											client_id: 'channels.integration',
											client_secret: 'not-a-secret',
											name: 'Channels Agent',
											redirect_uris: ['http://localhost/callback'],
											bot_name: 'channels-bot',
											bot_id: slackEmulatorBotId,
											bot_user_id: slackEmulatorBotUserId,
										},
										{
											client_id: 'integration.bot',
											client_secret: 'not-a-secret-either',
											name: 'External Integration',
											redirect_uris: ['http://localhost/integration-callback'],
											bot_name: 'integration-bot',
											bot_id: slackEmulatorIntegrationBotId,
											bot_user_id: slackEmulatorIntegrationUserId,
										},
									],
									tokens: [
										{
											token: slackEmulatorBotToken,
											user_id: slackEmulatorBotUserId,
											bot_id: slackEmulatorBotId,
											bot_user_id: slackEmulatorBotUserId,
										},
										{
											token: slackEmulatorIntegrationToken,
											user_id: slackEmulatorIntegrationUserId,
											bot_id: slackEmulatorIntegrationBotId,
											bot_user_id: slackEmulatorIntegrationUserId,
										},
										{ token: slackEmulatorAliceToken, user: 'alice' },
										{ token: slackEmulatorAdminToken, user: 'admin' },
									],
									signing_secret: slackEmulatorSigningSecret,
								},
							},
						}),
					catch: (cause) => SlackEmulatorError.make({ operation: `start_emulator:${port}`, cause }),
				})
			})
			const emulator = yield* Effect.acquireRelease(
				acquireEmulator.pipe(Effect.retry({ times: 4 })),
				(resource) => Effect.promise(() => resource.close()),
			)
			const execute = callSlackMethod(client, emulator)
			const call = <M extends SlackEmulatorMethod>(
				token: string,
				method: M,
				body: SlackEmulatorRequest<M>,
			): Effect.Effect<SlackEmulatorResponse<M>, SlackEmulatorError> =>
				execute(token, method, slackEmulatorMethods[method], body)
			const team = yield* call(slackEmulatorBotToken, 'auth.test', {})
			const users = yield* call(slackEmulatorBotToken, 'users.list', {})
			const conversations = yield* call(slackEmulatorAdminToken, 'conversations.list', {
				types: 'public_channel,private_channel',
			})
			const alice = users.members.find((user) => user.name === 'alice')
			const admin = users.members.find((user) => user.name === 'admin')
			const publicChannel = conversations.channels.find((channel) => channel.name === 'channels-public')
			const privateChannel = conversations.channels.find((channel) => channel.name === 'channels-private')
			if (
				alice === undefined ||
				admin === undefined ||
				publicChannel === undefined ||
				privateChannel === undefined
			) {
				return yield* SlackEmulatorError.make({ operation: 'seed' })
			}
			yield* call(slackEmulatorBotToken, 'conversations.join', { channel: publicChannel.id })
			yield* call(slackEmulatorIntegrationToken, 'conversations.join', { channel: publicChannel.id })
			yield* call(slackEmulatorAdminToken, 'conversations.invite', {
				channel: privateChannel.id,
				users: slackEmulatorBotUserId,
			})
			const signedWebhook = (event: typeof SlackEventCallback.Encoded) =>
				Effect.gen(function* () {
					const body = yield* Schema.encodeEffect(SlackEventCallbackJson)(event)
					const now = yield* wallClock.currentTimeMillis
					const timestamp = Math.floor(now / 1000).toString()
					const signature = yield* signSlackBody(body, timestamp, slackEmulatorSigningSecret)
					return new Request('http://channels.test/integrations/slack/webhook', {
						method: 'POST',
						headers: {
							'content-type': 'application/json',
							'x-slack-request-timestamp': timestamp,
							'x-slack-signature': signature,
						},
						body,
					})
				}).pipe(
					Effect.provideService(Crypto.Crypto, crypto),
					Effect.mapError((cause) => SlackEmulatorError.make({ operation: 'sign_webhook', cause })),
				)
			return SlackEmulator.of({
				emulator,
				teamId: team.team_id,
				aliceUserId: alice.id,
				adminUserId: admin.id,
				publicChannelId: publicChannel.id,
				privateChannelId: privateChannel.id,
				call,
				signedWebhook,
			})
		}),
	).pipe(Layer.provide([FetchHttpClient.layer, NodeCrypto.layer]))
}
