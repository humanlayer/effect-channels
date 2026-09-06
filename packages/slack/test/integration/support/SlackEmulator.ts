import { createHmac } from 'node:crypto'
import { createServer } from 'node:http'

import { Context, Effect, Layer, Predicate, Schema } from 'effect'
import { createEmulator, type Emulator } from 'emulate'

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
export const PostedMessageResponse = Schema.Struct({
	ok: Schema.Literal(true),
	channel: Schema.String,
	ts: Schema.String,
	message: Schema.Struct({
		user: Schema.optionalKey(Schema.String),
		text: Schema.String,
		thread_ts: Schema.optionalKey(Schema.String),
	}),
})
export const OpenConversationResponse = Schema.Struct({
	ok: Schema.Literal(true),
	channel: Schema.Struct({ id: Schema.String }),
})
export const EphemeralMessageResponse = Schema.Struct({
	ok: Schema.Literal(true),
	channel: Schema.String,
	message_ts: Schema.optionalKey(Schema.String),
	ts: Schema.optionalKey(Schema.String),
})
export const HistoryResponse = Schema.Struct({
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

export const FileInfoResponse = Schema.Struct({
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

export const slackEmulatorBotToken = 'xoxb-channels-emulator'
export const slackEmulatorAliceToken = 'xoxp-alice-emulator'
export const slackEmulatorAdminToken = 'xoxp-admin-emulator'
export const slackEmulatorBotUserId = 'U_CHANNELS_BOT'
export const slackEmulatorBotId = 'B_CHANNELS_BOT'
export const slackEmulatorIntegrationToken = 'xoxb-integration-emulator'
export const slackEmulatorIntegrationUserId = 'U_INTEGRATION_BOT'
export const slackEmulatorIntegrationBotId = 'B_INTEGRATION_BOT'
export const slackEmulatorSigningSecret = 'channels-emulator-signing-secret'

type SlackApiValue = string | number | boolean | ReadonlyArray<string>
type SlackApiBody = Readonly<Record<string, SlackApiValue>>

export interface SignedSlackWebhookEvent {
	readonly type: 'event_callback'
	readonly team_id: string
	readonly event_id: string
	readonly event_time: number
	readonly event:
		| {
				readonly type: 'app_mention' | 'message'
				readonly channel: string
				readonly ts: string
				readonly text: string
				readonly user: string
				readonly thread_ts?: string
				readonly bot_id?: string
				readonly channel_type?: 'channel' | 'group' | 'im' | 'mpim'
		  }
		| {
				readonly type: 'agent_session_stopped'
				readonly channel: string
				readonly thread_ts: string
				readonly user: string
				readonly event_ts: string
				readonly streaming_message_ts: ReadonlyArray<string>
		  }
}

const availablePort = Effect.tryPromise({
	try: () =>
		new Promise<number>((resolve, reject) => {
			const server = createServer()
			server.once('error', reject)
			server.listen(0, '127.0.0.1', () => {
				server.unref()
				const address = server.address()
				if (address === null || !Predicate.hasProperty(address, 'port') || !Predicate.isNumber(address.port)) {
					server.close(() => reject(new Error('Available-port probe did not bind a TCP address')))
					return
				}
				const port = address.port
				server.close((error) => (error === undefined ? resolve(port) : reject(error)))
			})
		}),
	catch: (cause) => new Error('Could not allocate a port for the Slack emulator', { cause }),
})

const apiCall = <S extends Schema.Top>(
	emulator: Emulator,
	token: string,
	method: string,
	body: SlackApiBody,
	schema: S,
): Effect.Effect<S['Type'], Error, S['DecodingServices']> =>
	Effect.tryPromise({
		try: (signal) =>
			fetch(`${emulator.url}/api/${method}`, {
				method: 'POST',
				signal,
				headers: {
					authorization: `Bearer ${token}`,
					connection: 'close',
					'content-type': 'application/json',
				},
				body: JSON.stringify(body),
			}).then(async (response) => {
				if (!response.ok) {
					throw new Error(`Slack emulator ${method} returned HTTP ${response.status}`)
				}
				return response.json()
			}),
		catch: (cause) => new Error(`Slack emulator ${method} request failed`, { cause }),
	}).pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)))

export class SlackEmulator extends Context.Service<
	SlackEmulator,
	{
		readonly emulator: Emulator
		readonly teamId: string
		readonly aliceUserId: string
		readonly adminUserId: string
		readonly publicChannelId: string
		readonly privateChannelId: string
		readonly call: <S extends Schema.Top>(
			token: string,
			method: string,
			body: SlackApiBody,
			schema: S,
		) => Effect.Effect<S['Type'], Error, S['DecodingServices']>
		readonly signedWebhook: (event: SignedSlackWebhookEvent) => Request
	}
>()('test/SlackEmulator') {
	static readonly layer = Layer.effect(
		SlackEmulator,
		Effect.gen(function* () {
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
					catch: (cause) => new Error(`Could not start Slack emulator on available port ${port}`, { cause }),
				})
			})
			const emulator = yield* Effect.acquireRelease(
				acquireEmulator.pipe(Effect.retry({ times: 4 })),
				(resource) => Effect.promise(() => resource.close()),
			)
			const call = <S extends Schema.Top>(token: string, method: string, body: SlackApiBody, schema: S) =>
				apiCall(emulator, token, method, body, schema)
			const team = yield* call(slackEmulatorBotToken, 'auth.test', {}, TeamResponse)
			const users = yield* call(slackEmulatorBotToken, 'users.list', {}, UsersResponse)
			const conversations = yield* call(
				slackEmulatorAdminToken,
				'conversations.list',
				{ types: 'public_channel,private_channel' },
				ConversationsResponse,
			)
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
				return yield* Effect.fail(
					new Error('Slack emulator seed did not expose the expected users and channels'),
				)
			}
			yield* call(slackEmulatorBotToken, 'conversations.join', { channel: publicChannel.id }, OkResponse)
			yield* call(slackEmulatorIntegrationToken, 'conversations.join', { channel: publicChannel.id }, OkResponse)
			yield* call(
				slackEmulatorAdminToken,
				'conversations.invite',
				{ channel: privateChannel.id, users: slackEmulatorBotUserId },
				OkResponse,
			)
			const signedWebhook = (event: SignedSlackWebhookEvent) => {
				const body = JSON.stringify(event)
				const timestamp = Math.floor(Date.now() / 1000).toString()
				const signature = `v0=${createHmac('sha256', slackEmulatorSigningSecret)
					.update(`v0:${timestamp}:${body}`)
					.digest('hex')}`
				return new Request('http://channels.test/api/v1/integrations/slack/webhook', {
					method: 'POST',
					headers: {
						'content-type': 'application/json',
						'x-slack-request-timestamp': timestamp,
						'x-slack-signature': signature,
					},
					body,
				})
			}
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
	)
}
