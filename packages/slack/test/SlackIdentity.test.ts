import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { ConfigProvider, Context, Effect, Layer, Queue, Redacted, Ref, Schema } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { CredentialStoreError } from '../src/Errors'
import {
	IngressAccepted,
	IngressDropped,
	SlackIngress as Ingress,
	UserId,
	type IngressResult,
	type Message,
	type NormalizedMessage,
} from '../src/index'
import {
	SlackEventCallback,
	SlackGetUserInput,
	SlackHistoryInput,
	SlackMessageTs,
	SlackRepliesInput,
	SlackTeamId,
	type SlackLoadCredentialsInput,
} from '../src/Schema'
import { SlackClient } from '../src/SlackClient'
import { SlackRoutes } from '../src/SlackRoutes'
import { SlackTenantCredentials } from '../src/SlackTenantCredentials'
import {
	makeSlackClientHarness,
	makeTestIngress,
	signedSlackRequest,
	slackJsonResponse,
	testBotToken,
	testRouteSlackClientLayer,
	testChannelId,
	type RecordedSlackRequest,
} from './support'

const identityIngressLayer = Layer.succeed(
	Ingress,
	makeTestIngress({
		acceptMessage: (message) =>
			Effect.succeed(
				message.message.author.isMe
					? IngressDropped.make({ reason: 'bot' })
					: IngressAccepted.make({ idempotencyKey: message.idempotencyKey }),
			),
	}),
)

type EncodedCallback = typeof SlackEventCallback.Encoded

interface RecordedIngress {
	readonly message: NormalizedMessage
	readonly result: IngressResult
}

const loadIdentityCredentials = (input: SlackLoadCredentialsInput) => {
	if (input.teamId === 'T_TENANT') {
		return Effect.succeedSome({ botToken: Redacted.make(testBotToken), botUserId: 'UTENANT' })
	}
	if (input.teamId === 'T_PLAIN') {
		return Effect.succeedSome({ botToken: Redacted.make(testBotToken) })
	}
	if (input.teamId === 'T_BROKEN') {
		return Effect.fail(CredentialStoreError.make({ operation: 'load', teamId: input.teamId }))
	}
	return Effect.succeedNone
}

const identityCredentialsLayer = SlackTenantCredentials.make({
	load: loadIdentityCredentials,
	save: () => Effect.void,
})

const routeLayer = SlackRoutes.layer.pipe(
	HttpRouter.provideRequest(NodeCrypto.layer),
	Layer.provide(
		ConfigProvider.layer(
			ConfigProvider.fromUnknown({
				SLACK_SIGNING_SECRET: 'test-signing-secret',
				SLACK_BOT_USER_ID: 'UCONFIG',
			}),
		),
	),
	Layer.provide(identityCredentialsLayer),
	Layer.provide(testRouteSlackClientLayer),
)

const mentionFrom = (teamId: string, user: string, ts: string, text: string): EncodedCallback => ({
	type: 'event_callback',
	team_id: teamId,
	event_id: `Ev_${teamId}_${user}_${ts}`,
	event_time: 1_788_000_000,
	event: { type: 'app_mention', user, text, ts, channel: 'C_TEST' },
})

const deliver = (
	handler: (request: Request, context: Context.Context<Ingress>) => Promise<Response>,
	recorded: Queue.Queue<RecordedIngress>,
	callback: EncodedCallback,
) =>
	Effect.gen(function* () {
		const ingress = yield* Ingress
		const recording = Ingress.of({
			...ingress,
			acceptMessage: (message) =>
				ingress.acceptMessage(message).pipe(Effect.tap((result) => Queue.offer(recorded, { message, result }))),
		})
		const decoded = yield* Schema.decodeEffect(SlackEventCallback)(callback)
		const request = yield* signedSlackRequest(decoded)
		const response = yield* Effect.promise(() => handler(request, Context.make(Ingress, recording)))
		assert.strictEqual(response.status, 200)
		return yield* Queue.take(recorded)
	})

const withWebhook = <A, E, R>(
	program: (
		handler: (request: Request, context: Context.Context<Ingress>) => Promise<Response>,
		recorded: Queue.Queue<RecordedIngress>,
	) => Effect.Effect<A, E, R>,
) =>
	Effect.gen(function* () {
		const recorded = yield* Queue.unbounded<RecordedIngress>()
		const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		return yield* program(handler, recorded)
	}).pipe(Effect.provide(Layer.merge(identityIngressLayer, NodeCrypto.layer)))

const droppedAsBot = IngressDropped.make({ reason: 'bot' })
const acceptedFor = (recorded: RecordedIngress) =>
	IngressAccepted.make({ idempotencyKey: recorded.message.idempotencyKey })

it.effect('drops the tenant bot user as our own echo and lets the configured bot user through in that team', () =>
	withWebhook((handler, recorded) =>
		Effect.gen(function* () {
			const ownEcho = yield* deliver(handler, recorded, mentionFrom('T_TENANT', 'UTENANT', '200.1', 'done'))
			assert.strictEqual(ownEcho.message.message.author.isMe, true)
			assert.deepStrictEqual(ownEcho.result, droppedAsBot)

			const configUser = yield* deliver(
				handler,
				recorded,
				mentionFrom('T_TENANT', 'UCONFIG', '200.2', '<@UTENANT> hello from the config user'),
			)
			assert.strictEqual(configUser.message.message.author.isMe, false)
			assert.strictEqual(configUser.message.message.text, 'hello from the config user')
			assert.deepStrictEqual(configUser.result, acceptedFor(configUser))
			assert.strictEqual(yield* Queue.size(recorded), 0)
		}),
	),
)

it.effect('uses the configured bot identity only after the workspace credentials resolve', () =>
	withWebhook((handler, recorded) =>
		Effect.gen(function* () {
			const ownEcho = yield* deliver(handler, recorded, mentionFrom('T_PLAIN', 'UCONFIG', '300.1', 'done'))
			assert.strictEqual(ownEcho.message.message.author.isMe, true)
			assert.deepStrictEqual(ownEcho.result, droppedAsBot)

			const human = yield* deliver(
				handler,
				recorded,
				mentionFrom('T_PLAIN', 'U_HUMAN', '300.2', '<@UCONFIG> hello from a human'),
			)
			assert.strictEqual(human.message.message.author.isMe, false)
			assert.strictEqual(human.message.message.text, 'hello from a human')
			assert.deepStrictEqual(human.result, acceptedFor(human))
		}),
	),
)

it.effect('isolates unknown and failed workspace credential lookups before ingress', () =>
	withWebhook((handler, recorded) =>
		Effect.gen(function* () {
			const request = (callback: EncodedCallback) =>
				Effect.gen(function* () {
					const decoded = yield* Schema.decodeEffect(SlackEventCallback)(callback)
					const signed = yield* signedSlackRequest(decoded)
					return yield* Effect.promise(() => handler(signed, Context.make(Ingress, makeTestIngress({}))))
				})
			const unknown = yield* request(mentionFrom('T_UNKNOWN', 'U_HUMAN', '400.1', '<@UCONFIG> hidden'))
			const failed = yield* request(mentionFrom('T_BROKEN', 'U_HUMAN', '400.2', '<@UCONFIG> hidden'))
			assert.strictEqual(unknown.status, 200)
			assert.strictEqual(failed.status, 503)
			assert.strictEqual(yield* Queue.size(recorded), 0)
		}),
	),
)

const clientIdentityConfigLayer = ConfigProvider.layer(ConfigProvider.fromUnknown({ SLACK_BOT_USER_ID: 'UCONFIG' }))

const threadOldestFirst = [
	{ type: 'message', user: 'U_HUMAN', text: 'root', ts: '500.1', thread_ts: '500.1' },
	{ type: 'message', user: 'UTENANT', text: 'tenant bot answer', ts: '500.2', thread_ts: '500.1' },
	{ type: 'message', user: 'UCONFIG', text: 'config bot answer', ts: '500.3', thread_ts: '500.1' },
]

const respondWithThread = (request: RecordedSlackRequest) => {
	if (request.url.pathname === '/api/users.info') {
		const user = { id: request.url.searchParams.get('user'), name: 'bot', is_bot: true }
		return slackJsonResponse(JSON.stringify({ ok: true, user }))
	}
	return slackJsonResponse(JSON.stringify({ ok: true, messages: threadOldestFirst, has_more: false }))
}

const makeCountingCredentials = Effect.gen(function* () {
	const loads = yield* Ref.make(0)
	const layer = SlackTenantCredentials.make({
		load: (input) => Ref.update(loads, (count) => count + 1).pipe(Effect.andThen(loadIdentityCredentials(input))),
		save: () => Effect.void,
	})
	return { loads, layer }
})

const isMeByUser = (messages: ReadonlyArray<Message>) =>
	messages.map((message) => [message.author.userId, message.author.isMe])

const clientCalls = (teamId: string) => {
	const team = SlackTeamId.make(teamId)
	const threadTs = SlackMessageTs.make('500.1')
	return {
		replies: Effect.flatMap(SlackClient, (client) =>
			client.replies(
				SlackRepliesInput.make({ teamId: team, channelId: testChannelId, threadTs, direction: 'forward' }),
			),
		),
		history: Effect.flatMap(SlackClient, (client) =>
			client.history(SlackHistoryInput.make({ teamId: team, channelId: testChannelId })),
		),
		getUser: (userId: string) =>
			Effect.flatMap(SlackClient, (client) =>
				client.getUser(SlackGetUserInput.make({ teamId: team, userId: UserId.make(userId) })),
			),
	}
}

it.effect(
	'marks the tenant bot user as isMe in client replies, history, and user lookups from one creds load per call',
	() =>
		Effect.gen(function* () {
			const credentials = yield* makeCountingCredentials
			const harness = yield* makeSlackClientHarness(
				respondWithThread,
				credentials.layer,
				clientIdentityConfigLayer,
			)
			const tenant = clientCalls('T_TENANT')
			yield* Effect.gen(function* () {
				const replies = yield* tenant.replies
				assert.deepStrictEqual(isMeByUser(replies.messages), [
					['U_HUMAN', false],
					['UTENANT', true],
					['UCONFIG', false],
				])
				const history = yield* tenant.history
				assert.deepStrictEqual(isMeByUser(history.messages), [
					['U_HUMAN', false],
					['UTENANT', true],
					['UCONFIG', false],
				])
				const tenantBot = yield* tenant.getUser('UTENANT')
				assert.strictEqual(tenantBot.author.isMe, true)
				const configBot = yield* tenant.getUser('UCONFIG')
				assert.strictEqual(configBot.author.isMe, false)
			}).pipe(Effect.provide(harness.layer))
			assert.strictEqual(yield* Ref.get(credentials.loads), 4)
		}),
)

it.effect('keeps the configured bot identity for client calls when the stored creds carry none', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(
			respondWithThread,
			identityCredentialsLayer,
			clientIdentityConfigLayer,
		)
		const plain = clientCalls('T_PLAIN')
		yield* Effect.gen(function* () {
			const replies = yield* plain.replies
			assert.deepStrictEqual(isMeByUser(replies.messages), [
				['U_HUMAN', false],
				['UTENANT', false],
				['UCONFIG', true],
			])
			const configBot = yield* plain.getUser('UCONFIG')
			assert.strictEqual(configBot.author.isMe, true)
			const tenantBot = yield* plain.getUser('UTENANT')
			assert.strictEqual(tenantBot.author.isMe, false)
		}).pipe(Effect.provide(harness.layer))
	}),
)
