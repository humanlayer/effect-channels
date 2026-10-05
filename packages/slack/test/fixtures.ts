import { NodeCrypto, NodeHttpServer } from '@effect/platform-node'
import { createServer, type WebhookDispatcher } from '@emulators/core'
import { getSlackStore, seedFromConfig, slackPlugin, type SlackSeedConfig } from '@emulators/slack'
import {
	type DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	processProviderEvent,
	type ProviderEventProcessor,
	webhookRoutes,
	type RawWebhookInput,
} from '@humanlayer/channels-delivery'
import { Clock, Context, Effect, Layer, Match, Queue, Redacted, Schema } from 'effect'
import { Headers, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from 'effect/http'

import { makeTestDeliveryExecution } from '../../delivery/test/delivery-execution'
import { SlackApi } from '../src/SlackApi'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signSlackBody } from '../src/SlackWebhookSignature'

export const slackEmulatorSigningSecret = 'slack-emulator-signing-secret'
export const slackEmulatorBotToken = 'xoxb-slack-emulator'
export const slackEmulatorAliceToken = 'xoxp-slack-alice'
export const slackEmulatorEventTime = 1_700_000_000

export type SlackEmulatorFixtureOptions = {
	readonly mailboxDelivery: typeof MailboxDelivery.Service
	readonly resolveReactionThread: (typeof SlackApi.Service)['resolveReactionThread']
}

const inMemoryMailboxKey = (admission: DeliveryAdmission) =>
	[admission.namespace, admission.provider, admission.installationId, admission.resourceId]
		.map((segment) => `${segment.length}:${segment}`)
		.join('|')

/** Test-only keyed mailbox that stores admissions until the test explicitly processes one. */
export const makeInMemoryMailboxFixture = <R>(processors: ReadonlyArray<ProviderEventProcessor<R>>) =>
	Effect.sync(() => {
		const mailboxes = new Map<string, Queue.Queue<DeliveryAdmission>>()

		const mailboxDelivery: typeof MailboxDelivery.Service = {
			deliver: (admission) =>
				Effect.gen(function* () {
					const key = inMemoryMailboxKey(admission)
					let mailbox = mailboxes.get(key)
					if (mailbox === undefined) {
						mailbox = yield* Queue.unbounded<DeliveryAdmission>()
						mailboxes.set(key, mailbox)
					}
					yield* Queue.offer(mailbox, admission)
					return DeliveryReceipt.make({ mailboxKey: key, accepted: true })
				}),
		}

		return {
			mailboxDelivery,
			mailboxKeys: Effect.sync(() => Array.from(mailboxes.keys())),
			processBatch: (mailboxKey: string, count?: number) => {
				const mailbox = mailboxes.get(mailboxKey)
				if (mailbox === undefined) return Effect.die(new Error(`In-memory mailbox not found: ${mailboxKey}`))
				return Effect.gen(function* () {
					const available = yield* Queue.size(mailbox)
					const requested = count ?? available
					const takeCount = Math.min(requested, available)
					if (takeCount < 1) return yield* Effect.die(new Error(`In-memory mailbox is empty: ${mailboxKey}`))
					const first = yield* Queue.take(mailbox)
					const rest = yield* Effect.forEach(Array.from({ length: takeCount - 1 }), () => Queue.take(mailbox))
					const { execution } = yield* makeTestDeliveryExecution(mailboxKey)
					return yield* processProviderEvent(processors)([first, ...rest], execution)
				})
			},
			processNext: (mailboxKey: string) => {
				const mailbox = mailboxes.get(mailboxKey)
				return mailbox === undefined
					? Effect.die(new Error(`In-memory mailbox not found: ${mailboxKey}`))
					: Queue.take(mailbox).pipe(
							Effect.flatMap((admission) =>
								Effect.flatMap(makeTestDeliveryExecution(mailboxKey), ({ execution }) =>
									processProviderEvent(processors)([admission], execution),
								),
							),
						)
			},
		}
	})

const textEncoder = new TextEncoder()

/** Signs raw body bytes the way Slack does, for webhook inputs that production ingress must authenticate. */
export const signedSlackBody = (signingSecret: string, body: Uint8Array, timestamp = '0') =>
	signSlackBody({ signingSecret: Redacted.make(signingSecret), timestamp, body }).pipe(
		Effect.map((signature): RawWebhookInput => ({
			headers: Headers.fromInput({
				'x-slack-request-timestamp': timestamp,
				'x-slack-signature': signature,
			}),
			body,
		})),
		Effect.provide(NodeCrypto.layer),
	)

/** Signs an arbitrary JSON webhook payload, including payloads production ingress must reject. */
export const signedSlackInput = (signingSecret: string, payload: Schema.Json, timestamp = '0') =>
	Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(payload).pipe(
		Effect.flatMap((bodyText) => signedSlackBody(signingSecret, textEncoder.encode(bodyText), timestamp)),
	)

export class SlackEmulatorFixtureError extends Schema.TaggedError<SlackEmulatorFixtureError>()(
	'SlackEmulatorFixtureError',
	{ reason: Schema.Literals(['non_tcp_address', 'incomplete_seed']) },
) {}

const slackSeed = {
	team: {
		name: 'Slack Test Workspace',
		domain: 'slack-test',
	},
	users: [
		{
			name: 'alice',
			real_name: 'Alice Example',
			email: 'alice@example.test',
			presence: 'active',
		},
		{
			name: 'bob',
			real_name: 'Bob Example',
			email: 'bob@example.test',
			presence: 'away',
		},
	],
	channels: [
		{
			name: 'agents',
			topic: 'Agent integration tests',
			purpose: 'Exercise Slack webhook delivery',
		},
	],
	oauth_apps: [
		{
			app_id: 'A_SLACK_NEXT',
			client_id: 'slack.test',
			client_secret: 'not-a-secret',
			name: 'Slack Agent',
			redirect_uris: ['http://localhost/callback'],
			bot_name: 'slack-agent',
			bot_id: 'B_SLACK_NEXT',
			bot_user_id: 'U_SLACK_NEXT',
		},
	],
	tokens: [
		{
			token: slackEmulatorBotToken,
			type: 'bot',
			user_id: 'U_SLACK_NEXT',
			app_id: 'A_SLACK_NEXT',
			bot_id: 'B_SLACK_NEXT',
			bot_user_id: 'U_SLACK_NEXT',
		},
		{
			token: slackEmulatorAliceToken,
			type: 'user',
			user: 'alice',
		},
	],
	signing_secret: slackEmulatorSigningSecret,
	strict_scopes: false,
} satisfies SlackSeedConfig

/** Serves an HTTP app on a fresh ephemeral TCP port for the current scope and returns the port. */
const serveOnEphemeralPort = <E, R>(app: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>) =>
	Effect.gen(function* () {
		const server = yield* Layer.build(Layer.fresh(NodeHttpServer.layerTest))
		yield* HttpServer.serveEffect(app).pipe(Effect.provideContext(server))
		return yield* Match.value(Context.get(server, HttpServer.HttpServer).address).pipe(
			Match.tagsExhaustive({
				InetAddressV4: (address) => Effect.succeed(address.port),
				InetAddressV6: (address) => Effect.succeed(address.port),
				UnixPathAddress: () => Effect.fail(SlackEmulatorFixtureError.make({ reason: 'non_tcp_address' })),
			}),
		)
	})

/**
 * `@emulators/core` defaults to GitHub webhook headers and `@emulators/slack` does not sign callbacks, so the
 * emulator posts plain JSON and the callback server signs each body before exercising production ingress.
 */
const sendJsonCallbacks = (webhooks: WebhookDispatcher) =>
	webhooks.setHeaderFactory(() => ({ 'content-type': 'application/json' }))

/**
 * Scoped Slack emulator fixture with seeded users, an app, a channel, and a real callback URL wired to the
 * delivery HttpRouter and SlackWebhookProvider.
 */
export const makeSlackEmulatorFixture = (options: SlackEmulatorFixtureOptions) =>
	Effect.gen(function* () {
		const clock = yield* Clock.Clock
		const webhookTimestamp = String(Math.floor((yield* Clock.currentTimeMillis) / 1000))
		const provider = makeSlackWebhookProvider({
			namespace: 'slack-emulator-test',
			signingSecret: Redacted.make(slackEmulatorSigningSecret),
		})
		const routes = webhookRoutes([provider]).pipe(HttpRouter.provideRequest(NodeCrypto.layer))
		const web = HttpRouter.toWebHandler(routes, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))

		const slackApiContext = yield* Layer.build(
			Layer.mock(SlackApi, { resolveReactionThread: options.resolveReactionThread }),
		)
		const context = Context.empty().pipe(
			Context.add(MailboxDelivery, options.mailboxDelivery),
			Context.add(Clock.Clock, clock),
			Context.merge(slackApiContext),
		)

		const callbackPort = yield* serveOnEphemeralPort(
			Effect.gen(function* () {
				const request = yield* HttpServerRequest.HttpServerRequest
				const body = yield* request.arrayBuffer
				const signed = yield* signedSlackBody(
					slackEmulatorSigningSecret,
					new Uint8Array(body),
					webhookTimestamp,
				)
				const response = yield* Effect.promise(() =>
					web.handler(
						new Request(`http://127.0.0.1${request.url}`, {
							method: request.method,
							headers: signed.headers,
							body,
						}),
						context,
					),
				)
				return HttpServerResponse.fromWeb(response)
			}),
		)

		const emulator = createServer(slackPlugin, { baseUrl: 'http://127.0.0.1' })
		slackPlugin.seed?.(emulator.store, emulator.baseUrl)
		seedFromConfig(emulator.store, emulator.baseUrl, slackSeed)

		sendJsonCallbacks(emulator.webhooks)
		emulator.webhooks.register({
			owner: 'slack',
			url: `http://127.0.0.1:${callbackPort}/integrations/slack/webhook`,
			events: ['*'],
			active: true,
		})

		const emulatorPort = yield* serveOnEphemeralPort(
			Effect.gen(function* () {
				const request = yield* HttpServerRequest.toWeb(yield* HttpServerRequest.HttpServerRequest)
				return HttpServerResponse.fromWeb(yield* Effect.promise(() => emulator.app.fetch(request)))
			}),
		)

		const store = getSlackStore(emulator.store)
		const team = store.teams.findOneBy('domain', 'slack-test')
		const alice = store.users.findOneBy('name', 'alice')
		const bob = store.users.findOneBy('name', 'bob')
		const channel = store.channels.findOneBy('name', 'agents')
		const app = store.oauthApps.findOneBy('client_id', 'slack.test')
		if (
			team === undefined ||
			alice === undefined ||
			bob === undefined ||
			channel === undefined ||
			app === undefined
		) {
			return yield* SlackEmulatorFixtureError.make({ reason: 'incomplete_seed' })
		}

		return {
			url: `http://127.0.0.1:${emulatorPort}`,
			teamId: team.team_id,
			aliceUserId: alice.user_id,
			bobUserId: bob.user_id,
			channelId: channel.channel_id,
			appId: app.app_id,
			botToken: slackEmulatorBotToken,
			aliceToken: slackEmulatorAliceToken,
			webhooks: emulator.webhooks,
		}
	})
