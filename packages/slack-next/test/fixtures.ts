import { createHmac } from 'node:crypto'
import * as NodeHttp from 'node:http'

import { NodeCrypto } from '@effect/platform-node'
import { createServer, serve } from '@emulators/core'
import { getSlackStore, seedFromConfig, slackPlugin, type SlackSeedConfig } from '@emulators/slack'
import {
	type DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	processProviderEvent,
	type ProviderEventProcessor,
	webhookRoutes,
	type RawWebhookInput,
} from '@humanlayer/channels-delivery-next'
import { Clock, Context, Effect, Queue, Redacted } from 'effect'
import { Headers, HttpRouter } from 'effect/unstable/http'

import { SlackReactionThreadResolver } from '../src/SlackReactionThreadResolver'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'

export const slackEmulatorSigningSecret = 'slack-next-emulator-signing-secret'
export const slackEmulatorBotToken = 'xoxb-slack-next-emulator'
export const slackEmulatorAliceToken = 'xoxp-slack-next-alice'
export const slackEmulatorEventTime = 1_700_000_000

export type SlackEmulatorFixtureOptions = {
	readonly mailboxDelivery: typeof MailboxDelivery.Service
	readonly reactionThreadResolver: typeof SlackReactionThreadResolver.Service
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
					return yield* processProviderEvent(processors)([first, ...rest])
				})
			},
			processNext: (mailboxKey: string) => {
				const mailbox = mailboxes.get(mailboxKey)
				return mailbox === undefined
					? Effect.die(new Error(`In-memory mailbox not found: ${mailboxKey}`))
					: Queue.take(mailbox).pipe(
							Effect.flatMap((admission) => processProviderEvent(processors)([admission])),
						)
			},
		}
	})

export const signedSlackInput = (signingSecret: string, payload: unknown, timestamp = '0'): RawWebhookInput => {
	const bodyText = JSON.stringify(payload)
	const body = new TextEncoder().encode(bodyText)
	const signature = createHmac('sha256', signingSecret).update(`v0:${timestamp}:${bodyText}`).digest('hex')
	return {
		headers: Headers.fromInput({
			'x-slack-request-timestamp': timestamp,
			'x-slack-signature': `v0=${signature}`,
		}),
		body,
	}
}

const slackSeed = {
	team: {
		name: 'Slack Next Test Workspace',
		domain: 'slack-next-test',
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
			client_id: 'slack-next.test',
			client_secret: 'not-a-secret',
			name: 'Slack Next Agent',
			redirect_uris: ['http://localhost/callback'],
			bot_name: 'slack-next-agent',
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

const listen = (server: NodeHttp.Server) =>
	Effect.callback<number, Error>((resume) => {
		server.once('error', (cause) => resume(Effect.fail(cause)))
		server.listen(0, '127.0.0.1', () => {
			const address = server.address()
			if (address === null || typeof address === 'string') {
				resume(Effect.fail(new Error('Emulator server did not expose a TCP port')))
				return
			}
			resume(Effect.succeed(address.port))
		})
	})

const close = (server: NodeHttp.Server) =>
	Effect.promise(
		() =>
			new Promise<void>((resolve, reject) => {
				server.close((cause) => (cause === undefined ? resolve() : reject(cause)))
			}),
	)

const requestListener =
	(handler: (request: Request) => Promise<Response>): NodeHttp.RequestListener =>
	(request, response) => {
		const chunks: Array<Uint8Array> = []
		request.on('data', (chunk: Uint8Array) => chunks.push(chunk))
		request.on('end', () => {
			const body = Buffer.concat(chunks)
			const url = `http://${request.headers.host ?? '127.0.0.1'}${request.url ?? '/'}`
			const headers = new globalThis.Headers()
			for (const [name, value] of Object.entries(request.headers)) {
				if (Array.isArray(value)) {
					for (const item of value) headers.append(name, item)
				} else if (value !== undefined) {
					headers.set(name, value)
				}
			}
			void handler(new Request(url, { method: request.method, headers, body })).then(
				async (result) => {
					response.writeHead(result.status, Object.fromEntries(result.headers))
					response.end(Buffer.from(await result.arrayBuffer()))
				},
				() => {
					response.writeHead(500)
					response.end()
				},
			)
		})
	}

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

		const context = Context.empty().pipe(
			Context.add(MailboxDelivery, options.mailboxDelivery),
			Context.add(SlackReactionThreadResolver, options.reactionThreadResolver),
			Context.add(Clock.Clock, clock),
		)

		const callbackServer = NodeHttp.createServer(requestListener((request) => web.handler(request, context)))
		const callbackPort = yield* listen(callbackServer)
		yield* Effect.addFinalizer(() => close(callbackServer))

		const emulator = createServer(slackPlugin, { baseUrl: 'http://127.0.0.1' })
		slackPlugin.seed?.(emulator.store, emulator.baseUrl)
		seedFromConfig(emulator.store, emulator.baseUrl, slackSeed)

		// @emulators/core defaults to GitHub webhook headers. @emulators/slack stores signing_secret but does not
		// install a Slack signature header factory, so the fixture must sign callbacks before exercising production ingress.
		emulator.webhooks.setHeaderFactory(({ body }) => {
			const signature = createHmac('sha256', slackEmulatorSigningSecret)
				.update(`v0:${webhookTimestamp}:${body}`)
				.digest('hex')
			return {
				'content-type': 'application/json',
				'x-slack-request-timestamp': webhookTimestamp,
				'x-slack-signature': `v0=${signature}`,
			}
		})
		emulator.webhooks.register({
			owner: 'slack',
			url: `http://127.0.0.1:${callbackPort}/integrations/slack/webhook`,
			events: ['*'],
			active: true,
		})

		const emulatorServer = serve({ fetch: emulator.app.fetch, hostname: '127.0.0.1', port: 0 })
		const emulatorPort = yield* listen(emulatorServer)
		yield* Effect.addFinalizer(() => close(emulatorServer))

		const store = getSlackStore(emulator.store)
		const team = store.teams.findOneBy('domain', 'slack-next-test')
		const alice = store.users.findOneBy('name', 'alice')
		const bob = store.users.findOneBy('name', 'bob')
		const channel = store.channels.findOneBy('name', 'agents')
		const app = store.oauthApps.findOneBy('client_id', 'slack-next.test')
		if (
			team === undefined ||
			alice === undefined ||
			bob === undefined ||
			channel === undefined ||
			app === undefined
		) {
			return yield* Effect.fail(new Error('Slack emulator fixture seed is incomplete'))
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
