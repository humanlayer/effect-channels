/**
 * A small application written only against the published packages. The release check typechecks it
 * with `NodeNext` resolution and `skipLibCheck: false`, so it fails on any declaration file that a
 * strict Node.js project cannot read.
 */
import { ChannelsCloudflare } from '@humanlayer/channels-alchemy-cloudflare'
import {
	Channels,
	ChannelsMemory,
	DeliveryActivity,
	QueueDeliveryMode,
	makeDeliveryClient,
	type DeliveryContext,
} from '@humanlayer/channels-delivery'
import { GitHubBot, GitHubId } from '@humanlayer/channels-github'
import { LinearAuth, LinearBot, LinearOrganizationId, LinearUserId } from '@humanlayer/channels-linear'
import { ChannelsRedis } from '@humanlayer/channels-redis'
import { SlackBot, SlackContent } from '@humanlayer/channels-slack'
import { ChannelsSql } from '@humanlayer/channels-sql'
import { Config, Effect, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/http'

const handOff = (delivery: DeliveryContext) => delivery.handoff({ failAfter: '1 hour' })

const slack = SlackBot.make({
	signingSecret: Config.Redacted('SLACK_SIGNING_SECRET'),
	deliveryMode: QueueDeliveryMode.make({}),
	handlers: {
		onNewMention: (event, delivery) =>
			Effect.gen(function* () {
				yield* event.thread.post(SlackContent.make({ markdown: 'On it.' }))
				return yield* handOff(delivery)
			}),
	},
})

const github = GitHubBot.make({
	webhookSecret: Config.Redacted('GITHUB_WEBHOOK_SECRET'),
	deliveryMode: QueueDeliveryMode.make({}),
	bot: { mentionNames: ['my-bot'], botUserId: GitHubId.make(1) },
	handlers: { onMentioned: (_event, delivery) => handOff(delivery) },
})

const linear = LinearBot.make({
	webhookSecret: Config.Redacted('LINEAR_WEBHOOK_SECRET'),
	bot: Config.all({
		organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
		appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
	}),
	auth: LinearAuth.fromEnvironment,
	handlers: { onAgentSessionCreated: (event) => event.session.respond('Hello.').pipe(Effect.asVoid) },
})

const eventProcessing = { concurrency: 1, maxAttempts: 3, leaseMs: 30_000 }

export const memoryBot = Channels.make({
	namespace: 'consumer',
	providers: [slack, github, linear],
	eventProcessing,
	storage: ChannelsMemory.make({ polling: { intervalMs: 1_000 } }),
})
export const postgresStorage = ChannelsSql.make({ claimLimit: 10, runMigrations: true, polling: { intervalMs: 1_000 } })
export const redisStorage = ChannelsRedis.make({ claimLimit: 10, polling: { intervalMs: 1_000 } })
export const cloudflareBot = ChannelsCloudflare.make({ namespace: 'consumer', providers: [slack], eventProcessing })

export const reportProgress = (deliveryId: string, accessToken: string) =>
	Effect.gen(function* () {
		const api = yield* makeDeliveryClient({ baseUrl: 'https://agent.example.com' })
		yield* api.activity.set({
			deliveryId,
			accessToken: Redacted.make(accessToken),
			activity: DeliveryActivity.cases.Working.make({ message: 'Working' }),
		})
	}).pipe(Effect.provide(FetchHttpClient.layer))
