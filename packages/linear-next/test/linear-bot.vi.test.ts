import { describe, it } from '@effect/vitest'
import { ChannelsProviderUnavailable, QueueDeliveryMode } from '@humanlayer/channels-delivery-next'
import { Config, ConfigProvider, Effect, Layer, Redacted } from 'effect'

import { LinearApi } from '../src/LinearApi'
import { LinearAuth, LinearBot, LinearOrganizationId, LinearUserId } from '../src'
import { linearWebhookSecret } from './fixtures'

describe('LinearBot.make', () => {
	it.effect('discovers callback configuration while building webhook ingress', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({
				webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
				deliveryMode: QueueDeliveryMode.make({}),
				bot: Config.all({
					organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
					appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
				}),
				auth: LinearAuth.clientCredentials({
					clientId: Config.succeed('test-client'),
					clientSecret: Config.succeed(Redacted.make('test-secret')),
				}),
				linearApi: Layer.mock(LinearApi, {}),
				handlers: {},
			})
			const error = yield* provider
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)
			expect(error).toEqual(ChannelsProviderUnavailable.make({ provider: 'linear' }))
		}),
	)

	it.effect('discovers default API credentials while building webhook ingress', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({
				webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
				deliveryMode: QueueDeliveryMode.make({}),
				bot: {
					organizationId: LinearOrganizationId.make('organization'),
					appUserId: LinearUserId.make('app-user'),
				},
				auth: LinearAuth.clientCredentials({
					clientId: Config.string('LINEAR_CLIENT_ID'),
					clientSecret: Config.redacted('LINEAR_CLIENT_SECRET'),
				}),
				handlers: {},
			})
			const error = yield* provider
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)
			expect(error).toEqual(ChannelsProviderUnavailable.make({ provider: 'linear' }))
		}),
	)
})
