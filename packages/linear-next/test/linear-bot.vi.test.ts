import { describe, it } from '@effect/vitest'
import { ChannelsProviderUnavailable, SerialDeliveryMode } from '@humanlayer/channels-delivery-next'
import { Config, ConfigProvider, Effect, Layer, Redacted, Schema } from 'effect'

import { LinearAuth, LinearBot, LinearOrganizationId, LinearUserId } from '../src'
import { LinearApi } from '../src/LinearApi'
import { unusedLinearApi } from './api-test-fixtures'
import { linearWebhookSecret } from './fixtures'

const providerOptions = () => ({
	webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
	bot: {
		organizationId: LinearOrganizationId.make('organization'),
		appUserId: LinearUserId.make('app-user'),
	},
	auth: LinearAuth.clientCredentials({
		clientId: Config.succeed('test-client'),
		clientSecret: Config.succeed(Redacted.make('test-secret')),
	}),
})

describe('LinearBot.make', () => {
	it.effect('selects a developer token before falling back to client credentials', ({ expect }) =>
		Effect.gen(function* () {
			const developer = yield* LinearAuth.fromEnvironment.pipe(
				Effect.provide(
					ConfigProvider.layer(
						ConfigProvider.fromUnknown({
							LINEAR_DEVELOPER_TOKEN: 'developer-token',
							LINEAR_CLIENT_ID: 'ignored-client',
							LINEAR_CLIENT_SECRET: 'ignored-secret',
						}),
					),
				),
			)
			expect(Schema.is(LinearAuth.LinearDeveloperToken)(developer)).toBe(true)

			const client = yield* LinearAuth.fromEnvironment.pipe(
				Effect.provide(
					ConfigProvider.layer(
						ConfigProvider.fromUnknown({
							LINEAR_CLIENT_ID: 'selected-client',
							LINEAR_CLIENT_SECRET: 'selected-secret',
						}),
					),
				),
			)
			expect(Schema.is(LinearAuth.LinearClientCredentials)(client)).toBe(true)
		}),
	)

	it.effect('discovers callback configuration while building webhook ingress', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({
				webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
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

	it.effect('discovers default API configuration while keeping construction network-free', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({
				webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
				bot: {
					organizationId: LinearOrganizationId.make('organization'),
					appUserId: LinearUserId.make('app-user'),
				},
				auth: LinearAuth.clientCredentials({
					clientId: Config.succeed('test-client'),
					clientSecret: Config.redacted('LINEAR_CLIENT_SECRET'),
				}),
				handlers: {},
			})
			const error = yield* provider
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)
			expect(error).toEqual(ChannelsProviderUnavailable.make({ provider: 'linear' }))

			const configured = yield* LinearBot.make({
				...providerOptions(),
				handlers: {},
			})
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped)
			expect(configured.providerName).toBe('linear')
			expect(provider.deliveryMode).toEqual(SerialDeliveryMode.make({}))
		}),
	)

	it.effect('discovers custom API layer configuration in webhook ingress', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({
				...providerOptions(),
				linearApi: Layer.effect(
					LinearApi,
					Effect.gen(function* () {
						yield* Config.string('CUSTOM_LINEAR_API_CONFIGURATION')
						return LinearApi.of(unusedLinearApi)
					}),
				),
				handlers: {},
			})
			const error = yield* provider
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)
			expect(error).toEqual(ChannelsProviderUnavailable.make({ provider: 'linear' }))
		}),
	)
})
