import { describe, it } from '@effect/vitest'
import { SerialDeliveryMode } from '@humanlayer/channels-delivery'
import { Config, ConfigProvider, Effect, Predicate, Redacted, Schema } from 'effect'

import { LinearAuth, LinearBot, LinearOrganizationId, LinearUserId } from '../src'
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
				handlers: {},
			})
			const error = yield* provider
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))), Effect.flip)
			expect(error).toBeInstanceOf(Config.ConfigError)
		}),
	)

	it.effect('builds webhook ingress without the API credentials the application provides', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({
				webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
				bot: {
					organizationId: LinearOrganizationId.make('organization'),
					appUserId: LinearUserId.make('app-user'),
				},
				auth: LinearAuth.clientCredentials({
					clientId: Config.succeed('test-client'),
					clientSecret: Config.Redacted('LINEAR_CLIENT_SECRET'),
				}),
				handlers: {},
			})
			const ingress = yield* provider
				.webhookProvider({ namespace: 'linear-bot-test' })
				.pipe(Effect.scoped, Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))))
			expect(ingress.providerName).toBe('linear')

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

	it.effect('builds both processing halves without a LinearApi, leaving it to the host', ({ expect }) =>
		Effect.gen(function* () {
			const provider = LinearBot.make({ ...providerOptions(), handlers: {} })
			const events = yield* provider.eventProcessor({ namespace: 'linear-bot-test' }).pipe(Effect.scoped)
			const outputs = Predicate.isUndefined(provider.outputProcessor)
				? undefined
				: yield* provider.outputProcessor({ namespace: 'linear-bot-test' }).pipe(Effect.scoped)
			expect([events.providerName, events.namespace]).toEqual(['linear', 'linear-bot-test'])
			expect([outputs?.providerName, outputs?.namespace]).toEqual(['linear', 'linear-bot-test'])
		}),
	)
})
