import { assert, it } from '@effect/vitest'
import { TenantId, UserId } from '@humanlayer/channels'
import { Effect, Layer, Queue } from 'effect'

import { expectTaggedFailure } from '../../channels/test/support.ts'
import { SlackGetUserInput } from '../src/Schema.ts'
import { SlackClient } from '../src/SlackClient.ts'
import { SlackProvider } from '../src/SlackProvider.ts'
import {
	makeSlackClientHarness,
	slackJsonResponse,
	testBotToken,
	testTeamId,
	unknownTenantCredentialsLayer,
	type RecordedSlackRequest,
} from './support.ts'

const params = (request: RecordedSlackRequest) => Object.fromEntries(request.url.searchParams)

const humanUser = {
	id: 'U_HUMAN',
	name: 'jane',
	real_name: 'Jane Doe',
	is_bot: false,
	profile: {
		display_name: 'jane.d',
		real_name: 'Jane Doe',
		email: 'jane@example.com',
		image_192: 'https://avatars.example/jane.png',
	},
}

const botUser = { id: 'U_BOT', name: 'channels-bot', is_bot: true }

const usersInfoResponse = (request: RecordedSlackRequest) => {
	const userId = request.url.searchParams.get('user')
	if (userId === 'U_HUMAN') {
		return slackJsonResponse(JSON.stringify({ ok: true, user: humanUser }))
	}
	if (userId === 'U_BOT') {
		return slackJsonResponse(JSON.stringify({ ok: true, user: botUser }))
	}
	return slackJsonResponse('{"ok":false,"error":"user_not_found"}')
}

const getUser = (userId: string) =>
	Effect.flatMap(SlackClient, (client) =>
		client.getUser(SlackGetUserInput.make({ teamId: testTeamId, userId: UserId.make(userId) })),
	)

it.effect('decodes users.info into a UserProfile with email and avatar', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(usersInfoResponse)
		const profile = yield* getUser('U_HUMAN').pipe(Effect.provide(harness.layer))
		const request = yield* Queue.take(harness.requests)
		assert.strictEqual(request.method, 'GET')
		assert.strictEqual(request.url.pathname, '/api/users.info')
		assert.strictEqual(request.authorization, `Bearer ${testBotToken}`)
		assert.deepStrictEqual(params(request), { user: 'U_HUMAN' })
		assert.deepStrictEqual(profile.author, {
			userId: UserId.make('U_HUMAN'),
			userName: 'jane.d',
			fullName: 'Jane Doe',
			isBot: false,
			isMe: false,
		})
		assert.strictEqual(profile.email, 'jane@example.com')
		assert.strictEqual(profile.avatarUrl?.href, 'https://avatars.example/jane.png')
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)

it.effect('marks the configured bot user as isMe and falls back to the handle for names', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(usersInfoResponse)
		const profile = yield* getUser('U_BOT').pipe(Effect.provide(harness.layer))
		assert.deepStrictEqual(profile, {
			author: {
				userId: UserId.make('U_BOT'),
				userName: 'channels-bot',
				fullName: 'channels-bot',
				isBot: true,
				isMe: true,
			},
		})
	}),
)

it.effect('surfaces user_not_found as SlackApiError', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(usersInfoResponse)
		const error = yield* expectTaggedFailure('SlackApiError')(getUser('U_MISSING')).pipe(
			Effect.provide(harness.layer),
		)
		assert.strictEqual(error.operation, 'users.info')
		assert.strictEqual(error.code, 'user_not_found')
	}),
)

it.effect('passes UnknownTenant through the provider without calling Slack', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness(usersInfoResponse, unknownTenantCredentialsLayer)
		const error = yield* Effect.flatMap(SlackProvider, (provider) =>
			expectTaggedFailure('UnknownTenant')(
				provider.getUser({
					provider: 'slack',
					tenant: TenantId.make('T_TEST'),
					userId: UserId.make('U_HUMAN'),
				}),
			),
		).pipe(Effect.provide(SlackProvider.layer.pipe(Layer.provide(harness.layer))))
		assert.strictEqual(error.provider, 'slack')
		assert.strictEqual(error.tenant, 'T_TEST')
		assert.strictEqual(yield* Queue.size(harness.requests), 0)
	}),
)

it.effect('returns the profile through the provider and narrows Slack transport failures to UserLookupFailed', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.searchParams.get('user') === 'U_HUMAN'
				? usersInfoResponse(request)
				: slackJsonResponse('gateway timeout', 504),
		)
		yield* Effect.gen(function* () {
			const provider = yield* SlackProvider
			const profile = yield* provider.getUser({
				provider: 'slack',
				tenant: TenantId.make('T_TEST'),
				userId: UserId.make('U_HUMAN'),
			})
			assert.strictEqual(profile.author.userName, 'jane.d')

			const error = yield* expectTaggedFailure('UserLookupFailed')(
				provider.getUser({ provider: 'slack', tenant: TenantId.make('T_TEST'), userId: UserId.make('U_BOT') }),
			)
			assert.strictEqual(error.provider, 'slack')
			assert.strictEqual(error.tenant, 'T_TEST')
			assert.strictEqual(error.userId, 'U_BOT')
			assert.strictEqual(error.reason, 'transport')
		}).pipe(Effect.provide(SlackProvider.layer.pipe(Layer.provide(harness.layer))))
	}),
)

it.effect('narrows user_not_found to UserLookupFailed not_found and other API errors to api', () =>
	Effect.gen(function* () {
		const harness = yield* makeSlackClientHarness((request) =>
			request.url.searchParams.get('user') === 'U_RATELIMITED'
				? slackJsonResponse('{"ok":false,"error":"ratelimited"}')
				: usersInfoResponse(request),
		)
		yield* Effect.gen(function* () {
			const provider = yield* SlackProvider
			const missing = yield* expectTaggedFailure('UserLookupFailed')(
				provider.getUser({
					provider: 'slack',
					tenant: TenantId.make('T_TEST'),
					userId: UserId.make('U_MISSING'),
				}),
			)
			assert.strictEqual(missing.userId, 'U_MISSING')
			assert.strictEqual(missing.reason, 'not_found')

			const rejected = yield* expectTaggedFailure('UserLookupFailed')(
				provider.getUser({
					provider: 'slack',
					tenant: TenantId.make('T_TEST'),
					userId: UserId.make('U_RATELIMITED'),
				}),
			)
			assert.strictEqual(rejected.reason, 'api')
		}).pipe(Effect.provide(SlackProvider.layer.pipe(Layer.provide(harness.layer))))
	}),
)
