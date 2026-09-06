import { assert, it } from '@effect/vitest'
import {
	SlackConnection,
	SlackConnectionCredentials,
	SlackTeamId,
	SlackTenantCredentials,
} from '@humanlayer/channels-slack'
import { Deferred, Effect, Fiber, Option, Redacted, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { makeConnectionServices, slack } from './support/SlackTestHost.ts'

it.effect('isolates workspace credentials and caches each callback result', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const provider = slack({
			loadConnection: ({ workspaceId }) =>
				Ref.update(calls, (n) => n + 1).pipe(
					Effect.as(
						workspaceId === 'T_UNKNOWN'
							? undefined
							: SlackConnection.make({
									credentials: SlackConnectionCredentials.make({
										botToken: Redacted.make(`token-${workspaceId}`),
										botUserId: `U_${workspaceId}`,
										botId: `B_${workspaceId}`,
									}),
								}),
					),
				),
		})
		yield* Effect.gen(function* () {
			const credentials = yield* SlackTenantCredentials
			const a = Option.getOrThrow(yield* credentials.load({ teamId: SlackTeamId.make('T_A') }))
			const b = Option.getOrThrow(yield* credentials.load({ teamId: SlackTeamId.make('T_B') }))
			assert.strictEqual(Redacted.value(a.botToken), 'token-T_A')
			assert.strictEqual(a.botUserId, 'U_T_A')
			assert.strictEqual(a.botId, 'B_T_A')
			assert.strictEqual(Redacted.value(b.botToken), 'token-T_B')
			yield* credentials.load({ teamId: SlackTeamId.make('T_A') })
			assert.ok(Option.isNone(yield* credentials.load({ teamId: SlackTeamId.make('T_UNKNOWN') })))
			assert.strictEqual(yield* Ref.get(calls), 3)
		}).pipe(Effect.provide(makeConnectionServices(provider)))
	}),
)

it.effect('narrows malformed callback data without exposing credentials', () =>
	Effect.gen(function* () {
		const credentials = yield* SlackTenantCredentials
		const error = yield* Effect.flip(credentials.load({ teamId: SlackTeamId.make('T_BAD') }))
		assert.strictEqual(error._tag, 'CredentialStoreError')
		assert.strictEqual(error.operation, 'load')
	}).pipe(
		Effect.provide(
			makeConnectionServices(slack({ loadConnection: () => Effect.succeed({ credentials: 'malformed' }) })),
		),
	),
)

it.effect('narrows callback failure and refuses writes to read-only connections', () =>
	Effect.gen(function* () {
		const credentials = yield* SlackTenantCredentials
		const teamId = SlackTeamId.make('T_BAD')
		assert.strictEqual((yield* Effect.flip(credentials.load({ teamId }))).operation, 'load')
		assert.strictEqual(
			(yield* Effect.flip(credentials.save({ teamId, credentials: { botToken: Redacted.make('not-saved') } })))
				.operation,
			'save',
		)
	}).pipe(
		Effect.provide(makeConnectionServices(slack({ loadConnection: () => Effect.fail('database unavailable') }))),
	),
)

it.effect('retries failed credential lookups immediately and still caches successful results', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const layer = SlackTenantCredentials.layerWithLookup({
			loadConnection: ({ workspaceId }) =>
				Effect.gen(function* () {
					const attempt = yield* Ref.updateAndGet(calls, (count) => count + 1)
					if (attempt === 1) return yield* Effect.fail('temporary repository outage')
					return SlackConnection.make({
						credentials: SlackConnectionCredentials.make({
							botToken: Redacted.make(`token-${workspaceId}`),
							botUserId: 'U_BOT',
							botId: 'B_BOT',
						}),
					})
				}),
		})
		yield* Effect.gen(function* () {
			const credentials = yield* SlackTenantCredentials
			const input = { teamId: SlackTeamId.make('T_RECOVERY') }
			assert.strictEqual((yield* credentials.load(input).pipe(Effect.flip)).operation, 'load')
			assert.ok(Option.isSome(yield* credentials.load(input)))
			assert.ok(Option.isSome(yield* credentials.load(input)))
			assert.strictEqual(yield* Ref.get(calls), 2)
			yield* TestClock.adjust('1 minute')
			assert.ok(Option.isSome(yield* credentials.load(input)))
			assert.strictEqual(yield* Ref.get(calls), 3)
		}).pipe(Effect.provide(layer))
	}),
)

it.effect('isolates overlapping workspace lookups in one acquired credential Layer', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const layer = SlackTenantCredentials.layerWithLookup({
			loadConnection: ({ workspaceId }) =>
				Effect.gen(function* () {
					if (workspaceId === 'T_A') {
						yield* Deferred.succeed(started, undefined)
						yield* Deferred.await(release)
					}
					return SlackConnection.make({
						credentials: SlackConnectionCredentials.make({
							botToken: Redacted.make(`token-${workspaceId}`),
							botUserId: `U_${workspaceId}`,
							botId: `B_${workspaceId}`,
						}),
					})
				}),
		})
		yield* Effect.gen(function* () {
			const credentials = yield* SlackTenantCredentials
			const first = yield* credentials.load({ teamId: SlackTeamId.make('T_A') }).pipe(Effect.forkChild)
			yield* Deferred.await(started)
			const second = yield* credentials.load({ teamId: SlackTeamId.make('T_B') })
			assert.ok(Option.isSome(second))
			assert.strictEqual(Redacted.value(second.value.botToken), 'token-T_B')
			yield* Deferred.succeed(release, undefined)
			const value = yield* Fiber.join(first)
			assert.ok(Option.isSome(value))
			assert.strictEqual(Redacted.value(value.value.botToken), 'token-T_A')
		}).pipe(Effect.provide(layer))
	}),
)
