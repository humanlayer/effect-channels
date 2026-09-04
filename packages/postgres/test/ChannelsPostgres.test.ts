import { assert, describe, it } from '@effect/vitest'
import {
	ConversationCoordinator,
	SubscriptionCreated,
	Subscriptions,
	TenantId,
	ThreadId,
	UserId,
	UserProfile,
	UserProfileCache,
	UserProfileCacheKey,
	UserProfileFound,
} from '@humanlayer/channels'
import { Context, Effect, Layer, Option, Random, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

import { layerConfig } from '../src/index.ts'

const postgresTestsEnabled = import.meta.env.DATABASE_URL !== undefined
const applicationLayer = (userProfileCache?: { readonly timeToLive: number }) =>
	Subscriptions.layer.pipe(
		Layer.provideMerge(
			layerConfig(
				{
					leaseTtlMs: 1_000,
					heartbeatEveryMs: 250,
					acquireTimeoutMs: 2_000,
					retryBaseMs: 50,
					retryMaxMs: 200,
					alertAfterAttempts: 2,
				},
				userProfileCache,
			),
		),
	)

describe.skipIf(!postgresTestsEnabled)('@humanlayer/channels-postgres', () => {
	it.effect('composes PgClient, coordinator, and shared SQL persistence', () =>
		Effect.gen(function* () {
			const firstContext = yield* Layer.build(applicationLayer())
			const secondContext = yield* Layer.build(applicationLayer())
			const firstSubscriptions = Context.get(firstContext, Subscriptions)
			const secondSubscriptions = Context.get(secondContext, Subscriptions)
			const threadId = ThreadId.make(`slack:v1:T_TEST:C_TEST:${yield* Random.nextInt}`)

			const transition = yield* firstSubscriptions.subscribe({ threadId })
			assert.ok(Schema.is(SubscriptionCreated)(transition))
			assert.strictEqual(yield* secondSubscriptions.isSubscribed({ threadId }), true)

			assert.ok(Context.get(firstContext, ConversationCoordinator))
			const firstCache = Context.get(firstContext, UserProfileCache)
			const secondCache = Context.get(secondContext, UserProfileCache)
			const userId = UserId.make(`U_${yield* Random.nextInt}`)
			const cacheKey = UserProfileCacheKey.make({
				provider: 'slack',
				tenant: TenantId.make('T_TEST'),
				userId,
			})
			const cached = UserProfileFound.make({
				profile: UserProfile.make({
					author: {
						userId,
						userName: 'postgres-user',
						fullName: 'Postgres User',
						isBot: false,
						isMe: false,
					},
				}),
			})
			yield* firstCache.set(cacheKey, cached)
			assert.deepStrictEqual(yield* secondCache.get(cacheKey), Option.some(cached))
			const sql = Context.get(firstContext, SqlClient.SqlClient)
			const tables = yield* sql<{ readonly table_name: string }>`
				SELECT table_name
				FROM information_schema.tables
				WHERE table_schema = current_schema()
					AND table_name IN (
						'channels_conversations',
						'channels_conversation_mailbox',
						'channels_user_profile_cache',
						'effect_persistence'
					)
			`
			assert.deepStrictEqual(
				new Set(tables.map((row) => row.table_name)),
				new Set([
					'channels_conversations',
					'channels_conversation_mailbox',
					'channels_user_profile_cache',
					'effect_persistence',
				]),
			)
		}),
	)

	it.live('expires shared profile cache entries', () =>
		Effect.gen(function* () {
			const context = yield* Layer.build(applicationLayer({ timeToLive: 20 }))
			const cache = Context.get(context, UserProfileCache)
			const userId = UserId.make(`U_${yield* Random.nextInt}`)
			const cacheKey = UserProfileCacheKey.make({
				provider: 'slack',
				tenant: TenantId.make('T_TEST'),
				userId,
			})
			yield* cache.set(
				cacheKey,
				UserProfileFound.make({
					profile: UserProfile.make({
						author: {
							userId,
							userName: 'expires',
							fullName: 'Expires',
							isBot: false,
							isMe: false,
						},
					}),
				}),
			)
			yield* Effect.sleep(50)
			assert.ok(Option.isNone(yield* cache.get(cacheKey)))
		}),
	)
})
