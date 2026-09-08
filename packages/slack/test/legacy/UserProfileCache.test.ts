import { assert, it } from '@effect/vitest'
import { Effect, Option } from 'effect'
import { TestClock } from 'effect/testing'

import {
	TenantId,
	UserId,
	UserProfile,
	UserProfileCache,
	UserProfileCacheKey,
	UserProfileFound,
	UserProfileUnavailable,
} from '../../src/index.js'

const key = (userId: string) =>
	UserProfileCacheKey.make({ provider: 'slack', tenant: TenantId.make('T_TEST'), userId: UserId.make(userId) })

const found = (userId: string) =>
	UserProfileFound.make({
		profile: UserProfile.make({
			author: {
				userId: UserId.make(userId),
				userName: 'tester',
				fullName: 'Test User',
				isBot: false,
				isMe: false,
			},
		}),
	})

it.effect('stores schema-backed positive and negative profile entries', () =>
	Effect.gen(function* () {
		const cache = yield* UserProfileCache
		yield* cache.set(key('U_ONE'), found('U_ONE'))
		yield* cache.set(key('U_TWO'), UserProfileUnavailable.make({}))
		assert.deepStrictEqual(yield* cache.get(key('U_ONE')), Option.some(found('U_ONE')))
		assert.deepStrictEqual(yield* cache.get(key('U_TWO')), Option.some(UserProfileUnavailable.make({})))
	}).pipe(Effect.provide(UserProfileCache.memory())),
)

it.effect('expires entries and enforces bounded capacity through Effect Cache', () =>
	Effect.gen(function* () {
		const cache = yield* UserProfileCache
		yield* cache.set(key('U_ONE'), found('U_ONE'))
		yield* TestClock.adjust('2 seconds')
		assert.ok(Option.isNone(yield* cache.get(key('U_ONE'))))
		yield* cache.set(key('U_TWO'), found('U_TWO'))
		yield* cache.set(key('U_THREE'), found('U_THREE'))
		assert.ok(Option.isNone(yield* cache.get(key('U_TWO'))))
		assert.ok(Option.isSome(yield* cache.get(key('U_THREE'))))
	}).pipe(Effect.provide(UserProfileCache.memory({ capacity: 1, timeToLive: '1 second' }))),
)
