import { Cache, Context, Duration, Effect, Layer, Option, Schema } from 'effect'

import { ProviderName, TenantId, UserId, UserProfile } from './Model'

export const UserProfileCacheKey = Schema.Struct({
	provider: ProviderName,
	tenant: TenantId,
	userId: UserId,
})
export type UserProfileCacheKey = typeof UserProfileCacheKey.Type

export const UserProfileFound = Schema.TaggedStruct('UserProfileFound', { profile: UserProfile })
export type UserProfileFound = typeof UserProfileFound.Type

export const UserProfileUnavailable = Schema.TaggedStruct('UserProfileUnavailable', {})
export type UserProfileUnavailable = typeof UserProfileUnavailable.Type

export const UserProfileCacheEntry = Schema.Union([UserProfileFound, UserProfileUnavailable])
export type UserProfileCacheEntry = typeof UserProfileCacheEntry.Type

export class UserProfileCacheError extends Schema.TaggedError<UserProfileCacheError>()('UserProfileCacheError', {
	operation: Schema.NonEmptyString,
}) {}

const memoryKey = (key: UserProfileCacheKey) => `${key.provider}\u0000${key.tenant}\u0000${key.userId}`

export type UserProfileCacheMemoryOptions = {
	readonly capacity?: number
	readonly timeToLive?: Duration.Input
}

/** Stores successful and intentionally non-retryable provider profile outcomes behind a replaceable cache seam. */
export class UserProfileCache extends Context.Service<
	UserProfileCache,
	{
		readonly get: (
			key: UserProfileCacheKey,
		) => Effect.Effect<Option.Option<UserProfileCacheEntry>, UserProfileCacheError>
		readonly set: (
			key: UserProfileCacheKey,
			entry: UserProfileCacheEntry,
		) => Effect.Effect<void, UserProfileCacheError>
		readonly invalidate: (key: UserProfileCacheKey) => Effect.Effect<void, UserProfileCacheError>
	}
>()('channels/UserProfileCache') {
	/** Creates an in-process Effect Cache with bounded capacity and expiry. */
	static memory(options: UserProfileCacheMemoryOptions = {}) {
		return Layer.effect(
			UserProfileCache,
			Effect.gen(function* () {
				const cache = yield* Cache.make<string, UserProfileCacheEntry>({
					capacity: options.capacity ?? 10_000,
					timeToLive: options.timeToLive ?? '8 days',
					lookup: () => Effect.die(new Error('UserProfileCache.getOption must not invoke lookup')),
				})
				return UserProfileCache.of({
					get: (key) => Cache.getOption(cache, memoryKey(key)),
					set: (key, entry) => Cache.set(cache, memoryKey(key), entry),
					invalidate: (key) => Cache.invalidate(cache, memoryKey(key)),
				})
			}),
		)
	}

	/** Compatibility alias for the default memory cache Layer. */
	static readonly layerMemory = UserProfileCache.memory()
}
