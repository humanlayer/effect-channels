import {
	UserProfileCache,
	UserProfileCacheEntry,
	UserProfileCacheError,
	type UserProfileCacheKey,
} from '@humanlayer/channels'
import { Duration, Effect, Layer, Option, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

const CacheRow = Schema.Struct({ value_json: Schema.String })
const decodeRow = Schema.decodeUnknownEffect(CacheRow)
const entryJson = Schema.fromJsonString(Schema.toCodecJson(UserProfileCacheEntry))
const decodeEntry = Schema.decodeUnknownEffect(entryJson)
const encodeEntry = Schema.encodeUnknownEffect(entryJson)

export type UserProfileCachePostgresOptions = {
	readonly timeToLive?: Duration.Input
}

/** Creates the idempotent schema for shared provider profile cache entries. */
export const UserProfileCachePostgresMigrations = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`
		CREATE TABLE IF NOT EXISTS channels_user_profile_cache (
			provider text NOT NULL,
			tenant text NOT NULL,
			user_id text NOT NULL,
			value_json jsonb NOT NULL,
			expires_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (provider, tenant, user_id)
		)
	`
	yield* sql`
		CREATE INDEX IF NOT EXISTS channels_user_profile_cache_expiry_idx
		ON channels_user_profile_cache (expires_at)
	`
}).pipe(Effect.asVoid)

const cacheError = (operation: string) => UserProfileCacheError.make({ operation })

/** Provides a schema-decoded Postgres user profile cache with expiring entries. */
export const userProfileCachePostgresLayer = (options: UserProfileCachePostgresOptions = {}) =>
	Layer.effect(
		UserProfileCache,
		Effect.gen(function* () {
			const sql = (yield* SqlClient.SqlClient).withoutTransforms()
			const ttlMs = Duration.toMillis(Duration.fromInputUnsafe(options.timeToLive ?? '8 days'))
			yield* UserProfileCachePostgresMigrations.pipe(
				Effect.tapError((error) => Effect.logError('Postgres user profile cache migration failed', error)),
				Effect.mapError(() => cacheError('UserProfileCache.migrate')),
			)
			return UserProfileCache.of({
				get: (key: UserProfileCacheKey) =>
					Effect.gen(function* () {
						const rows = yield* sql<typeof CacheRow.Encoded>`
							SELECT value_json::text AS value_json
							FROM channels_user_profile_cache
							WHERE provider = ${key.provider}
								AND tenant = ${key.tenant}
								AND user_id = ${key.userId}
								AND expires_at > now()
						`
						const row = rows.at(0)
						if (row === undefined) {
							return Option.none()
						}
						const decoded = yield* decodeRow(row)
						return Option.some(yield* decodeEntry(decoded.value_json))
					}).pipe(
						Effect.tapError((error) => Effect.logError('Postgres user profile cache read failed', error)),
						Effect.mapError(() => cacheError('UserProfileCache.get')),
					),
				set: (key, entry) =>
					Effect.gen(function* () {
						const encoded = yield* encodeEntry(entry)
						yield* sql`
							INSERT INTO channels_user_profile_cache (provider, tenant, user_id, value_json, expires_at)
							VALUES (
								${key.provider},
								${key.tenant},
								${key.userId},
								${encoded}::jsonb,
								now() + (${ttlMs} * interval '1 millisecond')
							)
							ON CONFLICT (provider, tenant, user_id) DO UPDATE
							SET value_json = EXCLUDED.value_json,
								expires_at = EXCLUDED.expires_at,
								updated_at = now()
						`
					}).pipe(
						Effect.tapError((error) => Effect.logError('Postgres user profile cache write failed', error)),
						Effect.mapError(() => cacheError('UserProfileCache.set')),
					),
				invalidate: (key) =>
					sql`
						DELETE FROM channels_user_profile_cache
						WHERE provider = ${key.provider} AND tenant = ${key.tenant} AND user_id = ${key.userId}
					`.pipe(
						Effect.asVoid,
						Effect.tapError((error) =>
							Effect.logError('Postgres user profile cache invalidation failed', error),
						),
						Effect.mapError(() => cacheError('UserProfileCache.invalidate')),
					),
			})
		}),
	)
