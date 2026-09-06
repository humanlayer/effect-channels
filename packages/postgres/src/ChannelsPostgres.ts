import { PgClient } from '@effect/sql-pg'
import { Config, Layer } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

import { ConversationCoordinator, type ConversationCoordinatorOptions } from './ConversationCoordinator.ts'
import { userProfileCachePostgresLayer, type UserProfileCachePostgresOptions } from './UserProfileCachePostgres.ts'

export type ChannelsPostgresOptions = {
	readonly database: PgClient.PgPoolConfig
	readonly coordinator?: ConversationCoordinatorOptions
	readonly userProfileCache?: UserProfileCachePostgresOptions
}

const services = (coordinator?: ConversationCoordinatorOptions, userProfileCache?: UserProfileCachePostgresOptions) =>
	Layer.mergeAll(
		coordinator === undefined
			? ConversationCoordinator.layerPostgres()
			: ConversationCoordinator.layerPostgres(coordinator),
		Persistence.layerSql,
		userProfileCachePostgresLayer(userProfileCache),
	)

/** Provides Channels Postgres storage services from an ambient shared SqlClient. */
export const layerFromClient = (
	coordinator?: ConversationCoordinatorOptions,
	userProfileCache?: UserProfileCachePostgresOptions,
) => services(coordinator, userProfileCache)

export const layer = (options: ChannelsPostgresOptions) =>
	services(options.coordinator, options.userProfileCache).pipe(Layer.provideMerge(PgClient.layer(options.database)))

export const layerConfig = (
	coordinator?: ConversationCoordinatorOptions,
	userProfileCache?: UserProfileCachePostgresOptions,
) =>
	services(coordinator, userProfileCache).pipe(
		Layer.provideMerge(
			PgClient.layerConfig({
				url: Config.redacted('DATABASE_URL'),
			}),
		),
	)
