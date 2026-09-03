import { PgClient } from '@effect/sql-pg'
import { ConversationCoordinator, type ConversationCoordinatorOptions } from '@humanlayer/channels'
import { Config, Layer } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

export type ChannelsPostgresOptions = {
	readonly database: PgClient.PgPoolConfig
	readonly coordinator?: ConversationCoordinatorOptions
}

const services = (coordinator?: ConversationCoordinatorOptions) =>
	Layer.merge(
		coordinator === undefined
			? ConversationCoordinator.layerPostgres()
			: ConversationCoordinator.layerPostgres(coordinator),
		Persistence.layerSql,
	)

export const layer = (options: ChannelsPostgresOptions) =>
	services(options.coordinator).pipe(Layer.provideMerge(PgClient.layer(options.database)))

export const layerConfig = (coordinator?: ConversationCoordinatorOptions) =>
	services(coordinator).pipe(
		Layer.provideMerge(
			PgClient.layerConfig({
				url: Config.redacted('DATABASE_URL'),
			}),
		),
	)
