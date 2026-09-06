import { NodeCrypto } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { SlackClient, SlackTenantCredentials } from '@humanlayer/channels-slack'
import { Config, Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import { loadSlackConnection, SlackConnectionRepositoryLive } from './store.ts'

const credentials = SlackTenantCredentials.layerWithLookup({ loadConnection: loadSlackConnection }).pipe(
	Layer.provide(SlackConnectionRepositoryLive),
	Layer.provide(PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })),
)

export const transport = Layer.merge(SlackClient.layer, NodeCrypto.layer).pipe(
	Layer.provideMerge(credentials),
	Layer.provide(FetchHttpClient.layer),
)
