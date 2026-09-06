import { createServer } from 'node:http'

import { NodeHttpServer, NodeRuntime } from '@effect/platform-node'
import { PgClient } from '@effect/sql-pg'
import { Config, Layer } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { application, transport } from './app.ts'
import { SlackConnectionRepositoryLive } from './store.ts'

const ApplicationRoutes = application.pipe(Layer.provide(transport), Layer.provide(SlackConnectionRepositoryLive))
const DatabaseLive = PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })

const HttpLive = HttpRouter.serve(ApplicationRoutes).pipe(
	Layer.provide(
		NodeHttpServer.layerConfig(createServer, {
			port: Config.number('PORT').pipe(Config.withDefault(3000)),
			gracefulShutdownTimeout: Config.succeed('10 seconds'),
		}),
	),
	Layer.provide(DatabaseLive),
)

NodeRuntime.runMain(Layer.launch(HttpLive))
