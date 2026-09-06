import { createServer } from 'node:http'

import { NodeHttpServer, NodeRuntime } from '@effect/platform-node'
import { Config, Layer } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { application, transport } from './app.ts'

const HttpLive = HttpRouter.serve(application.pipe(Layer.provide(transport))).pipe(
	Layer.provide(
		NodeHttpServer.layerConfig(createServer, {
			port: Config.number('PORT').pipe(Config.withDefault(3000)),
			gracefulShutdownTimeout: Config.succeed('10 seconds'),
		}),
	),
)

NodeRuntime.runMain(Layer.launch(HttpLive))
