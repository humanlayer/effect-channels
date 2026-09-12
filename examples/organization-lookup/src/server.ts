import { createServer } from 'node:http'

import { NodeHttpServer, NodeRuntime } from '@effect/platform-node'
import { Config, Layer } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { application } from './app.js'
import { storage } from './storage.js'
import { transport } from './transport.js'

export const server = HttpRouter.serve(application.pipe(Layer.provide(Layer.merge(storage, transport)))).pipe(
	Layer.provide(
		NodeHttpServer.layerConfig(createServer, {
			port: Config.port('PORT').pipe(Config.withDefault(3000)),
			gracefulShutdownTimeout: Config.succeed('10 seconds'),
		}),
	),
)

if (import.meta.main) NodeRuntime.runMain(Layer.launch(server))
