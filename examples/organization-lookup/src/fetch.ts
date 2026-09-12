import { Layer } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { application } from './app.js'
import { storage } from './storage.js'
import { transport } from './transport.js'

export const makeHost = () => HttpRouter.toWebHandler(application.pipe(Layer.provide(Layer.merge(storage, transport))))
