import { Layer } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { application } from './app.js'
import { transport } from './transport.js'

const web = HttpRouter.toWebHandler(application.pipe(Layer.provide(transport)))
let closed = false

export const handle = (request: Request): Promise<Response> =>
	closed ? Promise.resolve(new Response(null, { status: 503 })) : web.handler(request)

export const close = () => {
	closed = true
	return web.dispose()
}
