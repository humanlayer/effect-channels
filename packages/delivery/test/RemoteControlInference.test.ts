import { Effect } from 'effect'
import { expectTypeOf, it } from 'vite-plus/test'

import { deliveryApiServerLayer } from '../src/server'

it('supports neither request context nor endpoint middleware', () => {
	expectTypeOf(deliveryApiServerLayer()).not.toBeNever()
})

it('supports request context without endpoint middleware', () => {
	expectTypeOf(
		deliveryApiServerLayer({
			context: ({ delivery }) => {
				expectTypeOf(delivery.organizationId).toEqualTypeOf<string>()
				expectTypeOf(delivery.provider).toEqualTypeOf<string>()
				return Effect.succeed({ actor: 'agent' as const })
			},
		}),
	).not.toBeNever()
})

it('infers null when middleware is configured without request context', () => {
	deliveryApiServerLayer({
		middleware: {
			complete: ({ context, next }) => {
				expectTypeOf(context).toEqualTypeOf<null>()
				return next()
			},
		},
	})
})

it('infers the exact successful context value', () => {
	deliveryApiServerLayer({
		context: () => Effect.succeed({ actor: 'agent' as const, permissions: ['complete'] as const }),
		middleware: {
			complete: ({ context, next }) => {
				expectTypeOf(context.actor).toEqualTypeOf<'agent'>()
				expectTypeOf(context.permissions[0]).toEqualTypeOf<'complete'>()
				return next()
			},
		},
	})
})
