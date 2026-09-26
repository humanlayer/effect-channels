import { type Context, Effect, Layer } from 'effect'
import type { HttpRouter, HttpServerRequest } from 'effect/unstable/http'
import { HttpApiBuilder } from 'effect/unstable/httpapi'

import { DeliveryContract, Forbidden, Unauthorized, Unavailable } from './contract'
import {
	DeliveryControl,
	type DeliveryControlService,
	type DeliveryControlError,
	DeliveryNotFound,
	type DeliveryTerminalOutcome,
	type DeliveryTerminalReceipt,
	type ResolvedDelivery,
} from './DeliveryControl'
import type { DeliveryId } from './DeliveryReference'

export type DeliveryApiRejection = Unauthorized | Forbidden | Unavailable
export type DeliveryApiContextRejection = DeliveryApiRejection | DeliveryNotFound

export interface DeliveryApiContextInput {
	readonly request: HttpServerRequest.HttpServerRequest
	/** Saved provider identity and encoded native event/resource data for delivery-scoped access checks. */
	readonly delivery: ResolvedDelivery
}

export interface DeliveryApiMiddlewareInput<C> extends DeliveryApiContextInput {
	readonly input: { readonly deliveryId: DeliveryId; readonly markdown?: string }
	readonly delivery: ResolvedDelivery
	readonly context: C
	readonly next: Effect.Effect<DeliveryTerminalReceipt, DeliveryControlError>
}

export interface DeliveryApiOptions<C, RC, RM> {
	readonly mountPath?: HttpRouter.PathInput
	/**
	 * Application-owned authentication and request context. When omitted, the delivery API performs no
	 * authentication or access check.
	 */
	readonly context?: (input: DeliveryApiContextInput) => Effect.Effect<C, DeliveryApiContextRejection, RC>
	/**
	 * Application-owned authorization and policy hooks. When omitted together with `context`, requests have no
	 * built-in authentication or access check.
	 */
	readonly middleware?: {
		readonly complete?: (
			input: DeliveryApiMiddlewareInput<C>,
		) => Effect.Effect<DeliveryTerminalReceipt, DeliveryApiRejection | DeliveryControlError, RM>
		readonly fail?: (
			input: DeliveryApiMiddlewareInput<C>,
		) => Effect.Effect<DeliveryTerminalReceipt, DeliveryApiRejection | DeliveryControlError, RM>
	}
}

export type DeliveryApiOptionsWithoutContext<RM> = Omit<DeliveryApiOptions<null, never, RM>, 'context'> & {
	readonly context?: undefined
}

export type DeliveryApiOptionsWithContext<C, RC, RM> = Omit<DeliveryApiOptions<C, RC, RM>, 'context'> & {
	readonly context: (input: DeliveryApiContextInput) => Effect.Effect<C, DeliveryApiContextRejection, RC>
}

const makeEndpoint = <C, RC, RM>(
	options: DeliveryApiOptions<C, RC, RM>,
	control: DeliveryControlService,
	runtime: Context.Context<RC | RM>,
	outcome: DeliveryTerminalOutcome,
	middleware:
		| ((
				input: DeliveryApiMiddlewareInput<C>,
		  ) => Effect.Effect<DeliveryTerminalReceipt, DeliveryApiRejection | DeliveryControlError, RM>)
		| undefined,
) =>
	Effect.fn(`delivery.api.${outcome}`)(function* (request: {
		readonly params: { readonly deliveryId: DeliveryId }
		readonly payload: { readonly markdown?: string }
		readonly request: HttpServerRequest.HttpServerRequest
	}) {
		const delivery = yield* control.resolve({ deliveryId: request.params.deliveryId })
		const context =
			options.context === undefined
				? null
				: yield* options.context({ request: request.request, delivery }).pipe(Effect.provide(runtime))
		const input = { deliveryId: request.params.deliveryId, ...request.payload }
		const next = yield* Effect.cached(control.finish({ ...input, outcome }))
		if (middleware === undefined) return yield* next
		// SAFETY: the overload fixes C to null without context; otherwise this value came from the context callback.
		return yield* middleware({
			input,
			request: request.request,
			delivery,
			context: context as C,
			next,
		}).pipe(Effect.provide(runtime))
	})

const makeDeliveryApiServerLayer = <C = null, RC = never, RM = never>(options: DeliveryApiOptions<C, RC, RM> = {}) => {
	const layer =
		options.mountPath === undefined
			? HttpApiBuilder.group(DeliveryContract, 'deliveries', (handlers) =>
					Effect.gen(function* () {
						const control = yield* DeliveryControl
						const runtime = yield* Effect.context<RC | RM>()
						return handlers.handleAll({
							complete: makeEndpoint(
								options,
								control,
								runtime,
								'completed',
								options.middleware?.complete,
							),
							fail: makeEndpoint(options, control, runtime, 'failed', options.middleware?.fail),
						})
					}),
				)
			: HttpApiBuilder.group(DeliveryContract.prefix(options.mountPath), 'deliveries', (handlers) =>
					Effect.gen(function* () {
						const control = yield* DeliveryControl
						const runtime = yield* Effect.context<RC | RM>()
						return handlers.handleAll({
							complete: makeEndpoint(
								options,
								control,
								runtime,
								'completed',
								options.middleware?.complete,
							),
							fail: makeEndpoint(options, control, runtime, 'failed', options.middleware?.fail),
						})
					}),
				)
	return layer.pipe(
		Layer.tap(() =>
			Effect.logInfo('Delivery control API registered').pipe(
				Effect.annotateLogs({ mount_path: options.mountPath ?? '/' }),
			),
		),
	)
}

export function deliveryApiServerLayer<RM = never>(
	options?: DeliveryApiOptionsWithoutContext<RM>,
): ReturnType<typeof makeDeliveryApiServerLayer<null, never, RM>>
export function deliveryApiServerLayer<C, RC = never, RM = never>(
	options: DeliveryApiOptionsWithContext<C, RC, RM>,
): ReturnType<typeof makeDeliveryApiServerLayer<C, RC, RM>>
export function deliveryApiServerLayer<C = null, RC = never, RM = never>(options: DeliveryApiOptions<C, RC, RM> = {}) {
	return makeDeliveryApiServerLayer(options)
}
