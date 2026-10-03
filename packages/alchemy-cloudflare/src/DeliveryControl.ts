/**
 * This file carries delivery control from the Worker to the mailbox Durable Object that owns the delivery.
 *
 * The Worker only routes: it reads the mailbox key out of the delivery ID and forwards the request
 * with the `deliveryRequest` RPC. The mailbox object checks the token and applies the change. Both
 * the request and the response cross the RPC as encoded schemas, and a typed failure travels as a
 * response value, so nothing depends on how Cloudflare serializes thrown errors.
 */
import {
	DeliveryControl,
	DeliveryControlUnavailable,
	DeliveryMutation,
	DeliveryMutationError,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryStatus,
	DeliveryStatusError,
	parseDeliveryId,
} from '@humanlayer/channels-delivery'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Data, Effect, Layer, Match, Redacted, Schema } from 'effect'

import { DeliveryMailboxes } from './MailboxDelivery'

/** Read a delivery's status. The token crosses the RPC as a plain string. */
export const DeliveryStatusRequest = Schema.TaggedStruct('DeliveryStatusRequest', {
	deliveryId: Schema.String,
	accessToken: Schema.String,
})

/** Apply a change to a delivery. The token crosses the RPC as a plain string. */
export const DeliveryMutationRequest = Schema.TaggedStruct('DeliveryMutationRequest', {
	deliveryId: Schema.String,
	accessToken: Schema.String,
	mutation: DeliveryMutation,
})

export const DeliveryRequest = Schema.Union([DeliveryStatusRequest, DeliveryMutationRequest])
export type DeliveryRequest = typeof DeliveryRequest.Type

export const DeliveryStatusSucceeded = Schema.TaggedStruct('DeliveryStatusSucceeded', { status: DeliveryStatus })
export const DeliveryStatusFailed = Schema.TaggedStruct('DeliveryStatusFailed', { error: DeliveryStatusError })
export const DeliveryStatusResponse = Schema.Union([DeliveryStatusSucceeded, DeliveryStatusFailed])

export const DeliveryMutationSucceeded = Schema.TaggedStruct('DeliveryMutationSucceeded', {
	receipt: DeliveryMutationReceipt,
})
export const DeliveryMutationFailed = Schema.TaggedStruct('DeliveryMutationFailed', { error: DeliveryMutationError })
export const DeliveryMutationResponse = Schema.Union([DeliveryMutationSucceeded, DeliveryMutationFailed])

export const DeliveryResponse = Schema.Union([DeliveryStatusResponse, DeliveryMutationResponse])
export type DeliveryResponse = typeof DeliveryResponse.Type

/** The mailbox object was sent a request it cannot read. Only a Worker built from other code can cause it. */
class DeliveryRequestInvalid extends Data.TaggedError('DeliveryRequestInvalid')<{}> {}

/**
 * Build the `deliveryRequest` RPC over the mailbox object's own `DeliveryControl`.
 *
 * The decode error is not logged: it can quote the request, and the request carries the token.
 */
export const makeDeliveryRequestHandler = Effect.gen(function* () {
	const control = yield* DeliveryControl

	return Effect.fn('delivery.cloudflare_durable_object.delivery_request')(function* (
		input: typeof DeliveryRequest.Encoded,
	) {
		const request = yield* Schema.decodeEffect(DeliveryRequest)(input).pipe(
			Effect.catchTag('SchemaError', () =>
				Effect.logError('Cloudflare mailbox delivery request decode failed').pipe(
					Effect.andThen(Effect.die(new DeliveryRequestInvalid())),
				),
			),
		)
		const response = yield* Match.value(request).pipe(
			Match.tagsExhaustive({
				DeliveryStatusRequest: ({ deliveryId, accessToken }) =>
					control.status({ deliveryId, accessToken: Redacted.make(accessToken) }).pipe(
						Effect.match({
							onSuccess: (status): DeliveryResponse => DeliveryStatusSucceeded.make({ status }),
							onFailure: (error): DeliveryResponse => DeliveryStatusFailed.make({ error }),
						}),
					),
				DeliveryMutationRequest: ({ deliveryId, accessToken, mutation }) =>
					control.apply({ deliveryId, accessToken: Redacted.make(accessToken), mutation }).pipe(
						Effect.match({
							onSuccess: (receipt): DeliveryResponse => DeliveryMutationSucceeded.make({ receipt }),
							onFailure: (error): DeliveryResponse => DeliveryMutationFailed.make({ error }),
						}),
					),
			}),
		)
		return yield* Schema.encodeEffect(DeliveryResponse)(response).pipe(Effect.orDie)
	})
})

/** Log the raw RPC or codec failure, then narrow it to `DeliveryControlUnavailable`. */
const unavailable = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError((error) => Effect.logError('Cloudflare delivery request failed', error)),
		Effect.mapError(() => new DeliveryControlUnavailable({ reason: 'cloudflare_unavailable' })),
		Effect.catchDefect((defect) =>
			Effect.logError('Cloudflare delivery request failed', defect).pipe(
				Effect.andThen(Effect.fail(new DeliveryControlUnavailable({ reason: 'cloudflare_unavailable' }))),
			),
		),
	)

/** An ID that is not a delivery ID names no delivery. */
const referenceOrNotFound = (deliveryId: string) =>
	Effect.fromOption(parseDeliveryId(deliveryId), () => new DeliveryNotFound())

/** `DeliveryControl` in the Worker: each request goes to the mailbox object named by the delivery ID. */
export const DeliveryControlAlchemyCloudflare = Layer.effect(
	DeliveryControl,
	Effect.gen(function* () {
		const mailboxes = yield* DeliveryMailboxes
		/** Send one request and decode the answer with the schema its kind expects. */
		const send = <S extends Schema.Top>(mailboxKey: string, request: DeliveryRequest, responseSchema: S) =>
			Schema.encodeEffect(DeliveryRequest)(request).pipe(
				Effect.flatMap((encoded) => mailboxes.getByName(mailboxKey).deliveryRequest(encoded)),
				Effect.flatMap(Schema.decodeUnknownEffect(responseSchema)),
				Effect.provide(RuntimeContext.phantom),
				unavailable,
			)

		return DeliveryControl.of({
			status: Effect.fn('delivery.cloudflare.delivery_status')(function* (input) {
				const reference = yield* referenceOrNotFound(input.deliveryId)
				const response = yield* send(
					reference.mailboxKey,
					DeliveryStatusRequest.make({
						deliveryId: reference.deliveryId,
						accessToken: Redacted.value(input.accessToken),
					}),
					DeliveryStatusResponse,
				)
				return yield* Match.value(response).pipe(
					Match.tagsExhaustive({
						DeliveryStatusSucceeded: ({ status }) => Effect.succeed(status),
						DeliveryStatusFailed: ({ error }) => Effect.fail(error),
					}),
				)
			}),
			apply: Effect.fn('delivery.cloudflare.delivery_apply')(function* (input) {
				const reference = yield* referenceOrNotFound(input.deliveryId)
				const response = yield* send(
					reference.mailboxKey,
					DeliveryMutationRequest.make({
						deliveryId: reference.deliveryId,
						accessToken: Redacted.value(input.accessToken),
						mutation: input.mutation,
					}),
					DeliveryMutationResponse,
				)
				return yield* Match.value(response).pipe(
					Match.tagsExhaustive({
						DeliveryMutationSucceeded: ({ receipt }) => Effect.succeed(receipt),
						DeliveryMutationFailed: ({ error }) => Effect.fail(error),
					}),
				)
			}),
		})
	}),
)
