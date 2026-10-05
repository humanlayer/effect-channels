/**
 * This file defines the identities of a delivery: the permanent batch ID, the public delivery ID,
 * the conversation ID, and the access token a remote worker presents.
 *
 * A batch ID names one frozen batch for good. A claim ID only names one attempt at it, and changes
 * on every retry or recovery. The public delivery ID carries the mailbox key and the batch ID, so a
 * host can route a request to the owning mailbox without a lookup table.
 */
import { Crypto, Effect, Option, Result, Schema } from 'effect'
import { Base64Url } from 'effect/encoding'

/** The longest public delivery ID, so it fits in a request path on every host. */
export const DELIVERY_ID_MAX_LENGTH = 2_048

/** The longest mailbox key, once base64url-encoded, that still leaves room in a delivery ID. */
export const ENCODED_MAILBOX_KEY_MAX_LENGTH = 1_500

const UrlSafe = /^[A-Za-z0-9_-]+$/

/** The permanent identity of one frozen batch. Kept across every retry and recovery. */
export const BatchId = Schema.NonEmptyString.check(Schema.isMaxLength(64), Schema.isPattern(UrlSafe)).pipe(
	Schema.brand('BatchId'),
)
export type BatchId = typeof BatchId.Type

/** The public identity of one delivery: `delivery:v1:<base64url mailbox key>:<batch ID>`. */
export const DeliveryId = Schema.NonEmptyString.check(
	Schema.isMaxLength(DELIVERY_ID_MAX_LENGTH),
	Schema.isPattern(/^delivery:v1:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+$/),
).pipe(Schema.brand('DeliveryId'))
export type DeliveryId = typeof DeliveryId.Type

/**
 * The stable identity of the conversation a delivery belongs to: a Slack thread, a GitHub issue or
 * pull request, a Linear issue, or a Linear agent session. Each delivery is one turn in it.
 */
export const ConversationId = Schema.NonEmptyString.check(Schema.isPattern(/^conversation:v1:[A-Za-z0-9_-]+$/)).pipe(
	Schema.brand('ConversationId'),
)
export type ConversationId = typeof ConversationId.Type

/**
 * The bearer token a remote worker presents for one delivery. It is random, saved with the batch,
 * and never logged or returned by status.
 */
export const DeliveryAccessToken = Schema.NonEmptyString.check(Schema.isPattern(UrlSafe))
export type DeliveryAccessToken = typeof DeliveryAccessToken.Type

/** A delivery ID taken apart into the parts a host routes on. */
export const DeliveryReference = Schema.Struct({
	deliveryId: DeliveryId,
	mailboxKey: Schema.NonEmptyString,
	batchId: BatchId,
})
export type DeliveryReference = typeof DeliveryReference.Type

/** Whether a mailbox key is short enough for its delivery IDs to fit in a request path. */
export const isRoutableMailboxKey = (mailboxKey: string) =>
	Base64Url.encode(mailboxKey).length <= ENCODED_MAILBOX_KEY_MAX_LENGTH

/** The delivery ID of a batch. The mailbox key must be routable; see {@link isRoutableMailboxKey}. */
export const makeDeliveryId = (input: { readonly mailboxKey: string; readonly batchId: BatchId }) =>
	DeliveryId.make(`delivery:v1:${Base64Url.encode(input.mailboxKey)}:${input.batchId}`)

/** The conversation ID of a mailbox. */
export const makeConversationId = (mailboxKey: string) =>
	ConversationId.make(`conversation:v1:${Base64Url.encode(mailboxKey)}`)

/**
 * Take a delivery ID apart. Returns none for anything that is not a canonical v1 delivery ID,
 * so a caller can answer "not found" without saying why.
 */
export const parseDeliveryId = (input: string): Option.Option<DeliveryReference> => {
	const deliveryId = Schema.decodeUnknownOption(DeliveryId)(input)
	if (Option.isNone(deliveryId)) return Option.none()
	const [, , encodedKey, rawBatchId] = deliveryId.value.split(':')
	const mailboxKey = Result.getOrUndefined(Base64Url.decodeString(encodedKey ?? ''))
	const batchId = Schema.decodeUnknownOption(BatchId)(rawBatchId)
	if (mailboxKey === undefined || mailboxKey === '' || Option.isNone(batchId)) return Option.none()
	if (makeDeliveryId({ mailboxKey, batchId: batchId.value }) !== deliveryId.value) return Option.none()
	return Option.some(DeliveryReference.make({ deliveryId: deliveryId.value, mailboxKey, batchId: batchId.value }))
}

/** A new batch ID. */
export const makeBatchId = Effect.gen(function* () {
	const crypto = yield* Crypto.Crypto
	return BatchId.make(Base64Url.encode(yield* crypto.randomBytes(16)))
})

/** A new access token: 32 random bytes. */
export const makeDeliveryAccessToken = Effect.gen(function* () {
	const crypto = yield* Crypto.Crypto
	return DeliveryAccessToken.make(Base64Url.encode(yield* crypto.randomBytes(32)))
})

/** Sixteen bytes as a UUID v4 string: sets the version and variant bits, then formats them. */
const formatUuidV4 = (bytes: Uint8Array) => {
	const hex = Array.from(bytes.subarray(0, 16), (byte, index) => {
		if (index === 6) return ((byte & 0x0f) | 0x40).toString(16).padStart(2, '0')
		if (index === 8) return ((byte & 0x3f) | 0x80).toString(16).padStart(2, '0')
		return byte.toString(16).padStart(2, '0')
	}).join('')
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`
}

/**
 * A UUID made from a delivery ID: the same on every attempt at the delivery, and different for every
 * delivery. It has the UUID v4 layout, because providers that take a client-chosen ID, such as Linear,
 * ask for one. A provider sends it with output it makes before the callback runs, so a retry cannot
 * make that output twice.
 */
export const makeDeliveryIdempotencyKey = Effect.fn('delivery.make_idempotency_key')(function* (deliveryId: DeliveryId) {
	const crypto = yield* Crypto.Crypto
	const digest = yield* crypto.digest('SHA-256', new TextEncoder().encode(`delivery-idempotency:v1:${deliveryId}`))
	return formatUuidV4(digest)
})

/**
 * Compare a presented token with the saved one. Takes the same time for any presented token of the
 * saved token's length, so the comparison does not reveal how much of a guess was right.
 */
export const deliveryAccessTokenMatches = (input: { readonly saved: string; readonly presented: string }) => {
	if (input.saved.length !== input.presented.length) return false
	let difference = 0
	for (let index = 0; index < input.saved.length; index += 1) {
		difference |= input.saved.charCodeAt(index) ^ input.presented.charCodeAt(index)
	}
	return difference === 0
}
