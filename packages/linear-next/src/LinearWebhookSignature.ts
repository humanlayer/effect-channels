import { WebhookAuthenticationError } from '@humanlayer/channels-delivery-next'
import { Clock, Crypto, Effect, Redacted, Schema } from 'effect'

const LinearSignatureInput = Schema.Struct({
	body: Schema.Uint8Array,
	signature: Schema.String,
	timestamp: Schema.String,
	webhookSecret: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
	maxAgeMs: Schema.Number,
})
type LinearSignatureInput = typeof LinearSignatureInput.Type

const hexadecimal = /^[\da-f]+$/i
const hmacBlockSize = 64
const textEncoder = new TextEncoder()

const concatenate = (left: Uint8Array, right: Uint8Array) => {
	const bytes = new Uint8Array(left.length + right.length)
	bytes.set(left)
	bytes.set(right, left.length)
	return bytes
}

const hmacSha256 = Effect.fn('linear.crypto.hmac_sha256')(function* (
	secret: Redacted.Redacted<string>,
	body: Uint8Array,
) {
	const crypto = yield* Crypto.Crypto
	const encoded = textEncoder.encode(Redacted.value(secret))
	const key = encoded.length > hmacBlockSize ? yield* crypto.digest('SHA-256', encoded) : encoded
	const padded = new Uint8Array(hmacBlockSize)
	padded.set(key)
	const inner = new Uint8Array(hmacBlockSize)
	const outer = new Uint8Array(hmacBlockSize)
	for (let index = 0; index < hmacBlockSize; index += 1) {
		inner[index] = (padded.at(index) ?? 0) ^ 0x36
		outer[index] = (padded.at(index) ?? 0) ^ 0x5c
	}
	const innerDigest = yield* crypto.digest('SHA-256', concatenate(inner, body))
	return yield* crypto.digest('SHA-256', concatenate(outer, innerDigest))
})

const decodeHex = (value: string) => {
	const signature = value.startsWith('sha256=') ? value.slice(7) : value
	if (signature.length !== 64 || !hexadecimal.test(signature)) return undefined
	return Uint8Array.from({ length: 32 }, (_, index) => Number.parseInt(signature.slice(index * 2, index * 2 + 2), 16))
}

const constantTimeEqual = (left: Uint8Array, right: Uint8Array) => {
	let difference = left.length ^ right.length
	for (let index = 0; index < Math.max(left.length, right.length); index += 1)
		difference |= (left.at(index) ?? 0) ^ (right.at(index) ?? 0)
	return difference === 0
}

export const verifyLinearWebhookSignature = Effect.fn('linear.signature.verify')(function* (
	input: LinearSignatureInput,
) {
	const timestamp = Number(input.timestamp)
	const now = yield* Clock.currentTimeMillis
	if (!Number.isFinite(timestamp) || Math.abs(now - timestamp) > input.maxAgeMs)
		return yield* WebhookAuthenticationError.make({ reason: 'stale_timestamp' })
	const expected = decodeHex(input.signature)
	if (expected === undefined) return yield* WebhookAuthenticationError.make({ reason: 'invalid_signature' })
	const actual = yield* hmacSha256(input.webhookSecret, input.body).pipe(
		Effect.tapError((error) => Effect.logError('Linear HMAC calculation failed', error)),
		Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'crypto' })),
	)
	if (!constantTimeEqual(actual, expected))
		return yield* WebhookAuthenticationError.make({ reason: 'invalid_signature' })
})
