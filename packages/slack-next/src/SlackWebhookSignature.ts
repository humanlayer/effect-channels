import { WebhookAuthenticationError } from '@humanlayer/channels-delivery-next'
import { Clock, Crypto, Effect, Redacted, Schema } from 'effect'

const SlackHmacInput = Schema.Struct({
	secret: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
	data: Schema.Uint8Array,
})
type SlackHmacInput = typeof SlackHmacInput.Type

const SlackSignatureInput = Schema.Struct({
	body: Schema.Uint8Array,
	timestamp: Schema.String,
	signature: Schema.String,
	signingSecret: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
})
type SlackSignatureInput = typeof SlackSignatureInput.Type

const hexadecimal = /^[\da-f]+$/i
const hmacBlockSize = 64
const innerPadByte = 0x36
const outerPadByte = 0x5c
const textEncoder = new TextEncoder()

const concatenateBytes = (left: Uint8Array, right: Uint8Array) => {
	const output = new Uint8Array(left.length + right.length)
	output.set(left)
	output.set(right, left.length)
	return output
}

const paddedHmacKey = (key: Uint8Array) =>
	Effect.gen(function* () {
		const crypto = yield* Crypto.Crypto
		const normalized = key.length > hmacBlockSize ? yield* crypto.digest('SHA-256', key) : key
		const padded = new Uint8Array(hmacBlockSize)
		padded.set(normalized.slice(0, hmacBlockSize))
		return padded
	})

const hmacSha256 = Effect.fn('slack.crypto.hmac_sha256')(function* (input: SlackHmacInput) {
	const crypto = yield* Crypto.Crypto
	const key = yield* paddedHmacKey(textEncoder.encode(Redacted.value(input.secret)))
	const innerPad = new Uint8Array(hmacBlockSize)
	const outerPad = new Uint8Array(hmacBlockSize)
	for (let index = 0; index < hmacBlockSize; index += 1) {
		const byte = key.at(index) ?? 0
		innerPad[index] = byte ^ innerPadByte
		outerPad[index] = byte ^ outerPadByte
	}
	const innerDigest = yield* crypto.digest('SHA-256', concatenateBytes(innerPad, input.data))
	return yield* crypto.digest('SHA-256', concatenateBytes(outerPad, innerDigest))
})

const constantTimeEqual = (left: Uint8Array, right: Uint8Array) => {
	let difference = left.length ^ right.length
	const length = Math.max(left.length, right.length)
	for (let index = 0; index < length; index += 1) {
		difference |= (left.at(index) ?? 0) ^ (right.at(index) ?? 0)
	}
	return difference === 0
}

const decodeSignature = (signature: string) => {
	if (!signature.startsWith('v0=')) {
		return undefined
	}
	const value = signature.slice(3)
	if (value.length !== 64 || !hexadecimal.test(value)) {
		return undefined
	}
	const bytes = new Uint8Array(value.length / 2)
	for (let index = 0; index < bytes.length; index += 1) {
		bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16)
	}
	return bytes
}

/**
 * Slack signs the UTF-8 bytes for `v0:<timestamp>:` followed by the exact request body bytes.
 * Encoding the body through string interpolation would instead sign its comma-separated numbers.
 */
const slackSignatureBase = (timestamp: string, body: Uint8Array): Uint8Array =>
	concatenateBytes(textEncoder.encode(`v0:${timestamp}:`), body)

export const verifySlackSignature = Effect.fn('slack.signature.verify')(function* (input: SlackSignatureInput) {
	const timestamp = Number(input.timestamp)
	if (!Number.isFinite(timestamp)) {
		return yield* WebhookAuthenticationError.make({ reason: 'invalid_timestamp' })
	}
	const now = Math.floor((yield* Clock.currentTimeMillis) / 1000)
	if (Math.abs(now - timestamp) > 300) {
		return yield* WebhookAuthenticationError.make({ reason: 'stale' })
	}
	const expected = decodeSignature(input.signature)
	if (expected === undefined) {
		return yield* WebhookAuthenticationError.make({ reason: 'invalid_signature' })
	}
	const actual = yield* hmacSha256({
		secret: input.signingSecret,
		data: slackSignatureBase(input.timestamp, input.body),
	}).pipe(
		Effect.tapError((error) => Effect.logError('Slack HMAC calculation failed', error)),
		Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'crypto' })),
	)
	if (!constantTimeEqual(actual, expected)) {
		return yield* WebhookAuthenticationError.make({ reason: 'invalid_signature' })
	}
})
