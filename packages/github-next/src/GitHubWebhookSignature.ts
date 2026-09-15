import { WebhookAuthenticationError } from '@humanlayer/channels-delivery-next'
import { Crypto, Effect, Redacted, Schema } from 'effect'

const GitHubSignatureInput = Schema.Struct({
	body: Schema.Uint8Array,
	signature: Schema.String,
	webhookSecret: Schema.Redacted(Schema.String, { disallowJsonEncode: true }),
})
type GitHubSignatureInput = typeof GitHubSignatureInput.Type

const hexadecimal = /^[\da-f]+$/
const hmacBlockSize = 64
const textEncoder = new TextEncoder()

const concatenateBytes = (left: Uint8Array, right: Uint8Array) => {
	const output = new Uint8Array(left.length + right.length)
	output.set(left)
	output.set(right, left.length)
	return output
}

const hmacSha256 = Effect.fn('github.crypto.hmac_sha256')(function* (
	secret: Redacted.Redacted<string>,
	data: Uint8Array,
) {
	const crypto = yield* Crypto.Crypto
	const encodedKey = textEncoder.encode(Redacted.value(secret))
	const key = encodedKey.length > hmacBlockSize ? yield* crypto.digest('SHA-256', encodedKey) : encodedKey
	const padded = new Uint8Array(hmacBlockSize)
	padded.set(key)
	const inner = new Uint8Array(hmacBlockSize)
	const outer = new Uint8Array(hmacBlockSize)
	for (let index = 0; index < hmacBlockSize; index += 1) {
		inner[index] = (padded.at(index) ?? 0) ^ 0x36
		outer[index] = (padded.at(index) ?? 0) ^ 0x5c
	}
	const innerDigest = yield* crypto.digest('SHA-256', concatenateBytes(inner, data))
	return yield* crypto.digest('SHA-256', concatenateBytes(outer, innerDigest))
})

const decodeSignature = (signature: string) => {
	if (!signature.startsWith('sha256=')) return undefined
	const hexadecimalSignature = signature.slice(7)
	if (hexadecimalSignature.length !== 64 || !hexadecimal.test(hexadecimalSignature)) return undefined
	return Uint8Array.from({ length: 32 }, (_, index) =>
		Number.parseInt(hexadecimalSignature.slice(index * 2, index * 2 + 2), 16),
	)
}

const constantTimeEqual = (left: Uint8Array, right: Uint8Array) => {
	let difference = left.length ^ right.length
	for (let index = 0; index < Math.max(left.length, right.length); index += 1) {
		difference |= (left.at(index) ?? 0) ^ (right.at(index) ?? 0)
	}
	return difference === 0
}

export const verifyGitHubWebhookSignature = Effect.fn('github.signature.verify')(function* (
	input: GitHubSignatureInput,
) {
	const expected = decodeSignature(input.signature)
	if (expected === undefined) return yield* WebhookAuthenticationError.make({ reason: 'invalid_signature' })
	const actual = yield* hmacSha256(input.webhookSecret, input.body).pipe(
		Effect.tapError((error) => Effect.logError('GitHub HMAC calculation failed', error)),
		Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'crypto' })),
	)
	if (!constantTimeEqual(actual, expected)) {
		return yield* WebhookAuthenticationError.make({ reason: 'invalid_signature' })
	}
})
