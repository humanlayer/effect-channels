import { Context, Effect, Encoding, Layer, Redacted, Schema } from 'effect'

import { GitHubError, GitHubWebhookError } from './GitHubErrors'

class GitHubCryptoFailure extends Schema.TaggedError<GitHubCryptoFailure>()('GitHubCryptoFailure', {
	operation: Schema.Literals(['sign_app', 'verify_webhook']),
}) {}

const pemWhitespace = /\s/g
const signaturePattern = /^sha256=[0-9a-f]{64}$/
const hexPairs = /../g
const der = (tag: number, bytes: Uint8Array): Uint8Array<ArrayBuffer> => {
	const length =
		bytes.length < 128
			? [bytes.length]
			: bytes.length < 256
				? [0x81, bytes.length]
				: [0x82, bytes.length >> 8, bytes.length & 255]
	return new Uint8Array([tag, ...length, ...bytes])
}
const privateKeyBytes = Effect.fnUntraced(function* (pem: string) {
	const pkcs1 = pem.startsWith('-----BEGIN RSA PRIVATE KEY-----')
	const label = pkcs1 ? 'RSA PRIVATE KEY' : 'PRIVATE KEY'
	if (!pem.startsWith(`-----BEGIN ${label}-----`) || !pem.trimEnd().endsWith(`-----END ${label}-----`))
		return yield* GitHubCryptoFailure.make({ operation: 'sign_app' })
	const bytes = yield* Effect.fromResult(
		Encoding.decodeBase64(
			pem
				.replace(`-----BEGIN ${label}-----`, '')
				.replace(`-----END ${label}-----`, '')
				.replace(pemWhitespace, ''),
		),
	).pipe(Effect.mapError(() => GitHubCryptoFailure.make({ operation: 'sign_app' })))
	if (!pkcs1) return new Uint8Array(bytes)
	return der(
		0x30,
		new Uint8Array([
			0x02,
			0x01,
			0x00,
			0x30,
			0x0d,
			0x06,
			0x09,
			0x2a,
			0x86,
			0x48,
			0x86,
			0xf7,
			0x0d,
			0x01,
			0x01,
			0x01,
			0x05,
			0x00,
			...der(0x04, bytes),
		]),
	)
})

export class GitHubCrypto extends Context.Service<
	GitHubCrypto,
	{
		readonly signApp: (input: {
			readonly privateKey: Redacted.Redacted<string>
			readonly data: string
		}) => Effect.Effect<string, GitHubError>
		readonly verifyWebhook: (input: {
			readonly secret: Redacted.Redacted<string>
			readonly body: Uint8Array
			readonly signature: string
		}) => Effect.Effect<void, GitHubWebhookError>
	}
>()('github/GitHubCrypto') {
	static readonly layerWebCrypto = Layer.sync(GitHubCrypto, () => {
		const encoder = new TextEncoder()
		return GitHubCrypto.of({
			signApp: Effect.fn('github.crypto.sign_app')(
				function* (input) {
					const bytes = yield* privateKeyBytes(Redacted.value(input.privateKey))
					const key = yield* Effect.tryPromise({
						try: () =>
							globalThis.crypto.subtle.importKey(
								'pkcs8',
								bytes,
								{ name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
								false,
								['sign'],
							),
						catch: () => GitHubCryptoFailure.make({ operation: 'sign_app' }),
					})
					const signature = yield* Effect.tryPromise({
						try: () => globalThis.crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, encoder.encode(input.data)),
						catch: () => GitHubCryptoFailure.make({ operation: 'sign_app' }),
					})
					return Encoding.encodeBase64Url(new Uint8Array(signature))
				},
				Effect.tapError((error) => Effect.logError('GitHub crypto failed', error)),
				Effect.catchTag('GitHubCryptoFailure', () =>
					Effect.fail(GitHubError.make({ reason: 'configuration' })),
				),
			),
			verifyWebhook: Effect.fn('github.crypto.verify_webhook')((input) =>
				Effect.gen(function* () {
					if (!signaturePattern.test(input.signature))
						return yield* GitHubWebhookError.make({ reason: 'signature' })
					const signature = Uint8Array.from(input.signature.slice(7).match(hexPairs) ?? [], (byte) =>
						Number.parseInt(byte, 16),
					)
					const valid = yield* Effect.gen(function* () {
						const key = yield* Effect.tryPromise({
							try: () =>
								globalThis.crypto.subtle.importKey(
									'raw',
									encoder.encode(Redacted.value(input.secret)),
									{ name: 'HMAC', hash: 'SHA-256' },
									false,
									['verify'],
								),
							catch: () => GitHubCryptoFailure.make({ operation: 'verify_webhook' }),
						})
						return yield* Effect.tryPromise({
							try: () =>
								globalThis.crypto.subtle.verify('HMAC', key, signature, new Uint8Array(input.body)),
							catch: () => GitHubCryptoFailure.make({ operation: 'verify_webhook' }),
						})
					}).pipe(
						Effect.tapError((error) => Effect.logError('GitHub crypto failed', error)),
						Effect.catchTag('GitHubCryptoFailure', () =>
							Effect.fail(GitHubWebhookError.make({ reason: 'crypto' })),
						),
					)
					if (!valid) return yield* GitHubWebhookError.make({ reason: 'signature' })
				}),
			),
		})
	})
}
