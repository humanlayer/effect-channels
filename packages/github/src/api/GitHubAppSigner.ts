import { Context, Effect, Layer, Redacted, Result, Schema } from 'effect'
import { Base64, Base64Url } from 'effect/encoding'

export class GitHubSigningError extends Schema.TaggedError<GitHubSigningError>()('GitHubSigningError', {}) {}

/** Package-private signing seam. Production uses Web Crypto; tests replace it with a Layer. */
export class GitHubAppSigner extends Context.Service<
	GitHubAppSigner,
	{
		readonly sign: (input: {
			readonly privateKey: Redacted.Redacted<string>
			readonly data: string
		}) => Effect.Effect<string, GitHubSigningError>
	}
>()('@humanlayer/channels-github/internal/GitHubAppSigner') {
	static readonly layerWebCrypto = Layer.sync(GitHubAppSigner, () => {
		const encoder = new TextEncoder()
		return GitHubAppSigner.of({
			sign: Effect.fn('github.app_signer.sign')(function* ({ privateKey, data }) {
				const bytes = yield* privateKeyBytes(Redacted.value(privateKey))
				const key = yield* Effect.tryPromise({
					try: () =>
						globalThis.crypto.subtle.importKey(
							'pkcs8',
							bytes,
							{ name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
							false,
							['sign'],
						),
					catch: () => GitHubSigningError.make({}),
				})
				const signature = yield* Effect.tryPromise({
					try: () => globalThis.crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, encoder.encode(data)),
					catch: () => GitHubSigningError.make({}),
				})
				return Base64Url.encode(new Uint8Array(signature))
			}),
		})
	})
}

const pemWhitespace = /\s/g

const der = (tag: number, bytes: Uint8Array): Uint8Array<ArrayBuffer> => {
	let length: ReadonlyArray<number>
	if (bytes.length < 128) length = [bytes.length]
	else if (bytes.length < 256) length = [0x81, bytes.length]
	else length = [0x82, bytes.length >> 8, bytes.length & 255]
	return new Uint8Array([tag, ...length, ...bytes])
}

const privateKeyBytes = (pem: string): Effect.Effect<Uint8Array<ArrayBuffer>, GitHubSigningError> => {
	const pkcs1 = pem.startsWith('-----BEGIN RSA PRIVATE KEY-----')
	let label = 'PRIVATE KEY'
	if (pkcs1) label = 'RSA PRIVATE KEY'
	if (!pem.startsWith(`-----BEGIN ${label}-----`) || !pem.trimEnd().endsWith(`-----END ${label}-----`)) {
		return Effect.fail(GitHubSigningError.make({}))
	}
	return Base64.decode(
		pem.replace(`-----BEGIN ${label}-----`, '').replace(`-----END ${label}-----`, '').replace(pemWhitespace, ''),
	).pipe(
		Result.map((bytes) => (pkcs1 ? pkcs1ToPkcs8(bytes) : new Uint8Array(bytes))),
		Result.mapError(() => GitHubSigningError.make({})),
		Effect.fromResult,
	)
}

const pkcs1ToPkcs8 = (bytes: Uint8Array) =>
	der(
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
