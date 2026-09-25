import { Context, Effect, Encoding, Layer, Redacted, Schema } from 'effect'

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
>()('@humanlayer/channels-github-next/internal/GitHubAppSigner') {
	static readonly layerWebCrypto = Layer.sync(GitHubAppSigner, () => {
		const encoder = new TextEncoder()
		return GitHubAppSigner.of({
			sign: ({ privateKey, data }) =>
				Effect.tryPromise({
					try: async () => {
						const bytes = privateKeyBytes(Redacted.value(privateKey))
						const key = await globalThis.crypto.subtle.importKey(
							'pkcs8',
							bytes,
							{ name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
							false,
							['sign'],
						)
						const signature = await globalThis.crypto.subtle.sign(
							'RSASSA-PKCS1-v1_5',
							key,
							encoder.encode(data),
						)
						return Encoding.encodeBase64Url(new Uint8Array(signature))
					},
					catch: () => GitHubSigningError.make({}),
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

const privateKeyBytes = (pem: string) => {
	const pkcs1 = pem.startsWith('-----BEGIN RSA PRIVATE KEY-----')
	let label = 'PRIVATE KEY'
	if (pkcs1) label = 'RSA PRIVATE KEY'
	if (!pem.startsWith(`-----BEGIN ${label}-----`) || !pem.trimEnd().endsWith(`-----END ${label}-----`)) {
		throw new Error('Invalid PEM')
	}
	const bytes = Uint8Array.from(
		atob(
			pem
				.replace(`-----BEGIN ${label}-----`, '')
				.replace(`-----END ${label}-----`, '')
				.replace(pemWhitespace, ''),
		),
		(character) => character.charCodeAt(0),
	)
	if (!pkcs1) return bytes
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
}
