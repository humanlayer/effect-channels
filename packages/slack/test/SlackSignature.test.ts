import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Effect, Redacted } from 'effect'
import { TestClock } from 'effect/testing'

import { hmacSha256, verifySlackSignature } from '../src/SlackSignature.ts'
import { bytesToHex, signSlackBody, signingSecret } from './support.ts'

it.effect('verifies the exact body bytes inside the replay window', () =>
	Effect.gen(function* () {
		const body = '{"type":"event_callback","text":"exact bytes"}'
		const timestamp = '0'
		const signature = yield* signSlackBody(body, timestamp)

		yield* verifySlackSignature({ body, timestamp, signature, signingSecret })
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('rejects stale and modified requests', () =>
	Effect.gen(function* () {
		const body = '{"type":"event_callback"}'
		const timestamp = '0'
		const signature = yield* signSlackBody(body, timestamp)
		yield* TestClock.adjust('301 seconds')

		const stale = yield* Effect.flip(verifySlackSignature({ body, timestamp, signature, signingSecret }))
		const modified = yield* Effect.flip(
			verifySlackSignature({ body: `${body} `, timestamp: '301', signature, signingSecret }),
		)

		assert.strictEqual(stale.reason, 'stale')
		assert.strictEqual(modified.reason, 'invalid_signature')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('matches the RFC 4231 HMAC-SHA256 vector', () =>
	Effect.gen(function* () {
		const digest = yield* hmacSha256({
			secret: Redacted.make('Jefe'),
			data: new TextEncoder().encode('what do ya want for nothing?'),
		})

		assert.strictEqual(bytesToHex(digest), '5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)
