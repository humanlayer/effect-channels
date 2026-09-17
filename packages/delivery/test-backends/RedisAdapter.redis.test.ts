import { assert, it } from '@effect/vitest'
import { Config, Effect, Layer, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import * as Redis from 'effect/unstable/persistence/Redis'

import { emptyMailbox } from '../src/Mailbox'
import { MailboxReadiness, MailboxStore, MailboxStoreError } from '../src/MailboxStore'
import { layer } from '../src/redis'
import * as Client from '../src/redis/client'
import { readyKey, readyKeys, recordKey } from '../src/redis/keys'
import { encodeKey } from '../test/AdapterCommands'
import {
	interruptForReconstruction,
	policy,
	resumeAfterReconstruction,
	staleAttemptContract,
	storageContract,
	storedState,
} from './StoreContract'

const client = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		return Client.layer({
			socket: { host: '127.0.0.1', port: 56379, connectTimeout: 3000, reconnectStrategy: false },
		})
	}),
)

it.effect(
	'isolated Redis: atomic CAS/index, no expiry, wrong-type preflight, script reload and fresh-layer recovery',
	() =>
		Effect.gen(function* () {
			const redis = yield* Redis.Redis
			assert.strictEqual(
				yield* redis.send('DBSIZE'),
				0,
				'Use a fresh disposable Redis; this suite will not clear an existing store',
			)
			yield* storageContract.pipe(Effect.provide(layer))
			for (const prefix of readyKeys({ key: 'contract%_!\\:cas' })) {
				assert.strictEqual(yield* redis.send('ZSCORE', prefix, encodeKey('contract%_!\\:cas')), null)
			}
			assert.strictEqual(yield* redis.send('TTL', recordKey({ key: 'contract%_!\\:cas' })), -1)
			yield* Effect.gen(function* () {
				const store = yield* MailboxStore
				const readiness = yield* MailboxReadiness
				const key = 'reload:😀\ud83d'
				assert.strictEqual(
					yield* store.commitMailbox({ key, expectedRevision: null, nextState: storedState }),
					'committed',
				)
				assert.strictEqual(yield* redis.send('TTL', recordKey({ key })), -1)
				for (const index of readyKeys({ key })) {
					assert.strictEqual(yield* redis.send('TTL', index), -1)
					assert.strictEqual(yield* redis.send('ZSCORE', index, encodeKey(key)), '10')
				}
				yield* redis.send('SCRIPT', 'FLUSH', 'SYNC')
				assert.strictEqual(
					yield* store.commitMailbox({
						key,
						expectedRevision: 0,
						nextState: { ...storedState, readyAt: 20 },
					}),
					'committed',
				)
				assert.deepStrictEqual(yield* store.loadMailbox({ key }), {
					revision: 1,
					state: { ...storedState, readyAt: 20 },
				})
				assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 19, limit: 1 }), [])
				assert.deepStrictEqual(yield* readiness.scanReady({ prefix: key, now: 20, limit: 1 }), [key])
				const broken = 'wrong-type-index'
				yield* redis.send('SET', readyKey({ prefix: broken }), 'not-a-sorted-set')
				assert.deepStrictEqual(
					yield* store
						.commitMailbox({ key: broken, expectedRevision: null, nextState: storedState })
						.pipe(Effect.flip),
					MailboxStoreError.make({ operation: 'commit' }),
				)
				assert.strictEqual(yield* store.loadMailbox({ key: broken }), undefined)
				assert.strictEqual(yield* redis.send('ZSCORE', readyKey({ prefix: '' }), encodeKey(broken)), null)
				yield* redis.send('DEL', readyKey({ prefix: broken }))
				assert.strictEqual(
					yield* store.commitMailbox({ key, expectedRevision: 1, nextState: emptyMailbox() }),
					'committed',
				)
				for (const index of readyKeys({ key }))
					assert.strictEqual(yield* redis.send('ZSCORE', index, encodeKey(key)), null)
			}).pipe(Effect.provide(layer))
			yield* staleAttemptContract.pipe(Effect.provide(Layer.fresh(layer)))
			const receipt = yield* interruptForReconstruction.pipe(Effect.provide(Layer.fresh(layer)))
			yield* TestClock.adjust(policy.leaseMs)
			yield* resumeAfterReconstruction(receipt).pipe(Effect.provide(Layer.fresh(layer)))
		}).pipe(Effect.provide(client)),
	{ timeout: 30_000 },
)
