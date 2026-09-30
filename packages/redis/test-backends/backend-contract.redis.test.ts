/**
 * Runs the shared mailbox backend contract against a real, disposable Redis.
 *
 * Every contract test builds the layer afresh, and building it runs FLUSHDB, so each test starts from
 * an empty store. Point it only at a Redis you can afford to lose: it needs
 * DELIVERY_BACKEND_TEST_CONFIRM=disposable and reads the port from REDIS_CONTRACT_TEST_PORT.
 */
import * as NodeRedis from '@effect/platform-node/NodeRedis'
import { assert, it } from '@effect/vitest'
import { Config, Effect, Layer, Option, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import * as Redis from 'effect/unstable/persistence/Redis'

import {
	ClaimWaitingEvents,
	DeliveryAdmission,
	MailboxDelivery,
	MailboxProcessingBackend,
} from '../../delivery-next/src'
import {
	claimAll,
	claimFrozen,
	deliver,
	findWaiting,
	leaseMs,
	mailboxBackendContract,
	mailboxKey as contractMailboxKey,
	nextBatchIdentity,
} from '../../delivery-next/test/backend-contract'
import { handoffUnsupportedContract } from '../../delivery-next/test/delivery-handoff-contract'
import { MailboxDeliveryRedis, MailboxProcessingBackendRedis } from '../src'
import { mailboxStateKey } from '../src/Keys'

const disposableRedis = Layer.unwrap(
	Effect.gen(function* () {
		yield* Config.schema(Schema.Literal('disposable'), 'DELIVERY_BACKEND_TEST_CONFIRM')
		const port = yield* Config.port('REDIS_CONTRACT_TEST_PORT')
		return NodeRedis.layer({
			socket: { host: '127.0.0.1', port, connectTimeout: 3000, reconnectStrategy: false },
		})
	}),
)

const flushed = Layer.effectDiscard(
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		yield* redis.send('FLUSHDB')
	}),
)

const makeEmptyStore = () =>
	Layer.mergeAll(MailboxDeliveryRedis, MailboxProcessingBackendRedis({ claimLimit: 100 })).pipe(
		Layer.provide(flushed),
		Layer.provideMerge(disposableRedis),
	)

mailboxBackendContract('redis', makeEmptyStore)
handoffUnsupportedContract('redis', makeEmptyStore)

it.effect(
	'redis: hands back a claimed admission exactly as delivered, with no cjson round trip turning [] into {}',
	() =>
		Effect.gen(function* () {
			const admission = DeliveryAdmission.make({
				namespace: 'contract',
				provider: 'example',
				installationId: 'installation',
				resourceId: 'thread|with|pipes',
				eventId: 'event|1',
				payload: { empty: [], nested: { list: [[]], text: 'a|b', big: 9007199254740991, ratio: 0.1 } },
				interrupt: true,
			})
			const { mailboxKey } = yield* (yield* MailboxDelivery).deliver(admission)
			const claim = yield* (yield* MailboxProcessingBackend).claimMailbox(
				ClaimWaitingEvents.make({ mailboxKey, upToSequence: 0, leaseMs: 1_000, ...nextBatchIdentity() }),
			)
			assert.deepStrictEqual(Option.getOrThrow(claim).admissions, [admission])
		}).pipe(Effect.provide(makeEmptyStore())),
)

it.effect('redis: gives a frozen batch saved before batches had IDs an ID and token on its next claim', () =>
	Effect.gen(function* () {
		yield* deliver('a')
		const claim = yield* claimAll(yield* findWaiting)
		yield* (yield* Redis.Redis).send('HDEL', mailboxStateKey(contractMailboxKey), 'batch_id', 'access_token')
		yield* TestClock.adjust(leaseMs)
		const recovered = Option.getOrThrow(yield* claimFrozen)
		assert.strictEqual(recovered.attempt, 2)
		assert.match(recovered.batchId, /^legacy-[0-9a-f]{32}$/)
		assert.match(recovered.accessToken, /^[0-9a-f]{40}$/)
		assert.notStrictEqual(recovered.claimId, claim.claimId)
		yield* TestClock.adjust(leaseMs)
		const again = Option.getOrThrow(yield* claimFrozen)
		assert.strictEqual(again.batchId, recovered.batchId)
		assert.strictEqual(again.accessToken, recovered.accessToken)
	}).pipe(Effect.provide(makeEmptyStore())),
)
