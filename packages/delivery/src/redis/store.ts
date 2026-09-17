import { Context, Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { deliveryIds, MailboxSnapshot } from '../Mailbox'
import {
	CommitMailbox,
	DeliveryLocatorStore,
	LoadMailbox,
	LocateDelivery,
	MailboxReadiness,
	MailboxStore,
	MailboxStoreError,
	ScanReady,
} from '../MailboxStore'
import { deliveryLocatorKey, readyKey, recordKey } from './keys'
import * as Scripts from './scripts'

const snapshotCodec = Schema.fromJsonString(MailboxSnapshot)
const storedRecord = Schema.Union([
	Schema.Tuple([Schema.Null, Schema.Null]),
	Schema.Tuple([
		Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
		snapshotCodec,
	]).check(Schema.makeFilter(([revision, snapshot]) => revision === snapshot.revision)),
])

type StoreOperation = Pick<MailboxStoreError, 'operation'>
const storeErrors =
	(input: StoreOperation) =>
	<A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
		effect.pipe(
			Effect.tapErrorTag('RedisError', () =>
				Effect.logError('Delivery Redis command failed', {
					operation: input.operation,
					reason: 'RedisError',
				}),
			),
			Effect.tapErrorTag('SchemaError', () =>
				Effect.logError('Delivery Redis codec failed', {
					operation: input.operation,
					reason: 'SchemaError',
				}),
			),
			Effect.catchTags({
				RedisError: () => Effect.fail(MailboxStoreError.make(input)),
				SchemaError: () => Effect.fail(MailboxStoreError.make(input)),
			}),
		)

const loadMailbox = Effect.fn('delivery.redis.load')(
	function* (input: LoadMailbox) {
		yield* Schema.decodeEffect(LoadMailbox)(input)
		const redis = yield* Redis.Redis
		const result = yield* redis.send('HMGET', recordKey(input), 'revision', 'snapshot')
		const [, snapshot] = yield* Schema.decodeUnknownEffect(storedRecord)(result)
		return snapshot ?? undefined
	},
	storeErrors({ operation: 'load' }),
)

const commitMailbox = Effect.fn('delivery.redis.commit')(
	function* (input: CommitMailbox) {
		yield* Schema.decodeEffect(CommitMailbox)(input)
		const revision = yield* Schema.decodeEffect(Schema.Natural)((input.expectedRevision ?? -1) + 1)
		const json = yield* Schema.encodeEffect(snapshotCodec)({ revision, state: input.nextState })
		const redis = yield* Redis.Redis
		const result = yield* redis.eval(Scripts.commit)({
			...input,
			revision,
			json,
			readyAt: input.nextState.readyAt,
			deliveryIds: deliveryIds(input.nextState).filter((deliveryId) => deliveryId.startsWith('delivery:v2:')),
		})
		const committed = yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(result)
		return committed === 1 ? ('committed' as const) : ('conflict' as const)
	},
	storeErrors({ operation: 'commit' }),
)

const locateDelivery = Effect.fn('delivery.redis.locate')(
	function* (input: LocateDelivery) {
		yield* Schema.decodeEffect(LocateDelivery)(input)
		const redis = yield* Redis.Redis
		const result = yield* redis.send('HGET', deliveryLocatorKey, input.deliveryId)
		return yield* Schema.decodeUnknownEffect(Schema.NullOr(Schema.fromJsonString(Schema.NonEmptyString)))(
			result,
		).pipe(Effect.map((key) => key ?? undefined))
	},
	storeErrors({ operation: 'locate' }),
)

const scanReady = Effect.fn('delivery.redis.scan')(
	function* (input: ScanReady) {
		yield* Schema.decodeEffect(ScanReady)(input)
		const redis = yield* Redis.Redis
		const result = yield* redis.send(
			'ZRANGEBYSCORE',
			readyKey(input),
			'-inf',
			String(input.now),
			'LIMIT',
			'0',
			String(input.limit),
		)
		return yield* Schema.decodeUnknownEffect(
			Schema.Array(Schema.fromJsonString(Schema.String)).check(
				Schema.isMaxLength(input.limit),
				Schema.makeFilter((keys) => keys.every((key) => key.startsWith(input.prefix))),
			),
		)(result)
	},
	storeErrors({ operation: 'scan' }),
)

export const layer = Layer.effectContext(
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		return Context.make(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: (input) => loadMailbox(input).pipe(Effect.provideService(Redis.Redis, redis)),
				commitMailbox: (input) => commitMailbox(input).pipe(Effect.provideService(Redis.Redis, redis)),
			}),
		).pipe(
			Context.add(
				MailboxReadiness,
				MailboxReadiness.of({
					scanReady: (input) => scanReady(input).pipe(Effect.provideService(Redis.Redis, redis)),
				}),
			),
			Context.add(
				DeliveryLocatorStore,
				DeliveryLocatorStore.of({
					locateDelivery: (input) => locateDelivery(input).pipe(Effect.provideService(Redis.Redis, redis)),
				}),
			),
		)
	}),
)
