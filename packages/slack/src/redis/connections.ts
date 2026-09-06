import { Effect, Layer, Schema } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { SlackConnectionLookupInput } from '../Schema.ts'
import { SlackConnectionStore, UpsertSlackConnection } from '../SlackConnectionStore.ts'
import { connectionJson } from '../storage/ConnectionCodec.ts'
import { connectionErrors } from './errors.ts'
import { connectionKey } from './keys.ts'

const get = Effect.fn('slack.redis.connections.get')(
	function* (input: SlackConnectionLookupInput) {
		yield* Schema.decodeEffect(SlackConnectionLookupInput)(input)
		const redis = yield* Redis.Redis
		const result = yield* redis.send('GET', connectionKey(input))
		return (yield* Schema.decodeUnknownEffect(Schema.NullOr(connectionJson))(result)) ?? undefined
	},
	connectionErrors({ operation: 'get' }),
)

const upsert = Effect.fn('slack.redis.connections.upsert')(
	function* (input: UpsertSlackConnection) {
		yield* Schema.decodeEffect(UpsertSlackConnection)(input)
		const redis = yield* Redis.Redis
		const json = yield* Schema.encodeEffect(connectionJson)(input.connection)
		yield* Schema.decodeUnknownEffect(Schema.Literal('OK'))(yield* redis.send('SET', connectionKey(input), json))
	},
	connectionErrors({ operation: 'upsert' }),
)

const remove = Effect.fn('slack.redis.connections.remove')(
	function* (input: SlackConnectionLookupInput) {
		yield* Schema.decodeEffect(SlackConnectionLookupInput)(input)
		const redis = yield* Redis.Redis
		yield* Schema.decodeUnknownEffect(Schema.Literals([0, 1]))(yield* redis.send('DEL', connectionKey(input)))
	},
	connectionErrors({ operation: 'remove' }),
)

export const connections = Layer.effect(
	SlackConnectionStore,
	Effect.gen(function* () {
		const redis = yield* Redis.Redis
		return SlackConnectionStore.of({
			get: (input) => get(input).pipe(Effect.provideService(Redis.Redis, redis)),
			upsert: (input) => upsert(input).pipe(Effect.provideService(Redis.Redis, redis)),
			remove: (input) => remove(input).pipe(Effect.provideService(Redis.Redis, redis)),
		})
	}),
)
