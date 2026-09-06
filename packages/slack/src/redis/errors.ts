import { Effect, Predicate, Schema } from 'effect'
import type * as Redis from 'effect/unstable/persistence/Redis'

import { SubscriptionStoreError } from '../DomainErrors.ts'
import { SlackConnectionStoreError } from '../SlackConnectionStore.ts'

type ConnectionOperation = Pick<SlackConnectionStoreError, 'operation'>
type SubscriptionOperation = Pick<SubscriptionStoreError, 'operation' | 'threadId'>

const commandCause = Schema.Union([
	Schema.String,
	Schema.Struct({ code: Schema.optionalKey(Schema.String), message: Schema.optionalKey(Schema.String) }),
])
const reasonCode = Schema.Literals([
	'ECONNREFUSED',
	'ECONNRESET',
	'ETIMEDOUT',
	'EPIPE',
	'ENOTFOUND',
	'WRONGTYPE',
	'OOM',
	'NOAUTH',
	'NOPERM',
	'READONLY',
	'LOADING',
	'CLUSTERDOWN',
	'NOSCRIPT',
	'MOVED',
	'ASK',
	'SLACK_SUBSCRIPTION_VALUE',
])

const capture =
	(input: ConnectionOperation | SubscriptionOperation) =>
	<A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
		effect.pipe(
			Effect.tapErrorTag('RedisError', (error) =>
				Schema.decodeUnknownEffect(commandCause)(error.cause).pipe(
					Effect.flatMap((cause) =>
						Schema.decodeUnknownEffect(reasonCode)(
							Predicate.isString(cause)
								? cause.split(' ', 1)[0]
								: (cause.code ?? cause.message?.split(' ', 1)[0]),
						),
					),
					Effect.catchTag('SchemaError', () => Effect.succeed('Unclassified')),
					Effect.flatMap((code) =>
						Effect.logError('Slack Redis command failed', { ...input, reason: 'RedisError', code }),
					),
				),
			),
			Effect.tapErrorTag('SchemaError', (error) =>
				Effect.logError('Slack Redis codec failed', {
					...input,
					reason: 'SchemaError',
					issue: error.issue._tag,
				}),
			),
		)

export const connectionErrors =
	(input: ConnectionOperation) =>
	<A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
		effect.pipe(
			capture(input),
			Effect.catchTags({
				RedisError: () => Effect.fail(new SlackConnectionStoreError(input)),
				SchemaError: () => Effect.fail(new SlackConnectionStoreError(input)),
			}),
		)

export const subscriptionErrors =
	(input: SubscriptionOperation) =>
	<A, R>(effect: Effect.Effect<A, Redis.RedisError | Schema.SchemaError, R>) =>
		effect.pipe(
			capture(input),
			Effect.catchTags({
				RedisError: () => Effect.fail(new SubscriptionStoreError(input)),
				SchemaError: () => Effect.fail(new SubscriptionStoreError(input)),
			}),
		)
