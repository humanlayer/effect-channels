import { Effect } from 'effect'
import type { Schema } from 'effect'
import type { SqlError } from 'effect/unstable/sql/SqlError'

import { SubscriptionStoreError } from '../DomainErrors.js'
import { SlackConnectionStoreError } from '../SlackConnectionStore.js'

type ConnectionOperation = Pick<SlackConnectionStoreError, 'operation'>
type SubscriptionOperation = Pick<SubscriptionStoreError, 'operation' | 'threadId'>

const capture =
	(input: ConnectionOperation | SubscriptionOperation) =>
	<A, R>(effect: Effect.Effect<A, SqlError | Schema.SchemaError, R>) =>
		effect.pipe(
			Effect.tapErrorTag('SqlError', (error) =>
				Effect.logError('Slack Postgres command failed', {
					...input,
					reason: error.reason._tag,
					retryable: error.isRetryable,
				}),
			),
			Effect.tapErrorTag('SchemaError', (error) =>
				Effect.logError('Slack Postgres codec failed', {
					...input,
					reason: 'SchemaError',
					issue: error.issue._tag,
				}),
			),
		)

export const connectionErrors =
	(input: ConnectionOperation) =>
	<A, R>(effect: Effect.Effect<A, SqlError | Schema.SchemaError, R>) =>
		effect.pipe(
			capture(input),
			Effect.catchTags({
				SqlError: () => Effect.fail(new SlackConnectionStoreError(input)),
				SchemaError: () => Effect.fail(new SlackConnectionStoreError(input)),
			}),
		)

export const subscriptionErrors =
	(input: SubscriptionOperation) =>
	<A, R>(effect: Effect.Effect<A, SqlError | Schema.SchemaError, R>) =>
		effect.pipe(
			capture(input),
			Effect.catchTags({
				SqlError: () => Effect.fail(new SubscriptionStoreError(input)),
				SchemaError: () => Effect.fail(new SubscriptionStoreError(input)),
			}),
		)
