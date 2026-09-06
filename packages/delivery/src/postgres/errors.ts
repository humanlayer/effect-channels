import { Effect } from 'effect'
import type { Schema } from 'effect'
import type { SqlError } from 'effect/unstable/sql/SqlError'

import { MailboxStoreError } from '../MailboxStore.ts'

type StoreOperation = Pick<MailboxStoreError, 'operation'>

export const storeErrors =
	(input: StoreOperation) =>
	<A, R>(effect: Effect.Effect<A, SqlError | Schema.SchemaError, R>) =>
		effect.pipe(
			Effect.tapErrorTag('SqlError', (error) =>
				Effect.logError('Delivery Postgres command failed', {
					operation: input.operation,
					reason: error.reason._tag,
					retryable: error.isRetryable,
				}),
			),
			Effect.tapErrorTag('SchemaError', () =>
				Effect.logError('Delivery Postgres codec failed', {
					operation: input.operation,
					reason: 'SchemaError',
				}),
			),
			Effect.catchTags({
				SqlError: () => Effect.fail(MailboxStoreError.make(input)),
				SchemaError: () => Effect.fail(MailboxStoreError.make(input)),
			}),
		)
