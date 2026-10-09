/**
 * A fold event log in a Durable Object's SQLite: one object is one session's log. Opening the log loads
 * every row, so `resumeSession` replays them; appends write through before they publish. This mirrors
 * fold-agent's JSONL log with a table in place of the file.
 *
 * The object creates the table once when it wakes ({@link createTable}); each Fold session opens the log
 * ({@link open}) through Fold's `eventLogSource`.
 */
import * as SqliteClient from '@effect/sql-sqlite-do/SqliteClient'
import {
	EventLogCorruptEntryError,
	EventLogInvalidEntryError,
	EventLogUnavailableError,
	Ids,
	LogEntry,
	decodeStoredLogEntry,
	layerLiveIdFactory,
	storedLogEntry,
	type EventLogError,
	type EventLogService,
	type LogEntryInput,
	type LogSeq,
} from '@humanlayer/fold-core'
import { Effect, Layer, PubSub, Ref, Schema, Semaphore, Stream } from 'effect'
import * as SqlClient from 'effect/sql/SqlClient'

type LogRow = { readonly seq: number; readonly entry: string }

const entriesFrom = (entries: ReadonlyArray<LogEntry>, fromSeq: LogSeq) =>
	entries.filter((entry) => entry.seq >= fromSeq)

const LogEntryJson = Schema.fromJsonString(LogEntry)

/** Parse a row's JSON, then decode it by its stored version (older versions upcast to the current shape). */
const decodeRow = (row: LogRow) =>
	Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(row.entry).pipe(
		Effect.mapError(
			(cause) =>
				new EventLogCorruptEntryError({
					operation: 'entries',
					message: `Invalid JSON in fold_log at seq ${row.seq}`,
					seq: row.seq,
					cause,
				}),
		),
		Effect.flatMap((json) => decodeStoredLogEntry(json)),
	)

const encodeEntry = (entry: LogEntry) =>
	Schema.encodeEffect(LogEntryJson)(entry).pipe(
		Effect.mapError(
			(cause) =>
				new EventLogInvalidEntryError({
					operation: 'append',
					message: 'Unable to encode EventLog entry',
					cause,
				}),
		),
	)

const queryError = (operation: 'append' | 'entries') =>
	Effect.mapError(
		(cause) =>
			new EventLogUnavailableError({
				operation,
				message: 'fold_log query failed',
				retryable: false,
				cause,
			}),
	)

/** A Durable Object's storage, as the SQLite client takes it. */
type ObjectStorage = NonNullable<SqliteClient.SqliteClientConfig['storage']>

/** The Durable Object's SQLite as an Effect SQL client. */
const sqliteFor = (storage: ObjectStorage) => SqliteClient.layer({ storage })

/** Create the log's table if it does not exist. The object runs this once each time it wakes. */
export const createTable = (storage: ObjectStorage) =>
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient
		yield* sql`CREATE TABLE IF NOT EXISTS fold_log (seq INTEGER PRIMARY KEY, entry TEXT NOT NULL)`.pipe(
			queryError('entries'),
		)
	}).pipe(Effect.provide(sqliteFor(storage)))

/** Fold's event log over the current Durable Object's Effect SQL client. */
const make: Effect.Effect<EventLogService, EventLogError, SqlClient.SqlClient | Ids> = Effect.gen(function* () {
	const sql = yield* SqlClient.SqlClient
	const ids = yield* Ids

	const rows = yield* sql<LogRow>`SELECT seq, entry FROM fold_log ORDER BY seq`.pipe(queryError('entries'))
	const initialEntries = yield* Effect.forEach(rows, decodeRow)

	const entriesRef = yield* Ref.make<ReadonlyArray<LogEntry>>(initialEntries)
	const pubsub = yield* PubSub.unbounded<LogEntry>()
	const appendLock = yield* Semaphore.make(1)

	const append = Effect.fn('alchemy_cloudflare.event_log.append')((input: LogEntryInput) =>
		appendLock.withPermit(
			Effect.gen(function* () {
				const current = yield* Ref.get(entriesRef)
				const stored = yield* storedLogEntry(input, current.length, yield* ids.makeEventId)
				const entry = yield* encodeEntry(stored)

				yield* sql`INSERT INTO fold_log (seq, entry) VALUES (${stored.seq}, ${entry})`.pipe(
					queryError('append'),
				)
				yield* Ref.set(entriesRef, [...current, stored])
				yield* PubSub.publish(pubsub, stored)

				return stored
			}),
		),
	)

	const entries: EventLogService['entries'] = (fromSeq = 0) =>
		Stream.fromIterableEffect(Ref.get(entriesRef).pipe(Effect.map((snapshot) => entriesFrom(snapshot, fromSeq))))

	const subscribe: EventLogService['subscribe'] = (fromSeq = 0) =>
		Stream.unwrap(
			appendLock.withPermit(
				Effect.gen(function* () {
					const subscription = yield* PubSub.subscribe(pubsub)
					const snapshot = yield* Ref.get(entriesRef)

					return Stream.fromIterable(entriesFrom(snapshot, fromSeq)).pipe(
						Stream.concat(
							Stream.fromSubscription(subscription).pipe(Stream.filter((entry) => entry.seq >= fromSeq)),
						),
					)
				}),
			),
		)

	return { append, entries, subscribe } satisfies EventLogService
})

/**
 * Open the log in the object's SQLite, loading its rows. Fold opens it once per session through
 * `eventLogSource`; the SQLite client exists only on a live object, so it is built here.
 */
export const open = (storage: ObjectStorage) =>
	make.pipe(Effect.provide(Layer.merge(sqliteFor(storage), layerLiveIdFactory)))
