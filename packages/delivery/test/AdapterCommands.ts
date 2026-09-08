import { Context, DateTime, Effect, Layer, Option, Queue, Schema, Stream } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'
import * as Reactivity from 'effect/unstable/reactivity/Reactivity'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import type { Connection } from 'effect/unstable/sql/SqlConnection'
import type { SqlError } from 'effect/unstable/sql/SqlError'
import * as Statement from 'effect/unstable/sql/Statement'

import { ActiveBatch, currentMailbox, Envelope, eventIdentity, MailboxSnapshot, MailboxState } from '../src/Mailbox.js'

export interface SqlCommand {
	readonly sql: string
	readonly params: ReadonlyArray<unknown>
}

export const sqlCommands = Effect.gen(function* () {
	const commands = yield* Queue.unbounded<SqlCommand>()
	const replies = yield* Queue.unbounded<Effect.Effect<ReadonlyArray<unknown>, SqlError>>()
	const lockReplies = yield* Queue.unbounded<Effect.Effect<void, SqlError>>()
	const execute: Connection['execute'] = (sql, params) =>
		Effect.gen(function* () {
			yield* Queue.offer(commands, { sql, params })
			if (sql.startsWith('SELECT pg_advisory_xact_lock')) {
				const reply = yield* Queue.poll(lockReplies)
				if (Option.isSome(reply)) yield* reply.value
				return []
			}
			if (sql.includes('humanlayer_delivery_v1_mailboxes')) return yield* yield* Queue.take(replies)
			if (sql.startsWith('SELECT migration_id')) {
				return [{ migration_id: 1, name: 'mailboxes', created_at: DateTime.toDateUtc(DateTime.makeUnsafe(0)) }]
			}
			if (
				sql === 'BEGIN' ||
				sql === 'COMMIT' ||
				sql === 'ROLLBACK' ||
				sql.startsWith('SAVEPOINT') ||
				sql.startsWith('ROLLBACK TO SAVEPOINT') ||
				sql.startsWith('SELECT pg_advisory_xact_lock') ||
				sql.startsWith('CREATE TABLE IF NOT EXISTS humanlayer_delivery_v1_migrations') ||
				sql.startsWith('select ') ||
				sql.startsWith('LOCK TABLE')
			)
				return []
			return yield* Effect.die(`Unexpected SQL command: ${sql}`)
		})
	const connection: Connection = {
		execute,
		executeUnprepared: execute,
		executeRaw: () => Effect.die('Unexpected executeRaw'),
		executeStream: () => Stream.die('Unexpected executeStream'),
		executeValues: () => Effect.die('Unexpected executeValues'),
		executeValuesUnprepared: () => Effect.die('Unexpected executeValuesUnprepared'),
	}
	const compiler = Statement.makeCompiler({
		dialect: 'pg',
		placeholder: (index) => `$${index}`,
		onIdentifier: Statement.defaultEscape('"'),
		onRecordUpdate: () => {
			throw new Error('Unexpected record update fragment')
		},
		onCustom: () => {
			throw new Error('Unexpected custom fragment')
		},
	})
	const client = yield* SqlClient.make({ acquirer: Effect.succeed(connection), compiler, spanAttributes: [] })
	return { commands, replies, lockReplies, layer: Layer.succeed(SqlClient.SqlClient, client) }
}).pipe(Effect.provide(Reactivity.layer))

export interface RedisCommand {
	readonly command: string
	readonly args: ReadonlyArray<string>
}

interface RedisCommandSeam {
	readonly send: (command: string, ...args: ReadonlyArray<string>) => Effect.Effect<unknown, Redis.RedisError>
	readonly eval: <C extends { readonly params: ReadonlyArray<unknown>; readonly result: unknown }>(
		script: Redis.Script<C>,
	) => (...params: C['params']) => Effect.Effect<unknown, Redis.RedisError>
}

const redisCommandSeam: Context.Key<Redis.Redis, RedisCommandSeam> = Redis.Redis

export const redisCommands = Effect.gen(function* () {
	const commands = yield* Queue.unbounded<RedisCommand>()
	const replies = yield* Queue.unbounded<Effect.Effect<unknown, Redis.RedisError>>()
	const send = (command: string, ...args: ReadonlyArray<string>) =>
		Queue.offer(commands, { command, args }).pipe(Effect.andThen(Queue.take(replies)), Effect.flatten)
	const service: RedisCommandSeam = {
		send,
		eval:
			(script) =>
			(...params) =>
				send(
					'EVAL',
					script.lua,
					String(script.numberOfKeys(...params)),
					...script.params(...params).map(String),
				),
	}
	return { commands, replies, layer: Layer.succeed(redisCommandSeam, service) }
})

const envelope = Envelope.make({
	definition: 'codec.test',
	version: '1',
	eventId: 'A',
	resource: 'root',
	payload: '{"private":"payload"}',
	acceptedAt: 10,
})
const active = ActiveBatch.make({ envelopes: [envelope], attempt: 3, owner: 17, leaseUntil: 200, cancelled: true })
export const completeMailbox = MailboxState.make({
	version: 1,
	pending: [{ ...envelope, eventId: 'B' }],
	active,
	failed: [{ ...active, owner: null }],
	outcomes: [{ identity: 'previous', kind: 'control', expiresAt: 300 }],
	readyAt: 200,
})
export const mailboxCodecCases: ReadonlyArray<MailboxState> = [
	completeMailbox,
	{
		...currentMailbox(completeMailbox),
		additionalActive: [{ ...active, owner: 18, envelopes: [{ ...envelope, eventId: 'C' }] }],
		pendingReadyAt: 150,
		outcomes: [{ identity: 'dropped', kind: 'dropped', expiresAt: 300 }],
	},
	{ ...currentMailbox(completeMailbox), pendingReadyAt: 250, burstDraining: true },
	{
		...currentMailbox(completeMailbox),
		outcomes: [
			{
				identity: 'control:targeted',
				kind: 'control',
				expiresAt: 300,
				cancellationTarget: { identity: eventIdentity(envelope), acceptedAt: envelope.acceptedAt },
			},
			{ identity: 'control:empty', kind: 'control', expiresAt: 300, cancellationTarget: null },
		],
	},
]
export const encodeState = Schema.encodeSync(Schema.fromJsonString(MailboxState))
export const encodeSnapshot = Schema.encodeSync(Schema.fromJsonString(MailboxSnapshot))
export const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.String))
