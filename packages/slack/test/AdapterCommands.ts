import { Context, DateTime, Effect, Layer, Option, Queue, Stream } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'
import * as Reactivity from 'effect/unstable/reactivity/Reactivity'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import type { Connection } from 'effect/unstable/sql/SqlConnection'
import type { SqlError } from 'effect/unstable/sql/SqlError'
import * as Statement from 'effect/unstable/sql/Statement'

export interface SqlCommand {
	readonly sql: string
	readonly params: ReadonlyArray<unknown>
}

export const sqlCommands = Effect.gen(function* () {
	const commands = yield* Queue.unbounded<SqlCommand>()
	const replies = yield* Queue.unbounded<Effect.Effect<ReadonlyArray<unknown>, SqlError>>()
	const lockReplies = yield* Queue.unbounded<Effect.Effect<void, SqlError>>()
	const migrationReplies = yield* Queue.unbounded<Effect.Effect<ReadonlyArray<unknown>, SqlError>>()
	const ddlReplies = yield* Queue.unbounded<Effect.Effect<void, SqlError>>()
	const execute: Connection['execute'] = (sql, params) =>
		Effect.gen(function* () {
			yield* Queue.offer(commands, { sql, params })
			if (sql.startsWith('SELECT pg_advisory_xact_lock')) {
				const reply = yield* Queue.poll(lockReplies)
				if (Option.isSome(reply)) yield* reply.value
				return []
			}
			if (sql.startsWith('SELECT migration_id')) {
				const reply = yield* Queue.poll(migrationReplies)
				return Option.isSome(reply)
					? yield* reply.value
					: [
							{
								migration_id: 1,
								name: 'slack_state',
								created_at: DateTime.toDateUtc(DateTime.makeUnsafe(0)),
							},
						]
			}
			if (sql.startsWith('CREATE ')) {
				const reply = yield* Queue.poll(ddlReplies)
				if (Option.isSome(reply)) yield* reply.value
				return []
			}
			if (
				sql === 'BEGIN' ||
				sql === 'COMMIT' ||
				sql === 'ROLLBACK' ||
				sql.startsWith('SAVEPOINT') ||
				sql.startsWith('ROLLBACK TO SAVEPOINT') ||
				sql.startsWith('select ') ||
				sql.startsWith('LOCK TABLE') ||
				sql.startsWith('INSERT INTO "humanlayer_slack_v1_migrations"') ||
				sql.includes('LIMIT 128 FOR UPDATE SKIP LOCKED')
			)
				return []
			if (sql.includes('humanlayer_slack_v1_')) {
				const reply = yield* Queue.poll(replies)
				return Option.isSome(reply) ? yield* reply.value : yield* Effect.die('Unscripted SQL command')
			}
			return yield* Effect.die('Unexpected SQL command')
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
			throw new Error('Unexpected record update')
		},
		onCustom: () => {
			throw new Error('Unexpected custom fragment')
		},
	})
	const client = yield* SqlClient.make({ acquirer: Effect.succeed(connection), compiler, spanAttributes: [] })
	return {
		commands,
		replies,
		lockReplies,
		migrationReplies,
		ddlReplies,
		layer: Layer.succeed(SqlClient.SqlClient, client),
	}
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
		Effect.gen(function* () {
			yield* Queue.offer(commands, { command, args })
			const reply = yield* Queue.poll(replies)
			return Option.isSome(reply) ? yield* reply.value : yield* Effect.die('Unscripted Redis command')
		})
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
