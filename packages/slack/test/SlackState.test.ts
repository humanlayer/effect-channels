import { assert, it } from '@effect/vitest'
import { MailboxStore } from '@humanlayer/channels-delivery'
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Option, Queue, Redacted, Ref } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import {
	Slack,
	SlackBot,
	SlackClient,
	SlackConnectionStore,
	SlackConnectionStoreError,
	SlackState,
	SlackTeamId,
	SlackTenantCredentials,
	TenantId,
	UserId,
} from '../src/index'
import * as Memory from '../src/memory'
import { expectTaggedFailure, testAuthor } from './nativeSupport'
import { makeStubSlackClient } from './support'

const workspaceId = SlackTeamId.make('T_STATE')
const input = { provider: 'slack' as const, tenant: TenantId.make(workspaceId), userId: UserId.make('U_PERSON') }
const installation = (token: string) => ({
	workspaceId,
	connection: { credentials: { botToken: Redacted.make(token), botUserId: 'U_BOT', botId: 'B_BOT' } },
})

it.effect('native-only lookup pins its authorization to the cache key even if storage changes between reads', () =>
	Effect.gen(function* () {
		const loads = yield* Ref.make(0)
		const requests = yield* Queue.unbounded<string>()
		const store = Layer.succeed(
			SlackConnectionStore,
			SlackConnectionStore.of({
				get: () =>
					Ref.updateAndGet(loads, (n) => n + 1).pipe(
						Effect.map((n) => installation(n === 1 ? 'first' : 'second').connection),
					),
				upsert: () => Effect.die(new Error('unexpected write')),
				remove: () => Effect.die(new Error('unexpected removal')),
			}),
		)
		const http = Layer.succeed(
			HttpClient.HttpClient,
			HttpClient.make((request) =>
				Queue.offer(requests, request.headers.authorization ?? '').pipe(
					Effect.as(
						HttpClientResponse.fromWeb(
							request,
							Response.json({ ok: true, user: { id: input.userId, real_name: 'Person' } }),
						),
					),
				),
			),
		)
		yield* Effect.gen(function* () {
			assert.ok(Option.isNone(yield* Effect.serviceOption(MailboxStore)))
			const slack = yield* Slack
			yield* slack.getUser(input)
			yield* slack.getUser(input)
			assert.deepStrictEqual(yield* Queue.takeAll(requests), ['Bearer first', 'Bearer second'])
			assert.strictEqual(yield* Ref.get(loads), 2)
		}).pipe(
			Effect.provide(
				Slack.layerFromStore.pipe(
					Layer.provide(Layer.merge(store, http)),
					Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
				),
			),
		)
	}),
)

it.effect('same-credential writes invalidate only their workspace, including uncertain write failures', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const failWrite = yield* Ref.make(false)
		const other = SlackTeamId.make('T_OTHER')
		const otherInput = { ...input, tenant: TenantId.make(other) }
		const store = Layer.effect(
			SlackConnectionStore,
			Effect.gen(function* () {
				const inner = yield* SlackConnectionStore
				return SlackConnectionStore.of({
					get: inner.get,
					remove: inner.remove,
					upsert: (value) =>
						Effect.gen(function* () {
							yield* inner.upsert(value)
							if (yield* Ref.get(failWrite))
								return yield* SlackConnectionStoreError.make({ operation: 'upsert' })
						}),
				})
			}),
		).pipe(
			Layer.provide(
				Memory.connections({
					connections: [installation('first'), { ...installation('first'), workspaceId: other }],
				}),
			),
		)
		const client = Layer.succeed(
			SlackClient,
			makeStubSlackClient({
				getUser: () =>
					Ref.updateAndGet(calls, (n) => n + 1).pipe(
						Effect.map((n) => ({ author: { ...testAuthor, fullName: String(n) } })),
					),
			}),
		)
		yield* Effect.gen(function* () {
			const state = yield* SlackState
			assert.strictEqual((yield* state.getUser(input)).author.fullName, '1')
			assert.strictEqual((yield* state.getUser(otherInput)).author.fullName, '2')
			yield* state.upsertConnection(installation('first'))
			assert.strictEqual((yield* state.getUser(input)).author.fullName, '3')
			assert.strictEqual((yield* state.getUser(otherInput)).author.fullName, '2')
			yield* Ref.set(failWrite, true)
			assert.deepStrictEqual(
				yield* state.upsertConnection(installation('first')).pipe(Effect.flip),
				SlackConnectionStoreError.make({ operation: 'upsert' }),
			)
			assert.strictEqual((yield* state.getUser(input)).author.fullName, '4')
			assert.strictEqual((yield* state.getUser(otherInput)).author.fullName, '2')
		}).pipe(Effect.provide(SlackState.layer.pipe(Layer.provide(store), Layer.provideMerge(client))))
	}),
)

it.effect(
	'memory connections implement atomic upsert, independent workspaces, idempotent remove and reusable capacity',
	() =>
		Effect.gen(function* () {
			const store = yield* SlackConnectionStore
			assert.strictEqual(yield* store.get({ workspaceId }), undefined)
			yield* store.upsert(installation('first'))
			yield* store.upsert(installation('second'))
			const stored = yield* store.get({ workspaceId })
			assert.ok(stored !== undefined)
			assert.strictEqual(Redacted.value(stored.credentials.botToken), 'second')
			const other = { ...installation('other'), workspaceId: SlackTeamId.make('T_OTHER') }
			yield* store.upsert(other).pipe(expectTaggedFailure('SlackConnectionStoreError'))
			yield* store.remove({ workspaceId })
			yield* store.remove({ workspaceId })
			yield* store.upsert(other)
			assert.strictEqual(yield* store.get({ workspaceId }), undefined)
			assert.ok((yield* store.get({ workspaceId: other.workspaceId })) !== undefined)
		}).pipe(Effect.provide(Memory.connections({ capacity: 1 }))),
)

it.effect(
	'credential resolution sees insert, replacement and removal immediately without TTL or a negative cache',
	() =>
		Effect.gen(function* () {
			const state = yield* SlackState
			const credentials = yield* SlackTenantCredentials
			assert.ok(Option.isNone(yield* credentials.load({ teamId: workspaceId })))
			yield* state.upsertConnection(installation('first'))
			assert.strictEqual(
				Redacted.value(Option.getOrThrow(yield* credentials.load({ teamId: workspaceId })).botToken),
				'first',
			)
			yield* state.upsertConnection(installation('second'))
			assert.strictEqual(
				Redacted.value(Option.getOrThrow(yield* credentials.load({ teamId: workspaceId })).botToken),
				'second',
			)
			yield* state.removeConnection({ workspaceId })
			assert.ok(Option.isNone(yield* credentials.load({ teamId: workspaceId })))
		}).pipe(
			Effect.provide(
				SlackTenantCredentials.layer.pipe(
					Layer.provideMerge(SlackState.layer),
					Layer.provide(Memory.connections()),
				),
			),
		),
)

it.effect('rotation invalidates an in-flight profile, and removal denies even an already cached profile', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const calls = yield* Queue.unbounded<string>()
		const client = Layer.effect(
			SlackClient,
			Effect.gen(function* () {
				const state = yield* SlackState
				return makeStubSlackClient({
					getUser: () =>
						Effect.gen(function* () {
							const connection = yield* state.getConnection({ workspaceId }).pipe(Effect.orDie)
							assert.ok(connection !== undefined)
							const token = Redacted.value(connection.credentials.botToken)
							yield* Queue.offer(calls, token)
							if (token === 'first') {
								yield* Deferred.succeed(started, undefined)
								yield* Deferred.await(release)
							}
							return { author: { ...testAuthor, fullName: token } }
						}),
				})
			}),
		)
		const services = client.pipe(
			Layer.provideMerge(SlackState.layer),
			Layer.provide(Memory.connections({ connections: [installation('first')] })),
		)
		yield* Effect.gen(function* () {
			const state = yield* SlackState
			const old = yield* state.getUser(input).pipe(Effect.forkChild)
			yield* Deferred.await(started)
			yield* state.upsertConnection(installation('second'))
			assert.strictEqual((yield* state.getUser(input)).author.fullName, 'second')
			yield* Deferred.succeed(release, undefined)
			yield* Fiber.join(old)
			assert.strictEqual((yield* state.getUser(input)).author.fullName, 'second')
			assert.deepStrictEqual(yield* Queue.takeAll(calls), ['first', 'second'])
			yield* state.removeConnection({ workspaceId })
			yield* state.getUser(input).pipe(expectTaggedFailure('UnknownTenant'))
			assert.strictEqual(yield* Queue.size(calls), 0)
		}).pipe(Effect.provide(services))
	}),
)

it.effect('the real bot resolves changed credentials for HTTP and refuses requests after direct store removal', () =>
	Effect.gen(function* () {
		const requests = yield* Queue.unbounded<string>()
		const http = Layer.succeed(
			HttpClient.HttpClient,
			HttpClient.make((request) =>
				Queue.offer(requests, request.headers.authorization ?? '').pipe(
					Effect.as(
						HttpClientResponse.fromWeb(
							request,
							Response.json({ ok: true, user: { id: input.userId, real_name: 'Person', is_bot: false } }),
						),
					),
				),
			),
		)
		const bot = SlackBot.make({ namespace: 'state-http', handlers: {} })
		const stores = Memory.layer({ connections: [installation('first')] })
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			const store = yield* SlackConnectionStore
			yield* slack.getUser(input)
			yield* slack.getUser(input)
			assert.deepStrictEqual(yield* Queue.takeAll(requests), ['Bearer first'])
			yield* store.upsert(installation('second'))
			yield* slack.getUser(input)
			assert.deepStrictEqual(yield* Queue.takeAll(requests), ['Bearer second'])
			yield* store.remove({ workspaceId })
			yield* slack.getUser(input).pipe(expectTaggedFailure('UnknownTenant'))
			assert.strictEqual(yield* Queue.size(requests), 0)
		}).pipe(Effect.provide(bot.pipe(Layer.provideMerge(stores), Layer.provide(http))))
	}),
)
