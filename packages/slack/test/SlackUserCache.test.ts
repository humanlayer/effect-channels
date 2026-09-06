import { assert, it } from '@effect/vitest'
import { Deferred, Effect, Fiber, Layer, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { Slack, SlackApiError, SlackTransportError, TenantId, UserId, UserProfile } from '../src/index.ts'
import { expectTaggedFailure } from './nativeSupport.ts'
import { stubSlackClientLayer } from './support.ts'

const input = { provider: 'slack' as const, tenant: TenantId.make('T_ONE'), userId: UserId.make('U_SHARED') }
const profile = (workspace: string) =>
	UserProfile.make({
		author: { userId: input.userId, userName: workspace, fullName: workspace, isBot: false, isMe: false },
	})

it.effect('Slack.getUser shares pending lookups, remembers results, and refreshes after five minutes', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const started = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const client = stubSlackClientLayer({
			getUser: () =>
				Ref.update(calls, (n) => n + 1).pipe(
					Effect.andThen(Deferred.succeed(started, undefined)),
					Effect.andThen(Deferred.await(release)),
					Effect.as(profile('T_ONE')),
				),
		})
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			const requests = yield* Effect.all([slack.getUser(input), slack.getUser(input), slack.getUser(input)], {
				concurrency: 3,
			}).pipe(Effect.forkChild)
			yield* Deferred.await(started)
			assert.strictEqual(yield* Ref.get(calls), 1)
			yield* Deferred.succeed(release, undefined)
			assert.deepStrictEqual(
				(yield* Fiber.join(requests)).map((user) => user.author.fullName),
				['T_ONE', 'T_ONE', 'T_ONE'],
			)
			yield* slack.getUser(input)
			assert.strictEqual(yield* Ref.get(calls), 1)
			yield* TestClock.adjust('5 minutes')
			yield* slack.getUser(input)
			assert.strictEqual(yield* Ref.get(calls), 2)
		}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(client))))
	}),
)

it.effect('an overlapping workspace lookup does not wait for or reuse another workspace profile', () =>
	Effect.gen(function* () {
		const started = yield* Deferred.make<void>()
		const release = yield* Deferred.make<void>()
		const client = stubSlackClientLayer({
			getUser: ({ teamId }) =>
				(teamId === 'T_ONE'
					? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
					: Effect.void
				).pipe(Effect.as(profile(teamId))),
		})
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			const first = yield* slack.getUser(input).pipe(Effect.forkChild)
			yield* Deferred.await(started)
			assert.strictEqual(
				(yield* slack.getUser({ ...input, tenant: TenantId.make('T_TWO') })).author.fullName,
				'T_TWO',
			)
			yield* Deferred.succeed(release, undefined)
			assert.strictEqual((yield* Fiber.join(first)).author.fullName, 'T_ONE')
		}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(client))))
	}),
)

it.effect('missing users are negatively cached for one minute, not indefinitely', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const client = stubSlackClientLayer({
			getUser: () =>
				Ref.updateAndGet(calls, (n) => n + 1).pipe(
					Effect.flatMap((n) =>
						n === 1
							? Effect.fail(SlackApiError.make({ operation: 'users.info', code: 'user_not_found' }))
							: Effect.succeed(profile('T_ONE')),
					),
				),
		})
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			for (const _ of [1, 2]) {
				const failure = yield* slack.getUser(input).pipe(expectTaggedFailure('UserLookupFailed'))
				assert.strictEqual(failure.reason, 'not_found')
			}
			assert.strictEqual(yield* Ref.get(calls), 1)
			yield* TestClock.adjust('1 minute')
			assert.strictEqual((yield* slack.getUser(input)).author.fullName, 'T_ONE')
			assert.strictEqual(yield* Ref.get(calls), 2)
		}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(client))))
	}),
)

it.effect('transient user lookup failures can recover on the very next call', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const client = stubSlackClientLayer({
			getUser: () =>
				Ref.updateAndGet(calls, (n) => n + 1).pipe(
					Effect.flatMap((n) =>
						n === 1
							? Effect.fail(SlackTransportError.make({ operation: 'users.info' }))
							: Effect.succeed(profile('T_ONE')),
					),
				),
		})
		yield* Effect.gen(function* () {
			const slack = yield* Slack
			const failure = yield* slack.getUser(input).pipe(expectTaggedFailure('UserLookupFailed'))
			assert.strictEqual(failure.retryable, true)
			assert.strictEqual((yield* slack.getUser(input)).author.fullName, 'T_ONE')
			assert.strictEqual(yield* Ref.get(calls), 2)
		}).pipe(Effect.provide(Slack.layer.pipe(Layer.provide(client))))
	}),
)
