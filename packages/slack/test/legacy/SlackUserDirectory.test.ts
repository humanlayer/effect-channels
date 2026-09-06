import { assert, it } from '@effect/vitest'
import { Cause, Effect, Exit, Fiber, Queue, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import {
	MessagePage,
	Slack,
	SlackApiError,
	SlackIngress,
	SlackTransportError,
	SlackUserDirectory,
	TenantId,
	UserId,
	UserProfile,
} from '../../src/index.ts'
import {
	ingressLayer,
	makeTestAuthor,
	makeTestMessage,
	makeTestNormalizedMessage,
	nativeSlackLayer,
	runnerOptions,
	expectTaggedFailure,
	testThreadRef,
} from './support.ts'

const profile = (tenant: string, userId: string) =>
	UserProfile.make({
		author: {
			userId: UserId.make(userId),
			userName: `${tenant}-name`,
			fullName: `${tenant} Profile`,
			isBot: false,
			isMe: false,
		},
	})

it.effect('hydrates message and thread authors before invoking the native application handler', () =>
	Effect.gen(function* () {
		const delivered = yield* Queue.unbounded<ReadonlyArray<string>>()
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))
			yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
			yield* TestClock.adjust('20 millis')
			assert.deepStrictEqual(yield* Queue.takeAll(delivered), [['T_TEST Profile', 'T_TEST Profile']])
			yield* Fiber.interrupt(worker)
		}).pipe(
			Effect.provide(
				ingressLayer(
					{
						onNewMention: [
							{
								id: 'mention',
								handler: ({ thread, message }) =>
									Queue.offer(delivered, [
										message.author.fullName,
										thread.currentMessage?.author.fullName ?? 'missing',
									]).pipe(Effect.asVoid),
							},
						],
					},
					{ getUser: (input) => Effect.succeed(profile(input.teamId, input.userId)) },
				),
			),
		)
	}),
)

it.effect('hydrates native thread and channel history with a shared lookup per author', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const messages = [makeTestMessage({ messageTs: '100.1' }), makeTestMessage({ messageTs: '100.2' })]
		const slack = nativeSlackLayer({
			getUser: (input) =>
				Ref.update(calls, (count) => count + 1).pipe(Effect.as(profile(input.teamId, input.userId))),
			replies: () => Effect.succeed(MessagePage.make({ messages })),
			history: () => Effect.succeed(MessagePage.make({ messages })),
		})
		yield* Effect.gen(function* () {
			const service = yield* Slack
			for (const page of [
				yield* service.messages({ threadId: testThreadRef.id }),
				yield* service.containerMessages({ channel: testThreadRef.channel }),
			]) {
				const hydrated = page.messages
				assert.deepStrictEqual(
					hydrated.map((message) => message.author.fullName),
					['T_TEST Profile', 'T_TEST Profile'],
				)
			}
			assert.strictEqual(yield* Ref.get(calls), 1)
		}).pipe(Effect.provide(slack))
	}),
)

it.effect('isolates cached authors by Slack installation and user identity', () =>
	Effect.gen(function* () {
		const calls = yield* Ref.make(0)
		const slack = nativeSlackLayer({
			getUser: (input) =>
				Ref.update(calls, (count) => count + 1).pipe(Effect.as(profile(input.teamId, input.userId))),
		})
		yield* Effect.gen(function* () {
			const directory = yield* SlackUserDirectory
			const author = makeTestAuthor({ userId: 'U_SHARED' })
			const hydrated = yield* Effect.forEach(['T_ONE', 'T_TWO', 'T_ONE'], (tenant) =>
				directory.getUser({ provider: 'slack', tenant: TenantId.make(tenant), userId: author.userId }),
			)
			assert.deepStrictEqual(
				hydrated.map((value) => value.author.fullName),
				['T_ONE Profile', 'T_TWO Profile', 'T_ONE Profile'],
			)
			const other = makeTestAuthor({ userId: 'U_OTHER' })
			assert.strictEqual(
				(yield* directory.getUser({ provider: 'slack', tenant: TenantId.make('T_ONE'), userId: other.userId }))
					.author.userId,
				'U_OTHER',
			)
			assert.strictEqual(yield* Ref.get(calls), 3)
		}).pipe(Effect.provide(slack))
	}),
)

it.effect('falls back to the original author on retryable failures and retries the lookup, not delivery', () =>
	Effect.gen(function* () {
		const lookups = yield* Ref.make(0)
		const delivered = yield* Queue.unbounded<string>()
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const worker = yield* Effect.forkChild(ingress.run(runnerOptions))
			for (const messageTs of ['100.1', '100.2']) {
				yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs, rootTs: '100.1', mentioned: true }))
				yield* TestClock.adjust('20 millis')
			}
			assert.deepStrictEqual(yield* Queue.takeAll(delivered), ['Test User', 'Test User'])
			assert.strictEqual(yield* Ref.get(lookups), 2)
			yield* Fiber.interrupt(worker)
		}).pipe(
			Effect.provide(
				ingressLayer(
					{
						onNewMention: [
							{
								id: 'mention',
								handler: ({ message }) =>
									Queue.offer(delivered, message.author.fullName).pipe(Effect.asVoid),
							},
						],
					},
					{
						getUser: () =>
							Ref.update(lookups, (count) => count + 1).pipe(
								Effect.andThen(Effect.fail(SlackTransportError.make({ operation: 'users.info' }))),
							),
					},
				),
			),
		)
	}),
)

it.effect('caches non-retryable lookup failures', () =>
	Effect.gen(function* () {
		const lookups = yield* Ref.make(0)
		const slack = nativeSlackLayer({
			getUser: () =>
				Ref.update(lookups, (count) => count + 1).pipe(
					Effect.andThen(
						Effect.fail(SlackApiError.make({ operation: 'users.info', code: 'user_not_found' })),
					),
				),
		})
		yield* Effect.gen(function* () {
			const directory = yield* SlackUserDirectory
			const author = makeTestAuthor({ userId: 'U_TEST' })
			const input = { provider: 'slack' as const, tenant: TenantId.make('T_ONE'), userId: author.userId }
			assert.strictEqual(
				(yield* directory.getUser(input).pipe(expectTaggedFailure('UserLookupFailed'))).reason,
				'not_found',
			)
			assert.strictEqual(
				(yield* directory.getUser(input).pipe(expectTaggedFailure('UserLookupFailed'))).reason,
				'not_found',
			)
			assert.strictEqual(yield* Ref.get(lookups), 1)
		}).pipe(Effect.provide(slack))
	}),
)

it.effect('does not turn profile lookup interruptions or defects into successful hydration', () =>
	Effect.gen(function* () {
		for (const lookup of [Effect.interrupt, Effect.die('profile defect')]) {
			const layer = nativeSlackLayer({ getUser: () => lookup })
			const result = yield* Effect.flatMap(SlackUserDirectory, (directory) =>
				directory.getUser({
					provider: 'slack',
					tenant: TenantId.make('T_TEST'),
					userId: UserId.make('U_TEST'),
				}),
			).pipe(Effect.provide(layer), Effect.exit)
			assert.ok(Exit.isFailure(result))
			assert.ok(Cause.hasInterrupts(result.cause) || Cause.hasDies(result.cause))
		}
	}),
)
