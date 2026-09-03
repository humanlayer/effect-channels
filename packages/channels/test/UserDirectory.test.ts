import { assert, it } from '@effect/vitest'
import { Effect, Fiber, Queue, Ref } from 'effect'

import {
	Channels,
	Ingress,
	MessagePage,
	ProviderRegistry,
	TenantId,
	UserDirectory,
	UserId,
	UserLookupFailed,
	UserProfile,
} from '../src/index.ts'
import {
	ChannelsWithIngressLayer,
	makeFakeProvider,
	makeTestAuthor,
	makeTestMessage,
	makeTestNormalizedMessage,
	noCapabilities,
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

it.effect('hydrates message and thread authors before invoking the application handler', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const registry = yield* ProviderRegistry
		const delivered = yield* Queue.unbounded<ReadonlyArray<string>>()
		yield* registry.register(
			makeFakeProvider({
				getUser: (input) => Effect.succeed(profile(input.tenant, input.userId)),
			}),
		)
		yield* channels.onNewMention((thread, message) =>
			Queue.offer(delivered, [message.author.fullName, thread.currentMessage?.author.fullName ?? 'missing']).pipe(
				Effect.asVoid,
			),
		)
		const worker = yield* Effect.forkChild(channels.run)
		yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
		assert.deepStrictEqual(yield* Queue.take(delivered), ['T_TEST Profile', 'T_TEST Profile'])
		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('hydrates thread and channel history while reusing one lookup per author', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const registry = yield* ProviderRegistry
		const calls = yield* Ref.make(0)
		const repeated = [
			makeTestMessage({ messageTs: '100.1' }),
			makeTestMessage({ messageTs: '100.2', threadRef: testThreadRef }),
		]
		yield* registry.register(
			makeFakeProvider({
				capabilities: {
					...noCapabilities,
					history: { thread: true, channelMessages: true, channelThreads: false },
				},
				getUser: (input) =>
					Ref.update(calls, (count) => count + 1).pipe(Effect.as(profile(input.tenant, input.userId))),
				messages: () => Effect.succeed(MessagePage.make({ messages: repeated })),
				containerMessages: () => Effect.succeed(MessagePage.make({ messages: repeated })),
			}),
		)
		const threadPage = yield* channels.messages({ threadId: testThreadRef.id })
		const channelPage = yield* channels.containerMessages({ channel: testThreadRef.channel })
		assert.deepStrictEqual(
			threadPage.messages.map((message) => message.author.fullName),
			['T_TEST Profile', 'T_TEST Profile'],
		)
		assert.deepStrictEqual(
			channelPage.messages.map((message) => message.author.fullName),
			['T_TEST Profile', 'T_TEST Profile'],
		)
		assert.strictEqual(yield* Ref.get(calls), 1)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('scopes cached profiles by provider, tenant, and user id', () =>
	Effect.gen(function* () {
		const directory = yield* UserDirectory
		const registry = yield* ProviderRegistry
		const calls = yield* Ref.make(0)
		yield* registry.register(
			makeFakeProvider({
				getUser: (input) =>
					Ref.update(calls, (count) => count + 1).pipe(
						Effect.as(profile(`${input.provider}-${input.tenant}`, input.userId)),
					),
			}),
		)
		yield* registry.register(
			makeFakeProvider({
				name: 'github',
				getUser: (input) =>
					Ref.update(calls, (count) => count + 1).pipe(
						Effect.as(profile(`${input.provider}-${input.tenant}`, input.userId)),
					),
			}),
		)
		const author = makeTestAuthor({ userId: 'U_SHARED' })
		const first = yield* directory.hydrateAuthor(
			{ provider: 'slack', tenant: TenantId.make('T_ONE'), userId: author.userId },
			author,
		)
		const second = yield* directory.hydrateAuthor(
			{ provider: 'slack', tenant: TenantId.make('T_TWO'), userId: author.userId },
			author,
		)
		const firstAgain = yield* directory.hydrateAuthor(
			{ provider: 'slack', tenant: TenantId.make('T_ONE'), userId: author.userId },
			author,
		)
		const otherProvider = yield* directory.hydrateAuthor(
			{ provider: 'github', tenant: TenantId.make('T_ONE'), userId: author.userId },
			author,
		)
		assert.deepStrictEqual(
			[first.fullName, second.fullName, firstAgain.fullName, otherProvider.fullName],
			['slack-T_ONE Profile', 'slack-T_TWO Profile', 'slack-T_ONE Profile', 'github-T_ONE Profile'],
		)
		assert.strictEqual(yield* Ref.get(calls), 3)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('falls back to the original author without retrying delivery when lookup fails', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		const ingress = yield* Ingress
		const registry = yield* ProviderRegistry
		const lookups = yield* Ref.make(0)
		const delivered = yield* Queue.unbounded<string>()
		yield* registry.register(
			makeFakeProvider({
				getUser: (input) =>
					Ref.update(lookups, (count) => count + 1).pipe(
						Effect.andThen(
							Effect.fail(
								UserLookupFailed.make({
									provider: input.provider,
									tenant: input.tenant,
									userId: input.userId,
									reason: 'api',
									retryable: true,
								}),
							),
						),
					),
			}),
		)
		yield* channels.onNewMention((_thread, message) =>
			Queue.offer(delivered, message.author.fullName).pipe(Effect.asVoid),
		)
		const worker = yield* Effect.forkChild(channels.run)
		yield* ingress.acceptMessage(makeTestNormalizedMessage({ messageTs: '100.1', mentioned: true }))
		assert.strictEqual(yield* Queue.take(delivered), 'Test User')
		assert.strictEqual(yield* Ref.get(lookups), 1)
		yield* Fiber.interrupt(worker)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)

it.effect('caches non-retryable lookup failures', () =>
	Effect.gen(function* () {
		const directory = yield* UserDirectory
		const registry = yield* ProviderRegistry
		const lookups = yield* Ref.make(0)
		yield* registry.register(
			makeFakeProvider({
				getUser: (input) =>
					Ref.update(lookups, (count) => count + 1).pipe(
						Effect.andThen(
							Effect.fail(
								UserLookupFailed.make({
									provider: input.provider,
									tenant: input.tenant,
									userId: input.userId,
									reason: 'api',
									retryable: false,
								}),
							),
						),
					),
			}),
		)
		const author = makeTestAuthor({ userId: 'U_TEST' })
		const input = { provider: 'slack' as const, tenant: TenantId.make('T_ONE'), userId: author.userId }
		const first = yield* directory.hydrateAuthor(input, author)
		const second = yield* directory.hydrateAuthor(input, author)
		assert.strictEqual(first, author)
		assert.strictEqual(second, author)
		assert.strictEqual(yield* Ref.get(lookups), 1)
	}).pipe(Effect.provide(ChannelsWithIngressLayer)),
)
