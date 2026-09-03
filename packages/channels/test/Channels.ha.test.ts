import { assert, describe, it } from '@effect/vitest'
import { Context, Deferred, Effect, Fiber, Layer, Queue, Ref } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

import {
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationCoordinator,
	ConversationSignals,
	Organizations,
	ProviderRegistry,
	Subscriptions,
	UserDirectory,
} from '../src/index.ts'
import {
	createPostgresMessageEvent,
	postgresCoordinatorOptions,
	postgresTestLayer,
	postgresTestsEnabled,
} from './support/PostgresTestResource.ts'

const applicationLayer = () => {
	const persistent = Layer.merge(
		ConversationCoordinator.layerPostgres(postgresCoordinatorOptions()),
		Persistence.layerSql,
	)
	const subscriptions = Subscriptions.layer.pipe(Layer.provideMerge(persistent))
	const registry = ProviderRegistry.layer
	const dependencies = Layer.mergeAll(
		ConversationSignals.layerMemory,
		registry,
		UserDirectory.layer.pipe(Layer.provide(registry)),
		Organizations.layerDefault,
		ChannelsGate.layerAllowAll,
		ChannelsObserver.layerLogger,
		subscriptions,
	)
	return Channels.layer().pipe(Layer.provideMerge(dependencies))
}

const buildApplication = Layer.build(applicationLayer())

describe.skipIf(!postgresTestsEnabled)('Channels Postgres HA delivery', () => {
	it.effect('runs two fresh application graphs with one owner per thread and parallel owners across threads', () =>
		Effect.gen(function* () {
			const firstContext = yield* buildApplication
			const secondContext = yield* buildApplication
			const firstChannels = Context.get(firstContext, Channels)
			const secondChannels = Context.get(secondContext, Channels)
			const coordinator = Context.get(firstContext, ConversationCoordinator)
			const entered = yield* Queue.unbounded<string>()
			const sameThreadGate = yield* Deferred.make<void>()
			const otherThreadGate = yield* Deferred.make<void>()
			const activeSameThread = yield* Ref.make(0)
			const sameThreadOverlap = yield* Ref.make(false)
			const sameThreadKey = 'channels-ha'
			const otherThreadKey = 'channels-other'
			const first = yield* createPostgresMessageEvent({ threadKey: sameThreadKey })
			const second = yield* createPostgresMessageEvent({ threadKey: sameThreadKey })
			const other = yield* createPostgresMessageEvent({ threadKey: otherThreadKey })

			const register = (channels: Channels['Service']) =>
				channels.onNewMention((thread, message) =>
					Effect.gen(function* () {
						const sameThread = thread.ref.id === first.thread.ref.id
						if (sameThread) {
							const active = yield* Ref.updateAndGet(activeSameThread, (count) => count + 1)
							if (active > 1) {
								yield* Ref.set(sameThreadOverlap, true)
							}
						}
						yield* Queue.offer(entered, message.ref)
						yield* sameThread ? Deferred.await(sameThreadGate) : Deferred.await(otherThreadGate)
						if (sameThread) {
							yield* Ref.update(activeSameThread, (count) => count - 1)
						}
					}),
				)

			yield* register(firstChannels)
			yield* register(secondChannels)
			const firstWorker = yield* Effect.forkChild(firstChannels.run)
			const secondWorker = yield* Effect.forkChild(secondChannels.run)
			yield* coordinator.submit(first)
			yield* coordinator.submit(second)
			yield* coordinator.submit(other)

			const firstEntered = yield* Queue.take(entered)
			const otherEntered = yield* Queue.take(entered)
			assert.ok(
				[firstEntered, otherEntered].includes(first.message.ref) &&
					[firstEntered, otherEntered].includes(other.message.ref),
			)
			assert.strictEqual(yield* Queue.size(entered), 0)
			assert.strictEqual(yield* Ref.get(sameThreadOverlap), false)

			yield* Deferred.succeed(otherThreadGate, undefined)
			yield* Deferred.succeed(sameThreadGate, undefined)
			assert.strictEqual(yield* Queue.take(entered), second.message.ref)
			assert.strictEqual(yield* Ref.get(sameThreadOverlap), false)

			yield* Fiber.interrupt(firstWorker)
			yield* Fiber.interrupt(secondWorker)
		}).pipe(Effect.provide(postgresTestLayer)),
	)
})
