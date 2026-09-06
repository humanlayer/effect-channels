import { assert } from '@effect/vitest'
import { Context, Deferred, Effect, Fiber, Schema } from 'effect'

import { IdempotencyKey } from '../src/Model.ts'
import { SlackConnectionStore } from '../src/SlackConnectionStore.ts'
import { SubscriptionCreated, SubscriptionExisting } from '../src/SlackEvents.ts'
import { SlackSubscriptions } from '../src/SlackSubscriptions.ts'
import {
	installation,
	proactiveThread,
	rootedThread,
	rotatedInstallation,
	routeInput,
	workspaceId,
} from '../test/AdapterFixtures.ts'

export class OtherConnections extends Context.Service<OtherConnections, SlackConnectionStore['Service']>()(
	'test/OtherConnections',
) {}
export class OtherSubscriptions extends Context.Service<OtherSubscriptions, SlackSubscriptions['Service']>()(
	'test/OtherSubscriptions',
) {}

export const replicaContract = Effect.gen(function* () {
	const first = yield* SlackConnectionStore
	const second = yield* OtherConnections
	assert.strictEqual(yield* first.get({ workspaceId }), undefined)
	assert.strictEqual(yield* second.get({ workspaceId }), undefined)
	yield* first.upsert({ workspaceId, connection: installation })
	assert.deepStrictEqual(yield* second.get({ workspaceId }), installation)
	yield* second.upsert({ workspaceId, connection: rotatedInstallation })
	assert.deepStrictEqual(yield* first.get({ workspaceId }), rotatedInstallation)
	yield* first.remove({ workspaceId })
	assert.strictEqual(yield* second.get({ workspaceId }), undefined)
	yield* second.remove({ workspaceId })
	assert.strictEqual(yield* first.get({ workspaceId }), undefined)
	yield* first.upsert({ workspaceId, connection: installation })

	const left = yield* SlackSubscriptions
	const right = yield* OtherSubscriptions
	const root = { threadId: rootedThread.id }
	const proactive = { threadId: proactiveThread.id }
	assert.strictEqual(yield* left.isSubscribed(root), false)
	const created = yield* Effect.all([left.subscribe(root), right.subscribe(root)], { concurrency: 2 })
	assert.strictEqual(created.filter(Schema.is(SubscriptionCreated)).length, 1)
	assert.strictEqual(created.filter(Schema.is(SubscriptionExisting)).length, 1)
	assert.strictEqual(yield* right.isSubscribed(root), true)
	yield* right.subscribe(proactive)

	const start = yield* Deferred.make<void>()
	const a = yield* Deferred.await(start).pipe(
		Effect.andThen(left.resolveDirectMessageRoute(routeInput)),
		Effect.forkChild,
	)
	const b = yield* Deferred.await(start).pipe(
		Effect.andThen(
			right.resolveDirectMessageRoute({
				...routeInput,
				rootedThread: { ...rootedThread, isNew: false },
			}),
		),
		Effect.forkChild,
	)
	yield* Deferred.succeed(start, undefined)
	const winner = yield* Fiber.join(a)
	assert.deepStrictEqual(yield* Fiber.join(b), winner)
	assert.strictEqual(winner.thread.id, rootedThread.id)
	assert.strictEqual(winner.subscribed, true)
	yield* left.unsubscribe(root)
	yield* right.unsubscribe(proactive)
	assert.strictEqual(yield* right.isSubscribed(root), false)
	assert.deepStrictEqual(yield* right.resolveDirectMessageRoute(routeInput), winner)

	const proactiveEvent = { ...routeInput, eventId: IdempotencyKey.make('evt_00000000000000000000000000000002') }
	yield* right.subscribe(proactive)
	assert.deepStrictEqual(yield* left.resolveDirectMessageRoute(proactiveEvent), {
		thread: proactiveThread,
		subscribed: true,
	})
	yield* left.subscribe(root)
	assert.deepStrictEqual(yield* right.resolveDirectMessageRoute(proactiveEvent), {
		thread: proactiveThread,
		subscribed: true,
	})
	yield* left.unsubscribe(root)
	yield* right.unsubscribe(proactive)
	const fallbackEvent = { ...routeInput, eventId: IdempotencyKey.make('evt_00000000000000000000000000000003') }
	assert.deepStrictEqual(yield* left.resolveDirectMessageRoute(fallbackEvent), {
		thread: rootedThread,
		subscribed: false,
	})
	yield* right.subscribe(proactive)
	assert.deepStrictEqual(yield* right.resolveDirectMessageRoute(fallbackEvent), {
		thread: rootedThread,
		subscribed: false,
	})
})
