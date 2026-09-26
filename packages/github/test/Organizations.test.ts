import { assert, it } from '@effect/vitest'
import {
	DeliveryQueue,
	enqueueDelivery,
	MailboxReadiness,
	MailboxStore,
	MailboxStoreError,
	mailboxKey,
} from '@humanlayer/channels-delivery'
import { Context, Deferred, Effect, Fiber, Layer, Logger, Queue, Ref, Schema } from 'effect'

import {
	GitHubIngress,
	GitHubIngressError,
	GitHubOrganizationLookupError,
	GitHubOrganizations,
	issueResourceKey,
	type GitHubOrganizationLookup,
} from '../src/index'
import { layer as memory } from '../src/memory'
import { event, policy, unusedGitHub } from './fixtures'

const lookupBoundary: Context.Key<
	GitHubOrganizations,
	{
		readonly resolve: (
			input: GitHubOrganizationLookup,
		) => Effect.Effect<Schema.Json | undefined, GitHubOrganizationLookupError>
	}
> = GitHubOrganizations

it.effect('custom GitHub lookup results are decoded before writes and failures log only safe classifications', () =>
	Effect.gen(function* () {
		const retained = yield* Layer.build(memory({ maxMailboxes: 10 }))
		const store = Context.get(retained, MailboxStore)
		const writes = yield* Ref.make(0)
		const storage = Layer.succeedContext(
			Context.add(
				retained,
				MailboxStore,
				MailboxStore.of({
					...store,
					commitMailbox: (input) =>
						Ref.update(writes, (n) => n + 1).pipe(Effect.andThen(store.commitMailbox(input))),
				}),
			),
		)
		const logs: string[] = []
		const logger = Logger.layer([
			Logger.make((entry) => logs.push(JSON.stringify(Logger.formatStructured.log(entry)))),
		])
		const results: ReadonlyArray<Schema.Json | undefined> = [
			null,
			undefined,
			{},
			{ organizationId: '' },
			{ organizationId: { private: 'private-result-sentinel' } },
			'failed',
		]
		for (const result of results) {
			const lookup = Layer.succeed(lookupBoundary, {
				resolve: () =>
					result === 'failed'
						? Effect.fail(
								Object.assign(GitHubOrganizationLookupError.make({}), {
									message: 'private-error-sentinel',
								}),
							)
						: Effect.succeed(result),
			})
			yield* Effect.gen(function* () {
				const ingress = yield* GitHubIngress
				const attempt = ingress.acceptActivity({ event, mentioned: false, own: false })
				if (result === null) yield* attempt
				else
					assert.deepStrictEqual(
						yield* attempt.pipe(Effect.flip),
						GitHubIngressError.make({ operation: 'admit' }),
					)
				assert.deepStrictEqual(
					yield* Context.get(retained, MailboxReadiness).scanReady({ prefix: '', now: 0, limit: 10 }),
					[],
				)
			}).pipe(
				Effect.provide(
					GitHubIngress.layer({
						namespace: 'invalid',
						policy,
						handlers: [{ id: 'reply', onCreation: () => Effect.die('Unexpected handler') }],
					}).pipe(
						Layer.provide(storage),
						Layer.provide(lookup),
						Layer.provide(unusedGitHub),
						Layer.provide(logger),
					),
				),
			)
		}
		assert.strictEqual(yield* Ref.get(writes), 0)
		assert.ok(logs.some((log) => log.includes('invalid_result')))
		assert.ok(logs.some((log) => log.includes('lookup_failed')))
		assert.ok(logs.every((log) => !log.includes('private-')))
	}),
)

for (const organizationId of ['default', 'fixed']) {
	it.effect(`GitHub ${organizationId} organization reaches ordinary handlers from saved metadata`, () =>
		Effect.gen(function* () {
			const observed = yield* Queue.unbounded<string>()
			const ingressLayer = GitHubIngress.layer({
				namespace: 'fixed',
				policy,
				handlers: [
					{
						id: 'reply',
						onCreation: (_, context) => Queue.offer(observed, context.organizationId).pipe(Effect.asVoid),
					},
				],
			}).pipe(Layer.provide(unusedGitHub), Layer.provideMerge(memory()))
			const environment = yield* Layer.build(
				organizationId === 'default'
					? ingressLayer
					: ingressLayer.pipe(Layer.provide(GitHubOrganizations.fixed({ organizationId }))),
			)
			const ingress = Context.get(environment, GitHubIngress)
			yield* ingress.acceptActivity({ event, mentioned: false, own: false })
			const [key] = yield* Context.get(environment, MailboxReadiness).scanReady({ prefix: '', now: 0, limit: 10 })
			assert.ok(key !== undefined)
			assert.strictEqual(
				(yield* Context.get(environment, MailboxStore).loadMailbox({ key }))?.state.pending[0]?.organizationId,
				organizationId,
			)
			yield* ingress.processActivity({ event }).pipe(Effect.provide(environment))
			assert.strictEqual(yield* Queue.take(observed), organizationId)
		}),
	)
}

for (const callback of ['onCreation', 'onMention'] as const) {
	it.effect(`${callback} partial fan-out retains attribution across ingress reconstruction`, () =>
		Effect.gen(function* () {
			const retained = yield* Layer.build(memory({ maxMailboxes: 30 }))
			const real = Context.get(retained, MailboxStore)
			const fail = yield* Ref.make(true)
			const current = yield* Ref.make('A')
			const calls = yield* Ref.make(0)
			const secondKey = mailboxKey({
				namespace: 'org',
				provider: 'github',
				handlerId: callback === 'onCreation' ? '["second","creation"]' : '["second","mention"]',
				installation: '100',
				resourceKey: issueResourceKey(event.resource),
			})
			const store = MailboxStore.of({
				...real,
				commitMailbox: (input) =>
					Effect.gen(function* () {
						if (input.key === secondKey && (yield* Ref.getAndSet(fail, false)))
							return yield* MailboxStoreError.make({ operation: 'commit' })
						return yield* real.commitMailbox(input)
					}),
			})
			const queue = DeliveryQueue.of({
				enqueue: (input) => enqueueDelivery(input).pipe(Effect.provideService(MailboxStore, store)),
			})
			const dependencies = Layer.succeedContext(
				Context.add(Context.add(retained, MailboxStore, store), DeliveryQueue, queue),
			)
			const organizations = Layer.succeed(
				GitHubOrganizations,
				GitHubOrganizations.of({
					resolve: () =>
						Ref.update(calls, (count) => count + 1).pipe(
							Effect.andThen(Ref.get(current)),
							Effect.map((organizationId) => ({ organizationId })),
						),
				}),
			)
			const handlers = ['first', 'second'].map((id) =>
				callback === 'onCreation'
					? { id, onCreation: () => Effect.void }
					: { id, onMention: () => Effect.void },
			)
			const registration = { handlers }
			const make = () =>
				GitHubIngress.layer({ namespace: 'org', policy, ...registration }).pipe(
					Layer.provide(dependencies),
					Layer.provide(organizations),
					Layer.provide(unusedGitHub),
				)
			const accept = Effect.flatMap(GitHubIngress, (ingress) =>
				ingress.acceptActivity({ event, mentioned: true, own: false }),
			)
			yield* accept.pipe(Effect.flip, Effect.provide(make()))
			assert.strictEqual(yield* real.loadMailbox({ key: secondKey }), undefined)
			yield* Ref.set(current, 'B')
			yield* accept.pipe(Effect.provide(make()))
			assert.strictEqual((yield* real.loadMailbox({ key: secondKey }))?.state.pending[0]?.organizationId, 'A')
			assert.strictEqual(yield* Ref.get(calls), 1)
		}),
	)
}

it.effect('overlapping installations in one acquired ingress retain isolated handler contexts', () =>
	Effect.gen(function* () {
		const entered = yield* Queue.unbounded<number>()
		const first = yield* Deferred.make<void>()
		const second = yield* Deferred.make<void>()
		const observed = yield* Queue.unbounded<string>()
		const organizations = GitHubOrganizations.layer(({ installationId }) =>
			Queue.offer(entered, installationId).pipe(
				Effect.andThen(Deferred.await(installationId === 100 ? first : second)),
				Effect.as({ organizationId: installationId === 100 ? 'A' : 'B' }),
			),
		)
		const environment = yield* Layer.build(
			GitHubIngress.layer({
				namespace: 'overlap',
				policy,
				handlers: [
					{
						id: 'reply',
						onCreation: (event, context) =>
							Queue.offer(
								observed,
								`${event.resource.repository.installationId}:${context.organizationId}`,
							).pipe(Effect.asVoid),
					},
				],
			}).pipe(Layer.provideMerge(memory()), Layer.provide(organizations), Layer.provide(unusedGitHub)),
		)
		const ingress = Context.get(environment, GitHubIngress)
		const other = {
			...event,
			deliveryId: 'event-b',
			resource: { ...event.resource, repository: { ...event.resource.repository, installationId: 200 } },
		}
		const a = yield* ingress.acceptActivity({ event, mentioned: false, own: false }).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(entered), 100)
		const b = yield* ingress.acceptActivity({ event: other, mentioned: false, own: false }).pipe(Effect.forkChild)
		assert.strictEqual(yield* Queue.take(entered), 200)
		yield* Deferred.succeed(second, undefined)
		yield* Fiber.join(b)
		yield* ingress.processActivity({ event: other }).pipe(Effect.provide(environment))
		assert.strictEqual(yield* Queue.take(observed), '200:B')
		yield* Deferred.succeed(first, undefined)
		yield* Fiber.join(a)
		yield* ingress.processActivity({ event }).pipe(Effect.provide(environment))
		assert.strictEqual(yield* Queue.take(observed), '100:A')
		assert.deepStrictEqual(
			yield* Context.get(environment, MailboxReadiness).scanReady({ prefix: '', now: 0, limit: 10 }),
			[],
		)
	}),
)
