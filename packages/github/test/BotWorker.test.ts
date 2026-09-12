import { assert, it } from '@effect/vitest'
import {
	MailboxReadiness,
	MailboxStore,
	mailboxPrefix,
	type ScanReady,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Deferred, Effect, Exit, Layer, Queue, Scope } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient } from 'effect/unstable/http'

import { GitHubBot, GitHubIngress, type GitHubIngressOptions } from '../src/index.js'
import { layer as memory } from '../src/memory.js'
import { event, policy, routeCredentials } from './fixtures.js'

const cases: ReadonlyArray<{
	readonly name: string
	readonly runner?: Partial<RunnerOptions>
	readonly concurrency: number
	readonly pollMs: number
}> = [
	{ name: 'omitted runner', concurrency: 8, pollMs: 25 },
	{ name: 'partial concurrency', runner: { concurrency: 2 }, concurrency: 2, pollMs: 25 },
	{ name: 'partial polling', runner: { pollMs: 50 }, concurrency: 8, pollMs: 50 },
]

for (const fixture of cases) {
	it.effect(`actual bot worker honors ${fixture.name}, scan limit, concurrency and polling`, () =>
		Effect.gen(function* () {
			const retained = yield* Layer.build(memory({ maxMailboxes: 30 }))
			const store = Context.get(retained, MailboxStore)
			const readiness = Context.get(retained, MailboxReadiness)
			const scans = yield* Queue.unbounded<ScanReady>()
			const started = yield* Queue.unbounded<number>()
			const completed = yield* Queue.unbounded<string>()
			const release = yield* Deferred.make<void>()
			const dependencies = Layer.mergeAll(
				Layer.succeedContext(retained),
				Layer.succeed(MailboxReadiness, {
					scanReady: (input) => readiness.scanReady(input).pipe(Effect.tap(() => Queue.offer(scans, input))),
				}),
				Layer.succeed(MailboxStore, {
					loadMailbox: store.loadMailbox,
					commitMailbox: (input) =>
						store
							.commitMailbox(input)
							.pipe(
								Effect.tap((result) =>
									result === 'committed' && input.nextState.outcomes.length === 1
										? Queue.offer(completed, input.key)
										: Effect.void,
								),
							),
				}),
				routeCredentials,
				Layer.succeed(
					HttpClient.HttpClient,
					HttpClient.make(() => Effect.die('Unexpected provider call')),
				),
			)
			const options: GitHubIngressOptions<never, never> & { runner?: Partial<RunnerOptions> } = {
				namespace: 'worker',
				policy,
				handlers: [
					{
						id: 'receive',
						onCreation: (event) =>
							Queue.offer(started, event.resource.number).pipe(Effect.andThen(Deferred.await(release))),
					},
				],
			}
			if (fixture.runner !== undefined) options.runner = fixture.runner
			const bot = GitHubBot.make(options)
			const memoMap = yield* Layer.makeMemoMap
			const environment = yield* Layer.buildWithMemoMap(
				bot.services.pipe(Layer.provide(dependencies)),
				memoMap,
				yield* Scope.Scope,
			)
			const ingress = Context.get(environment, GitHubIngress)
			for (let number = 1; number <= fixture.concurrency + 1; number++) {
				yield* ingress.acceptActivity({
					event: {
						...event,
						deliveryId: `worker-${number}`,
						resource: { ...event.resource, number },
						issue: { ...event.issue, number },
					},
					mentioned: false,
					own: false,
				})
			}
			assert.equal(yield* Queue.size(started), 0)
			const workerScope = yield* Scope.make()
			yield* Effect.addFinalizer(() => Scope.close(workerScope, Exit.void))
			yield* Layer.buildWithMemoMap(bot.worker.pipe(Layer.provide(dependencies)), memoMap, workerScope)
			assert.deepEqual(yield* Queue.take(scans), {
				now: 0,
				limit: 100,
				prefix: mailboxPrefix({ namespace: 'worker', provider: 'github', handlerId: '["receive","creation"]' }),
			})
			for (let i = 0; i < fixture.concurrency; i++) yield* Queue.take(started)
			yield* TestClock.adjust(fixture.pollMs - 1)
			assert.equal(yield* Queue.size(scans), 0)
			yield* TestClock.adjust(1)
			const nextScan = yield* Queue.take(scans)
			assert.equal(nextScan.now, fixture.pollMs)
			assert.equal(nextScan.limit, 100)
			assert.equal(yield* Queue.size(started), 0)
			yield* Deferred.succeed(release, undefined)
			for (let i = 0; i < fixture.concurrency; i++) yield* Queue.take(completed)
			yield* TestClock.adjust(fixture.pollMs)
			yield* Queue.take(started)
			const finalKey = yield* Queue.take(completed)
			assert.equal((yield* store.loadMailbox({ key: finalKey }))?.state.outcomes[0]?.kind, 'completed')
			yield* Scope.close(workerScope, Exit.void)
			yield* Queue.takeAll(scans)
			yield* TestClock.adjust(fixture.pollMs)
			assert.equal(yield* Queue.size(scans), 0)
		}),
	)
}
