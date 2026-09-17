import { assert, it } from '@effect/vitest'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Deferred, Effect, Fiber, Layer, Logger, Redacted, Ref } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { GitHub, GitHubCredentials, GitHubIngress } from '../src/index'
import { policy } from './fixtures'
import { event } from './fixtures'

const credentials = Layer.mock(GitHubCredentials, {
	apiUrl: 'https://api.github.test',
	botUserId: 99,
	acceptsInstallation: () => true,
	token: () => Effect.succeed(Redacted.make('token-never-log')),
	invalidate: () => Effect.die('Rate limits must not invalidate credentials'),
})
interface Case {
	readonly name: string
	readonly headers: Readonly<Record<string, string>>
	readonly body: string
	readonly delay?: number
}
const cases: ReadonlyArray<Case> = [
	{
		name: 'primary limit',
		headers: { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': '2' },
		body: 'never-log',
		delay: 2_000,
	},
	{
		name: 'secondary Retry-After',
		headers: { 'x-ratelimit-remaining': '42', 'retry-after': '2' },
		body: 'never-log',
		delay: 2_000,
	},
	{
		name: 'both deadlines use the later value',
		headers: { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': '3', 'retry-after': '2' },
		body: 'never-log',
		delay: 3_000,
	},
	{
		name: 'secondary message without headers',
		headers: {},
		body: '{"message":"You have exceeded a secondary rate limit. never-log"}',
		delay: 60_000,
	},
	{
		name: 'primary message without headers',
		headers: {},
		body: '{"message":"API rate limit exceeded for never-log"}',
		delay: 60_000,
	},
	{
		name: 'malformed timing on primary limit',
		headers: { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': 'Infinity', 'retry-after': '-1' },
		body: 'never-log',
		delay: 60_000,
	},
	{
		name: 'permission failure with normal rate metadata',
		headers: { 'x-ratelimit-remaining': '42', 'x-ratelimit-reset': '2' },
		body: '{"message":"Resource not accessible by integration never-log"}',
	},
	{ name: 'permission failure with invalid Retry-After', headers: { 'retry-after': 'Infinity' }, body: 'never-log' },
	{ name: 'oversized untrusted body', headers: {}, body: `{"message":"${'x'.repeat(8_192)} never-log"}` },
]

for (const fixture of cases) {
	it.effect(`403 classification: ${fixture.name}`, () =>
		Effect.gen(function* () {
			const calls = yield* Ref.make(0)
			const logs: Array<string> = []
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Ref.update(calls, (n) => n + 1).pipe(
						Effect.as(
							HttpClientResponse.fromWeb(
								request,
								new Response(fixture.body, { status: 403, headers: fixture.headers }),
							),
						),
					),
				),
			)
			const error = yield* Effect.flatMap(GitHub, (github) =>
				github.createIssue({ repository: event.resource.repository, title: 'never-log', body: 'never-log' }),
			).pipe(
				Effect.provide(
					Layer.merge(
						GitHub.layer.pipe(Layer.provide(Layer.merge(credentials, http))),
						Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))]),
					),
				),
				Effect.flip,
			)
			assert.equal(error.reason, fixture.delay === undefined ? 'forbidden' : 'unavailable')
			assert.equal(error.retryAfterMs, fixture.delay)
			assert.equal(yield* Ref.get(calls), 1)
			assert.ok(logs.length > 0)
			assert.ok(!logs.join('').includes('never-log'))
		}),
	)
}

for (const fixture of [cases[0], cases[1], cases[3], cases[6]]) {
	if (fixture === undefined) throw new Error('Missing rate-limit fixture')
	it.effect(`real ingress retry disposition and timing: ${fixture.name}`, () =>
		Effect.gen(function* () {
			const calls = yield* Ref.make(0)
			const entered = yield* Deferred.make<void>()
			const storage = yield* Layer.build(memory({ maxMailboxes: 10 }))
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Effect.gen(function* () {
						const n = yield* Ref.updateAndGet(calls, (n) => n + 1)
						yield* Deferred.succeed(entered, undefined)
						return HttpClientResponse.fromWeb(
							request,
							n === 1
								? new Response(fixture.body, { status: 403, headers: fixture.headers })
								: Response.json(event.issue),
						)
					}),
				),
			)
			const environment = yield* Layer.build(
				GitHubIngress.layer({
					namespace: 'rate-limit',
					policy,
					handlers: [
						{
							id: 'reply',
							onCreation: () =>
								Effect.flatMap(GitHub, (github) =>
									github.createIssue({
										repository: event.resource.repository,
										title: 'Hello',
										body: '',
									}),
								).pipe(Effect.asVoid),
						},
					],
				}).pipe(
					Layer.provide(
						Layer.merge(
							Layer.succeedContext(storage),
							GitHub.layer.pipe(Layer.provide(Layer.merge(credentials, http))),
						),
					),
				),
			)
			const ingress = Context.get(environment, GitHubIngress)
			yield* ingress.acceptActivity({ event, mentioned: false, own: false })
			const first = yield* ingress.processActivity({ event }).pipe(Effect.provide(storage), Effect.forkChild)
			yield* Deferred.await(entered)
			if (fixture.delay !== undefined) {
				yield* TestClock.adjust(fixture.delay - 1)
				yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
				assert.equal(yield* Ref.get(calls), 1)
				yield* TestClock.adjust(1)
			}
			yield* Fiber.join(first)
			yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
			assert.equal(yield* Ref.get(calls), 1)
			yield* TestClock.adjust(policy.retryBaseMs)
			yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
			assert.equal(yield* Ref.get(calls), fixture.delay === undefined ? 1 : 2)
			yield* TestClock.adjust(60_000)
			yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
			assert.equal(yield* Ref.get(calls), fixture.delay === undefined ? 1 : 2)
		}),
	)
}
