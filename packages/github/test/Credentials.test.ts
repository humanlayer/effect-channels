import { assert, it } from '@effect/vitest'
import { Context, DateTime, Deferred, Effect, Fiber, Layer, Logger, Redacted, Ref } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { GitHub, GitHubCredentials, GitHubCrypto, GitHubError } from '../src/index'
import { event } from './fixtures'

const options = {
	appId: 42,
	privateKey: Redacted.make('private-key-never-log'),
	installationIds: [100, 101] as const,
	botUserId: 99,
	apiUrl: 'https://api.github.test',
}
const signer = Layer.mock(GitHubCrypto, { signApp: () => Effect.succeed('signature') })

it.effect(
	'singleflight token cache scopes installation/repository, refreshes before expiry, does not cache failures',
	() =>
		Effect.gen(function* () {
			const calls = yield* Ref.make(0)
			const entered = yield* Deferred.make<void>()
			const release = yield* Deferred.make<void>()
			const fail = yield* Ref.make(false)
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Effect.gen(function* () {
						const n = yield* Ref.updateAndGet(calls, (n) => n + 1)
						yield* Deferred.succeed(entered, undefined)
						yield* Deferred.await(release)
						const now = yield* DateTime.now
						return HttpClientResponse.fromWeb(
							request,
							(yield* Ref.get(fail))
								? new Response('private-key-never-log', { status: 500 })
								: Response.json({
										token: `token-${n}`,
										expires_at: DateTime.formatIso(DateTime.add(now, { seconds: 120 })),
									}),
						)
					}),
				),
			)
			const environment = yield* Layer.build(
				GitHubCredentials.layer(options).pipe(Layer.provide(signer), Layer.provide(http)),
			)
			const credentials = Context.get(environment, GitHubCredentials)
			const repository = event.resource.repository
			const first = yield* credentials.token(repository).pipe(Effect.forkChild)
			yield* Deferred.await(entered)
			const second = yield* credentials.token(repository).pipe(Effect.forkChild)
			yield* Deferred.succeed(release, undefined)
			assert.equal(Redacted.value(yield* Fiber.join(first)), Redacted.value(yield* Fiber.join(second)))
			assert.equal(yield* Ref.get(calls), 1)
			yield* credentials.token({ ...repository, id: 21 })
			yield* credentials.token({ ...repository, installationId: 101 })
			assert.equal(yield* Ref.get(calls), 3)
			yield* TestClock.adjust(60_001)
			yield* credentials.token(repository)
			assert.equal(yield* Ref.get(calls), 4)
			yield* credentials.invalidate(repository)
			yield* Ref.set(fail, true)
			assert.equal((yield* credentials.token(repository).pipe(Effect.flip)).reason, 'unavailable')
			assert.equal((yield* credentials.token(repository).pipe(Effect.flip)).reason, 'unavailable')
			assert.equal(yield* Ref.get(calls), 6)
			yield* Ref.set(fail, false)
			yield* credentials.token(repository)
			assert.equal(yield* Ref.get(calls), 7)
			assert.equal(
				(yield* credentials.token({ ...repository, installationId: 999 }).pipe(Effect.flip)).reason,
				'authentication',
			)
			assert.equal(yield* Ref.get(calls), 7)
		}),
)

for (const status of [401, 403, 404, 422, 429, 500, 200]) {
	it.effect(`safe HTTP failure ${status}, no mutation retry, authorization invalidation only on 401`, () =>
		Effect.gen(function* () {
			const calls = yield* Ref.make(0)
			const invalidations = yield* Ref.make(0)
			const logs: Array<string> = []
			const credentials = Layer.mock(GitHubCredentials, {
				apiUrl: 'https://api.github.test',
				botUserId: 99,
				acceptsInstallation: () => true,
				token: () => Effect.succeed(Redacted.make('token-never-log')),
				invalidate: () => Ref.update(invalidations, (n) => n + 1),
			})
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Ref.update(calls, (n) => n + 1).pipe(
						Effect.as(
							HttpClientResponse.fromWeb(
								request,
								new Response('token-never-log private-key-never-log', { status }),
							),
						),
					),
				),
			)
			const result = yield* Effect.flatMap(GitHub, (github) =>
				github.createIssue({ repository: event.resource.repository, title: 'Hello', body: 'body-never-log' }),
			).pipe(
				Effect.provide(
					Layer.merge(
						GitHub.layer.pipe(Layer.provide(credentials), Layer.provide(http)),
						Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))]),
					),
				),
				Effect.flip,
			)
			assert.ok(SchemaCheck(result))
			assert.equal(yield* Ref.get(calls), 1)
			assert.equal(yield* Ref.get(invalidations), status === 401 ? 1 : 0)
			assert.ok(logs.length > 0)
			assert.ok(!logs.join('').includes('never-log'))
		}),
	)
}
const SchemaCheck = (error: GitHubError) =>
	['authentication', 'forbidden', 'not_found', 'invalid_input', 'unavailable', 'response'].includes(error.reason)

for (const expiresAt of ['token-never-log', '1970-01-01T00:01:00.000Z']) {
	it.effect('captures invalid/too-short token expiry without caching the token or logging provider values', () =>
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
								Response.json({ token: 'token-never-log', expires_at: expiresAt }),
							),
						),
					),
				),
			)
			yield* Effect.gen(function* () {
				const credentials = yield* GitHubCredentials
				for (let attempt = 0; attempt < 2; attempt++)
					assert.equal(
						(yield* credentials.token(event.resource.repository).pipe(Effect.flip)).reason,
						'response',
					)
			}).pipe(
				Effect.provide(
					Layer.merge(
						GitHubCredentials.layer(options).pipe(Layer.provide(signer), Layer.provide(http)),
						Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))]),
					),
				),
			)
			assert.equal(yield* Ref.get(calls), 2)
			assert.equal(logs.length, 2)
			assert.ok(logs.every((entry) => entry.includes('expiry')))
			assert.ok(!logs.join('').includes('never-log'))
		}),
	)
}

it.effect('captures real signing failure before narrowing without disclosing key or JWT data', () =>
	Effect.gen(function* () {
		const logs: Array<string> = []
		const error = yield* Effect.flatMap(GitHubCrypto, (crypto) =>
			crypto.signApp({ privateKey: Redacted.make('private-key-never-log'), data: 'jwt-never-log' }),
		).pipe(
			Effect.provide(
				Layer.merge(
					GitHubCrypto.layerWebCrypto,
					Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))]),
				),
			),
			Effect.flip,
		)
		assert.equal(error.reason, 'configuration')
		assert.equal(logs.length, 1)
		assert.ok(logs[0]?.includes('sign_app'))
		assert.ok(!logs.join('').includes('never-log'))
	}),
)

it.effect('interrupting an outbound mutation releases transport work without retry or auth invalidation', () =>
	Effect.gen(function* () {
		const entered = yield* Deferred.make<void>()
		const released = yield* Ref.make(0)
		const calls = yield* Ref.make(0)
		const http = Layer.succeed(
			HttpClient.HttpClient,
			HttpClient.make(() =>
				Ref.update(calls, (n) => n + 1).pipe(
					Effect.andThen(Deferred.succeed(entered, undefined)),
					Effect.andThen(Effect.never),
					Effect.ensuring(Ref.update(released, (n) => n + 1)),
				),
			),
		)
		const credentials = Layer.mock(GitHubCredentials, {
			apiUrl: options.apiUrl,
			botUserId: options.botUserId,
			acceptsInstallation: () => true,
			token: () => Effect.succeed(Redacted.make('token-never-log')),
			invalidate: () => Effect.die('Interruption must not invalidate credentials'),
		})
		const work = yield* Effect.flatMap(GitHub, (github) =>
			github.createIssue({ repository: event.resource.repository, title: 'Hello', body: '' }),
		).pipe(Effect.provide(GitHub.layer.pipe(Layer.provide(credentials), Layer.provide(http))), Effect.forkChild)
		yield* Deferred.await(entered)
		yield* Fiber.interrupt(work)
		assert.equal(yield* Ref.get(calls), 1)
		assert.equal(yield* Ref.get(released), 1)
	}),
)
