import { assert, it } from '@effect/vitest'
import { Clock, Deferred, Effect, Fiber, Layer, Logger, Match, Predicate, Redacted, Schema } from 'effect'
import {
	FetchHttpClient,
	HttpClient,
	HttpClientError,
	HttpClientRequest,
	HttpClientResponse,
} from 'effect/unstable/http'

import {
	GitHub,
	GitHubCredentials,
	GitHubCrypto,
	GitHubIssueData,
	GitHubCommentData,
	GitHubReactionContent,
	type GitHubReactionTarget,
	type GitHubError,
} from '../src/index.js'
import { event, user } from './fixtures.js'
import { adminCall, emulator } from './support.js'

const origin = 'https://api.github.test'
const repository = event.resource.repository
const issue = event.resource
const pr = { ...issue, kind: 'github.pull-request' } as const
const targets: ReadonlyArray<GitHubReactionTarget> = [
	issue,
	pr,
	{ kind: 'github.issue-comment', issue, id: 50 },
	{ kind: 'github.issue-comment', issue: pr, id: 50 },
]
const reaction = { id: 70, node_id: 'REACTION_70', user, content: '+1', created_at: '2026-01-01T00:00:00Z' } as const
const basePath = '/repos/alice/project'
const reactionPath = (target: GitHubReactionTarget) =>
	target.kind === 'github.issue-comment'
		? `${basePath}/issues/comments/${target.id}/reactions`
		: `${basePath}/issues/${target.number}/reactions`

interface HarnessOptions {
	readonly target: GitHubReactionTarget
	readonly status?: number
	readonly body?: string
	readonly transportFailure?: boolean
	readonly repositoryId?: number
	readonly issueNumber?: number
	readonly wrongKind?: boolean
	readonly commentUrl?: string
	readonly commentId?: number
	readonly headers?: Readonly<Record<string, string>>
}
const harness = (options: HarnessOptions) => {
	const requests: Array<HttpClientRequest.HttpClientRequest> = []
	const logs: Array<string> = []
	const invalidated: Array<number> = []
	const visible = new Map<number, typeof reaction>()
	const parent = options.target.kind === 'github.issue-comment' ? options.target.issue : options.target
	const isPr = (parent.kind === 'github.pull-request') !== (options.wrongKind === true)
	const parentData = { ...event.issue, number: options.issueNumber ?? 1 }
	const parentResponses = new Map<string, Schema.Json>([
		[basePath, { id: options.repositoryId ?? repository.id }],
		[
			`${basePath}/issues/1`,
			isPr ? { ...parentData, pull_request: { url: `${origin}${basePath}/pulls/1` } } : parentData,
		],
		[
			`${basePath}/issues/comments/50`,
			{ id: options.commentId ?? 50, issue_url: options.commentUrl ?? `${origin}${basePath}/issues/1` },
		],
	])
	const credentials = Layer.mock(GitHubCredentials, {
		apiUrl: origin,
		botUserId: 99,
		acceptsInstallation: () => true,
		token: () => Effect.succeed(Redacted.make('never-log-token')),
		invalidate: ({ id }) =>
			Effect.sync(() => {
				invalidated.push(id)
			}),
	})
	const http = Layer.succeed(
		HttpClient.HttpClient,
		HttpClient.make((request) =>
			Effect.gen(function* () {
				requests.push(request)
				const url = new URL(request.url)
				const parentResponse = parentResponses.get(url.pathname)
				if (request.method === 'GET' && parentResponse !== undefined)
					return HttpClientResponse.fromWeb(request, Response.json(parentResponse))
				if (!url.pathname.startsWith(reactionPath(options.target)))
					return yield* Effect.die(`Unexpected request: ${request.method} ${url}`)
				if (options.transportFailure)
					return yield* new HttpClientError.HttpClientError({
						reason: new HttpClientError.TransportError({ request, cause: 'never-log-cause' }),
					})
				const status =
					options.status ??
					Match.value(request.method).pipe(
						Match.when('DELETE', () => 204),
						Match.when('POST', () => 201),
						Match.orElse(() => 200),
					)
				if (status >= 200 && status < 300 && options.body === undefined) {
					if (request.method === 'POST') visible.set(reaction.id, reaction)
					if (request.method === 'DELETE') visible.delete(reaction.id)
				}
				return HttpClientResponse.fromWeb(
					request,
					new Response(
						status === 204
							? null
							: (options.body ??
									JSON.stringify(request.method === 'GET' ? [...visible.values()] : reaction)),
						{ status, headers: options.headers },
					),
				)
			}),
		),
	)
	return {
		requests,
		logs,
		invalidated,
		visible,
		layer: Layer.merge(
			GitHub.layer.pipe(Layer.provide(Layer.merge(credentials, http))),
			Logger.layer([
				Logger.make((entry) => {
					logs.push(JSON.stringify(entry.message))
				}),
			]),
		),
	}
}

for (const target of targets) {
	it.effect(
		`native reaction add/list/remove contract: ${target.kind} ${target.kind === 'github.issue-comment' ? target.issue.kind : ''}`,
		() => {
			const h = harness({ target })
			return Effect.gen(function* () {
				const github = yield* GitHub
				const added = yield* github.addReaction({ target, content: '+1' })
				assert.deepEqual(added, { ref: { kind: 'github.reaction', target, id: 70 }, data: reaction })
				assert.deepEqual([...h.visible.values()], [reaction])
				assert.deepEqual(yield* github.listReactions({ target, page: 2, perPage: 1, content: '+1' }), [added])
				assert.equal(yield* github.removeReaction({ reaction: added.ref }), undefined)
				assert.equal(h.visible.size, 0)
				const calls = h.requests.filter((request) => request.url.includes('/reactions'))
				assert.deepEqual(
					calls.map((r) => [r.method, r.url]),
					[
						['POST', `${origin}${reactionPath(target)}`],
						['GET', `${origin}${reactionPath(target)}?per_page=1&page=2&content=%2B1`],
						['DELETE', `${origin}${reactionPath(target)}/70`],
					],
				)
				for (const request of calls) {
					assert.equal(request.headers.authorization, 'Bearer never-log-token')
					assert.equal(request.headers.accept, 'application/vnd.github+json')
					assert.equal(request.headers['x-github-api-version'], '2022-11-28')
					if (request.method === 'POST') {
						assert.equal(request.body._tag, 'Uint8Array')
						if (Predicate.isTagged(request.body, 'Uint8Array'))
							assert.equal(new TextDecoder().decode(request.body.body), '{"content":"+1"}')
					}
					if (request.method === 'DELETE') assert.equal(request.body._tag, 'Empty')
				}
				assert.deepEqual(h.invalidated, [])
			}).pipe(Effect.provide(h.layer))
		},
	)
}

it.effect(
	'existing reaction 200 and deleted-user null are native success; bounded empty page is not an automatic traversal',
	() => {
		const h = harness({ target: issue, status: 200, body: JSON.stringify({ ...reaction, user: null }) })
		const empty = harness({ target: issue, body: '[]', headers: { link: '<https://evil.test/next>; rel="next"' } })
		return Effect.gen(function* () {
			assert.equal(
				(yield* Effect.flatMap(GitHub, (github) => github.addReaction({ target: issue, content: '+1' })).pipe(
					Effect.provide(h.layer),
				)).data.user,
				null,
			)
			assert.deepEqual(
				yield* Effect.flatMap(GitHub, (github) =>
					github.listReactions({ target: issue, page: 3, perPage: 100 }),
				).pipe(Effect.provide(empty.layer)),
				[],
			)
			assert.equal(empty.requests.length, 3)
		})
	},
)

interface FailureCase {
	readonly status: number
	readonly reason: GitHubError['reason']
	readonly body?: string
	readonly headers?: Readonly<Record<string, string>>
}
const failures: ReadonlyArray<FailureCase> = [
	{ status: 401, reason: 'authentication' },
	{ status: 403, reason: 'forbidden' },
	{ status: 404, reason: 'not_found' },
	{ status: 410, reason: 'not_found' },
	{ status: 422, reason: 'invalid_input' },
	{ status: 429, reason: 'unavailable', headers: { 'retry-after': '2' } },
	{ status: 403, reason: 'unavailable', headers: { 'x-ratelimit-remaining': '0' } },
	{ status: 500, reason: 'unavailable' },
	{ status: 200, reason: 'response', body: 'not-json-never-log' },
	{
		status: 201,
		reason: 'response',
		body: JSON.stringify({ ...reaction, content: 'thumbsup', secret: 'never-log' }),
	},
	{ status: 201, reason: 'response', body: JSON.stringify({ ...reaction, content: 'heart' }) },
	{ status: 202, reason: 'response' },
	{ status: 204, reason: 'response' },
]
for (const fixture of failures) {
	it.effect(
		`reaction failure captured then narrowed, no mutation replay: ${fixture.status} ${fixture.reason}`,
		() => {
			const h = harness({ target: issue, ...fixture })
			return Effect.gen(function* () {
				const github = yield* GitHub
				const error = yield* github.addReaction({ target: issue, content: '+1' }).pipe(Effect.flip)
				assert.equal(error.reason, fixture.reason)
				if (fixture.status === 429) assert.equal(error.retryAfterMs, 2_000)
				assert.equal(h.requests.filter((r) => r.method === 'POST').length, 1)
				assert.deepEqual(h.invalidated, fixture.status === 401 ? [repository.id] : [])
				assert.ok(h.logs.length > 0)
				assert.ok(!h.logs.join('').includes('never-log'))
			}).pipe(Effect.provide(h.layer))
		},
	)
}

it.effect('transport, malformed list, overfull page and non-204 delete remain typed failures without retry', () =>
	Effect.gen(function* () {
		for (const operation of ['transport', 'list', 'overfull', 'delete'] as const) {
			const h = harness({
				target: issue,
				transportFailure: operation === 'transport',
				status: 200,
				body: operation === 'overfull' ? JSON.stringify([reaction, reaction]) : '{}',
			})
			const error = yield* Effect.gen(function* () {
				const github = yield* GitHub
				if (operation === 'transport') return yield* github.addReaction({ target: issue, content: '+1' })
				if (operation === 'delete')
					return yield* github.removeReaction({
						reaction: { kind: 'github.reaction', target: issue, id: 70 },
					})
				return yield* github.listReactions({ target: issue, page: 1, perPage: 1 })
			}).pipe(Effect.provide(h.layer), Effect.flip)
			assert.equal(error.reason, operation === 'transport' ? 'unavailable' : 'response')
			assert.equal(h.requests.length, 3)
			assert.ok(!h.logs.join('').includes('never-log'))
		}
	}),
)

it.effect(
	'target identity checks reject wrong repository, kind, number, comment ID, parent and foreign URL before reactions',
	() =>
		Effect.gen(function* () {
			for (const mismatch of [
				{ repositoryId: 999 },
				{ wrongKind: true },
				{ issueNumber: 2 },
				{ commentId: 51 },
				{ commentUrl: `${origin}${basePath}/issues/2` },
				{ commentUrl: `${origin}/repos/alice/second/issues/1` },
				{ commentUrl: `https://foreign.test${basePath}/issues/1` },
			]) {
				const target = { kind: 'github.issue-comment', issue, id: 50 } as const
				const h = harness({ target, ...mismatch })
				yield* Effect.gen(function* () {
					const github = yield* GitHub
					assert.equal(
						(yield* github.addReaction({ target, content: '+1' }).pipe(Effect.flip)).reason,
						'invalid_input',
					)
					assert.equal(
						(yield* github.listReactions({ target, page: 1, perPage: 10 }).pipe(Effect.flip)).reason,
						'invalid_input',
					)
					assert.equal(
						(yield* github
							.removeReaction({ reaction: { kind: 'github.reaction', target, id: 70 } })
							.pipe(Effect.flip)).reason,
						'invalid_input',
					)
				}).pipe(Effect.provide(h.layer))
				assert.ok(h.requests.every((r) => !r.url.includes('/reactions')))
			}
		}),
)

it.effect('runtime input bounds reject invalid pages, IDs and native content before I/O', () => {
	const h = harness({ target: issue })
	return Effect.gen(function* () {
		const github = yield* GitHub
		for (const page of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
			assert.equal(
				(yield* github.listReactions({ target: issue, page, perPage: 100 }).pipe(Effect.flip)).reason,
				'invalid_input',
			)
		for (const perPage of [0, 101, 1.5])
			assert.equal(
				(yield* github.listReactions({ target: issue, page: 1, perPage }).pipe(Effect.flip)).reason,
				'invalid_input',
			)
		assert.equal(
			(yield* github
				.removeReaction({ reaction: { kind: 'github.reaction', target: issue, id: 0 } })
				.pipe(Effect.flip)).reason,
			'invalid_input',
		)
		assert.equal(Schema.is(GitHubReactionContent)('thumbsup'), false)
		assert.equal(h.requests.length, 0)
		for (const content of ['+1', '-1', 'laugh', 'confused', 'heart', 'hooray', 'rocket', 'eyes'])
			assert.ok(Schema.is(GitHubReactionContent)(content))
	}).pipe(Effect.provide(h.layer))
})

it.live(
	'emulator-created issue/PR bodies and discussion comments: reaction routes are absent, not provider success',
	() =>
		Effect.gen(function* () {
			const em = yield* emulator()
			const issueData = yield* adminCall(em.resource.url, `${basePath}/issues`, GitHubIssueData, {
				title: 'Reaction issue',
				body: '',
			})
			const prData = yield* adminCall(em.resource.url, `${basePath}/pulls`, GitHubIssueData, {
				title: 'Reaction PR',
				head: 'feature',
				base: 'main',
				body: '',
			})
			const client = yield* HttpClient.HttpClient
			for (const data of [issueData, prData]) {
				const comment = yield* adminCall(
					em.resource.url,
					`${basePath}/issues/${data.number}/comments`,
					GitHubCommentData,
					{ body: 'Reaction target' },
				)
				for (const path of [
					`${basePath}/issues/${data.number}/reactions`,
					`${basePath}/issues/comments/${comment.id}/reactions`,
				]) {
					for (const method of ['GET', 'POST', 'DELETE'] as const) {
						const request = HttpClientRequest.make(method)(
							`${em.resource.url}${path}${method === 'DELETE' ? '/1' : ''}`,
						).pipe(HttpClientRequest.bearerToken('test_token_admin'))
						const response = yield* client.execute(
							method === 'POST' ? yield* HttpClientRequest.bodyJson(request, { content: '+1' }) : request,
						)
						assert.equal(
							response.status,
							404,
							`${method} ${path}: update coverage if emulator adds support`,
						)
					}
				}
			}
			yield* Effect.gen(function* () {
				const github = yield* GitHub
				const target = { kind: 'github.issue', repository: em.repository, number: issueData.number } as const
				assert.equal(
					(yield* github.addReaction({ target, content: '+1' }).pipe(Effect.flip)).reason,
					'not_found',
				)
				assert.equal(
					(yield* github
						.addReaction({
							target: { ...target, repository: { ...em.repository, installationId: 101 } },
							content: '+1',
						})
						.pipe(Effect.flip)).reason,
					'authentication',
				)
				assert.equal(
					(yield* github
						.addReaction({
							target: { ...target, repository: { ...em.repository, name: 'second' } },
							content: '+1',
						})
						.pipe(Effect.flip)).reason,
					'invalid_input',
				)
			}).pipe(Effect.provide(GitHub.layer.pipe(Layer.provide(em.credentials))))
		}).pipe(Effect.provide(FetchHttpClient.layer)),
)

it.effect(
	'overlapping reaction lifecycles use real credential cache with isolated installation/repository tokens and state',
	() =>
		Effect.gen(function* () {
			const bothEntered = yield* Deferred.make<void>()
			const release = yield* Deferred.make<void>()
			const minted: Array<string> = []
			const visible = new Map<string, typeof reaction>()
			let entered = 0
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Effect.gen(function* () {
						const path = new URL(request.url).pathname
						if (path.endsWith('/access_tokens')) {
							if (!Predicate.isTagged(request.body, 'Uint8Array'))
								return yield* Effect.die('Expected JSON token scope')
							const scope = yield* Schema.decodeEffect(
								Schema.fromJsonString(
									Schema.Struct({
										repository_ids: Schema.Array(Schema.Int),
										permissions: Schema.Struct({ issues: Schema.Literal('write') }),
									}),
								),
							)(new TextDecoder().decode(request.body.body)).pipe(Effect.orDie)
							const token = `${path}:${scope.repository_ids.join(',')}`
							minted.push(token)
							return HttpClientResponse.fromWeb(
								request,
								Response.json({
									token,
									expires_at: new Date((yield* Clock.currentTimeMillis) + 120_000).toISOString(),
								}),
							)
						}
						const second = path.startsWith('/repos/alice/second')
						const id = second ? 21 : 20
						const installationId = second ? 101 : 100
						const expected = `Bearer /app/installations/${installationId}/access_tokens:${id}`
						assert.equal(request.headers.authorization, expected)
						if (path === '/repos/alice/project' || path === '/repos/alice/second')
							return HttpClientResponse.fromWeb(request, Response.json({ id }))
						if (path.endsWith('/issues/1'))
							return HttpClientResponse.fromWeb(request, Response.json(event.issue))
						if (!path.includes('/reactions')) return yield* Effect.die(`Unexpected ${path}`)
						if (request.method === 'POST') {
							visible.set(expected, reaction)
							entered += 1
							if (entered === 2) yield* Deferred.succeed(bothEntered, undefined)
							yield* Deferred.await(release)
							return HttpClientResponse.fromWeb(request, Response.json(reaction, { status: 201 }))
						}
						if (request.method === 'DELETE') {
							visible.delete(expected)
							return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))
						}
						return HttpClientResponse.fromWeb(
							request,
							Response.json(visible.has(expected) ? [reaction] : []),
						)
					}),
				),
			)
			const credentials = GitHubCredentials.layer({
				appId: 42,
				privateKey: Redacted.make('test-key'),
				installationIds: [100, 101],
				botUserId: 99,
				apiUrl: origin,
			}).pipe(
				Layer.provide(Layer.mock(GitHubCrypto, { signApp: () => Effect.succeed('signature') })),
				Layer.provide(http),
			)
			yield* Effect.gen(function* () {
				const github = yield* GitHub
				const second = { ...issue, repository: { ...repository, id: 21, installationId: 101, name: 'second' } }
				const firstFiber = yield* github.addReaction({ target: issue, content: '+1' }).pipe(Effect.forkChild)
				const secondFiber = yield* github.addReaction({ target: second, content: '+1' }).pipe(Effect.forkChild)
				yield* Deferred.await(bothEntered)
				assert.equal(visible.size, 2)
				yield* Deferred.succeed(release, undefined)
				const firstAdded = yield* Fiber.join(firstFiber)
				const secondAdded = yield* Fiber.join(secondFiber)
				yield* github.removeReaction({ reaction: firstAdded.ref })
				assert.deepEqual(yield* github.listReactions({ target: issue, page: 1, perPage: 100 }), [])
				assert.deepEqual(yield* github.listReactions({ target: second, page: 1, perPage: 100 }), [secondAdded])
				yield* github.removeReaction({ reaction: secondAdded.ref })
				assert.equal(visible.size, 0)
				assert.deepEqual(minted.sort(), [
					'/app/installations/100/access_tokens:20',
					'/app/installations/101/access_tokens:21',
				])
			}).pipe(Effect.provide(GitHub.layer.pipe(Layer.provide(credentials), Layer.provide(http))))
		}),
)
