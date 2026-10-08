import { describe, it } from '@effect/vitest'
import { Array as Arr, Clock, ConfigProvider, Effect, Layer, Logger, Match, Queue, Redacted, Ref } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http'
import { TestClock } from 'effect/testing'

import { GitHubApi } from '../src/GitHubApi'
import { GitHubApiLiveBase, GitHubAppSigner } from '../src/GitHubApiLive'
import { GitHubGitCredentials } from '../src/GitHubGitCredentials'
import { GitHubId } from '../src/GitHubIdentity'
import {
	GitHubAccessLevel,
	GitHubAccessLevelOrder,
	GitHubContent,
	GitHubIssueCommentRef,
	GitHubIssueRef,
	GitHubPullRequestRef,
	hasGitHubAccess,
} from '../src/GitHubModels'

const issue = GitHubIssueRef.make({
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'humanlayer',
	repository: 'channels',
	number: GitHubId.make(42),
})

const pullRequest = GitHubPullRequestRef.make({ ...issue })

const participant = { id: 999, login: 'agent[bot]', type: 'Bot' }
const issueCommentJson = (id: number, body: string, user: typeof participant | null = participant) => ({
	id,
	body,
	html_url: `https://github.test/humanlayer/channels/issues/42#issuecomment-${id}`,
	user,
})

const reviewCommentJson = (id: number, body: string) => ({
	id,
	node_id: `PRRC_${id}`,
	body,
	html_url: `https://github.test/humanlayer/channels/pull/42#discussion_r${id}`,
	user: participant,
	pull_request_review_id: 300,
	path: 'src/index.ts',
	commit_id: 'head-sha',
	original_commit_id: 'base-sha',
	diff_hunk: '@@ -1 +1 @@',
	line: 1,
	start_line: null,
	side: 'RIGHT',
})

const baseConfig = {
	GITHUB_APP_ID: 1,
	GITHUB_PRIVATE_KEY: 'private-key-never-log',
	GITHUB_API_ORIGIN: 'https://api.github.test',
}

const makeLayer = (httpClient: HttpClient.HttpClient, botUserId: number | null = 999) =>
	GitHubApiLiveBase.pipe(
		Layer.provide(
			Layer.mergeAll(
				Layer.succeed(HttpClient.HttpClient, httpClient),
				Layer.succeed(GitHubAppSigner, GitHubAppSigner.of({ sign: () => Effect.succeed('test-signature') })),
				ConfigProvider.layer(
					ConfigProvider.fromUnknown(
						botUserId === null ? baseConfig : { ...baseConfig, GITHUB_BOT_USER_ID: botUserId },
					),
				),
			),
		),
	)

const tokenResponse = () =>
	Response.json({ token: 'installation-token-never-log', expires_at: '2099-01-01T00:00:00.000Z' })

describe('GitHubApiLive', () => {
	it.effect('caches installation tokens, decodes results, and follows pagination links', ({ expect }) =>
		Effect.gen(function* () {
			const tokenRequests = yield* Ref.make(0)
			const apiRequests = yield* Queue.unbounded<string>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname === '/app/installations/100/access_tokens') {
						yield* Ref.update(tokenRequests, (count) => count + 1)
						expect(web.headers.get('authorization')).toContain('test-signature')
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					expect(web.headers.get('authorization')).toBe('Bearer installation-token-never-log')
					yield* Queue.offer(apiRequests, `${url.pathname}${url.search}`)
					if (url.pathname.endsWith('/comments') && url.searchParams.get('page') === '2') {
						return HttpClientResponse.fromWeb(request, Response.json([issueCommentJson(2, 'second')]))
					}
					if (url.pathname.endsWith('/comments')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([issueCommentJson(1, 'first', null)], {
								headers: {
									link: '<https://api.github.test/repos/humanlayer/channels/issues/42/comments?per_page=100&page=2>; rel="next"',
								},
							}),
						)
					}
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							number: 42,
							title: 'Adapter',
							body: 'Build it',
							state: 'open',
							html_url: 'https://github.test/humanlayer/channels/issues/42',
							user: participant,
						}),
					)
				}),
			)
			const layer = makeLayer(httpClient)

			const [comments, info] = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				const comments = yield* api.listIssueComments({ issue })
				const info = yield* api.fetchIssue({ issue })
				return [comments, info] as const
			}).pipe(Effect.provide(layer))

			expect(comments.map((comment) => comment.body)).toEqual(['first', 'second'])
			expect(comments[0]?.author).toBeNull()
			expect(info.title).toBe('Adapter')
			expect(yield* Ref.get(tokenRequests)).toBe(1)
			expect(yield* Queue.take(apiRequests)).toBe('/repos/humanlayer/channels/issues/42/comments?per_page=100')
			expect(yield* Queue.take(apiRequests)).toBe(
				'/repos/humanlayer/channels/issues/42/comments?per_page=100&page=2',
			)
			expect(yield* Queue.take(apiRequests)).toBe('/repos/humanlayer/channels/issues/42')
		}),
	)

	it.effect('logs app credentials GitHub refuses even with a new installation token, without the key', ({ expect }) =>
		Effect.gen(function* () {
			const logs: Array<string> = []
			const logger = Logger.layer([
				Logger.make((entry) => logs.push(JSON.stringify(Logger.formatStructured.log(entry)))),
			])
			const httpClient = HttpClient.make((request) =>
				Effect.succeed(
					HttpClientResponse.fromWeb(request, Response.json({ message: 'Bad credentials' }, { status: 401 })),
				),
			)
			const error = yield* Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(
				Effect.provide(makeLayer(httpClient)),
				Effect.provide(logger),
				Effect.flip,
			)
			expect(error).toMatchObject({ reason: 'authentication', retryable: false })
			const output = logs.join('\n')
			expect(output).toContain('GitHub rejected the app credentials; check GITHUB_APP_ID and GITHUB_PRIVATE_KEY')
			expect(output).toContain('"level":"ERROR"')
			expect(output).not.toContain('private-key-never-log')
		}),
	)

	it.effect('invalidates a rejected installation token and retries once', ({ expect }) =>
		Effect.gen(function* () {
			const tokenRequests = yield* Ref.make(0)
			const apiRequests = yield* Ref.make(0)
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						const count = yield* Ref.updateAndGet(tokenRequests, (value) => value + 1)
						return HttpClientResponse.fromWeb(
							request,
							Response.json({
								token: `installation-token-${count}`,
								expires_at: '2099-01-01T00:00:00.000Z',
							}),
						)
					}
					const count = yield* Ref.updateAndGet(apiRequests, (value) => value + 1)
					if (count === 1) return HttpClientResponse.fromWeb(request, Response.json({}, { status: 401 }))
					expect(web.headers.get('authorization')).toBe('Bearer installation-token-2')
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							number: 42,
							title: 'Adapter',
							body: null,
							state: 'open',
							html_url: 'https://github.test/humanlayer/channels/issues/42',
							user: participant,
						}),
					)
				}),
			)

			const info = yield* Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(
				Effect.provide(makeLayer(httpClient)),
			)
			expect(info.title).toBe('Adapter')
			expect(yield* Ref.get(tokenRequests)).toBe(2)
			expect(yield* Ref.get(apiRequests)).toBe(2)
		}),
	)

	it.effect('gives git the same cached installation token as Basic auth, and narrows a refused token', ({ expect }) =>
		Effect.gen(function* () {
			const tokenRequests = yield* Ref.make(0)
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname === '/app/installations/100/access_tokens') {
						yield* Ref.update(tokenRequests, (count) => count + 1)
						expect(yield* Effect.promise(() => web.json())).toEqual({ repository_ids: [200] })
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					if (url.pathname === '/app/installations/101/access_tokens') {
						return HttpClientResponse.fromWeb(request, Response.json({}, { status: 403 }))
					}
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							number: 42,
							title: 'Adapter',
							body: null,
							state: 'open',
							html_url: 'https://github.test/humanlayer/channels/issues/42',
							user: participant,
						}),
					)
				}),
			)

			const [authorization, refused] = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				const git = yield* GitHubGitCredentials
				yield* api.fetchIssue({ issue })
				const authorization = yield* git.authorization({ repository: issue })
				const refused = yield* Effect.flip(
					git.authorization({ repository: { ...issue, installationId: GitHubId.make(101) } }),
				)
				return [authorization, refused] as const
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(Redacted.value(authorization)).toBe(`Basic ${btoa('x-access-token:installation-token-never-log')}`)
			expect(JSON.stringify(authorization)).not.toContain('installation-token-never-log')
			expect(yield* Ref.get(tokenRequests)).toBe(1)
			expect(refused).toMatchObject({ operation: 'create_git_credentials', reason: 'forbidden', status: 403 })
		}),
	)

	it.effect('encodes comment, reply, delete, and reaction operations at the HTTP seam', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<{
				readonly method: string
				readonly path: string
				readonly body: string
			}>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					const body =
						web.method === 'GET' || web.method === 'DELETE' ? '' : yield* Effect.promise(() => web.text())
					yield* Queue.offer(calls, { method: web.method, path: url.pathname, body })
					if (web.method === 'DELETE')
						return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))
					if (url.pathname.endsWith('/reactions') && web.method === 'GET') {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([{ id: 700, content: 'eyes', user: participant }]),
						)
					}
					if (url.pathname.includes('/pulls/42/comments/500/replies')) {
						return HttpClientResponse.fromWeb(request, Response.json(reviewCommentJson(501, 'reply')))
					}
					if (url.pathname.endsWith('/reactions')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json({ id: 700, content: 'eyes', user: participant }, { status: 201 }),
						)
					}
					return HttpClientResponse.fromWeb(
						request,
						Response.json(issueCommentJson(400, 'posted'), { status: 201 }),
					)
				}),
			)
			const layer = makeLayer(httpClient)
			const content = GitHubContent.make({ markdown: 'hello' })
			const issueComment = GitHubIssueCommentRef.make({
				discussion: { _tag: 'Issue', ref: issue },
				id: GitHubId.make(400),
			})

			yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				yield* api.postIssueComment({ issue, content })
				yield* api.updateComment({ comment: issueComment, content })
				yield* api.deleteComment({ comment: issueComment })
				yield* api.replyToReviewComment({
					pullRequest,
					comment: { pullRequest, id: GitHubId.make(500) },
					content,
				})
				yield* api.addReaction({ target: { _tag: 'Comment', comment: issueComment }, reaction: 'eyes' })
				yield* api.removeReaction({ target: { _tag: 'Comment', comment: issueComment }, reaction: 'eyes' })
			}).pipe(Effect.provide(layer))

			expect(yield* Queue.take(calls)).toEqual({
				method: 'POST',
				path: '/repos/humanlayer/channels/issues/42/comments',
				body: '{"body":"hello"}',
			})
			expect(yield* Queue.take(calls)).toEqual({
				method: 'PATCH',
				path: '/repos/humanlayer/channels/issues/comments/400',
				body: '{"body":"hello"}',
			})
			expect((yield* Queue.take(calls)).method).toBe('DELETE')
			expect((yield* Queue.take(calls)).path).toBe('/repos/humanlayer/channels/pulls/42/comments/500/replies')
			expect((yield* Queue.take(calls)).path).toBe('/repos/humanlayer/channels/issues/comments/400/reactions')
			expect((yield* Queue.take(calls)).method).toBe('GET')
			expect(yield* Queue.take(calls)).toEqual({
				method: 'DELETE',
				path: '/repos/humanlayer/channels/issues/comments/400/reactions/700',
				body: '',
			})
		}),
	)

	it.effect('reacts to an issue or pull request itself, and removes only the bot’s own reaction', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<{
				readonly method: string
				readonly path: string
				readonly body: string
			}>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					const body = web.method === 'POST' ? yield* Effect.promise(() => web.text()) : ''
					yield* Queue.offer(calls, { method: web.method, path: url.pathname, body })
					if (web.method === 'GET') {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([
								{ id: 701, content: 'eyes', user: { id: 1, login: 'someone', type: 'User' } },
								{ id: 702, content: 'eyes', user: participant },
							]),
						)
					}
					if (web.method === 'DELETE') {
						return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))
					}
					/** GitHub answers 200, not 201, when the bot already has this reaction. */
					return HttpClientResponse.fromWeb(
						request,
						Response.json({ id: 702, content: 'eyes', user: participant }),
					)
				}),
			)

			yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				yield* api.addReaction({
					target: { _tag: 'Discussion', discussion: { _tag: 'Issue', ref: issue } },
					reaction: 'eyes',
				})
				yield* api.addReaction({
					target: { _tag: 'Discussion', discussion: { _tag: 'PullRequest', ref: pullRequest } },
					reaction: 'eyes',
				})
				yield* api.removeReaction({
					target: { _tag: 'Discussion', discussion: { _tag: 'PullRequest', ref: pullRequest } },
					reaction: 'eyes',
				})
				yield* api.removeReaction({
					target: { _tag: 'Discussion', discussion: { _tag: 'Issue', ref: issue } },
					reaction: 'rocket',
				})
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(Array.from(yield* Queue.takeAll(calls))).toEqual([
				{ method: 'POST', path: '/repos/humanlayer/channels/issues/42/reactions', body: '{"content":"eyes"}' },
				{ method: 'POST', path: '/repos/humanlayer/channels/issues/42/reactions', body: '{"content":"eyes"}' },
				{ method: 'GET', path: '/repos/humanlayer/channels/issues/42/reactions', body: '' },
				{ method: 'DELETE', path: '/repos/humanlayer/channels/issues/42/reactions/702', body: '' },
				/** No `rocket` from the bot: nothing to delete. */
				{ method: 'GET', path: '/repos/humanlayer/channels/issues/42/reactions', body: '' },
			])
		}),
	)

	it.effect('resolves and caches the bot identity before removing its reaction', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<{ readonly method: string; readonly path: string }>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					yield* Queue.offer(calls, { method: web.method, path: url.pathname })
					if (url.pathname === '/app') {
						expect(web.headers.get('authorization')).toContain('test-signature')
						return HttpClientResponse.fromWeb(request, Response.json({ slug: 'agent' }))
					}
					if (url.pathname === '/users/agent%5Bbot%5D') {
						expect(web.headers.get('authorization')).toBeNull()
						return HttpClientResponse.fromWeb(request, Response.json(participant))
					}
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					if (web.method === 'GET') {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([{ id: 700, content: 'eyes', user: participant }]),
						)
					}
					return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))
				}),
			)
			const comment = GitHubIssueCommentRef.make({
				discussion: { _tag: 'Issue', ref: issue },
				id: GitHubId.make(400),
			})

			yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				yield* api.removeReaction({ target: { _tag: 'Comment', comment }, reaction: 'eyes' })
				yield* api.removeReaction({ target: { _tag: 'Comment', comment }, reaction: 'eyes' })
			}).pipe(Effect.provide(makeLayer(httpClient, null)))

			const observed = yield* Queue.takeAll(calls)
			expect(observed.filter(({ path }) => path === '/app')).toHaveLength(1)
			expect(observed.filter(({ path }) => path === '/users/agent%5Bbot%5D')).toHaveLength(1)
			expect(observed.some(({ method, path }) => method === 'POST' && path.endsWith('/reactions'))).toBe(false)
			expect(observed.filter(({ method }) => method === 'DELETE')).toHaveLength(2)
		}),
	)

	it.effect('narrows provider status and malformed JSON failures to GitHubApiError', ({ expect }) =>
		Effect.gen(function* () {
			const mode = yield* Ref.make<'rate' | 'decode' | 'validation'>('rate')
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					const current = yield* Ref.get(mode)
					return HttpClientResponse.fromWeb(
						request,
						Match.value(current).pipe(
							Match.when('rate', () =>
								Response.json({}, { status: 429, headers: { 'retry-after': '3' } }),
							),
							Match.when('validation', () => Response.json({}, { status: 422 })),
							Match.when('decode', () => Response.json({ number: 'not-a-number' })),
							Match.exhaustive,
						),
					)
				}),
			)
			const layer = makeLayer(httpClient)
			const rateLimited = yield* Effect.flip(
				Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(Effect.provide(layer)),
			)
			expect(rateLimited).toMatchObject({
				_tag: 'GitHubApiError',
				operation: 'fetch_issue',
				reason: 'rate_limited',
				retryable: true,
				retryAfterMs: 3_000,
			})

			yield* Ref.set(mode, 'decode')
			const malformed = yield* Effect.flip(
				Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(Effect.provide(layer)),
			)
			expect(malformed).toMatchObject({
				_tag: 'GitHubApiError',
				operation: 'fetch_issue',
				reason: 'invalid_response',
				retryable: false,
			})

			yield* Ref.set(mode, 'validation')
			const validation = yield* Effect.flip(
				Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(Effect.provide(layer)),
			)
			expect(validation).toMatchObject({
				_tag: 'GitHubApiError',
				operation: 'fetch_issue',
				retryable: false,
			})
		}),
	)

	it.effect('uses the rate-limit reset epoch when Retry-After is absent and clamps elapsed resets', ({ expect }) =>
		Effect.gen(function* () {
			yield* TestClock.adjust(10_000)
			const now = yield* Clock.currentTimeMillis
			const futureResetSeconds = Math.ceil(now / 1_000) + 5
			const reset = yield* Ref.make(String(futureResetSeconds))
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					return HttpClientResponse.fromWeb(
						request,
						Response.json(
							{ message: 'API rate limit exceeded' },
							{
								status: 403,
								headers: {
									'x-ratelimit-remaining': '0',
									'x-ratelimit-reset': yield* Ref.get(reset),
								},
							},
						),
					)
				}),
			)
			const layer = makeLayer(httpClient)
			const fetchError = Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(
				Effect.provide(layer),
				Effect.flip,
			)

			const future = yield* fetchError
			expect(future).toMatchObject({
				reason: 'rate_limited',
				retryAfterMs: futureResetSeconds * 1_000 - now,
			})

			yield* Ref.set(reset, '0')
			const elapsed = yield* fetchError
			expect(elapsed).toMatchObject({ reason: 'rate_limited', retryAfterMs: 0 })
		}),
	)
})

describe('GitHub user access', () => {
	/** Answers the permission API from `answers` by login, and records every permission request it gets. */
	const permissionClient = (
		requests: Queue.Queue<{ readonly method: string; readonly path: string }>,
		answers: ReadonlyMap<string, Response>,
	) =>
		HttpClient.make((request) =>
			Effect.gen(function* () {
				const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
				const url = new URL(web.url)
				if (url.pathname.startsWith('/app/installations/')) {
					return HttpClientResponse.fromWeb(request, tokenResponse())
				}
				yield* Queue.offer(requests, { method: web.method, path: url.pathname })
				const login = decodeURIComponent(url.pathname.split('/').at(-2) ?? '')
				const answer = answers.get(login)
				if (answer === undefined)
					return yield* Effect.die(new Error(`Unexpected GitHub request ${url.pathname}`))
				return HttpClientResponse.fromWeb(request, answer)
			}),
		)

	const permission = (legacy: string, roleName: string) =>
		Response.json({ permission: legacy, role_name: roleName, user: { login: 'someone', id: 1, type: 'User' } })

	it.effect('uses a built-in role, and falls back to the legacy permission for a custom role', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<{ readonly method: string; readonly path: string }>()
			const answers = new Map([
				['K-Mistele', permission('admin', 'admin')],
				['octocat', permission('read', 'read')],
				['maintainer', permission('write', 'maintain')],
				['triager', permission('read', 'triage')],
				['custom[bot]', permission('write', 'security-reviewer')],
			])
			const access = yield* Effect.forEach(Array.from(answers.keys()), (login) =>
				Effect.flatMap(GitHubApi, (api) => api.fetchUserAccess({ repository: issue, login })),
			).pipe(Effect.provide(makeLayer(permissionClient(requests, answers))))

			expect(access).toEqual(['admin', 'read', 'maintain', 'triage', 'write'])
			expect(Array.from(yield* Queue.takeAll(requests))).toEqual([
				{ method: 'GET', path: '/repos/humanlayer/channels/collaborators/K-Mistele/permission' },
				{ method: 'GET', path: '/repos/humanlayer/channels/collaborators/octocat/permission' },
				{ method: 'GET', path: '/repos/humanlayer/channels/collaborators/maintainer/permission' },
				{ method: 'GET', path: '/repos/humanlayer/channels/collaborators/triager/permission' },
				{ method: 'GET', path: '/repos/humanlayer/channels/collaborators/custom%5Bbot%5D/permission' },
			])
		}),
	)

	it.effect('fails with GitHubApiError for an unknown user, a refusal, or a response it cannot read', ({ expect }) =>
		Effect.gen(function* () {
			const requests = yield* Queue.unbounded<{ readonly method: string; readonly path: string }>()
			const answers = new Map([
				['ghost', Response.json({ message: 'ghost is not a user' }, { status: 404 })],
				['blocked', Response.json({ message: 'Resource not accessible by integration' }, { status: 403 })],
				['outage', Response.json({}, { status: 502 })],
				['odd-permission', permission('owner', 'admin')],
				['no-role', Response.json({ permission: 'write' })],
			])
			const errors = yield* Effect.forEach(Array.from(answers.keys()), (login) =>
				Effect.flatMap(GitHubApi, (api) => api.fetchUserAccess({ repository: issue, login })).pipe(Effect.flip),
			).pipe(Effect.provide(makeLayer(permissionClient(requests, answers))))

			expect(errors).toMatchObject([
				{
					_tag: 'GitHubApiError',
					operation: 'fetch_user_access',
					reason: 'not_found',
					retryable: false,
					status: 404,
					message: 'ghost is not a user',
				},
				{ operation: 'fetch_user_access', reason: 'forbidden', retryable: false, status: 403 },
				{ operation: 'fetch_user_access', reason: 'unavailable', retryable: true, status: 502 },
				{ operation: 'fetch_user_access', reason: 'invalid_response', retryable: false },
				{ operation: 'fetch_user_access', reason: 'invalid_response', retryable: false },
			])
			expect(yield* Queue.size(requests)).toBe(5)
		}),
	)

	it.effect('orders access levels from none to admin', ({ expect }) =>
		Effect.sync(() => {
			expect(Arr.sort(['admin', 'none', 'write', 'read', 'maintain', 'triage'], GitHubAccessLevelOrder)).toEqual(
				GitHubAccessLevel.literals,
			)
			expect(
				GitHubAccessLevel.literals.filter((access) => hasGitHubAccess({ access, minimum: 'write' })),
			).toEqual(['write', 'maintain', 'admin'])
		}),
	)
})
