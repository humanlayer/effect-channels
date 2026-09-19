import { describe, it } from '@effect/vitest'
import { ConfigProvider, Effect, Layer, Queue, Ref } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { GitHubApi } from '../src/GitHubApi'
import { GitHubApiLiveBase, GitHubAppSigner } from '../src/GitHubApiLive'
import { GitHubId } from '../src/GitHubIdentity'
import { GitHubContent, GitHubIssueCommentRef, GitHubIssueRef, GitHubPullRequestRef } from '../src/GitHubModels'

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

const makeLayer = (httpClient: HttpClient.HttpClient) =>
	GitHubApiLiveBase.pipe(
		Layer.provide(
			Layer.mergeAll(
				Layer.succeed(HttpClient.HttpClient, httpClient),
				Layer.succeed(GitHubAppSigner, GitHubAppSigner.of({ sign: () => Effect.succeed('test-signature') })),
				ConfigProvider.layer(
					ConfigProvider.fromUnknown({
						GITHUB_APP_ID: 1,
						GITHUB_PRIVATE_KEY: 'private-key-never-log',
						GITHUB_API_ORIGIN: 'https://api.github.test',
						GITHUB_BOT_USER_ID: 999,
					}),
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
				yield* api.addReaction({ comment: issueComment, reaction: 'eyes' })
				yield* api.removeReaction({ comment: issueComment, reaction: 'eyes' })
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

	it.effect('narrows provider status and malformed JSON failures to GitHubApiError', ({ expect }) =>
		Effect.gen(function* () {
			const mode = yield* Ref.make<'rate' | 'decode' | 'validation'>('rate')
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					return HttpClientResponse.fromWeb(
						request,
						(yield* Ref.get(mode)) === 'rate'
							? Response.json({}, { status: 429, headers: { 'retry-after': '3' } })
							: (yield* Ref.get(mode)) === 'validation'
								? Response.json({}, { status: 422 })
								: Response.json({ number: 'not-a-number' }),
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
})
