import { describe, it } from '@effect/vitest'
import { ConfigProvider, Effect, Layer, Queue, Ref } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { GitHubApi } from '../src/GitHubApi'
import { GitHubApiLiveBase, GitHubAppSigner } from '../src/GitHubApiLive'
import { GitHubId } from '../src/GitHubIdentity'
import {
	GitHubActionsJobRef,
	GitHubCheckRunRef,
	GitHubContent,
	GitHubIssueRef,
	GitHubPullRequestRef,
} from '../src/GitHubModels'

const issue = GitHubIssueRef.make({
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'humanlayer',
	repository: 'channels',
	number: GitHubId.make(42),
})

const pullRequest = GitHubPullRequestRef.make({ ...issue, number: GitHubId.make(43) })

const checkRun = GitHubCheckRunRef.make({
	installationId: issue.installationId,
	repositoryId: issue.repositoryId,
	owner: issue.owner,
	repository: issue.repository,
	id: GitHubId.make(700),
})

const actionsJob = GitHubActionsJobRef.make({
	installationId: issue.installationId,
	repositoryId: issue.repositoryId,
	owner: issue.owner,
	repository: issue.repository,
	id: GitHubId.make(900),
})

const participant = { id: 999, login: 'agent[bot]', type: 'Bot' }

const issueJson = (state: 'open' | 'closed') => ({
	number: 42,
	title: 'Issue title',
	body: 'Issue body',
	state,
	html_url: 'https://github.test/humanlayer/channels/issues/42',
	user: participant,
})

const pullRequestJson = (state: 'open' | 'closed') => ({
	number: 43,
	title: 'Pull request title',
	body: 'Pull request body',
	state,
	html_url: 'https://github.test/humanlayer/channels/pull/43',
	user: participant,
	draft: false,
	merged: false,
	head: { ref: 'feature', sha: 'head-sha' },
	base: { ref: 'main', sha: 'base-sha' },
})

const reviewCommentJson = (id: number) => ({
	id,
	node_id: `PRRC_${id}`,
	body: `review-${id}`,
	html_url: `https://github.test/humanlayer/channels/pull/43#discussion_r${id}`,
	user: participant,
	pull_request_review_id: 300,
	path: 'src/index.ts',
	commit_id: 'head-sha',
	original_commit_id: 'base-sha',
	diff_hunk: '@@ -1 +1 @@',
	line: 10,
	start_line: null,
	side: 'RIGHT',
})

const checkRunJson = (id = 700) => ({
	id,
	name: 'build',
	head_sha: 'head-sha',
	status: 'completed',
	conclusion: 'failure',
	started_at: '2025-01-01T00:00:00Z',
	completed_at: '2025-01-01T00:01:00Z',
	url: `https://api.github.test/repos/humanlayer/channels/check-runs/${id}`,
	html_url: `https://github.test/humanlayer/channels/runs/${id}`,
	details_url: `https://ci.test/builds/${id}`,
	check_suite: { id: 800 },
	output: {
		title: 'Tests failed',
		summary: 'One test failed',
		text: 'Failure details',
		annotations_count: 1,
	},
})

const actionsJobJson = (id = 900, checkRunId = 700) => ({
	id,
	run_id: 850,
	name: 'test',
	status: 'completed',
	conclusion: 'failure',
	head_sha: 'head-sha',
	url: `https://api.github.test/repos/humanlayer/channels/actions/jobs/${id}`,
	html_url: `https://github.test/humanlayer/channels/actions/runs/850/job/${id}`,
	started_at: '2025-01-01T00:00:00Z',
	completed_at: '2025-01-01T00:01:00Z',
	check_run_url: `https://api.github.test/repos/humanlayer/channels/check-runs/${checkRunId}`,
	workflow_name: 'CI',
	head_branch: 'feature',
	steps: [
		{
			name: 'Run tests',
			status: 'completed',
			conclusion: 'failure',
			number: 1,
		},
	],
})

const tokenResponse = () =>
	Response.json({ token: 'installation-token-never-log', expires_at: '2099-01-01T00:00:00.000Z' })

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

type ObservedRequest = {
	readonly method: string
	readonly path: string
	readonly search: string
	readonly accept: string | null
	readonly authorization: string | null
	readonly body: string
}

const observeRequest = (request: Request): Effect.Effect<ObservedRequest> =>
	Effect.gen(function* () {
		const url = new URL(request.url)
		const body = yield* Effect.promise(() => request.text())
		return {
			method: request.method,
			path: url.pathname,
			search: url.search,
			accept: request.headers.get('accept'),
			authorization: request.headers.get('authorization'),
			body,
		}
	})

describe('GitHubApiLive agent capabilities', () => {
	it.effect('fetches pull request files, a text diff, and commits through their HTTP seams', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ObservedRequest>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					yield* Queue.offer(calls, yield* observeRequest(web))
					if (url.pathname.endsWith('/files')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([
								{
									sha: 'file-sha-1',
									filename: 'src/new.ts',
									previous_filename: 'src/old.ts',
									status: 'renamed',
									additions: 4,
									deletions: 2,
									changes: 6,
									blob_url: 'https://github.test/blob/file-sha-1/src/new.ts',
									raw_url: 'https://github.test/raw/file-sha-1/src/new.ts',
									contents_url: 'https://api.github.test/contents/src/new.ts',
									patch: '@@ -1 +1 @@',
								},
								{
									sha: 'file-sha-2',
									filename: 'assets/removed.png',
									status: 'removed',
									additions: 0,
									deletions: 1,
									changes: 1,
									blob_url: 'https://github.test/blob/file-sha-2/assets/removed.png',
									raw_url: 'https://github.test/raw/file-sha-2/assets/removed.png',
									contents_url: 'https://api.github.test/contents/assets/removed.png',
								},
								...['added', 'modified', 'copied', 'changed', 'unchanged'].map((status, index) => ({
									sha: status === 'copied' ? null : `file-sha-${index + 3}`,
									filename: `src/status-${status}.ts`,
									status,
									additions: 1,
									deletions: 0,
									changes: 1,
									blob_url:
										status === 'copied'
											? null
											: `https://github.test/blob/file-sha-${index + 3}/src/status-${status}.ts`,
									raw_url:
										status === 'copied'
											? null
											: `https://github.test/raw/file-sha-${index + 3}/src/status-${status}.ts`,
									contents_url: `https://api.github.test/contents/src/status-${status}.ts`,
								})),
							]),
						)
					}
					if (url.pathname.endsWith('/commits')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([
								{
									sha: 'commit-sha',
									commit: { message: 'Implement capability' },
									url: 'https://api.github.test/commits/commit-sha',
									html_url: 'https://github.test/commit/commit-sha',
									author: null,
									committer: participant,
								},
								{
									sha: 'empty-participants-sha',
									commit: { message: 'Preserve empty participants' },
									url: 'https://api.github.test/commits/empty-participants-sha',
									html_url: 'https://github.test/commit/empty-participants-sha',
									author: {},
									committer: {},
								},
							]),
						)
					}
					return HttpClientResponse.fromWeb(request, new Response('diff --git a/a.ts b/a.ts\n+added\n'))
				}),
			)

			const result = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				return {
					files: yield* api.listPullRequestFiles({ pullRequest }),
					diff: yield* api.fetchPullRequestDiff({ pullRequest }),
					commits: yield* api.listPullRequestCommits({ pullRequest }),
				}
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(result.files[0]).toMatchObject({
				filename: 'src/new.ts',
				previousFilename: 'src/old.ts',
				status: 'renamed',
				patch: '@@ -1 +1 @@',
			})
			expect(result.files.map(({ status }) => status)).toEqual([
				'renamed',
				'deleted',
				'added',
				'modified',
				'copied',
				'changed',
				'unchanged',
			])
			expect(result.files[1]).not.toHaveProperty('patch')
			expect(result.files[4]).toMatchObject({ sha: null, blobUrl: null, rawUrl: null })
			expect(result.diff).toBe('diff --git a/a.ts b/a.ts\n+added\n')
			expect(result.commits).toMatchObject([
				{
					sha: 'commit-sha',
					message: 'Implement capability',
					author: null,
					committer: participant,
				},
				{
					sha: 'empty-participants-sha',
					author: null,
					committer: null,
				},
			])

			const observed = Array.from(yield* Queue.takeAll(calls))
			expect(observed).toMatchObject([
				{
					method: 'GET',
					path: '/repos/humanlayer/channels/pulls/43/files',
					search: '?per_page=100',
				},
				{
					method: 'GET',
					path: '/repos/humanlayer/channels/pulls/43',
					accept: 'application/vnd.github.diff',
				},
				{
					method: 'GET',
					path: '/repos/humanlayer/channels/pulls/43/commits',
					search: '?per_page=100',
				},
			])
		}),
	)

	it.effect(
		'uses the Issues API and distinct add, set, remove, and remove-all label verbs for issues and PRs',
		({ expect }) =>
			Effect.gen(function* () {
				const calls = yield* Queue.unbounded<ObservedRequest>()
				const httpClient = HttpClient.make((request) =>
					Effect.gen(function* () {
						const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
						const url = new URL(web.url)
						if (url.pathname.startsWith('/app/installations/')) {
							return HttpClientResponse.fromWeb(request, tokenResponse())
						}
						yield* Queue.offer(calls, yield* observeRequest(web))
						if (web.method === 'DELETE' && url.pathname.endsWith('/labels')) {
							return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))
						}
						return HttpClientResponse.fromWeb(
							request,
							Response.json([{ id: 1, name: 'bug', color: 'd73a4a', description: null }]),
						)
					}),
				)

				const results = yield* Effect.gen(function* () {
					const api = yield* GitHubApi
					const issueLabels = [
						yield* api.listIssueLabels({ issue }),
						yield* api.addIssueLabels({ issue, labels: ['bug'] }),
						yield* api.setIssueLabels({ issue, labels: ['bug', 'urgent'] }),
						yield* api.removeIssueLabel({ issue, label: 'needs review' }),
					]
					yield* api.removeAllIssueLabels({ issue })
					const pullRequestLabels = [
						yield* api.listPullRequestLabels({ pullRequest }),
						yield* api.addPullRequestLabels({ pullRequest, labels: ['bug'] }),
						yield* api.setPullRequestLabels({ pullRequest, labels: ['bug', 'urgent'] }),
						yield* api.removePullRequestLabel({ pullRequest, label: 'needs review' }),
					]
					yield* api.removeAllPullRequestLabels({ pullRequest })
					return [...issueLabels, ...pullRequestLabels]
				}).pipe(Effect.provide(makeLayer(httpClient)))

				for (const result of results) expect(result[0]?.name).toBe('bug')
				const observed = Array.from(yield* Queue.takeAll(calls))
				expect(observed.map(({ method, path, body }) => ({ method, path, body }))).toEqual([
					{ method: 'GET', path: '/repos/humanlayer/channels/issues/42/labels', body: '' },
					{
						method: 'POST',
						path: '/repos/humanlayer/channels/issues/42/labels',
						body: '{"labels":["bug"]}',
					},
					{
						method: 'PUT',
						path: '/repos/humanlayer/channels/issues/42/labels',
						body: '{"labels":["bug","urgent"]}',
					},
					{
						method: 'DELETE',
						path: '/repos/humanlayer/channels/issues/42/labels/needs%20review',
						body: '',
					},
					{ method: 'DELETE', path: '/repos/humanlayer/channels/issues/42/labels', body: '' },
					{ method: 'GET', path: '/repos/humanlayer/channels/issues/43/labels', body: '' },
					{
						method: 'POST',
						path: '/repos/humanlayer/channels/issues/43/labels',
						body: '{"labels":["bug"]}',
					},
					{
						method: 'PUT',
						path: '/repos/humanlayer/channels/issues/43/labels',
						body: '{"labels":["bug","urgent"]}',
					},
					{
						method: 'DELETE',
						path: '/repos/humanlayer/channels/issues/43/labels/needs%20review',
						body: '',
					},
					{ method: 'DELETE', path: '/repos/humanlayer/channels/issues/43/labels', body: '' },
				])
			}),
	)

	it.effect('encodes line, range, and file-level pull request review comments', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ObservedRequest>()
			const nextId = yield* Ref.make(500)
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					yield* Queue.offer(calls, yield* observeRequest(web))
					const id = yield* Ref.updateAndGet(nextId, (value) => value + 1)
					return HttpClientResponse.fromWeb(request, Response.json(reviewCommentJson(id), { status: 201 }))
				}),
			)

			const comments = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				const content = GitHubContent.make({ markdown: 'Please revise' })
				return [
					yield* api.postPullRequestReviewComment({
						pullRequest,
						content,
						commitId: 'head-sha',
						path: 'src/index.ts',
						location: { _tag: 'Line', line: 10, side: 'RIGHT' },
					}),
					yield* api.postPullRequestReviewComment({
						pullRequest,
						content,
						commitId: 'head-sha',
						path: 'src/index.ts',
						location: { _tag: 'Range', startLine: 5, startSide: 'LEFT', line: 10, side: 'RIGHT' },
					}),
					yield* api.postPullRequestReviewComment({
						pullRequest,
						content,
						commitId: 'head-sha',
						path: 'src/index.ts',
						location: { _tag: 'File' },
					}),
				]
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(comments.map(({ body }) => body)).toEqual(['review-501', 'review-502', 'review-503'])
			const observed = Array.from(yield* Queue.takeAll(calls))
			expect(observed.map(({ method, path, body }) => ({ method, path, body }))).toEqual([
				{
					method: 'POST',
					path: '/repos/humanlayer/channels/pulls/43/comments',
					body: '{"body":"Please revise","commit_id":"head-sha","path":"src/index.ts","line":10,"side":"RIGHT"}',
				},
				{
					method: 'POST',
					path: '/repos/humanlayer/channels/pulls/43/comments',
					body: '{"body":"Please revise","commit_id":"head-sha","path":"src/index.ts","start_line":5,"start_side":"LEFT","line":10,"side":"RIGHT"}',
				},
				{
					method: 'POST',
					path: '/repos/humanlayer/channels/pulls/43/comments',
					body: '{"body":"Please revise","commit_id":"head-sha","path":"src/index.ts","subject_type":"file"}',
				},
			])
		}),
	)

	it.effect('encodes issue and PR state changes and preserves merge payloads and results', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ObservedRequest>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					const observed = yield* observeRequest(web)
					yield* Queue.offer(calls, observed)
					if (url.pathname.endsWith('/merge')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json({
								merged: true,
								sha: 'merge-sha',
								message: 'Pull Request successfully merged',
							}),
						)
					}
					if (url.pathname.includes('/pulls/')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json(pullRequestJson(observed.body.includes('closed') ? 'closed' : 'open')),
						)
					}
					return HttpClientResponse.fromWeb(
						request,
						Response.json(issueJson(observed.body.includes('closed') ? 'closed' : 'open')),
					)
				}),
			)

			const result = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				const completed = yield* api.closeIssue({ issue, reason: 'completed' })
				const notPlanned = yield* api.closeIssue({ issue, reason: 'not_planned' })
				const reopenedIssue = yield* api.reopenIssue({ issue })
				const closedPullRequest = yield* api.closePullRequest({ pullRequest })
				const reopenedPullRequest = yield* api.reopenPullRequest({ pullRequest })
				const merged = yield* api.mergePullRequest({
					pullRequest,
					method: 'squash',
					expectedHeadSha: 'head-sha',
					commitTitle: 'Ship it',
					commitMessage: 'Complete implementation',
				})
				return { completed, notPlanned, reopenedIssue, closedPullRequest, reopenedPullRequest, merged }
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(result.completed.state).toBe('closed')
			expect(result.notPlanned.state).toBe('closed')
			expect(result.reopenedIssue.state).toBe('open')
			expect(result.closedPullRequest.state).toBe('closed')
			expect(result.reopenedPullRequest.state).toBe('open')
			expect(result.merged).toEqual({
				merged: true,
				sha: 'merge-sha',
				message: 'Pull Request successfully merged',
			})
			const observed = Array.from(yield* Queue.takeAll(calls))
			expect(observed.map(({ method, path, body }) => ({ method, path, body }))).toEqual([
				{
					method: 'PATCH',
					path: '/repos/humanlayer/channels/issues/42',
					body: '{"state":"closed","state_reason":"completed"}',
				},
				{
					method: 'PATCH',
					path: '/repos/humanlayer/channels/issues/42',
					body: '{"state":"closed","state_reason":"not_planned"}',
				},
				{
					method: 'PATCH',
					path: '/repos/humanlayer/channels/issues/42',
					body: '{"state":"open","state_reason":"reopened"}',
				},
				{
					method: 'PATCH',
					path: '/repos/humanlayer/channels/pulls/43',
					body: '{"state":"closed"}',
				},
				{
					method: 'PATCH',
					path: '/repos/humanlayer/channels/pulls/43',
					body: '{"state":"open"}',
				},
				{
					method: 'PUT',
					path: '/repos/humanlayer/channels/pulls/43/merge',
					body: '{"merge_method":"squash","sha":"head-sha","commit_title":"Ship it","commit_message":"Complete implementation"}',
				},
			])
		}),
	)

	it.effect('decodes check-run list envelopes, details, and annotations', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ObservedRequest>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					yield* Queue.offer(calls, yield* observeRequest(web))
					if (url.pathname.endsWith('/annotations')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([
								{
									path: 'src/index.ts',
									start_line: 5,
									end_line: 7,
									start_column: 2,
									end_column: null,
									annotation_level: 'failure',
									title: 'Type error',
									message: 'Unknown property',
									raw_details: 'TS2353',
									blob_href: 'https://github.test/blob/head-sha/src/index.ts',
								},
							]),
						)
					}
					if (url.pathname.includes('/commits/')) {
						expect(url.searchParams.get('per_page')).toBe('100')
						expect(url.searchParams.get('filter')).toBe('all')
						if (url.searchParams.get('page') === '2') {
							return HttpClientResponse.fromWeb(
								request,
								Response.json({ total_count: 1, check_runs: [] }),
							)
						}
						return HttpClientResponse.fromWeb(
							request,
							Response.json(
								{ total_count: 1, check_runs: [checkRunJson()] },
								{
									headers: {
										link: '<https://api.github.test/repos/humanlayer/channels/commits/feature%2Fhead/check-runs?per_page=100&filter=all&page=2>; rel="next"',
									},
								},
							),
						)
					}
					return HttpClientResponse.fromWeb(request, Response.json(checkRunJson()))
				}),
			)

			const result = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				return {
					checks: yield* api.listCheckRunsForRef({ pullRequest, sha: 'feature/head' }),
					info: yield* api.fetchCheckRun({ checkRun }),
					annotations: yield* api.listCheckRunAnnotations({ checkRun }),
				}
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(result.checks).toHaveLength(1)
			expect(result.checks[0]?.ref).toEqual(checkRun)
			expect(result.info).toMatchObject({
				ref: checkRun,
				name: 'build',
				headSha: 'head-sha',
				status: 'completed',
				conclusion: 'failure',
				checkSuiteId: 800,
				outputTitle: 'Tests failed',
				outputSummary: 'One test failed',
				outputText: 'Failure details',
				annotationCount: 1,
			})
			expect(result.annotations).toEqual([
				expect.objectContaining({
					path: 'src/index.ts',
					startLine: 5,
					endLine: 7,
					startColumn: 2,
					endColumn: null,
					level: 'failure',
					title: 'Type error',
					message: 'Unknown property',
					rawDetails: 'TS2353',
				}),
			])
			const observed = Array.from(yield* Queue.takeAll(calls))
			expect(observed.map(({ path, search }) => `${path}${search}`)).toEqual([
				'/repos/humanlayer/channels/commits/feature%2Fhead/check-runs?per_page=100&filter=all',
				'/repos/humanlayer/channels/commits/feature%2Fhead/check-runs?per_page=100&filter=all&page=2',
				'/repos/humanlayer/channels/check-runs/700',
				'/repos/humanlayer/channels/check-runs/700/annotations?per_page=100',
			])
		}),
	)

	it.effect('preserves nullable check annotation level and message fields', ({ expect }) =>
		Effect.gen(function* () {
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					return HttpClientResponse.fromWeb(
						request,
						Response.json([
							{
								path: 'src/generated.ts',
								start_line: 1,
								end_line: 1,
								start_column: null,
								end_column: null,
								annotation_level: null,
								title: null,
								message: null,
								raw_details: null,
								blob_href: 'https://github.test/blob/head-sha/src/generated.ts',
							},
						]),
					)
				}),
			)

			const annotations = yield* Effect.flatMap(GitHubApi, (api) =>
				api.listCheckRunAnnotations({ checkRun }),
			).pipe(Effect.provide(makeLayer(httpClient)))

			expect(annotations).toMatchObject([{ level: null, message: null }])
		}),
	)

	it.effect('resolves Actions jobs through check-suite run and job envelopes and decodes job details', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ObservedRequest>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					yield* Queue.offer(calls, yield* observeRequest(web))
					if (url.pathname.endsWith('/actions/runs')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json({ total_count: 2, workflow_runs: [{ id: 850 }, { id: 851 }] }),
						)
					}
					if (url.pathname.endsWith('/actions/runs/850/jobs')) {
						expect(url.searchParams.get('per_page')).toBe('100')
						expect(url.searchParams.get('filter')).toBe('all')
						if (url.searchParams.get('page') === '2') {
							return HttpClientResponse.fromWeb(request, Response.json({ total_count: 1, jobs: [] }))
						}
						return HttpClientResponse.fromWeb(
							request,
							Response.json(
								{ total_count: 1, jobs: [actionsJobJson(899, 699)] },
								{
									headers: {
										link: '<https://api.github.test/repos/humanlayer/channels/actions/runs/850/jobs?per_page=100&filter=all&page=2>; rel="next"',
									},
								},
							),
						)
					}
					if (url.pathname.endsWith('/actions/runs/851/jobs')) {
						expect(url.searchParams.get('per_page')).toBe('100')
						expect(url.searchParams.get('filter')).toBe('all')
						const matching = actionsJobJson()
						return HttpClientResponse.fromWeb(
							request,
							Response.json({
								total_count: 1,
								jobs: [{ ...matching, check_run_url: `${matching.check_run_url}/` }],
							}),
						)
					}
					if (url.pathname.endsWith('/actions/jobs/900')) {
						return HttpClientResponse.fromWeb(request, Response.json(actionsJobJson()))
					}
					return HttpClientResponse.fromWeb(request, Response.json(checkRunJson()))
				}),
			)

			const result = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				return {
					resolved: yield* api.resolveActionsJob({ checkRun }),
					info: yield* api.fetchActionsJob({ job: actionsJob }),
				}
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(result.resolved?.ref).toEqual(actionsJob)
			expect(result.info).toMatchObject({
				ref: actionsJob,
				runId: 850,
				name: 'test',
				status: 'completed',
				conclusion: 'failure',
				workflowName: 'CI',
				headBranch: 'feature',
				steps: [
					{
						name: 'Run tests',
						number: 1,
						conclusion: 'failure',
						startedAt: null,
						completedAt: null,
					},
				],
			})
			const observed = Array.from(yield* Queue.takeAll(calls))
			expect(observed.map(({ path, search }) => `${path}${search}`)).toEqual([
				'/repos/humanlayer/channels/check-runs/700',
				'/repos/humanlayer/channels/actions/runs?per_page=100&check_suite_id=800',
				'/repos/humanlayer/channels/actions/runs/850/jobs?per_page=100&filter=all',
				'/repos/humanlayer/channels/actions/runs/850/jobs?per_page=100&filter=all&page=2',
				'/repos/humanlayer/channels/actions/runs/851/jobs?per_page=100&filter=all',
				'/repos/humanlayer/channels/actions/jobs/900',
			])
		}),
	)

	it.effect('preserves a nullable Actions job html URL', ({ expect }) =>
		Effect.gen(function* () {
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					return HttpClientResponse.fromWeb(request, Response.json({ ...actionsJobJson(), html_url: null }))
				}),
			)

			const info = yield* Effect.flatMap(GitHubApi, (api) => api.fetchActionsJob({ job: actionsJob })).pipe(
				Effect.provide(makeLayer(httpClient)),
			)

			expect(info.url).toBeNull()
		}),
	)

	it.effect('turns malformed boundary numbers into typed invalid-response failures', ({ expect }) =>
		Effect.gen(function* () {
			const annotationMode = yield* Ref.make<'line' | 'column'>('line')
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const path = new URL(web.url).pathname
					if (path.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					if (path.endsWith('/files')) {
						return HttpClientResponse.fromWeb(
							request,
							Response.json([
								{
									sha: null,
									filename: 'modules/example',
									status: 'modified',
									additions: -1,
									deletions: 0,
									changes: 0,
									blob_url: null,
									raw_url: null,
									contents_url: 'https://api.github.test/contents/modules/example',
								},
							]),
						)
					}
					if (path.endsWith('/annotations')) {
						const mode = yield* Ref.get(annotationMode)
						return HttpClientResponse.fromWeb(
							request,
							Response.json([
								{
									path: 'src/index.ts',
									start_line: mode === 'line' ? 0 : 1,
									end_line: 1,
									start_column: mode === 'column' ? 0 : null,
									end_column: null,
									annotation_level: 'failure',
									title: null,
									message: 'Malformed location',
									raw_details: null,
									blob_href: 'https://github.test/blob/head-sha/src/index.ts',
								},
							]),
						)
					}
					if (path.endsWith('/check-runs/700')) {
						const value = checkRunJson()
						return HttpClientResponse.fromWeb(
							request,
							Response.json({ ...value, output: { ...value.output, annotations_count: -1 } }),
						)
					}
					if (path.endsWith('/actions/jobs/900')) {
						const value = actionsJobJson()
						return HttpClientResponse.fromWeb(
							request,
							Response.json({
								...value,
								steps: value.steps.map((step) => ({ ...step, number: 0 })),
							}),
						)
					}
					return yield* Effect.die(new Error(`Unexpected GitHub API test request: ${path}`))
				}),
			)

			const errors = yield* Effect.gen(function* () {
				const api = yield* GitHubApi
				const fileCount = yield* Effect.flip(api.listPullRequestFiles({ pullRequest }))
				const annotationLine = yield* Effect.flip(api.listCheckRunAnnotations({ checkRun }))
				yield* Ref.set(annotationMode, 'column')
				const annotationColumn = yield* Effect.flip(api.listCheckRunAnnotations({ checkRun }))
				const annotationCount = yield* Effect.flip(api.fetchCheckRun({ checkRun }))
				const stepNumber = yield* Effect.flip(api.fetchActionsJob({ job: actionsJob }))
				return [fileCount, annotationLine, annotationColumn, annotationCount, stepNumber]
			}).pipe(Effect.provide(makeLayer(httpClient)))

			expect(errors.map(({ operation, reason }) => ({ operation, reason }))).toEqual([
				{ operation: 'list_pull_request_files', reason: 'invalid_response' },
				{ operation: 'list_check_run_annotations', reason: 'invalid_response' },
				{ operation: 'list_check_run_annotations', reason: 'invalid_response' },
				{ operation: 'fetch_check_run', reason: 'invalid_response' },
				{ operation: 'fetch_actions_job', reason: 'invalid_response' },
			])
		}),
	)

	it.effect('follows an Actions log redirect without forwarding GitHub authorization', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<ObservedRequest>()
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					const url = new URL(web.url)
					if (url.pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					yield* Queue.offer(calls, yield* observeRequest(web))
					if (url.origin === 'https://api.github.test') {
						return HttpClientResponse.fromWeb(
							request,
							new Response(null, {
								status: 302,
								headers: { location: 'https://logs.github.test/job-900.txt?signature=short-lived' },
							}),
						)
					}
					return HttpClientResponse.fromWeb(request, new Response('safe log output\n'))
				}),
			)

			const log = yield* Effect.flatMap(GitHubApi, (api) => api.downloadActionsJobLog({ job: actionsJob })).pipe(
				Effect.provide(makeLayer(httpClient)),
			)

			expect(log).toBe('safe log output\n')
			const observed = Array.from(yield* Queue.takeAll(calls))
			expect(observed).toHaveLength(2)
			expect(observed[0]).toMatchObject({
				path: '/repos/humanlayer/channels/actions/jobs/900/logs',
				authorization: 'Bearer installation-token-never-log',
			})
			expect(observed[1]).toMatchObject({
				path: '/job-900.txt',
				search: '?signature=short-lived',
				authorization: null,
			})
		}),
	)

	it.effect('classifies merge rejections and preserves safe provider status and message details', ({ expect }) =>
		Effect.gen(function* () {
			type ErrorResponse = { readonly status: number; readonly message: string }
			const response = yield* Ref.make<ErrorResponse>({
				status: 409,
				message: 'Provider-specific conflict text',
			})
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					const current = yield* Ref.get(response)
					return HttpClientResponse.fromWeb(
						request,
						Response.json({ message: current.message }, { status: current.status }),
					)
				}),
			)
			const layer = makeLayer(httpClient)
			const merge = Effect.flatMap(GitHubApi, (api) =>
				api.mergePullRequest({ pullRequest, method: 'merge', expectedHeadSha: 'head-sha' }),
			).pipe(Effect.provide(layer), Effect.flip)

			const stale = yield* merge
			expect(stale).toMatchObject({
				operation: 'merge_pull_request',
				reason: 'stale_head',
				retryable: false,
				status: 409,
				message: 'Provider-specific conflict text',
			})

			yield* Ref.set(response, { status: 405, message: 'Provider-specific method rejection' })
			const notMergeable = yield* merge
			expect(notMergeable).toMatchObject({
				reason: 'not_mergeable',
				status: 405,
				message: 'Provider-specific method rejection',
			})

			yield* Ref.set(response, { status: 403, message: 'Required status check "build" is failing' })
			const forbidden = yield* merge
			expect(forbidden).toMatchObject({
				reason: 'forbidden',
				status: 403,
				message: 'Required status check "build" is failing',
			})

			yield* Ref.set(response, { status: 422, message: 'Protected branch policy rejected the merge' })
			const validation = yield* merge
			expect(validation).toMatchObject({
				reason: 'validation',
				status: 422,
				message: 'Protected branch policy rejected the merge',
			})
		}),
	)

	it.effect('classifies common HTTP failures with status, message, and retryability', ({ expect }) =>
		Effect.gen(function* () {
			type ErrorResponse = { readonly status: number; readonly message: string }
			const response = yield* Ref.make<ErrorResponse>({
				status: 403,
				message: 'Resource not accessible by integration',
			})
			const httpClient = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					if (new URL(web.url).pathname.startsWith('/app/installations/')) {
						return HttpClientResponse.fromWeb(request, tokenResponse())
					}
					const current = yield* Ref.get(response)
					return HttpClientResponse.fromWeb(
						request,
						Response.json({ message: current.message }, { status: current.status }),
					)
				}),
			)
			const layer = makeLayer(httpClient)
			const fetchFailure = Effect.flatMap(GitHubApi, (api) => api.fetchIssue({ issue })).pipe(
				Effect.provide(layer),
				Effect.flip,
			)

			for (const scenario of [
				{
					status: 403,
					message: 'Resource not accessible by integration',
					reason: 'forbidden',
					retryable: false,
				},
				{ status: 404, message: 'Not Found', reason: 'not_found', retryable: false },
				{ status: 422, message: 'Validation Failed', reason: 'validation', retryable: false },
				{ status: 500, message: 'Server Error', reason: 'unavailable', retryable: true },
				{ status: 429, message: 'API rate limit exceeded', reason: 'rate_limited', retryable: true },
			]) {
				yield* Ref.set(response, { status: scenario.status, message: scenario.message })
				const error = yield* fetchFailure
				expect(error).toMatchObject({
					operation: 'fetch_issue',
					reason: scenario.reason,
					retryable: scenario.retryable,
					status: scenario.status,
					message: scenario.message,
				})
			}
		}),
	)
})
