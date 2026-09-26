import { createHmac } from 'node:crypto'
import * as NodeHttp from 'node:http'

import { NodeCrypto, NodeHttpServer } from '@effect/platform-node'
import { createServer } from '@emulators/core'
import { getGitHubStore, githubPlugin, seedFromConfig, type GitHubSeedConfig } from '@emulators/github'
import {
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	processProviderEvent,
	type ProviderEventProcessor,
	webhookRoutes,
	type RawWebhookInput,
} from '@humanlayer/channels-delivery-next'
import { Context, Effect, Layer, Match, Predicate, Queue, Redacted, Schema } from 'effect'
import {
	FetchHttpClient,
	Headers,
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
	HttpEffect,
	type HttpMethod,
	HttpRouter,
	HttpServer,
} from 'effect/unstable/http'

import { AddIssueLabelsBody } from '../src/api/AddIssueLabels'
import { CloseIssueBody } from '../src/api/CloseIssue'
import { ClosePullRequestBody } from '../src/api/ClosePullRequest'
import { Issue, IssueComment, PullRequest, Review, ReviewComment } from '../src/api/GitHubApiSchemas'
import { PostIssueCommentBody } from '../src/api/PostIssueComment'
import { PostPullRequestReviewCommentBody } from '../src/api/PostPullRequestReviewComment'
import { ReopenIssueBody } from '../src/api/ReopenIssue'
import { ReopenPullRequestBody } from '../src/api/ReopenPullRequest'
import { UpdateCommentBody } from '../src/api/UpdateComment'
import { GitHubId } from '../src/GitHubIdentity'
import { makeGitHubWebhookProvider } from '../src/GitHubWebhookProvider'

export const githubWebhookSecret = 'github-next-test-secret'
export const githubEmulatorAliceToken = 'github-next-alice-token'
export const githubEmulatorReviewerToken = 'github-next-reviewer-token'

const inMemoryMailboxKey = (admission: DeliveryAdmission) =>
	[admission.namespace, admission.provider, admission.installationId, admission.resourceId]
		.map((segment) => `${segment.length}:${segment}`)
		.join('|')

/** Test-only keyed mailbox that stores admissions until the test explicitly processes one. */
export const makeInMemoryMailboxFixture = <R>(processors: ReadonlyArray<ProviderEventProcessor<R>>) =>
	Effect.gen(function* () {
		const mailboxes = new Map<string, Queue.Queue<DeliveryAdmission>>()
		const createdMailboxKeys = yield* Queue.unbounded<string>()

		const mailboxDelivery: typeof MailboxDelivery.Service = {
			deliver: (admission) =>
				Effect.gen(function* () {
					const key = inMemoryMailboxKey(admission)
					let mailbox = mailboxes.get(key)
					if (mailbox === undefined) {
						mailbox = yield* Queue.unbounded<DeliveryAdmission>()
						mailboxes.set(key, mailbox)
						yield* Queue.offer(createdMailboxKeys, key)
					}
					yield* Queue.offer(mailbox, admission)
					return DeliveryReceipt.make({ mailboxKey: key, accepted: true })
				}),
		}

		return {
			mailboxDelivery,
			awaitMailboxKey: Queue.take(createdMailboxKeys),
			processNext: (mailboxKey: string) => {
				const mailbox = mailboxes.get(mailboxKey)
				return mailbox === undefined
					? Effect.die(new Error(`In-memory mailbox not found: ${mailboxKey}`))
					: Queue.take(mailbox).pipe(
							Effect.flatMap((admission) => processProviderEvent(processors)([admission])),
						)
			},
		}
	})

const githubUser = { id: 400, login: 'alice', type: 'User' }

export const issuePayload = (action: string) => ({
	action,
	installation: { id: 100 },
	repository: { id: 200, name: 'project', owner: { login: 'alice' } },
	issue: {
		id: 300,
		number: 42,
		title: 'Test issue',
		body: 'Issue body',
		state: 'open',
		html_url: 'https://github.com/alice/project/issues/42',
		user: githubUser,
	},
	sender: { id: 401, login: 'bob', type: 'User' },
})

export const issueCommentPayload = (input?: {
	readonly action?: string
	readonly issueNumber?: number
	readonly pullRequest?: boolean
}) => {
	const issue = {
		id: 300,
		number: input?.issueNumber ?? 42,
		title: 'Test discussion',
		body: 'Issue body',
		state: 'open',
		html_url: 'https://github.com/alice/project/issues/42',
		user: githubUser,
	}
	return {
		action: input?.action ?? 'created',
		installation: { id: 100 },
		repository: { id: 200, name: 'project', owner: { login: 'alice' } },
		issue:
			input?.pullRequest === true
				? { ...issue, pull_request: { url: 'https://api.github.com/repos/alice/project/pulls/42' } }
				: issue,
		comment: {
			id: 500,
			body: '@agent please review',
			html_url: 'https://github.com/alice/project/issues/42#issuecomment-500',
			user: { id: 401, login: 'bob', type: 'User' },
		},
		sender: { id: 401, login: 'bob', type: 'User' },
	}
}

export const pullRequestPayload = (action = 'opened') => ({
	action,
	installation: { id: 100 },
	repository: { id: 200, name: 'project', owner: { login: 'alice' } },
	pull_request: {
		id: 301,
		number: 42,
		title: 'Test pull request',
		body: 'Pull request body',
		state: 'open',
		html_url: 'https://github.com/alice/project/pull/42',
		user: githubUser,
		draft: false,
		head: { ref: 'feature', sha: 'abc123' },
		base: { ref: 'main', sha: 'def456' },
	},
	sender: githubUser,
})

export const pullRequestReviewPayload = (action = 'submitted') => ({
	...pullRequestPayload(),
	action,
	review: {
		id: 600,
		node_id: 'PRR_600',
		body: 'Looks good',
		user: githubUser,
		state: 'approved',
		commit_id: 'abc123',
		html_url: 'https://github.com/alice/project/pull/42#pullrequestreview-600',
	},
})

export const pullRequestReviewCommentPayload = (action = 'created') => ({
	...pullRequestPayload(),
	action,
	comment: {
		id: 700,
		node_id: 'PRRC_700',
		body: 'Change this',
		html_url: 'https://github.com/alice/project/pull/42#discussion_r700',
		user: githubUser,
		pull_request_review_id: 600,
		path: 'src/index.ts',
		commit_id: 'abc123',
		original_commit_id: 'abc123',
		diff_hunk: '@@ -1 +1 @@',
		pull_request_url: 'https://api.github.com/repos/alice/project/pulls/42',
		line: 1,
		side: 'RIGHT',
	},
})

export const pullRequestReviewThreadPayload = (action = 'resolved') => ({
	...pullRequestPayload(),
	action,
	thread: { node_id: 'PRRT_800', comments: [pullRequestReviewCommentPayload().comment] },
})

export const checkRunPayload = (pullRequestNumbers: ReadonlyArray<number> = [42]) => ({
	action: 'completed',
	installation: { id: 100 },
	repository: { id: 200, name: 'project', owner: { login: 'alice' } },
	sender: githubUser,
	check_run: {
		id: 900,
		name: 'build',
		status: 'completed',
		conclusion: 'success',
		details_url: 'https://github.com/alice/project/actions/runs/900',
		head_sha: 'abc123',
		check_suite: { id: 901 },
		started_at: '2025-01-01T00:00:00Z',
		completed_at: '2025-01-01T00:01:00Z',
		pull_requests: pullRequestNumbers.map((number) => ({ number })),
	},
})

export const pullRequestIssueCommentPayload = () => {
	const payload = issueCommentPayload({ pullRequest: true })
	return {
		...payload,
		issue: {
			...payload.issue,
			title: 'PR',
			body: 'body',
			html_url: 'https://github.com/alice/project/pull/42',
		},
		comment: {
			id: 500,
			body: 'comment',
			html_url: 'https://github.com/alice/project/pull/42#issuecomment-500',
			user: githubUser,
		},
		sender: githubUser,
	}
}

export const makeGitHubTestProvider = (namespace: string) => {
	const provider = makeGitHubWebhookProvider({ namespace, webhookSecret: Redacted.make(githubWebhookSecret) })
	return {
		handle: (input: RawWebhookInput) => provider.handle(input).pipe(Effect.provide(NodeCrypto.layer)),
	}
}

/** One GitHub REST endpoint the emulator tests call, with the Schemas of its request body and, when read, its response. */
export interface GitHubEmulatorEndpoint<
	Params,
	Body extends Schema.Codec<unknown, unknown>,
	Response extends Schema.Codec<unknown, unknown> | undefined = undefined,
> {
	readonly method: HttpMethod.HttpMethod
	readonly path: (params: Params) => string
	readonly body: Body
	readonly response: Response
}

const endpoint = <
	Params,
	Body extends Schema.Codec<unknown, unknown>,
	Response extends Schema.Codec<unknown, unknown> | undefined = undefined,
>(
	definition: GitHubEmulatorEndpoint<Params, Body, Response>,
) => definition

const repositoryPath = '/repos/alice/project'
type IssuePath = { readonly issue: GitHubId }
type PullRequestPath = { readonly pullRequest: GitHubId }
const issuePath = ({ issue }: IssuePath) => `${repositoryPath}/issues/${issue}`
const pullRequestPath = ({ pullRequest }: PullRequestPath) => `${repositoryPath}/pulls/${pullRequest}`
const Assignees = Schema.Struct({ assignees: Schema.Array(Schema.NonEmptyString) })
const IssueTitle = Schema.Struct({ title: Schema.String })

/** The GitHub REST endpoints the emulator tests call. */
export const githubEmulatorEndpoints = {
	createIssue: endpoint({
		method: 'POST',
		path: () => `${repositoryPath}/issues`,
		body: Schema.Struct({ title: Schema.String, body: Schema.String }),
		response: Issue,
	}),
	editIssueTitle: endpoint({ method: 'PATCH', path: issuePath, body: IssueTitle, response: undefined }),
	closeIssue: endpoint({ method: 'PATCH', path: issuePath, body: CloseIssueBody, response: undefined }),
	reopenIssue: endpoint({ method: 'PATCH', path: issuePath, body: ReopenIssueBody, response: undefined }),
	addAssignees: endpoint({
		method: 'POST',
		path: (params: IssuePath) => `${issuePath(params)}/assignees`,
		body: Assignees,
		response: undefined,
	}),
	removeAssignees: endpoint({
		method: 'DELETE',
		path: (params: IssuePath) => `${issuePath(params)}/assignees`,
		body: Assignees,
		response: undefined,
	}),
	createLabel: endpoint({
		method: 'POST',
		path: () => `${repositoryPath}/labels`,
		body: Schema.Struct({ name: Schema.NonEmptyString, color: Schema.NonEmptyString }),
		response: undefined,
	}),
	addIssueLabels: endpoint({
		method: 'POST',
		path: (params: IssuePath) => `${issuePath(params)}/labels`,
		body: AddIssueLabelsBody,
		response: undefined,
	}),
	removeIssueLabel: endpoint({
		method: 'DELETE',
		path: (params: IssuePath & { readonly label: string }) =>
			`${issuePath(params)}/labels/${encodeURIComponent(params.label)}`,
		body: Schema.Undefined,
		response: undefined,
	}),
	postIssueComment: endpoint({
		method: 'POST',
		path: (params: IssuePath) => `${issuePath(params)}/comments`,
		body: PostIssueCommentBody,
		response: IssueComment,
	}),
	updateIssueComment: endpoint({
		method: 'PATCH',
		path: ({ comment }: { readonly comment: GitHubId }) => `${repositoryPath}/issues/comments/${comment}`,
		body: UpdateCommentBody,
		response: undefined,
	}),
	deleteIssueComment: endpoint({
		method: 'DELETE',
		path: ({ comment }: { readonly comment: GitHubId }) => `${repositoryPath}/issues/comments/${comment}`,
		body: Schema.Undefined,
		response: undefined,
	}),
	createPullRequest: endpoint({
		method: 'POST',
		path: () => `${repositoryPath}/pulls`,
		body: Schema.Struct({
			title: Schema.String,
			body: Schema.String,
			head: Schema.NonEmptyString,
			base: Schema.NonEmptyString,
		}),
		response: PullRequest,
	}),
	editPullRequestTitle: endpoint({ method: 'PATCH', path: pullRequestPath, body: IssueTitle, response: undefined }),
	closePullRequest: endpoint({
		method: 'PATCH',
		path: pullRequestPath,
		body: ClosePullRequestBody,
		response: undefined,
	}),
	reopenPullRequest: endpoint({
		method: 'PATCH',
		path: pullRequestPath,
		body: ReopenPullRequestBody,
		response: undefined,
	}),
	requestReviewers: endpoint({
		method: 'POST',
		path: (params: PullRequestPath) => `${pullRequestPath(params)}/requested_reviewers`,
		body: Schema.Struct({ reviewers: Schema.Array(Schema.NonEmptyString) }),
		response: undefined,
	}),
	createReview: endpoint({
		method: 'POST',
		path: (params: PullRequestPath) => `${pullRequestPath(params)}/reviews`,
		body: Schema.Struct({ body: Schema.String, event: Schema.Literals(['APPROVE', 'REQUEST_CHANGES', 'COMMENT']) }),
		response: Review,
	}),
	dismissReview: endpoint({
		method: 'PUT',
		path: (params: PullRequestPath & { readonly review: GitHubId }) =>
			`${pullRequestPath(params)}/reviews/${params.review}/dismissals`,
		body: Schema.Struct({ message: Schema.String }),
		response: undefined,
	}),
	postReviewComment: endpoint({
		method: 'POST',
		path: (params: PullRequestPath) => `${pullRequestPath(params)}/comments`,
		body: PostPullRequestReviewCommentBody,
		response: ReviewComment,
	}),
	updateReviewComment: endpoint({
		method: 'PATCH',
		path: ({ comment }: { readonly comment: GitHubId }) => `${repositoryPath}/pulls/comments/${comment}`,
		body: UpdateCommentBody,
		response: undefined,
	}),
	deleteReviewComment: endpoint({
		method: 'DELETE',
		path: ({ comment }: { readonly comment: GitHubId }) => `${repositoryPath}/pulls/comments/${comment}`,
		body: Schema.Undefined,
		response: undefined,
	}),
}

/** What the emulator tests send to an endpoint. */
export type GitHubEmulatorCall<Params, Body extends Schema.Codec<unknown, unknown>> = {
	readonly params: Params
	readonly token: string
	readonly body: Body['Type']
}

export class GitHubEmulatorSeedError extends Schema.TaggedError<GitHubEmulatorSeedError>()(
	'GitHubEmulatorSeedError',
	{},
) {}

const serveOnLoopback = <E, R>(app: Layer.Layer<never, E, R>) =>
	Effect.gen(function* () {
		const context = yield* Layer.build(
			app.pipe(Layer.provideMerge(NodeHttpServer.layer(NodeHttp.createServer, { port: 0, host: '127.0.0.1' }))),
		)
		const address = Context.get(context, HttpServer.HttpServer).address
		if (!Predicate.isTagged(address, 'TcpAddress')) return yield* Effect.die('Expected a TCP address')
		return `http://127.0.0.1:${address.port}`
	})

export type GitHubEmulatorFixtureOptions = {
	readonly mailboxDelivery: typeof MailboxDelivery.Service
}

export const makeGitHubEmulatorFixture = (options: GitHubEmulatorFixtureOptions) =>
	Effect.gen(function* () {
		const provider = makeGitHubWebhookProvider({
			namespace: 'github-emulator-test',
			webhookSecret: Redacted.make(githubWebhookSecret),
		})
		const callbackUrl = yield* serveOnLoopback(
			HttpRouter.serve(
				webhookRoutes([provider]).pipe(
					HttpRouter.provideRequest(
						Layer.merge(NodeCrypto.layer, Layer.succeed(MailboxDelivery, options.mailboxDelivery)),
					),
				),
				{ disableLogger: true, disableListenLog: true },
			),
		)

		const emulator = createServer(githubPlugin, { baseUrl: 'http://127.0.0.1' })
		githubPlugin.seed?.(emulator.store, emulator.baseUrl)
		const seed = {
			users: [{ login: 'alice' }, { login: 'reviewer' }],
			tokens: {
				[githubEmulatorAliceToken]: { login: 'alice' },
				[githubEmulatorReviewerToken]: { login: 'reviewer' },
			},
			repos: [{ owner: 'alice', name: 'project', auto_init: true }],
			apps: [
				{
					app_id: 42,
					slug: 'github-next',
					name: 'GitHub Next Test App',
					private_key: 'github-next-test-private-key',
					permissions: { issues: 'write', pull_requests: 'write' },
					events: [
						'issues',
						'issue_comment',
						'pull_request',
						'pull_request_review',
						'pull_request_review_comment',
					],
					webhook_url: `${callbackUrl}/integrations/github/webhook`,
					webhook_secret: githubWebhookSecret,
					installations: [
						{
							installation_id: 100,
							account: 'alice',
							repositories: ['project'],
							repository_selection: 'selected',
						},
					],
				},
			],
		} satisfies GitHubSeedConfig
		seedFromConfig(emulator.store, emulator.baseUrl, seed)

		const emulatorUrl = yield* serveOnLoopback(HttpServer.serve(HttpEffect.fromWebHandler(emulator.app.fetch)))

		const store = getGitHubStore(emulator.store)
		const alice = store.users.findOneBy('login', 'alice')
		const reviewer = store.users.findOneBy('login', 'reviewer')
		const repository = store.repos.findOneBy('name', 'project')
		const installation = store.appInstallations.findOneBy('installation_id', 100)
		if (alice === undefined || reviewer === undefined || repository === undefined || installation === undefined) {
			return yield* GitHubEmulatorSeedError.make({})
		}
		emulator.tokenMap.set(githubEmulatorAliceToken, { login: alice.login, id: alice.id, scopes: [] })
		emulator.tokenMap.set(githubEmulatorReviewerToken, { login: reviewer.login, id: reviewer.id, scopes: [] })

		const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk)
		const execute = Effect.fn('github.emulator.execute')(function* <
			Params,
			Body extends Schema.Codec<unknown, unknown>,
			Response extends Schema.Codec<unknown, unknown> | undefined,
		>(endpoint: GitHubEmulatorEndpoint<Params, Body, Response>, call: GitHubEmulatorCall<Params, Body>) {
			const unsent = HttpClientRequest.make(endpoint.method)(`${emulatorUrl}${endpoint.path(call.params)}`).pipe(
				HttpClientRequest.bearerToken(call.token),
			)
			return yield* client.execute(
				Predicate.isUndefined(call.body)
					? unsent
					: yield* HttpClientRequest.schemaBodyJson(endpoint.body)(unsent, call.body),
			)
		})
		/** Calls an endpoint whose response the test does not read. */
		const send = Effect.fn('github.emulator.send')(function* <Params, Body extends Schema.Codec<unknown, unknown>>(
			endpoint: GitHubEmulatorEndpoint<Params, Body>,
			call: GitHubEmulatorCall<Params, Body>,
		) {
			yield* execute(endpoint, call)
		})
		/** Calls an endpoint and decodes its response with the endpoint's response Schema. */
		const request = Effect.fn('github.emulator.request')(function* <
			Params,
			Body extends Schema.Codec<unknown, unknown>,
			Response extends Schema.Codec<unknown, unknown>,
		>(endpoint: GitHubEmulatorEndpoint<Params, Body, Response>, call: GitHubEmulatorCall<Params, Body>) {
			const response = yield* execute(endpoint, call)
			return yield* HttpClientResponse.schemaBodyJson(endpoint.response)(response)
		})

		return {
			send,
			request,
			aliceToken: githubEmulatorAliceToken,
			reviewerToken: githubEmulatorReviewerToken,
			aliceUserId: alice.id,
			reviewerUserId: reviewer.id,
			repositoryId: repository.id,
			installationId: installation.installation_id,
		}
	}).pipe(Effect.provide(FetchHttpClient.layer))

export const signedGitHubInput = (event: string, payload: Schema.Json, deliveryId = 'delivery-1'): RawWebhookInput =>
	signedGitHubBody(event, new TextEncoder().encode(JSON.stringify(payload)), deliveryId)

export const signedGitHubBody = (event: string, body: Uint8Array, deliveryId = 'delivery-1'): RawWebhookInput => {
	const signature = createHmac('sha256', githubWebhookSecret).update(body).digest('hex')
	return {
		headers: Headers.fromInput({
			'x-github-delivery': deliveryId,
			'x-github-event': event,
			'x-hub-signature-256': `sha256=${signature}`,
		}),
		body,
	}
}

export const admitStoredGitHubWebhook = (namespace: string, event: string, payload: Schema.Json) =>
	Effect.gen(function* () {
		const outcome = yield* makeGitHubTestProvider(namespace).handle(signedGitHubInput(event, payload))
		const admission = yield* Match.value(outcome).pipe(
			Match.tagsExhaustive({
				Event: ({ event }) => Effect.succeed(event),
				Events: () =>
					Effect.die(new Error(`Expected GitHub ${event} webhook to produce exactly one admission`)),
				Ignored: () =>
					Effect.die(new Error(`Expected GitHub ${event} webhook to be admitted, but it was ignored`)),
				Response: () =>
					Effect.die(new Error(`Expected GitHub ${event} webhook to be admitted, but received a response`)),
			}),
		)
		const codec = Schema.fromJsonString(DeliveryAdmission)
		const encoded = yield* Schema.encodeEffect(codec)(admission)
		return yield* Schema.decodeEffect(codec)(encoded)
	})
