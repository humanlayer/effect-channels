import {
	Cache,
	Clock,
	Config,
	Context,
	Duration,
	Effect,
	Encoding,
	Exit,
	Layer,
	Match,
	Option,
	Predicate,
	Redacted,
	Schema,
	Stream,
} from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'
import type * as HttpClientResponse from 'effect/unstable/http/HttpClientResponse'

import {
	GitHubApi,
	GitHubApiError,
	type GitHubApiOperation,
	type GitHubDeleteComment,
	type GitHubPostPullRequestReviewComment,
	type GitHubReactionRequest,
	type GitHubUpdateComment,
} from './GitHubApi'
import { GitHubId } from './GitHubIdentity'
import {
	GitHubActionsJobInfo,
	type GitHubActionsJobRef,
	GitHubCheckAnnotation,
	GitHubCheckConclusion,
	GitHubCheckRunInfo,
	type GitHubCheckRunRef,
	GitHubCheckStatus,
	type GitHubCommentRef,
	GitHubCommit,
	type GitHubIssueCommentRef,
	GitHubIssueInfo,
	type GitHubIssueRef,
	GitHubLabel,
	GitHubMergeResult,
	GitHubParticipant,
	GitHubPullRequestFile,
	GitHubPullRequestInfo,
	type GitHubPullRequestRef,
	type GitHubRepositoryRef,
	GitHubReview,
	GitHubReviewCommentRef,
	type GitHubReviewState,
} from './GitHubModels'
import {
	GitHubActionsJob,
	GitHubCheckRun,
	GitHubIssueComment,
	type GitHubIssueComments,
	GitHubReviewComment,
	type GitHubReviewComments,
	type GitHubReviews,
} from './GitHubResources'

const GitHubApiConfig = Config.all({
	appId: Config.schema(GitHubId, 'GITHUB_APP_ID'),
	privateKey: Config.redacted('GITHUB_PRIVATE_KEY'),
	apiOrigin: Config.url('GITHUB_API_ORIGIN').pipe(Config.withDefault(new URL('https://api.github.com/'))),
	botUserId: Config.option(Config.schema(GitHubId, 'GITHUB_BOT_USER_ID')),
})

const GitHubInstallationTokenResponse = Schema.Struct({
	token: Schema.NonEmptyString,
	expires_at: Schema.String,
})

const GitHubAppResponse = Schema.Struct({
	slug: Schema.NonEmptyString,
})

const GitHubApiParticipant = Schema.Struct({
	id: GitHubId,
	login: Schema.NonEmptyString,
	type: Schema.String,
})

const GitHubApiIssue = Schema.Struct({
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: GitHubApiParticipant,
})

const GitHubApiPullRequest = Schema.Struct({
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
	draft: Schema.Boolean,
	merged: Schema.Boolean,
	head: Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString }),
	base: Schema.Struct({ ref: Schema.String, sha: Schema.NonEmptyString }),
})

const GitHubApiIssueComment = Schema.Struct({
	id: GitHubId,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
})

const GitHubApiReview = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.NullOr(Schema.String),
	user: Schema.NullOr(GitHubApiParticipant),
	state: Schema.String,
	commit_id: Schema.String,
	html_url: Schema.String,
})

const GitHubApiReviewComment = Schema.Struct({
	id: GitHubId,
	node_id: Schema.NonEmptyString,
	body: Schema.String,
	html_url: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
	pull_request_review_id: Schema.NullOr(GitHubId),
	path: Schema.String,
	commit_id: Schema.String,
	original_commit_id: Schema.String,
	diff_hunk: Schema.String,
	in_reply_to_id: Schema.optionalKey(Schema.NullOr(GitHubId)),
	line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	start_line: Schema.optionalKey(Schema.NullOr(Schema.Number)),
	side: Schema.optionalKey(Schema.Literals(['LEFT', 'RIGHT'])),
})

const GitHubApiReaction = Schema.Struct({
	id: GitHubId,
	content: Schema.String,
	user: Schema.NullOr(GitHubApiParticipant),
})

const GitHubApiErrorBody = Schema.Struct({
	message: Schema.String,
})

const GitHubApiNonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const GitHubApiPositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

const GitHubApiCommitParticipant = Schema.NullOr(
	Schema.Union([GitHubApiParticipant, Schema.Record(Schema.String, Schema.Never)]),
)

const GitHubApiPullRequestFile = Schema.Struct({
	sha: Schema.NullOr(Schema.NonEmptyString),
	filename: Schema.NonEmptyString,
	previous_filename: Schema.optionalKey(Schema.NonEmptyString),
	status: Schema.Literals(['added', 'removed', 'modified', 'renamed', 'copied', 'changed', 'unchanged']),
	additions: GitHubApiNonNegativeInt,
	deletions: GitHubApiNonNegativeInt,
	changes: GitHubApiNonNegativeInt,
	blob_url: Schema.NullOr(Schema.String),
	raw_url: Schema.NullOr(Schema.String),
	contents_url: Schema.String,
	patch: Schema.optionalKey(Schema.String),
})

const GitHubApiCommit = Schema.Struct({
	sha: Schema.NonEmptyString,
	commit: Schema.Struct({ message: Schema.String }),
	url: Schema.String,
	html_url: Schema.String,
	author: GitHubApiCommitParticipant,
	committer: GitHubApiCommitParticipant,
})

const GitHubApiLabel = Schema.Struct({
	id: Schema.optionalKey(GitHubId),
	name: Schema.NonEmptyString,
	color: Schema.String,
	description: Schema.NullOr(Schema.String),
})

const GitHubApiMergeResult = Schema.Struct({
	sha: Schema.String,
	merged: Schema.Boolean,
	message: Schema.String,
})

const GitHubApiCheckRun = Schema.Struct({
	id: GitHubId,
	name: Schema.String,
	head_sha: Schema.NonEmptyString,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	started_at: Schema.NullOr(Schema.String),
	completed_at: Schema.NullOr(Schema.String),
	url: Schema.String,
	html_url: Schema.NullOr(Schema.String),
	details_url: Schema.NullOr(Schema.String),
	check_suite: Schema.NullOr(Schema.Struct({ id: GitHubId })),
	output: Schema.Struct({
		title: Schema.NullOr(Schema.String),
		summary: Schema.NullOr(Schema.String),
		text: Schema.NullOr(Schema.String),
		annotations_count: GitHubApiNonNegativeInt,
	}),
})

const GitHubApiCheckRunsPage = Schema.Struct({
	check_runs: Schema.Array(GitHubApiCheckRun),
})

const GitHubApiCheckAnnotation = Schema.Struct({
	path: Schema.NonEmptyString,
	start_line: GitHubApiPositiveInt,
	end_line: GitHubApiPositiveInt,
	start_column: Schema.NullOr(GitHubApiPositiveInt),
	end_column: Schema.NullOr(GitHubApiPositiveInt),
	annotation_level: Schema.NullOr(Schema.Literals(['notice', 'warning', 'failure'])),
	title: Schema.NullOr(Schema.String),
	message: Schema.NullOr(Schema.String),
	raw_details: Schema.NullOr(Schema.String),
	blob_href: Schema.String,
})

const GitHubApiActionsJobStep = Schema.Struct({
	name: Schema.String,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	number: GitHubApiPositiveInt,
	started_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
	completed_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
})

const GitHubApiActionsJob = Schema.Struct({
	id: GitHubId,
	run_id: GitHubId,
	name: Schema.String,
	status: GitHubCheckStatus,
	conclusion: Schema.NullOr(GitHubCheckConclusion),
	head_sha: Schema.NonEmptyString,
	url: Schema.String,
	html_url: Schema.NullOr(Schema.String),
	started_at: Schema.NullOr(Schema.String),
	completed_at: Schema.NullOr(Schema.String),
	check_run_url: Schema.String,
	workflow_name: Schema.optionalKey(Schema.NullOr(Schema.String)),
	head_branch: Schema.optionalKey(Schema.NullOr(Schema.String)),
	steps: Schema.optionalKey(Schema.Array(GitHubApiActionsJobStep)),
})

const GitHubApiActionsJobsPage = Schema.Struct({
	jobs: Schema.Array(GitHubApiActionsJob),
})

const GitHubApiWorkflowRun = Schema.Struct({
	id: GitHubId,
})

const GitHubApiWorkflowRunsPage = Schema.Struct({
	workflow_runs: Schema.Array(GitHubApiWorkflowRun),
})

class GitHubTransportError extends Schema.TaggedError<GitHubTransportError>()('GitHubTransportError', {
	stage: Schema.Literals(['transport', 'status', 'decode', 'signing', 'token_expiry', 'pagination', 'redirect']),
	status: Schema.optionalKey(Schema.Int),
	message: Schema.optionalKey(Schema.String),
	rateLimited: Schema.optionalKey(Schema.Boolean),
	retryAfterMs: Schema.optionalKey(Schema.Int),
}) {}

class GitHubSigningError extends Schema.TaggedError<GitHubSigningError>()('GitHubSigningError', {}) {}

/** Package-private signing seam. Production uses Web Crypto; tests replace it with a Layer. */
export class GitHubAppSigner extends Context.Service<
	GitHubAppSigner,
	{
		readonly sign: (input: {
			readonly privateKey: Redacted.Redacted<string>
			readonly data: string
		}) => Effect.Effect<string, GitHubSigningError>
	}
>()('@humanlayer/channels-github-next/internal/GitHubAppSigner') {
	static readonly layerWebCrypto = Layer.sync(GitHubAppSigner, () => {
		const encoder = new TextEncoder()
		return GitHubAppSigner.of({
			sign: ({ privateKey, data }) =>
				Effect.tryPromise({
					try: async () => {
						const bytes = privateKeyBytes(Redacted.value(privateKey))
						const key = await globalThis.crypto.subtle.importKey(
							'pkcs8',
							bytes,
							{ name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
							false,
							['sign'],
						)
						const signature = await globalThis.crypto.subtle.sign(
							'RSASSA-PKCS1-v1_5',
							key,
							encoder.encode(data),
						)
						return Encoding.encodeBase64Url(new Uint8Array(signature))
					},
					catch: () => GitHubSigningError.make({}),
				}),
		})
	})
}

const pemWhitespace = /\s/g
const der = (tag: number, bytes: Uint8Array): Uint8Array<ArrayBuffer> => {
	const length =
		bytes.length < 128
			? [bytes.length]
			: bytes.length < 256
				? [0x81, bytes.length]
				: [0x82, bytes.length >> 8, bytes.length & 255]
	return new Uint8Array([tag, ...length, ...bytes])
}

const privateKeyBytes = (pem: string) => {
	const pkcs1 = pem.startsWith('-----BEGIN RSA PRIVATE KEY-----')
	const label = pkcs1 ? 'RSA PRIVATE KEY' : 'PRIVATE KEY'
	if (!pem.startsWith(`-----BEGIN ${label}-----`) || !pem.trimEnd().endsWith(`-----END ${label}-----`)) {
		throw new Error('Invalid PEM')
	}
	const bytes = Uint8Array.from(
		atob(
			pem
				.replace(`-----BEGIN ${label}-----`, '')
				.replace(`-----END ${label}-----`, '')
				.replace(pemWhitespace, ''),
		),
		(character) => character.charCodeAt(0),
	)
	if (!pkcs1) return bytes
	return der(
		0x30,
		new Uint8Array([
			0x02,
			0x01,
			0x00,
			0x30,
			0x0d,
			0x06,
			0x09,
			0x2a,
			0x86,
			0x48,
			0x86,
			0xf7,
			0x0d,
			0x01,
			0x01,
			0x01,
			0x05,
			0x00,
			...der(0x04, bytes),
		]),
	)
}

const repositoryPath = (ref: GitHubRepositoryRef) =>
	`/repos/${encodeURIComponent(ref.owner)}/${encodeURIComponent(ref.repository)}`

const issueCommentRef = (discussion: GitHubIssueCommentRef['discussion'], id: GitHubId): GitHubIssueCommentRef => ({
	discussion,
	id,
})

const participant = (value: typeof GitHubApiParticipant.Type) => GitHubParticipant.make(value)

const issueInfo = (ref: GitHubIssueRef, value: typeof GitHubApiIssue.Type) =>
	GitHubIssueInfo.make({
		ref,
		title: value.title,
		body: value.body,
		state: value.state,
		url: value.html_url,
		author: participant(value.user),
	})

const pullRequestInfo = (ref: GitHubPullRequestRef, value: typeof GitHubApiPullRequest.Type) =>
	GitHubPullRequestInfo.make({
		ref,
		title: value.title,
		body: value.body,
		state: value.state,
		url: value.html_url,
		author: value.user === null ? null : participant(value.user),
		draft: value.draft,
		merged: value.merged,
		headRef: value.head.ref,
		headSha: value.head.sha,
		baseRef: value.base.ref,
		baseSha: value.base.sha,
	})

const reviewState = (state: string): GitHubReviewState =>
	Match.value(state.toLowerCase()).pipe(
		Match.when('approved', () => 'approved' as const),
		Match.when('changes_requested', () => 'changes_requested' as const),
		Match.when('commented', () => 'commented' as const),
		Match.when('dismissed', () => 'dismissed' as const),
		Match.orElse(() => 'pending' as const),
	)

const issueComment = (discussion: GitHubIssueCommentRef['discussion'], value: typeof GitHubApiIssueComment.Type) =>
	GitHubIssueComment.make({
		ref: issueCommentRef(discussion, value.id),
		body: value.body,
		url: value.html_url,
		author: value.user === null ? null : participant(value.user),
	})

const reviewComment = (pullRequest: GitHubPullRequestRef, value: typeof GitHubApiReviewComment.Type) =>
	GitHubReviewComment.make({
		ref: { pullRequest, id: value.id },
		nodeId: value.node_id,
		body: value.body,
		url: value.html_url,
		author: value.user === null ? null : participant(value.user),
		reviewId: value.pull_request_review_id,
		path: value.path,
		commitId: value.commit_id,
		originalCommitId: value.original_commit_id,
		diffHunk: value.diff_hunk,
		...(Predicate.isUndefined(value.in_reply_to_id) ? {} : { inReplyToId: value.in_reply_to_id }),
		...(Predicate.isUndefined(value.line) ? {} : { line: value.line }),
		...(Predicate.isUndefined(value.start_line) ? {} : { startLine: value.start_line }),
		...(Predicate.isUndefined(value.side) ? {} : { side: value.side }),
	})

const pullRequestFileStatus = (status: (typeof GitHubApiPullRequestFile.Type)['status']) =>
	Match.value(status).pipe(
		Match.when('added', () => 'added' as const),
		Match.when('removed', () => 'deleted' as const),
		Match.when('modified', () => 'modified' as const),
		Match.when('renamed', () => 'renamed' as const),
		Match.when('copied', () => 'copied' as const),
		Match.when('changed', () => 'changed' as const),
		Match.when('unchanged', () => 'unchanged' as const),
		Match.exhaustive,
	)

const pullRequestFile = (value: typeof GitHubApiPullRequestFile.Type) =>
	GitHubPullRequestFile.make({
		sha: value.sha,
		filename: value.filename,
		...(Predicate.isUndefined(value.previous_filename) ? {} : { previousFilename: value.previous_filename }),
		status: pullRequestFileStatus(value.status),
		additions: value.additions,
		deletions: value.deletions,
		changes: value.changes,
		blobUrl: value.blob_url,
		rawUrl: value.raw_url,
		contentsUrl: value.contents_url,
		...(Predicate.isUndefined(value.patch) ? {} : { patch: value.patch }),
	})

const commitParticipant = (value: typeof GitHubApiCommitParticipant.Type) =>
	value !== null && Schema.is(GitHubApiParticipant)(value) ? participant(value) : null

const commit = (value: typeof GitHubApiCommit.Type) =>
	GitHubCommit.make({
		sha: value.sha,
		message: value.commit.message,
		apiUrl: value.url,
		url: value.html_url,
		author: commitParticipant(value.author),
		committer: commitParticipant(value.committer),
	})

const label = (value: typeof GitHubApiLabel.Type) =>
	GitHubLabel.make({
		...(Predicate.isUndefined(value.id) ? {} : { id: value.id }),
		name: value.name,
		color: value.color,
		description: value.description,
	})

const checkRunInfo = (ref: GitHubCheckRunRef, value: typeof GitHubApiCheckRun.Type) =>
	GitHubCheckRunInfo.make({
		ref,
		name: value.name,
		headSha: value.head_sha,
		status: value.status,
		conclusion: value.conclusion,
		startedAt: value.started_at,
		completedAt: value.completed_at,
		apiUrl: value.url,
		url: value.html_url,
		detailsUrl: value.details_url,
		checkSuiteId: value.check_suite?.id ?? null,
		outputTitle: value.output.title,
		outputSummary: value.output.summary,
		outputText: value.output.text,
		annotationCount: value.output.annotations_count,
	})

const checkRun = (repository: GitHubRepositoryRef, value: typeof GitHubApiCheckRun.Type) =>
	GitHubCheckRun.make({
		ref: {
			installationId: repository.installationId,
			repositoryId: repository.repositoryId,
			owner: repository.owner,
			repository: repository.repository,
			id: value.id,
		},
	})

const checkAnnotation = (value: typeof GitHubApiCheckAnnotation.Type) =>
	GitHubCheckAnnotation.make({
		path: value.path,
		startLine: value.start_line,
		endLine: value.end_line,
		startColumn: value.start_column,
		endColumn: value.end_column,
		level: value.annotation_level,
		title: value.title,
		message: value.message,
		rawDetails: value.raw_details,
		blobUrl: value.blob_href,
	})

const actionsJobInfo = (ref: GitHubActionsJobRef, value: typeof GitHubApiActionsJob.Type) =>
	GitHubActionsJobInfo.make({
		ref,
		runId: value.run_id,
		name: value.name,
		status: value.status,
		conclusion: value.conclusion,
		headSha: value.head_sha,
		apiUrl: value.url,
		url: value.html_url,
		startedAt: value.started_at,
		completedAt: value.completed_at,
		checkRunUrl: value.check_run_url,
		...(Predicate.isUndefined(value.workflow_name) || value.workflow_name === null
			? {}
			: { workflowName: value.workflow_name }),
		...(Predicate.isUndefined(value.head_branch) ? {} : { headBranch: value.head_branch }),
		steps: (value.steps ?? []).map((step) => ({
			name: step.name,
			status: step.status,
			conclusion: step.conclusion,
			number: step.number,
			startedAt: step.started_at ?? null,
			completedAt: step.completed_at ?? null,
		})),
	})

const actionsJob = (repository: GitHubRepositoryRef, value: typeof GitHubApiActionsJob.Type) =>
	GitHubActionsJob.make({
		ref: {
			installationId: repository.installationId,
			repositoryId: repository.repositoryId,
			owner: repository.owner,
			repository: repository.repository,
			id: value.id,
		},
	})

const sameUrl = (left: string, right: string) => left.replace(/\/$/, '') === right.replace(/\/$/, '')

const reviewCommentLocationBody = (location: GitHubPostPullRequestReviewComment['location']) =>
	Match.value(location).pipe(
		Match.tagsExhaustive({
			Line: ({ line, side }) => ({ line, side }),
			Range: ({ startLine, startSide, line, side }) => ({
				start_line: startLine,
				start_side: startSide,
				line,
				side,
			}),
			File: () => ({ subject_type: 'file' as const }),
		}),
	)

const commentRepository = (comment: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(comment)
		? comment.pullRequest
		: Match.value(comment.discussion).pipe(
				Match.tagsExhaustive({ Issue: ({ ref }) => ref, PullRequest: ({ ref }) => ref }),
			)

const commentPath = (comment: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(comment)
		? `${repositoryPath(comment.pullRequest)}/pulls/comments/${comment.id}`
		: `${repositoryPath(commentRepository(comment))}/issues/comments/${comment.id}`

const secondsPattern = /^\d+$/
const secondsToMillis = (value: string | undefined) => {
	if (Predicate.isUndefined(value) || !secondsPattern.test(value)) return undefined
	const milliseconds = Number(value) * 1_000
	return Number.isSafeInteger(milliseconds) ? milliseconds : undefined
}

const epochSecondsToDelayMillis = (value: string | undefined, now: number) => {
	if (Predicate.isUndefined(value) || !secondsPattern.test(value)) return undefined
	const resetAt = Number(value) * 1_000
	if (!Number.isSafeInteger(resetAt)) return undefined
	const delay = resetAt - now
	if (!Number.isSafeInteger(delay)) return undefined
	return Math.max(0, delay)
}

const parseNextLink = (response: HttpClientResponse.HttpClientResponse, apiOrigin: URL) => {
	const link = response.headers.link
	if (Predicate.isUndefined(link)) return Effect.succeedNone
	const entry = link.split(',').find((candidate) => candidate.includes('rel="next"'))
	if (Predicate.isUndefined(entry)) return Effect.succeedNone
	const target = /<([^>]+)>/.exec(entry)?.[1]
	if (Predicate.isUndefined(target)) return Effect.fail(GitHubTransportError.make({ stage: 'pagination' }))
	return Effect.try({
		try: () => new URL(target, apiOrigin),
		catch: () => GitHubTransportError.make({ stage: 'pagination' }),
	}).pipe(
		Effect.flatMap((next) =>
			next.origin === apiOrigin.origin
				? Effect.succeedSome(next.toString())
				: Effect.fail(GitHubTransportError.make({ stage: 'pagination' })),
		),
	)
}

type TokenEntry = {
	readonly token: Redacted.Redacted<string>
	readonly ttl: number
}

type TokenKey = {
	readonly installationId: GitHubId
	readonly repositoryId: GitHubId
}

const tokenKey = (ref: GitHubRepositoryRef): TokenKey => ({
	installationId: ref.installationId,
	repositoryId: ref.repositoryId,
})

const cacheKey = (key: TokenKey) => JSON.stringify([key.installationId, key.repositoryId])

/** Live GitHub API implementation with an injectable Effect HTTP transport and signer. */
export const GitHubApiLiveBase = Layer.effect(
	GitHubApi,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const signer = yield* GitHubAppSigner
		const config = yield* GitHubApiConfig
		const apiOrigin = new URL(config.apiOrigin.toString().replace(/\/?$/, '/'))

		const baseRequest = (method: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE', url: string) =>
			HttpClientRequest.make(method)(url).pipe(
				HttpClientRequest.setHeader('accept', 'application/vnd.github+json'),
				HttpClientRequest.setHeader('x-github-api-version', '2022-11-28'),
				HttpClientRequest.setHeader('user-agent', 'humanlayer-channels-github-next'),
			)

		const appJwt = Effect.fn('github.api.app_jwt')(function* () {
			const now = yield* Clock.currentTimeMillis
			const header = Encoding.encodeBase64Url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }))
			const payload = Encoding.encodeBase64Url(
				JSON.stringify({
					iss: String(config.appId),
					iat: Math.floor(now / 1_000) - 60,
					exp: Math.floor(now / 1_000) + 540,
				}),
			)
			const unsigned = `${header}.${payload}`
			const signature = yield* signer.sign({ privateKey: config.privateKey, data: unsigned }).pipe(
				Effect.tapError((error) => Effect.logError('GitHub App JWT signing failed', error)),
				Effect.mapError(() => GitHubTransportError.make({ stage: 'signing' })),
			)
			return `${unsigned}.${signature}`
		})

		const classifyTransport = (operation: GitHubApiOperation, error: GitHubTransportError): GitHubApiError => {
			const details = {
				...(Predicate.isUndefined(error.status) ? {} : { status: error.status }),
				...(Predicate.isUndefined(error.message) ? {} : { message: error.message }),
				...(Predicate.isUndefined(error.retryAfterMs) ? {} : { retryAfterMs: error.retryAfterMs }),
			}
			if (error.rateLimited === true) {
				return GitHubApiError.make({
					operation,
					reason: 'rate_limited',
					retryable: true,
					...details,
				})
			}
			if (error.status === 401) {
				return GitHubApiError.make({ operation, reason: 'authentication', retryable: false, ...details })
			}
			if (operation === 'merge_pull_request') {
				if (error.status === 409) {
					return GitHubApiError.make({ operation, reason: 'stale_head', retryable: false, ...details })
				}
				if (error.status === 405) {
					return GitHubApiError.make({ operation, reason: 'not_mergeable', retryable: false, ...details })
				}
			}
			if (error.status === 403) {
				return GitHubApiError.make({ operation, reason: 'forbidden', retryable: false, ...details })
			}
			if (error.status === 404 || error.status === 410) {
				return GitHubApiError.make({ operation, reason: 'not_found', retryable: false, ...details })
			}
			if (error.stage === 'signing') {
				return GitHubApiError.make({ operation, reason: 'authentication', retryable: false, ...details })
			}
			if (
				error.stage === 'decode' ||
				error.stage === 'token_expiry' ||
				error.stage === 'pagination' ||
				error.stage === 'redirect'
			) {
				return GitHubApiError.make({ operation, reason: 'invalid_response', retryable: false, ...details })
			}
			if (error.status === 409 || error.status === 422) {
				return GitHubApiError.make({ operation, reason: 'validation', retryable: false, ...details })
			}
			if (Predicate.isNotUndefined(error.status) && error.status >= 400 && error.status < 500) {
				return GitHubApiError.make({ operation, reason: 'validation', retryable: false, ...details })
			}
			return GitHubApiError.make({ operation, reason: 'unavailable', retryable: true, ...details })
		}

		const inspectStatus = Effect.fn('github.api.inspect_status')(function* (
			operation: GitHubApiOperation,
			response: HttpClientResponse.HttpClientResponse,
		) {
			if (response.status >= 200 && response.status < 300) return response
			if (response.status >= 300 && response.status < 400) {
				return yield* GitHubTransportError.make({ stage: 'redirect', status: response.status })
			}
			const retryAfterHeaderMs = secondsToMillis(response.headers['retry-after'])
			const body = yield* response.text.pipe(Effect.catch(() => Effect.succeed('')))
			const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(GitHubApiErrorBody))(body).pipe(
				Effect.map((value) => value.message.trim().slice(0, 1_024)),
				Effect.catch(() => Effect.succeed(undefined)),
			)
			const rateLimited =
				response.status === 429 ||
				(response.status === 403 &&
					(response.headers['x-ratelimit-remaining'] === '0' ||
						Predicate.isNotUndefined(retryAfterHeaderMs) ||
						(decoded?.toLowerCase().includes('rate limit') ?? false)))
			const now = yield* Clock.currentTimeMillis
			const retryAfterMs =
				Predicate.isNotUndefined(retryAfterHeaderMs) || !rateLimited
					? retryAfterHeaderMs
					: epochSecondsToDelayMillis(response.headers['x-ratelimit-reset'], now)
			return yield* GitHubTransportError.make({
				stage: 'status',
				status: response.status,
				...(Predicate.isUndefined(decoded) || decoded.length === 0 ? {} : { message: decoded }),
				...(rateLimited ? { rateLimited: true } : {}),
				...(Predicate.isUndefined(retryAfterMs) ? {} : { retryAfterMs }),
			})
		})

		const execute = <S extends Schema.Top>(input: {
			readonly operation: GitHubApiOperation
			readonly request: HttpClientRequest.HttpClientRequest
			readonly schema: S
		}) =>
			client.execute(input.request).pipe(
				Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
				Effect.flatMap((response) => inspectStatus(input.operation, response)),
				Effect.flatMap((response) =>
					response.json.pipe(
						Effect.flatMap(Schema.decodeUnknownEffect(input.schema)),
						Effect.mapError(() => GitHubTransportError.make({ stage: 'decode', status: response.status })),
					),
				),
				Effect.tapError((error) =>
					Effect.logError('GitHub API request failed', error).pipe(
						Effect.annotateLogs({ operation: input.operation }),
					),
				),
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
				Effect.withSpan('github.api.request', {
					attributes: { 'github.operation': input.operation, 'http.request.method': input.request.method },
				}),
			)

		const executeVoid = (input: {
			readonly operation: GitHubApiOperation
			readonly request: HttpClientRequest.HttpClientRequest
		}) =>
			client.execute(input.request).pipe(
				Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
				Effect.flatMap((response) => inspectStatus(input.operation, response)),
				Effect.asVoid,
				Effect.tapError((error) =>
					Effect.logError('GitHub API request failed', error).pipe(
						Effect.annotateLogs({ operation: input.operation }),
					),
				),
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
				Effect.withSpan('github.api.request', {
					attributes: { 'github.operation': input.operation, 'http.request.method': input.request.method },
				}),
			)

		const executeText = (input: {
			readonly operation: GitHubApiOperation
			readonly request: HttpClientRequest.HttpClientRequest
			readonly transport?: HttpClient.HttpClient
		}) =>
			(input.transport ?? client).execute(input.request).pipe(
				Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
				Effect.flatMap((response) => inspectStatus(input.operation, response)),
				Effect.flatMap((response) =>
					response.text.pipe(
						Effect.mapError(() => GitHubTransportError.make({ stage: 'decode', status: response.status })),
					),
				),
				Effect.tapError((error) =>
					Effect.logError('GitHub API text request failed', error).pipe(
						Effect.annotateLogs({ operation: input.operation }),
					),
				),
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
				Effect.withSpan('github.api.request', {
					attributes: { 'github.operation': input.operation, 'http.request.method': input.request.method },
				}),
			)

		const botUserId = yield* Effect.cached(
			Option.match(config.botUserId, {
				onSome: Effect.succeed,
				onNone: () =>
					Effect.gen(function* () {
						const jwt = yield* appJwt().pipe(
							Effect.catchTag('GitHubTransportError', (error) =>
								Effect.fail(classifyTransport('remove_reaction', error)),
							),
						)
						const app = yield* execute({
							operation: 'remove_reaction',
							request: baseRequest('GET', new URL('app', apiOrigin).toString()).pipe(
								HttpClientRequest.bearerToken(jwt),
							),
							schema: GitHubAppResponse,
						})
						const bot = yield* execute({
							operation: 'remove_reaction',
							request: baseRequest(
								'GET',
								new URL(`users/${encodeURIComponent(`${app.slug}[bot]`)}`, apiOrigin).toString(),
							),
							schema: GitHubApiParticipant,
						})
						return bot.id
					}),
			}),
		)

		const tokenCache = yield* Cache.makeWith(
			(key: string) =>
				Effect.gen(function* () {
					const [installationId, repositoryId] = yield* Schema.decodeEffect(
						Schema.fromJsonString(Schema.Tuple([GitHubId, GitHubId])),
					)(key).pipe(Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })))
					const jwt = yield* appJwt()
					const request = yield* HttpClientRequest.post(
						new URL(`app/installations/${installationId}/access_tokens`, apiOrigin).toString(),
					).pipe(
						HttpClientRequest.setHeader('accept', 'application/vnd.github+json'),
						HttpClientRequest.setHeader('x-github-api-version', '2022-11-28'),
						HttpClientRequest.setHeader('user-agent', 'humanlayer-channels-github-next'),
						HttpClientRequest.bearerToken(jwt),
						HttpClientRequest.schemaBodyJson(Schema.Json)({ repository_ids: [repositoryId] }),
						Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
					)
					const response = yield* client.execute(request).pipe(
						Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
						Effect.flatMap((value) => inspectStatus('remove_reaction', value)),
						Effect.flatMap((value) =>
							value.json.pipe(
								Effect.flatMap(Schema.decodeUnknownEffect(GitHubInstallationTokenResponse)),
								Effect.mapError(() => GitHubTransportError.make({ stage: 'decode' })),
							),
						),
					)
					const expiresAt = Date.parse(response.expires_at)
					const receivedAt = yield* Clock.currentTimeMillis
					if (!Number.isFinite(expiresAt) || expiresAt <= receivedAt + 60_000) {
						return yield* GitHubTransportError.make({ stage: 'token_expiry' })
					}
					return {
						token: Redacted.make(response.token),
						ttl: Math.min(expiresAt - receivedAt - 60_000, 3_540_000),
					} satisfies TokenEntry
				}).pipe(Effect.tapError((error) => Effect.logError('GitHub installation token request failed', error))),
			{
				capacity: 1_000,
				timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.millis(exit.value.ttl) : Duration.zero),
			},
		)

		const authenticatedRequest = Effect.fn('github.api.authenticated_request')(function* (input: {
			operation: GitHubApiOperation
			ref: GitHubRepositoryRef
			method: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'
			url: string
			body?: Schema.Json
		}) {
			const token = yield* Cache.get(tokenCache, cacheKey(tokenKey(input.ref))).pipe(
				Effect.catchTag('GitHubTransportError', (error) =>
					Effect.fail(classifyTransport(input.operation, error)),
				),
			)
			const request = baseRequest(input.method, input.url).pipe(HttpClientRequest.bearerToken(token.token))
			if (Predicate.isUndefined(input.body)) return request
			return yield* HttpClientRequest.schemaBodyJson(Schema.Json)(request, input.body).pipe(
				Effect.mapError(() =>
					GitHubApiError.make({ operation: input.operation, reason: 'invalid_response', retryable: false }),
				),
			)
		})

		const retryWithFreshToken = <A, R>(
			ref: GitHubRepositoryRef,
			operation: Effect.Effect<A, GitHubApiError, R>,
		): Effect.Effect<A, GitHubApiError, R> =>
			operation.pipe(
				Effect.catchTag('GitHubApiError', (error) =>
					error.reason === 'authentication'
						? Cache.invalidate(tokenCache, cacheKey(tokenKey(ref))).pipe(Effect.andThen(operation))
						: Effect.fail(error),
				),
			)

		const call = <S extends Schema.Top>(input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubRepositoryRef
			readonly method: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'
			readonly path: string
			readonly schema: S
			readonly body?: Schema.Json
		}) => {
			const operation = authenticatedRequest({
				operation: input.operation,
				ref: input.ref,
				method: input.method,
				url: new URL(input.path.replace(/^\//, ''), apiOrigin).toString(),
				...(Predicate.isUndefined(input.body) ? {} : { body: input.body }),
			}).pipe(Effect.flatMap((request) => execute({ operation: input.operation, request, schema: input.schema })))
			return retryWithFreshToken(input.ref, operation)
		}

		const callVoid = (input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubRepositoryRef
			readonly method: 'DELETE'
			readonly path: string
		}) => {
			const operation = authenticatedRequest({
				operation: input.operation,
				ref: input.ref,
				method: input.method,
				url: new URL(input.path.replace(/^\//, ''), apiOrigin).toString(),
			}).pipe(Effect.flatMap((request) => executeVoid({ operation: input.operation, request })))
			return retryWithFreshToken(input.ref, operation)
		}

		const urlWithQuery = (path: string, query: ReadonlyArray<readonly [string, string]> = []) => {
			const url = new URL(path.replace(/^\//, ''), apiOrigin)
			for (const [key, value] of query) url.searchParams.set(key, value)
			return url.toString()
		}

		const paginate = <Page, A>(input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubRepositoryRef
			readonly path: string
			readonly query?: ReadonlyArray<readonly [string, string]>
			readonly schema: Schema.Codec<Page, unknown, never, never>
			readonly items: (page: Page) => ReadonlyArray<A>
		}): Effect.Effect<ReadonlyArray<A>, GitHubApiError> =>
			Stream.paginate<string, A, GitHubApiError>(
				urlWithQuery(input.path, [['per_page', '100'], ...(input.query ?? [])]),
				(url) => {
					const operation = authenticatedRequest({
						operation: input.operation,
						ref: input.ref,
						method: 'GET',
						url,
					}).pipe(
						Effect.flatMap((request) =>
							client.execute(request).pipe(
								Effect.mapError(() => GitHubTransportError.make({ stage: 'transport' })),
								Effect.flatMap((response) => inspectStatus(input.operation, response)),
								Effect.flatMap((response) =>
									response.json.pipe(
										Effect.flatMap(Schema.decodeUnknownEffect(input.schema)),
										Effect.mapError(() =>
											GitHubTransportError.make({ stage: 'decode', status: response.status }),
										),
										Effect.flatMap((page) =>
											parseNextLink(response, apiOrigin).pipe(
												Effect.map((next) => [input.items(page), next] as const),
											),
										),
									),
								),
							),
						),
						Effect.tapError((error) =>
							Effect.logError('GitHub API pagination request failed', error).pipe(
								Effect.annotateLogs({ operation: input.operation }),
							),
						),
						Effect.catchTag('GitHubTransportError', (error) =>
							Effect.fail(classifyTransport(input.operation, error)),
						),
					)
					return retryWithFreshToken(input.ref, operation)
				},
			).pipe(
				Stream.runCollect,
				Effect.map((items) => Array.from(items)),
			)

		const list = <A>(input: {
			readonly operation: GitHubApiOperation
			readonly ref: GitHubRepositoryRef
			readonly path: string
			readonly query?: ReadonlyArray<readonly [string, string]>
			readonly schema: Schema.Codec<A, unknown, never, never>
		}): Effect.Effect<ReadonlyArray<A>, GitHubApiError> =>
			paginate({
				operation: input.operation,
				ref: input.ref,
				path: input.path,
				...(Predicate.isUndefined(input.query) ? {} : { query: input.query }),
				schema: Schema.Array(input.schema),
				items: (page) => page,
			})

		const fetchIssue = Effect.fn('github.api.fetch_issue')(function* (input: { issue: GitHubIssueRef }) {
			const value = yield* call({
				operation: 'fetch_issue',
				ref: input.issue,
				method: 'GET',
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
				schema: GitHubApiIssue,
			})
			if (value.number !== input.issue.number) {
				return yield* GitHubApiError.make({
					operation: 'fetch_issue',
					reason: 'invalid_response',
					retryable: false,
				})
			}
			return GitHubIssueInfo.make({
				ref: input.issue,
				title: value.title,
				body: value.body,
				state: value.state,
				url: value.html_url,
				author: participant(value.user),
			})
		})

		const fetchPullRequest = Effect.fn('github.api.fetch_pull_request')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}) {
			const value = yield* call({
				operation: 'fetch_pull_request',
				ref: input.pullRequest,
				method: 'GET',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
				schema: GitHubApiPullRequest,
			})
			if (value.number !== input.pullRequest.number) {
				return yield* GitHubApiError.make({
					operation: 'fetch_pull_request',
					reason: 'invalid_response',
					retryable: false,
				})
			}
			return GitHubPullRequestInfo.make({
				ref: input.pullRequest,
				title: value.title,
				body: value.body,
				state: value.state,
				url: value.html_url,
				author: value.user === null ? null : participant(value.user),
				draft: value.draft,
				merged: value.merged,
				headRef: value.head.ref,
				headSha: value.head.sha,
				baseRef: value.base.ref,
				baseSha: value.base.sha,
			})
		})

		const listIssueComments = Effect.fn('github.api.list_issue_comments')(function* (input: {
			issue: GitHubIssueRef
		}): Effect.fn.Return<GitHubIssueComments, GitHubApiError> {
			const values = yield* list({
				operation: 'list_issue_comments',
				ref: input.issue,
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/comments`,
				schema: GitHubApiIssueComment,
			})
			const discussion = { _tag: 'Issue', ref: input.issue } as const
			return values.map((value) => issueComment(discussion, value))
		})

		const listPullRequestComments = Effect.fn('github.api.list_pull_request_comments')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}): Effect.fn.Return<GitHubIssueComments, GitHubApiError> {
			const values = yield* list({
				operation: 'list_pull_request_comments',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/comments`,
				schema: GitHubApiIssueComment,
			})
			const discussion = { _tag: 'PullRequest', ref: input.pullRequest } as const
			return values.map((value) => issueComment(discussion, value))
		})

		const listPullRequestReviews = Effect.fn('github.api.list_reviews')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}): Effect.fn.Return<GitHubReviews, GitHubApiError> {
			const values = yield* list({
				operation: 'list_pull_request_reviews',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/reviews`,
				schema: GitHubApiReview,
			})
			return values.map((value) =>
				GitHubReview.make({
					ref: { pullRequest: input.pullRequest, id: value.id, nodeId: value.node_id },
					body: value.body,
					author: value.user === null ? null : participant(value.user),
					state: reviewState(value.state),
					commitId: value.commit_id,
					url: value.html_url,
				}),
			)
		})

		const listPullRequestReviewComments = Effect.fn('github.api.list_review_comments')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}): Effect.fn.Return<GitHubReviewComments, GitHubApiError> {
			const values = yield* list({
				operation: 'list_pull_request_review_comments',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments`,
				schema: GitHubApiReviewComment,
			})
			return values.map((value) => reviewComment(input.pullRequest, value))
		})

		const listPullRequestFiles = Effect.fn('github.api.list_pull_request_files')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}) {
			const values = yield* list({
				operation: 'list_pull_request_files',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/files`,
				schema: GitHubApiPullRequestFile,
			})
			return values.map(pullRequestFile)
		})

		const fetchPullRequestDiff = Effect.fn('github.api.fetch_pull_request_diff')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}) {
			const operation = authenticatedRequest({
				operation: 'fetch_pull_request_diff',
				ref: input.pullRequest,
				method: 'GET',
				url: urlWithQuery(`${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`),
			}).pipe(
				Effect.map((request) => HttpClientRequest.setHeader(request, 'accept', 'application/vnd.github.diff')),
				Effect.flatMap((request) => executeText({ operation: 'fetch_pull_request_diff', request })),
			)
			return yield* retryWithFreshToken(input.pullRequest, operation)
		})

		const listPullRequestCommits = Effect.fn('github.api.list_pull_request_commits')(function* (input: {
			pullRequest: GitHubPullRequestRef
		}) {
			const values = yield* list({
				operation: 'list_pull_request_commits',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/commits`,
				schema: GitHubApiCommit,
			})
			return values.map(commit)
		})

		const listLabels = (operation: 'list_issue_labels' | 'list_pull_request_labels', ref: GitHubIssueRef) =>
			list({
				operation,
				ref,
				path: `${repositoryPath(ref)}/issues/${ref.number}/labels`,
				schema: GitHubApiLabel,
			}).pipe(Effect.map((values) => values.map(label)))

		const changeLabels = (input: {
			readonly operation:
				| 'add_issue_labels'
				| 'add_pull_request_labels'
				| 'set_issue_labels'
				| 'set_pull_request_labels'
			readonly ref: GitHubIssueRef
			readonly method: 'POST' | 'PUT'
			readonly labels: ReadonlyArray<string>
		}) =>
			call({
				operation: input.operation,
				ref: input.ref,
				method: input.method,
				path: `${repositoryPath(input.ref)}/issues/${input.ref.number}/labels`,
				schema: Schema.Array(GitHubApiLabel),
				body: { labels: input.labels },
			}).pipe(Effect.map((values) => values.map(label)))

		const removeLabel = (input: {
			readonly operation: 'remove_issue_label' | 'remove_pull_request_label'
			readonly ref: GitHubIssueRef
			readonly label: string
		}) =>
			call({
				operation: input.operation,
				ref: input.ref,
				method: 'DELETE',
				path: `${repositoryPath(input.ref)}/issues/${input.ref.number}/labels/${encodeURIComponent(input.label)}`,
				schema: Schema.Array(GitHubApiLabel),
			}).pipe(Effect.map((values) => values.map(label)))

		const removeAllLabels = (input: {
			readonly operation: 'remove_all_issue_labels' | 'remove_all_pull_request_labels'
			readonly ref: GitHubIssueRef
		}) =>
			callVoid({
				operation: input.operation,
				ref: input.ref,
				method: 'DELETE',
				path: `${repositoryPath(input.ref)}/issues/${input.ref.number}/labels`,
			})

		const listIssueLabels = Effect.fn('github.api.list_issue_labels')((input: { issue: GitHubIssueRef }) =>
			listLabels('list_issue_labels', input.issue),
		)

		const listPullRequestLabels = Effect.fn('github.api.list_pull_request_labels')(
			(input: { pullRequest: GitHubPullRequestRef }) => listLabels('list_pull_request_labels', input.pullRequest),
		)

		const addIssueLabels = Effect.fn('github.api.add_issue_labels')(
			(input: { issue: GitHubIssueRef; labels: ReadonlyArray<string> }) =>
				changeLabels({ operation: 'add_issue_labels', ref: input.issue, method: 'POST', labels: input.labels }),
		)

		const addPullRequestLabels = Effect.fn('github.api.add_pull_request_labels')(
			(input: { pullRequest: GitHubPullRequestRef; labels: ReadonlyArray<string> }) =>
				changeLabels({
					operation: 'add_pull_request_labels',
					ref: input.pullRequest,
					method: 'POST',
					labels: input.labels,
				}),
		)

		const setIssueLabels = Effect.fn('github.api.set_issue_labels')(
			(input: { issue: GitHubIssueRef; labels: ReadonlyArray<string> }) =>
				changeLabels({ operation: 'set_issue_labels', ref: input.issue, method: 'PUT', labels: input.labels }),
		)

		const setPullRequestLabels = Effect.fn('github.api.set_pull_request_labels')(
			(input: { pullRequest: GitHubPullRequestRef; labels: ReadonlyArray<string> }) =>
				changeLabels({
					operation: 'set_pull_request_labels',
					ref: input.pullRequest,
					method: 'PUT',
					labels: input.labels,
				}),
		)

		const removeIssueLabel = Effect.fn('github.api.remove_issue_label')(
			(input: { issue: GitHubIssueRef; label: string }) =>
				removeLabel({ operation: 'remove_issue_label', ref: input.issue, label: input.label }),
		)

		const removePullRequestLabel = Effect.fn('github.api.remove_pull_request_label')(
			(input: { pullRequest: GitHubPullRequestRef; label: string }) =>
				removeLabel({ operation: 'remove_pull_request_label', ref: input.pullRequest, label: input.label }),
		)

		const removeAllIssueLabels = Effect.fn('github.api.remove_all_issue_labels')(
			(input: { issue: GitHubIssueRef }) =>
				removeAllLabels({ operation: 'remove_all_issue_labels', ref: input.issue }),
		)

		const removeAllPullRequestLabels = Effect.fn('github.api.remove_all_pull_request_labels')(
			(input: { pullRequest: GitHubPullRequestRef }) =>
				removeAllLabels({ operation: 'remove_all_pull_request_labels', ref: input.pullRequest }),
		)

		const postPullRequestReviewComment = Effect.fn('github.api.post_pull_request_review_comment')(function* (
			input: GitHubPostPullRequestReviewComment,
		) {
			const value = yield* call({
				operation: 'post_pull_request_review_comment',
				ref: input.pullRequest,
				method: 'POST',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments`,
				schema: GitHubApiReviewComment,
				body: {
					body: input.content.markdown,
					commit_id: input.commitId,
					path: input.path,
					...reviewCommentLocationBody(input.location),
				},
			})
			return reviewComment(input.pullRequest, value)
		})

		const closeIssue = Effect.fn('github.api.close_issue')(function* (input: {
			issue: GitHubIssueRef
			reason: 'completed' | 'not_planned'
		}) {
			const value = yield* call({
				operation: 'close_issue',
				ref: input.issue,
				method: 'PATCH',
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
				schema: GitHubApiIssue,
				body: { state: 'closed', state_reason: input.reason },
			})
			return issueInfo(input.issue, value)
		})

		const reopenIssue = Effect.fn('github.api.reopen_issue')(function* (input: { issue: GitHubIssueRef }) {
			const value = yield* call({
				operation: 'reopen_issue',
				ref: input.issue,
				method: 'PATCH',
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}`,
				schema: GitHubApiIssue,
				body: { state: 'open', state_reason: 'reopened' },
			})
			return issueInfo(input.issue, value)
		})

		const changePullRequestState = (input: {
			readonly operation: 'close_pull_request' | 'reopen_pull_request'
			readonly pullRequest: GitHubPullRequestRef
			readonly state: 'open' | 'closed'
		}) =>
			call({
				operation: input.operation,
				ref: input.pullRequest,
				method: 'PATCH',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}`,
				schema: GitHubApiPullRequest,
				body: { state: input.state },
			}).pipe(Effect.map((value) => pullRequestInfo(input.pullRequest, value)))

		const closePullRequest = Effect.fn('github.api.close_pull_request')(
			(input: { pullRequest: GitHubPullRequestRef }) =>
				changePullRequestState({
					operation: 'close_pull_request',
					pullRequest: input.pullRequest,
					state: 'closed',
				}),
		)

		const reopenPullRequest = Effect.fn('github.api.reopen_pull_request')(
			(input: { pullRequest: GitHubPullRequestRef }) =>
				changePullRequestState({
					operation: 'reopen_pull_request',
					pullRequest: input.pullRequest,
					state: 'open',
				}),
		)

		const mergePullRequest = Effect.fn('github.api.merge_pull_request')(function* (input: {
			pullRequest: GitHubPullRequestRef
			method: 'merge' | 'squash' | 'rebase'
			expectedHeadSha: string
			commitTitle?: string
			commitMessage?: string
		}) {
			const value = yield* call({
				operation: 'merge_pull_request',
				ref: input.pullRequest,
				method: 'PUT',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/merge`,
				schema: GitHubApiMergeResult,
				body: {
					merge_method: input.method,
					sha: input.expectedHeadSha,
					...(Predicate.isUndefined(input.commitTitle) ? {} : { commit_title: input.commitTitle }),
					...(Predicate.isUndefined(input.commitMessage) ? {} : { commit_message: input.commitMessage }),
				},
			})
			return GitHubMergeResult.make(value)
		})

		const listCheckRunsForRef = Effect.fn('github.api.list_check_runs_for_ref')(function* (input: {
			pullRequest: GitHubPullRequestRef
			sha: string
		}) {
			const values = yield* paginate({
				operation: 'list_check_runs_for_ref',
				ref: input.pullRequest,
				path: `${repositoryPath(input.pullRequest)}/commits/${encodeURIComponent(input.sha)}/check-runs`,
				query: [['filter', 'all']],
				schema: GitHubApiCheckRunsPage,
				items: (page) => page.check_runs,
			})
			return values.map((value) => checkRun(input.pullRequest, value))
		})

		const fetchCheckRun = Effect.fn('github.api.fetch_check_run')(function* (input: {
			checkRun: GitHubCheckRunRef
		}) {
			const value = yield* call({
				operation: 'fetch_check_run',
				ref: input.checkRun,
				method: 'GET',
				path: `${repositoryPath(input.checkRun)}/check-runs/${input.checkRun.id}`,
				schema: GitHubApiCheckRun,
			})
			if (value.id !== input.checkRun.id) {
				return yield* GitHubApiError.make({
					operation: 'fetch_check_run',
					reason: 'invalid_response',
					retryable: false,
				})
			}
			return checkRunInfo(input.checkRun, value)
		})

		const listCheckRunAnnotations = Effect.fn('github.api.list_check_run_annotations')(function* (input: {
			checkRun: GitHubCheckRunRef
		}) {
			const values = yield* list({
				operation: 'list_check_run_annotations',
				ref: input.checkRun,
				path: `${repositoryPath(input.checkRun)}/check-runs/${input.checkRun.id}/annotations`,
				schema: GitHubApiCheckAnnotation,
			})
			return values.map(checkAnnotation)
		})

		const resolveActionsJob = Effect.fn('github.api.resolve_actions_job')(function* (input: {
			checkRun: GitHubCheckRunRef
		}) {
			const check = yield* fetchCheckRun(input)
			if (check.checkSuiteId === null) return null
			const runs = yield* paginate({
				operation: 'resolve_check_run_actions_job',
				ref: input.checkRun,
				path: `${repositoryPath(input.checkRun)}/actions/runs`,
				query: [['check_suite_id', String(check.checkSuiteId)]],
				schema: GitHubApiWorkflowRunsPage,
				items: (page) => page.workflow_runs,
			})
			for (const run of runs) {
				const jobs = yield* paginate({
					operation: 'resolve_check_run_actions_job',
					ref: input.checkRun,
					path: `${repositoryPath(input.checkRun)}/actions/runs/${run.id}/jobs`,
					query: [['filter', 'all']],
					schema: GitHubApiActionsJobsPage,
					items: (page) => page.jobs,
				})
				const job = jobs.find((candidate) => sameUrl(candidate.check_run_url, check.apiUrl))
				if (Predicate.isNotUndefined(job)) return actionsJob(input.checkRun, job)
			}
			return null
		})

		const fetchActionsJob = Effect.fn('github.api.fetch_actions_job')(function* (input: {
			job: GitHubActionsJobRef
		}) {
			const value = yield* call({
				operation: 'fetch_actions_job',
				ref: input.job,
				method: 'GET',
				path: `${repositoryPath(input.job)}/actions/jobs/${input.job.id}`,
				schema: GitHubApiActionsJob,
			})
			if (value.id !== input.job.id) {
				return yield* GitHubApiError.make({
					operation: 'fetch_actions_job',
					reason: 'invalid_response',
					retryable: false,
				})
			}
			return actionsJobInfo(input.job, value)
		})

		const downloadActionsJobLog = Effect.fn('github.api.download_actions_job_log')(function* (input: {
			job: GitHubActionsJobRef
		}) {
			const operation = authenticatedRequest({
				operation: 'download_actions_job_log',
				ref: input.job,
				method: 'GET',
				url: urlWithQuery(`${repositoryPath(input.job)}/actions/jobs/${input.job.id}/logs`),
			}).pipe(
				Effect.flatMap((request) =>
					executeText({
						operation: 'download_actions_job_log',
						request,
						transport: HttpClient.followRedirects(client, 3),
					}).pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: 'manual' })),
				),
			)
			return yield* retryWithFreshToken(input.job, operation)
		})

		const postIssueComment = Effect.fn('github.api.post_comment')(function* (input: {
			issue: GitHubIssueRef
			content: { readonly markdown: string }
		}) {
			const value = yield* call({
				operation: 'post_issue_comment',
				ref: input.issue,
				method: 'POST',
				path: `${repositoryPath(input.issue)}/issues/${input.issue.number}/comments`,
				schema: GitHubApiIssueComment,
				body: { body: input.content.markdown },
			})
			return issueComment({ _tag: 'Issue', ref: input.issue }, value)
		})

		const postPullRequestComment = Effect.fn('github.api.post_pull_request_comment')(function* (input: {
			pullRequest: GitHubPullRequestRef
			content: { readonly markdown: string }
		}) {
			const value = yield* call({
				operation: 'post_pull_request_comment',
				ref: input.pullRequest,
				method: 'POST',
				path: `${repositoryPath(input.pullRequest)}/issues/${input.pullRequest.number}/comments`,
				schema: GitHubApiIssueComment,
				body: { body: input.content.markdown },
			})
			return issueComment({ _tag: 'PullRequest', ref: input.pullRequest }, value)
		})

		const replyToReviewComment = Effect.fn('github.api.reply_to_review_comment')(function* (input: {
			pullRequest: GitHubPullRequestRef
			comment: GitHubReviewCommentRef
			content: { readonly markdown: string }
		}) {
			const value = yield* call({
				operation: 'reply_to_review_comment',
				ref: input.pullRequest,
				method: 'POST',
				path: `${repositoryPath(input.pullRequest)}/pulls/${input.pullRequest.number}/comments/${input.comment.id}/replies`,
				schema: GitHubApiReviewComment,
				body: { body: input.content.markdown },
			})
			return reviewComment(input.pullRequest, value)
		})

		const updateComment = Effect.fn('github.api.update_comment')(function* (input: GitHubUpdateComment) {
			const ref = input.comment
			const repository = commentRepository(ref)
			const value = yield* Schema.is(GitHubReviewCommentRef)(ref)
				? call({
						operation: 'update_comment',
						ref: repository,
						method: 'PATCH',
						path: commentPath(ref),
						schema: GitHubApiReviewComment,
						body: { body: input.content.markdown },
					}).pipe(Effect.map((comment) => reviewComment(ref.pullRequest, comment)))
				: call({
						operation: 'update_comment',
						ref: repository,
						method: 'PATCH',
						path: commentPath(ref),
						schema: GitHubApiIssueComment,
						body: { body: input.content.markdown },
					}).pipe(Effect.map((comment) => issueComment(ref.discussion, comment)))
			return value
		})

		const deleteComment = Effect.fn('github.api.delete_comment')(function* (input: GitHubDeleteComment) {
			const ref = input.comment
			yield* callVoid({
				operation: 'delete_comment',
				ref: commentRepository(ref),
				method: 'DELETE',
				path: commentPath(ref),
			})
		})

		const reactionPath = (comment: GitHubCommentRef) => `${commentPath(comment)}/reactions`

		const addReaction = Effect.fn('github.api.add_reaction')(function* (input: GitHubReactionRequest) {
			const ref = commentRepository(input.comment)
			yield* call({
				operation: 'add_reaction',
				ref,
				method: 'POST',
				path: reactionPath(input.comment),
				schema: GitHubApiReaction,
				body: { content: input.reaction },
			})
		})

		const removeReaction = Effect.fn('github.api.remove_reaction')(function* (input: GitHubReactionRequest) {
			const ref = commentRepository(input.comment)
			const ownUserId = yield* botUserId
			const reactionId = yield* list({
				operation: 'remove_reaction',
				ref,
				path: reactionPath(input.comment),
				schema: GitHubApiReaction,
			}).pipe(
				Effect.map((reactions) =>
					Option.fromUndefinedOr(
						reactions.find(
							(reaction) => reaction.content === input.reaction && reaction.user?.id === ownUserId,
						)?.id,
					),
				),
			)
			if (Option.isNone(reactionId)) return
			yield* callVoid({
				operation: 'remove_reaction',
				ref,
				method: 'DELETE',
				path: `${reactionPath(input.comment)}/${reactionId.value}`,
			})
		})

		return GitHubApi.of({
			fetchIssue,
			fetchPullRequest,
			listIssueComments,
			listPullRequestComments,
			listPullRequestReviews,
			listPullRequestReviewComments,
			listPullRequestFiles,
			fetchPullRequestDiff,
			listPullRequestCommits,
			listIssueLabels,
			listPullRequestLabels,
			addIssueLabels,
			addPullRequestLabels,
			setIssueLabels,
			setPullRequestLabels,
			removeIssueLabel,
			removePullRequestLabel,
			removeAllIssueLabels,
			removeAllPullRequestLabels,
			postIssueComment,
			postPullRequestComment,
			postPullRequestReviewComment,
			replyToReviewComment,
			updateComment,
			deleteComment,
			addReaction,
			removeReaction,
			closeIssue,
			reopenIssue,
			closePullRequest,
			reopenPullRequest,
			mergePullRequest,
			listCheckRunsForRef,
			fetchCheckRun,
			listCheckRunAnnotations,
			resolveActionsJob,
			fetchActionsJob,
			downloadActionsJobLog,
		})
	}),
)

/** GitHub API implementation with Web Crypto signing and the standard Fetch transport. */
export const GitHubApiLive = GitHubApiLiveBase.pipe(
	Layer.provide(GitHubAppSigner.layerWebCrypto),
	Layer.provide(FetchHttpClient.layer),
)
