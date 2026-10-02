import { Effect, Layer } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'

import { GitHubApiClient, GitHubApiClientLive } from './api/GitHubApiClient'
import { GitHubApi } from './GitHubApi'
export { GitHubAppSigner } from './api/GitHubAppSigner'
import { GitHubAppSigner } from './api/GitHubAppSigner'
import {
	addIssueLabels,
	addPullRequestLabels,
	addReaction,
	closeIssue,
	closePullRequest,
	deleteComment,
	downloadActionsJobLog,
	fetchActionsJob,
	fetchCheckRun,
	fetchIssue,
	fetchPullRequest,
	fetchPullRequestDiff,
	fetchUserAccess,
	listCheckRunAnnotations,
	listCheckRunsForRef,
	listIssueComments,
	listIssueLabels,
	listPullRequestComments,
	listPullRequestCommits,
	listPullRequestFiles,
	listPullRequestLabels,
	listPullRequestReviewComments,
	listPullRequestReviews,
	mergePullRequest,
	postIssueComment,
	postPullRequestComment,
	postPullRequestReviewComment,
	removeAllIssueLabels,
	removeAllPullRequestLabels,
	removeIssueLabel,
	removePullRequestLabel,
	removeReaction,
	reopenIssue,
	reopenPullRequest,
	replyToReviewComment,
	resolveActionsJob,
	setIssueLabels,
	setPullRequestLabels,
	updateComment,
} from './api/Operations'

const GitHubApiOperationsLive = Layer.effect(
	GitHubApi,
	Effect.gen(function* () {
		const client = yield* GitHubApiClient
		return GitHubApi.of({
			fetchIssue: (input) => fetchIssue(input).pipe(Effect.provideService(GitHubApiClient, client)),
			fetchPullRequest: (input) => fetchPullRequest(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listIssueComments: (input) => listIssueComments(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listPullRequestComments: (input) =>
				listPullRequestComments(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listPullRequestReviews: (input) =>
				listPullRequestReviews(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listPullRequestReviewComments: (input) =>
				listPullRequestReviewComments(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listPullRequestFiles: (input) =>
				listPullRequestFiles(input).pipe(Effect.provideService(GitHubApiClient, client)),
			fetchPullRequestDiff: (input) =>
				fetchPullRequestDiff(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listPullRequestCommits: (input) =>
				listPullRequestCommits(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listIssueLabels: (input) => listIssueLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listPullRequestLabels: (input) =>
				listPullRequestLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			addIssueLabels: (input) => addIssueLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			addPullRequestLabels: (input) =>
				addPullRequestLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			setIssueLabels: (input) => setIssueLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			setPullRequestLabels: (input) =>
				setPullRequestLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			removeIssueLabel: (input) => removeIssueLabel(input).pipe(Effect.provideService(GitHubApiClient, client)),
			removePullRequestLabel: (input) =>
				removePullRequestLabel(input).pipe(Effect.provideService(GitHubApiClient, client)),
			removeAllIssueLabels: (input) =>
				removeAllIssueLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			removeAllPullRequestLabels: (input) =>
				removeAllPullRequestLabels(input).pipe(Effect.provideService(GitHubApiClient, client)),
			postIssueComment: (input) => postIssueComment(input).pipe(Effect.provideService(GitHubApiClient, client)),
			postPullRequestComment: (input) =>
				postPullRequestComment(input).pipe(Effect.provideService(GitHubApiClient, client)),
			postPullRequestReviewComment: (input) =>
				postPullRequestReviewComment(input).pipe(Effect.provideService(GitHubApiClient, client)),
			replyToReviewComment: (input) =>
				replyToReviewComment(input).pipe(Effect.provideService(GitHubApiClient, client)),
			updateComment: (input) => updateComment(input).pipe(Effect.provideService(GitHubApiClient, client)),
			deleteComment: (input) => deleteComment(input).pipe(Effect.provideService(GitHubApiClient, client)),
			addReaction: (input) => addReaction(input).pipe(Effect.provideService(GitHubApiClient, client)),
			removeReaction: (input) => removeReaction(input).pipe(Effect.provideService(GitHubApiClient, client)),
			closeIssue: (input) => closeIssue(input).pipe(Effect.provideService(GitHubApiClient, client)),
			reopenIssue: (input) => reopenIssue(input).pipe(Effect.provideService(GitHubApiClient, client)),
			closePullRequest: (input) => closePullRequest(input).pipe(Effect.provideService(GitHubApiClient, client)),
			reopenPullRequest: (input) => reopenPullRequest(input).pipe(Effect.provideService(GitHubApiClient, client)),
			mergePullRequest: (input) => mergePullRequest(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listCheckRunsForRef: (input) =>
				listCheckRunsForRef(input).pipe(Effect.provideService(GitHubApiClient, client)),
			fetchCheckRun: (input) => fetchCheckRun(input).pipe(Effect.provideService(GitHubApiClient, client)),
			listCheckRunAnnotations: (input) =>
				listCheckRunAnnotations(input).pipe(Effect.provideService(GitHubApiClient, client)),
			resolveActionsJob: (input) => resolveActionsJob(input).pipe(Effect.provideService(GitHubApiClient, client)),
			fetchActionsJob: (input) => fetchActionsJob(input).pipe(Effect.provideService(GitHubApiClient, client)),
			downloadActionsJobLog: (input) =>
				downloadActionsJobLog(input).pipe(Effect.provideService(GitHubApiClient, client)),
			fetchUserAccess: (input) => fetchUserAccess(input).pipe(Effect.provideService(GitHubApiClient, client)),
		})
	}),
)

/** Live GitHub API implementation with injectable Effect HTTP transport and signer. */
export const GitHubApiLiveBase = GitHubApiOperationsLive.pipe(Layer.provide(GitHubApiClientLive))

/** GitHub API implementation with Web Crypto signing and the standard Fetch transport. */
export const GitHubApiLive = GitHubApiLiveBase.pipe(
	Layer.provide(GitHubAppSigner.layerWebCrypto),
	Layer.provide(FetchHttpClient.layer),
)
