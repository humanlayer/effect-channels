import { Context, Effect, Layer } from 'effect'

import { GitHubApiClient } from './api/GitHubApiClient'
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
	listRepositoryLabels,
	mergePullRequest,
	listPullRequestsForBranch,
	createPullRequest,
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
import type { GitHubApiOperations } from './GitHubApiContract'

export * from './GitHubApiContract'

/** Application-wide GitHub operations. Transport, credentials, pagination, and decoding stay behind this service. */
export class GitHubApi extends Context.Service<GitHubApi, GitHubApiOperations>()(
	'@humanlayer/channels-github/GitHubApi',
	{
		make: Effect.gen(function* () {
			const client = yield* GitHubApiClient
			const run = <A, E>(effect: Effect.Effect<A, E, GitHubApiClient>) =>
				Effect.provideService(effect, GitHubApiClient, client)
			return {
				addIssueLabels: (input) => run(addIssueLabels(input)),
				addPullRequestLabels: (input) => run(addPullRequestLabels(input)),
				addReaction: (input) => run(addReaction(input)),
				closeIssue: (input) => run(closeIssue(input)),
				closePullRequest: (input) => run(closePullRequest(input)),
				deleteComment: (input) => run(deleteComment(input)),
				downloadActionsJobLog: (input) => run(downloadActionsJobLog(input)),
				fetchActionsJob: (input) => run(fetchActionsJob(input)),
				fetchCheckRun: (input) => run(fetchCheckRun(input)),
				fetchIssue: (input) => run(fetchIssue(input)),
				fetchPullRequest: (input) => run(fetchPullRequest(input)),
				fetchPullRequestDiff: (input) => run(fetchPullRequestDiff(input)),
				fetchUserAccess: (input) => run(fetchUserAccess(input)),
				listCheckRunAnnotations: (input) => run(listCheckRunAnnotations(input)),
				listCheckRunsForRef: (input) => run(listCheckRunsForRef(input)),
				listIssueComments: (input) => run(listIssueComments(input)),
				listIssueLabels: (input) => run(listIssueLabels(input)),
				listPullRequestComments: (input) => run(listPullRequestComments(input)),
				listPullRequestCommits: (input) => run(listPullRequestCommits(input)),
				listPullRequestFiles: (input) => run(listPullRequestFiles(input)),
				listPullRequestLabels: (input) => run(listPullRequestLabels(input)),
				listPullRequestReviewComments: (input) => run(listPullRequestReviewComments(input)),
				listPullRequestReviews: (input) => run(listPullRequestReviews(input)),
				listRepositoryLabels: (input) => run(listRepositoryLabels(input)),
				mergePullRequest: (input) => run(mergePullRequest(input)),
				listPullRequestsForBranch: (input) => run(listPullRequestsForBranch(input)),
				createPullRequest: (input) => run(createPullRequest(input)),
				postIssueComment: (input) => run(postIssueComment(input)),
				postPullRequestComment: (input) => run(postPullRequestComment(input)),
				postPullRequestReviewComment: (input) => run(postPullRequestReviewComment(input)),
				removeAllIssueLabels: (input) => run(removeAllIssueLabels(input)),
				removeAllPullRequestLabels: (input) => run(removeAllPullRequestLabels(input)),
				removeIssueLabel: (input) => run(removeIssueLabel(input)),
				removePullRequestLabel: (input) => run(removePullRequestLabel(input)),
				removeReaction: (input) => run(removeReaction(input)),
				reopenIssue: (input) => run(reopenIssue(input)),
				reopenPullRequest: (input) => run(reopenPullRequest(input)),
				replyToReviewComment: (input) => run(replyToReviewComment(input)),
				resolveActionsJob: (input) => run(resolveActionsJob(input)),
				setIssueLabels: (input) => run(setIssueLabels(input)),
				setPullRequestLabels: (input) => run(setPullRequestLabels(input)),
				updateComment: (input) => run(updateComment(input)),
			}
		}),
	},
) {
	static readonly layer = Layer.effect(this, this.make)
}
