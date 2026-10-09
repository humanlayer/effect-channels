import {
	GitHubCheckRuns,
	GitHubContent,
	GitHubIssueComments,
	GitHubIssueInfo,
	GitHubPullRequestInfo,
	GitHubReviewComments,
	GitHubReviews,
	type GitHubApi,
	type GitHubIssue,
	type GitHubPullRequest,
} from '@humanlayer/channels-github'
import { defineTool, ToolResultFailure, ToolResultText, type FoldTool } from '@humanlayer/fold-core'
import { Effect, Match, Schema } from 'effect'

import { NoParameters } from './ToolParameters'

type GitHubDiscussion = GitHubIssue | GitHubPullRequest

const failure = (error: { readonly message: string }) =>
	ToolResultFailure.make({ text: `GitHub request failed: ${error.message}` })

/** A tool result showing a GitHub value as the JSON its schema encodes it to. */
const jsonResult =
	<A, I>(schema: Schema.Codec<A, I>) =>
	(value: A) =>
		Schema.encodeEffect(schema)(value).pipe(
			Effect.map((encoded) => ToolResultText.make({ text: JSON.stringify(encoded, null, 2) })),
		)

/** Tools bound to the GitHub discussion that owns this AgentSession. */
export const githubTools = (discussion: GitHubDiscussion): ReadonlyArray<FoldTool<GitHubApi>> => {
	const context = defineTool({
		name: 'github_discussion',
		description: 'Fetch the current GitHub issue or pull request.',
		parameters: NoParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () =>
			Match.value(discussion).pipe(
				Match.tagsExhaustive({
					GitHubIssue: (issue) => issue.fetchInfo().pipe(Effect.flatMap(jsonResult(GitHubIssueInfo))),
					GitHubPullRequest: (pullRequest) =>
						pullRequest.fetchInfo().pipe(Effect.flatMap(jsonResult(GitHubPullRequestInfo))),
				}),
				Effect.mapError(failure),
			),
	})
	const comments = defineTool({
		name: 'github_comments',
		description: 'List comments on the current GitHub issue or pull request.',
		parameters: NoParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () =>
			discussion.listComments().pipe(Effect.flatMap(jsonResult(GitHubIssueComments)), Effect.mapError(failure)),
	})
	const postComment = defineTool({
		name: 'github_post_comment',
		description: 'Post a Markdown comment on the current GitHub issue or pull request.',
		parameters: Schema.Struct({ markdown: Schema.String }),
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: ({ markdown }) =>
			discussion.postComment(GitHubContent.make({ markdown })).pipe(
				Effect.map(() => ToolResultText.make({ text: 'Comment posted.' })),
				Effect.mapError(failure),
			),
	})
	const pullRequestTools = Match.value(discussion).pipe(
		Match.tag(
			'GitHubPullRequest',
			(pullRequest) =>
				[
					defineTool({
						name: 'github_pull_request_diff',
						description: 'Fetch the diff for the current GitHub pull request.',
						parameters: NoParameters,
						success: ToolResultText,
						failure: ToolResultFailure,
						handler: () =>
							pullRequest.fetchDiff().pipe(
								Effect.map((diff) => ToolResultText.make({ text: diff })),
								Effect.mapError(failure),
							),
					}),
					defineTool({
						name: 'github_pull_request_checks',
						description: 'List check runs for the current GitHub pull request.',
						parameters: NoParameters,
						success: ToolResultText,
						failure: ToolResultFailure,
						handler: () =>
							pullRequest
								.listCheckRuns()
								.pipe(Effect.flatMap(jsonResult(GitHubCheckRuns)), Effect.mapError(failure)),
					}),
					defineTool({
						name: 'github_pull_request_reviews',
						description:
							'List reviews on the current GitHub pull request: who reviewed, their verdict, and their summary.',
						parameters: NoParameters,
						success: ToolResultText,
						failure: ToolResultFailure,
						handler: () =>
							pullRequest
								.listReviews()
								.pipe(Effect.flatMap(jsonResult(GitHubReviews)), Effect.mapError(failure)),
					}),
					defineTool({
						name: 'github_pull_request_review_comments',
						description:
							'List line comments on the current GitHub pull request, with the file, line, and diff they are on.',
						parameters: NoParameters,
						success: ToolResultText,
						failure: ToolResultFailure,
						handler: () =>
							pullRequest
								.listReviewComments()
								.pipe(Effect.flatMap(jsonResult(GitHubReviewComments)), Effect.mapError(failure)),
					}),
				] satisfies ReadonlyArray<FoldTool<GitHubApi>>,
		),
		Match.orElse(() => []),
	)

	return [context, comments, postComment, ...pullRequestTools]
}
