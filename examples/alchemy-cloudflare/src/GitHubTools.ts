import { GitHubContent, type GitHubApi, type GitHubIssue, type GitHubPullRequest } from '@humanlayer/channels-github'
import { defineTool, ToolResultFailure, ToolResultText, type FoldTool } from '@humanlayer/fold-core'
import { Effect, Match, Schema } from 'effect'

type GitHubDiscussion = GitHubIssue | GitHubPullRequest

const EmptyParameters = Schema.Struct({})

const failure = (error: { readonly message: string }) =>
	ToolResultFailure.make({ text: `GitHub request failed: ${error.message}` })

const jsonResult = (value: unknown) => ToolResultText.make({ text: JSON.stringify(value, null, 2) })

/** Tools bound to the GitHub discussion that owns this AgentSession. */
export const githubTools = (discussion: GitHubDiscussion): ReadonlyArray<FoldTool<GitHubApi>> => {
	const context = defineTool({
		name: 'github_discussion',
		description: 'Fetch the current GitHub issue or pull request.',
		parameters: EmptyParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () => discussion.fetchInfo().pipe(Effect.map(jsonResult), Effect.mapError(failure)),
	})
	const comments = defineTool({
		name: 'github_comments',
		description: 'List comments on the current GitHub issue or pull request.',
		parameters: EmptyParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () => discussion.listComments().pipe(Effect.map(jsonResult), Effect.mapError(failure)),
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
						parameters: EmptyParameters,
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
						parameters: EmptyParameters,
						success: ToolResultText,
						failure: ToolResultFailure,
						handler: () =>
							pullRequest.listCheckRuns().pipe(Effect.map(jsonResult), Effect.mapError(failure)),
					}),
				] satisfies ReadonlyArray<FoldTool<GitHubApi>>,
		),
		Match.orElse(() => []),
	)

	return [context, comments, postComment, ...pullRequestTools]
}
