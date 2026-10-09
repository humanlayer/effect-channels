import {
	GitHubCheckRuns,
	GitHubContent,
	GitHubIssueComments,
	GitHubIssueInfo,
	GitHubPullRequestInfo,
	GitHubReviewComments,
	GitHubReviews,
	GitHubApi,
	GitHubRepositoryRef,
	type GitHubIssue,
	type GitHubPullRequest,
} from '@humanlayer/channels-github'
import { defineTool, ToolResultFailure, ToolResultText, type FoldTool } from '@humanlayer/fold-core'
import { Effect, Match, Predicate, Schema } from 'effect'

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

/** Where an issue's pull request goes: from the issue's branch into the default branch. */
export interface IssuePullRequestTarget {
	readonly issue: GitHubIssue
	readonly branch: string
	readonly base: string
}

/**
 * Open a pull request from the issue's branch, linked to the issue so it closes when the pull request merges. If
 * one is already open from the branch, return that one.
 */
export const openIssuePullRequest = Effect.fn('agent_session.open_issue_pull_request')(function* (
	target: IssuePullRequestTarget,
	request: { readonly title: string; readonly body: string },
) {
	const { issue, branch, base } = target
	const api = yield* GitHubApi
	const repository = GitHubRepositoryRef.make({
		installationId: issue.ref.installationId,
		repositoryId: issue.ref.repositoryId,
		owner: issue.ref.owner,
		repository: issue.ref.repository,
	})
	const [open] = yield* api.listPullRequestsForBranch({ repository, head: branch })
	if (Predicate.isNotUndefined(open)) {
		return ToolResultText.make({ text: `A pull request from ${branch} is already open: ${open.url}` })
	}
	const closes = `Closes #${issue.ref.number}`
	const body = request.body.includes(closes)
		? request.body
		: [request.body.trim(), closes].filter((part) => part.length > 0).join('\n\n')
	const created = yield* api.createPullRequest({ repository, head: branch, base, title: request.title, body })
	return ToolResultText.make({ text: `Opened ${created.url}` })
})

/** The tool that opens the issue's pull request, or returns the one already open. */
export const createPullRequestTool = (target: IssuePullRequestTarget): FoldTool<GitHubApi> =>
	defineTool({
		name: 'github_create_pull_request',
		description: `Open a pull request from ${target.branch} into ${target.base} for this issue. Push your commits with git_push first. If a pull request is already open from ${target.branch}, returns it instead.`,
		parameters: Schema.Struct({
			title: Schema.NonEmptyString.annotate({ description: 'The pull request title' }),
			body: Schema.String.annotate({
				description: `The pull request description, in GitHub Markdown. "Closes #${target.issue.ref.number}" is added if it is missing.`,
			}),
		}),
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: (request) => openIssuePullRequest(target, request).pipe(Effect.mapError(failure)),
	})
