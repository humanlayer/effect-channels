/**
 * The agent's git tools for the operations that reach GitHub: fetch, pull, and push, plus merge. The
 * workspace adds the GitHub App's credentials to each request, so no tool takes or returns a token. Local
 * git work (status, diff, add, commit, branch) stays in `bash`.
 */
import type { GitHubRepositoryRef } from '@humanlayer/channels-github'
import { defineTool, ToolResultFailure, ToolResultText, type FoldTool } from '@humanlayer/fold-core'
import { Effect, Schema } from 'effect'

import { NoParameters } from './ToolParameters'
import { Workspace } from './Workspace'

const failure = (error: { readonly message: string }) => ToolResultFailure.make({ text: error.message })

const shortCommit = (commit: string) => commit.slice(0, 7)

/**
 * Git tools for the session's repository. `git_push` exists only when the session has a branch of its own,
 * and pushes only that branch.
 */
export const gitTools = (input: {
	readonly repository: GitHubRepositoryRef
	readonly workBranch: string | null
}): ReadonlyArray<FoldTool<Workspace>> => {
	const { repository, workBranch } = input

	const fetch = defineTool({
		name: 'git_fetch',
		description:
			"Fetch a branch from GitHub into `origin/<branch>`, such as the default branch before merging it. Doesn't change the working tree.",
		parameters: Schema.Struct({ branch: Schema.NonEmptyString.annotate({ description: 'The remote branch' }) }),
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: ({ branch }) =>
			Effect.flatMap(Workspace, (workspace) => workspace.fetchBranch({ repository, branch })).pipe(
				Effect.map((fetched) =>
					ToolResultText.make({
						text: `Fetched origin/${fetched.branch} at ${shortCommit(fetched.commit)}.`,
					}),
				),
				Effect.mapError(failure),
			),
	})

	const pull = defineTool({
		name: 'git_pull',
		description:
			'Fast-forward the checked-out branch to its branch on GitHub. Fails if local commits or uncommitted changes are in the way.',
		parameters: NoParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () =>
			Effect.flatMap(Workspace, (workspace) => workspace.pull({ repository })).pipe(
				Effect.map(({ before, after }) =>
					ToolResultText.make({
						text:
							before === after
								? 'Already up to date.'
								: `Fast-forwarded from ${shortCommit(before)} to ${shortCommit(after)}.`,
					}),
				),
				Effect.mapError(failure),
			),
	})

	const merge = defineTool({
		name: 'git_merge',
		description:
			'Merge a branch or ref, such as `origin/main` after git_fetch, into the checked-out branch. Commit or discard your changes first. A conflict aborts the merge and changes nothing.',
		parameters: Schema.Struct({
			ref: Schema.NonEmptyString.annotate({ description: 'The branch or ref to merge' }),
		}),
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: ({ ref }) =>
			Effect.flatMap(Workspace, (workspace) => workspace.mergeRef({ repository, ref })).pipe(
				Effect.map((merged) =>
					ToolResultText.make({
						text: merged.alreadyMerged
							? `${ref} is already merged.`
							: `Merged ${ref}${merged.fastForward ? ' (fast-forward)' : ''}; HEAD is ${shortCommit(merged.commit)}.`,
					}),
				),
				Effect.mapError(failure),
			),
	})

	if (workBranch === null) return [fetch, pull, merge]

	const push = defineTool({
		name: 'git_push',
		description: `Push your commits on ${workBranch} to GitHub. ${workBranch} must be checked out. Never force-pushes.`,
		parameters: NoParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () =>
			Effect.flatMap(Workspace, (workspace) => workspace.pushBranch({ repository, branch: workBranch })).pipe(
				Effect.map((pushed) =>
					ToolResultText.make({ text: `Pushed ${pushed.branch} at ${shortCommit(pushed.commit)}.` }),
				),
				Effect.mapError(failure),
			),
	})

	return [fetch, pull, merge, push]
}
