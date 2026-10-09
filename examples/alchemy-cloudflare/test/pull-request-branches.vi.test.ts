import { describe, it } from '@effect/vitest'
import {
	GitHubApi,
	GitHubId,
	GitHubIssue,
	GitHubIssueRef,
	GitHubPullRequest,
	GitHubPullRequestInfo,
	GitHubPullRequestRef,
	type GitHubCreatePullRequest,
} from '@humanlayer/channels-github'
import { ToolResultText } from '@humanlayer/fold-core'
import { Effect, Layer, Ref, Result } from 'effect'

import { workBranchFor } from '../src/AgentSessionDO'
import { openIssuePullRequest } from '../src/GitHubTools'

const repository = {
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'humanlayer',
	repository: 'effect-channels',
}
const issue = GitHubIssue.make({
	ref: GitHubIssueRef.make({ ...repository, number: GitHubId.make(42) }),
	mailboxKey: 'github:issue:42',
})
const pullRequest = GitHubPullRequest.make({
	ref: GitHubPullRequestRef.make({ ...repository, number: GitHubId.make(43) }),
	mailboxKey: 'github:pr:43',
})

const infoFor = (number: number, headRepository: GitHubPullRequestInfo['headRepository']) =>
	GitHubPullRequestInfo.make({
		ref: GitHubPullRequestRef.make({ ...repository, number: GitHubId.make(number) }),
		title: 'Fix the crash',
		body: null,
		state: 'open',
		url: `https://github.com/humanlayer/effect-channels/pull/${number}`,
		author: null,
		draft: false,
		merged: false,
		headRef: 'fix-crash',
		headSha: 'abc',
		headRepository,
		baseRef: 'main',
		baseSha: 'def',
	})

const sameRepository = { repositoryId: GitHubId.make(200), owner: 'humanlayer', repository: 'effect-channels' }

describe('workBranchFor', () => {
	const branchOf = (discussion: GitHubIssue | GitHubPullRequest, info = infoFor(43, sameRepository)) =>
		workBranchFor(discussion).pipe(
			Effect.result,
			Effect.provide(Layer.mock(GitHubApi, { fetchPullRequest: () => Effect.succeed(info) })),
		)

	it.effect("uses the issue's own branch", ({ expect }) =>
		Effect.gen(function* () {
			expect(yield* branchOf(issue)).toEqual(Result.succeed('humanlayer/issue-42'))
		}),
	)

	it.effect("uses the pull request's branch", ({ expect }) =>
		Effect.gen(function* () {
			expect(yield* branchOf(pullRequest)).toEqual(Result.succeed('fix-crash'))
		}),
	)

	it.effect("refuses a pull request's branch in a fork, saying why", ({ expect }) =>
		Effect.gen(function* () {
			const fork = { repositoryId: GitHubId.make(300), owner: 'someone', repository: 'effect-channels' }
			const result = yield* branchOf(pullRequest, infoFor(43, fork))
			expect(Result.isFailure(result) && result.failure.message).toBe(
				"I can't work on this pull request's branch: its branch is in the fork someone/effect-channels, which I can't push to.",
			)
		}),
	)

	it.effect('refuses a pull request whose branch was deleted, saying why', ({ expect }) =>
		Effect.gen(function* () {
			const result = yield* branchOf(pullRequest, infoFor(43, null))
			expect(Result.isFailure(result) && result.failure.message).toBe(
				"I can't work on this pull request's branch: its branch has been deleted.",
			)
		}),
	)
})

describe('openIssuePullRequest', () => {
	const target = { issue, branch: 'humanlayer/issue-42', base: 'main' }

	const open = (existing: ReadonlyArray<GitHubPullRequestInfo>, body: string, draft?: boolean) =>
		Effect.gen(function* () {
			const created = yield* Ref.make<ReadonlyArray<GitHubCreatePullRequest>>([])
			const result = yield* openIssuePullRequest(target, { title: 'Fix the crash', body, draft }).pipe(
				Effect.provide(
					Layer.mock(GitHubApi, {
						listPullRequestsForBranch: () => Effect.succeed(existing),
						createPullRequest: (input) =>
							Ref.update(created, (all) => [...all, input]).pipe(Effect.as(infoFor(44, sameRepository))),
					}),
				),
			)
			return { result, created: yield* Ref.get(created) }
		})

	it.effect('opens a pull request from the branch, closing the issue', ({ expect }) =>
		Effect.gen(function* () {
			const { result, created } = yield* open([], 'Handles a null config.')

			expect(result).toEqual(
				ToolResultText.make({
					text: 'Opened a pull request: https://github.com/humanlayer/effect-channels/pull/44',
				}),
			)
			expect(created).toEqual([
				{
					repository,
					head: 'humanlayer/issue-42',
					base: 'main',
					title: 'Fix the crash',
					body: 'Handles a null config.\n\nCloses #42',
					draft: false,
				},
			])
		}),
	)

	it.effect('opens a draft pull request when asked to', ({ expect }) =>
		Effect.gen(function* () {
			const { result, created } = yield* open([], 'Handles a null config.', true)

			expect(result).toEqual(
				ToolResultText.make({
					text: 'Opened a draft pull request: https://github.com/humanlayer/effect-channels/pull/44',
				}),
			)
			expect(created.map((input) => input.draft)).toEqual([true])
		}),
	)

	it.effect('keeps a body that already closes the issue', ({ expect }) =>
		Effect.gen(function* () {
			const { created } = yield* open([], 'Closes #42 by handling a null config.')
			expect(created.map((input) => input.body)).toEqual(['Closes #42 by handling a null config.'])
		}),
	)

	it.effect('returns the pull request already open from the branch', ({ expect }) =>
		Effect.gen(function* () {
			const { result, created } = yield* open([infoFor(43, sameRepository)], 'Handles a null config.')

			expect(result).toEqual(
				ToolResultText.make({
					text: 'A pull request from humanlayer/issue-42 is already open: https://github.com/humanlayer/effect-channels/pull/43',
				}),
			)
			expect(created).toEqual([])
		}),
	)
})
