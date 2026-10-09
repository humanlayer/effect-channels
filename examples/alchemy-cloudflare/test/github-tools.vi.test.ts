import { describe, it } from '@effect/vitest'
import {
	GitHubApi,
	GitHubDiscussionRef,
	GitHubId,
	GitHubIssueComment,
	GitHubPullRequest,
	GitHubPullRequestRef,
	GitHubReviewComment,
	type GitHubReplyToReviewComment,
} from '@humanlayer/channels-github'
import { ToolResultText } from '@humanlayer/fold-core'
import { Effect, Layer, Ref, Result } from 'effect'

import { postDiscussionComment } from '../src/GitHubTools'

const pullRequest = GitHubPullRequest.make({
	ref: GitHubPullRequestRef.make({
		installationId: GitHubId.make(100),
		repositoryId: GitHubId.make(200),
		owner: 'humanlayer',
		repository: 'effect-channels',
		number: GitHubId.make(43),
	}),
	mailboxKey: 'github:pr:43',
})

const lineComment = (id: number, inReplyToId?: number) =>
	GitHubReviewComment.make({
		ref: { pullRequest: pullRequest.ref, id: GitHubId.make(id) },
		nodeId: `review-comment-${id}`,
		body: 'This can be null.',
		url: `https://github.com/humanlayer/effect-channels/pull/43#discussion_r${id}`,
		author: null,
		reviewId: null,
		path: 'src/app.ts',
		commitId: 'abc',
		originalCommitId: 'abc',
		diffHunk: '@@ -1 +1 @@',
		inReplyToId: inReplyToId === undefined ? null : GitHubId.make(inReplyToId),
	})

const post = (request: { readonly markdown: string; readonly replyTo?: number }) =>
	Effect.gen(function* () {
		const replies = yield* Ref.make<ReadonlyArray<GitHubReplyToReviewComment>>([])
		const posts = yield* Ref.make<ReadonlyArray<string>>([])
		const result = yield* postDiscussionComment(pullRequest, request).pipe(
			Effect.result,
			Effect.provide(
				Layer.mock(GitHubApi, {
					listPullRequestReviewComments: () => Effect.succeed([lineComment(40), lineComment(41, 40)]),
					replyToReviewComment: (input) =>
						Ref.update(replies, (all) => [...all, input]).pipe(Effect.as(lineComment(42, 40))),
					postPullRequestComment: ({ content }) =>
						Ref.update(posts, (all) => [...all, content.markdown]).pipe(
							Effect.as(
								GitHubIssueComment.make({
									ref: {
										discussion: GitHubDiscussionRef.cases.PullRequest.make({
											ref: pullRequest.ref,
										}),
										id: GitHubId.make(50),
									},
									body: content.markdown,
									url: 'https://github.com/humanlayer/effect-channels/pull/43#issuecomment-50',
									author: null,
								}),
							),
						),
				}),
			),
		)
		return { result, replies: yield* Ref.get(replies), posts: yield* Ref.get(posts) }
	})

describe('postDiscussionComment', () => {
	it.effect('posts on the pull request without reply_to', ({ expect }) =>
		Effect.gen(function* () {
			const { result, posts, replies } = yield* post({ markdown: 'Working on it.' })
			expect(result).toEqual(
				Result.succeed(
					ToolResultText.make({
						text: 'Posted https://github.com/humanlayer/effect-channels/pull/43#issuecomment-50',
					}),
				),
			)
			expect(posts).toEqual(['Working on it.'])
			expect(replies).toEqual([])
		}),
	)

	it.effect("replies under the thread's first comment, given any comment in it", ({ expect }) =>
		Effect.gen(function* () {
			const { result, replies, posts } = yield* post({ markdown: 'Fixed.', replyTo: 41 })
			expect(result).toEqual(
				Result.succeed(
					ToolResultText.make({
						text: 'Replied in the thread: https://github.com/humanlayer/effect-channels/pull/43#discussion_r42',
					}),
				),
			)
			expect(replies.map((input) => [input.comment.id, input.content.markdown])).toEqual([[40, 'Fixed.']])
			expect(posts).toEqual([])
		}),
	)

	it.effect('fails for a line comment the pull request does not have', ({ expect }) =>
		Effect.gen(function* () {
			const { result, replies } = yield* post({ markdown: 'Fixed.', replyTo: 99 })
			expect(Result.isFailure(result) && result.failure.message).toBe('This pull request has no line comment 99.')
			expect(replies).toEqual([])
		}),
	)
})
