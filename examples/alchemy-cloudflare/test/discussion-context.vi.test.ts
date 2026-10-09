import { describe, it } from '@effect/vitest'
import {
	GitHubApi,
	GitHubDiscussionRef,
	GitHubId,
	GitHubIssue,
	GitHubIssueComment,
	GitHubIssueInfo,
	GitHubIssueRef,
	GitHubParticipant,
	GitHubPullRequest,
	GitHubPullRequestInfo,
	GitHubPullRequestRef,
	GitHubReviewComment,
} from '@humanlayer/channels-github'
import { Effect, Layer, Option } from 'effect'

import { MentionedIn, readDiscussionContext, type DiscussionSeen } from '../src/DiscussionContext'

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
const BOT_USER_ID = 999
const alice = GitHubParticipant.make({ id: GitHubId.make(1), login: 'alice', type: 'User' })
const bob = GitHubParticipant.make({ id: GitHubId.make(2), login: 'bob', type: 'User' })
const bot = GitHubParticipant.make({ id: GitHubId.make(BOT_USER_ID), login: 'agent[bot]', type: 'Bot' })

const comment = (discussion: GitHubDiscussionRef, id: number, author: GitHubParticipant, body: string) =>
	GitHubIssueComment.make({
		ref: { discussion, id: GitHubId.make(id) },
		body,
		url: `https://github.com/humanlayer/effect-channels/issues/42#issuecomment-${id}`,
		author,
	})

const issueComments = [
	comment(GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref }), 10, alice, 'It crashes on start.'),
	comment(GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref }), 11, bot, 'I could not finish.'),
	comment(GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref }), 12, bob, 'Same here, on Linux.'),
	comment(GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref }), 13, alice, '@agent please fix it'),
]

const api = Layer.mock(GitHubApi, {
	fetchIssue: () =>
		Effect.succeed(
			GitHubIssueInfo.make({
				ref: issue.ref,
				title: 'App crashes',
				body: 'It crashes when I open it.',
				state: 'open',
				url: 'https://github.com/humanlayer/effect-channels/issues/42',
				author: alice,
			}),
		),
	listIssueComments: () => Effect.succeed(issueComments),
	fetchPullRequest: () =>
		Effect.succeed(
			GitHubPullRequestInfo.make({
				ref: pullRequest.ref,
				title: 'Fix the crash',
				body: null,
				state: 'open',
				url: 'https://github.com/humanlayer/effect-channels/pull/43',
				author: alice,
				draft: false,
				merged: false,
				headRef: 'fix',
				headSha: 'abc',
				baseRef: 'main',
				baseSha: 'def',
			}),
		),
	listPullRequestComments: () => Effect.succeed([]),
	listPullRequestReviews: () =>
		Effect.succeed([
			{
				ref: { pullRequest: pullRequest.ref, id: GitHubId.make(30), nodeId: 'review-30' },
				body: 'Needs a test.',
				author: bob,
				state: 'changes_requested' as const,
				commitId: 'abc',
				url: 'https://github.com/humanlayer/effect-channels/pull/43#pullrequestreview-30',
			},
		]),
	listPullRequestReviewComments: () =>
		Effect.succeed([
			GitHubReviewComment.make({
				ref: { pullRequest: pullRequest.ref, id: GitHubId.make(40) },
				nodeId: 'review-comment-40',
				body: 'This can be null.',
				url: 'https://github.com/humanlayer/effect-channels/pull/43#discussion_r40',
				author: bob,
				reviewId: GitHubId.make(30),
				path: 'src/app.ts',
				commitId: 'abc',
				originalCommitId: 'abc',
				diffHunk: '@@ -1 +1 @@',
				line: 12,
			}),
			GitHubReviewComment.make({
				ref: { pullRequest: pullRequest.ref, id: GitHubId.make(41) },
				nodeId: 'review-comment-41',
				body: '@agent fix this',
				url: 'https://github.com/humanlayer/effect-channels/pull/43#discussion_r41',
				author: alice,
				reviewId: null,
				path: 'src/app.ts',
				commitId: 'abc',
				originalCommitId: 'abc',
				diffHunk: '@@ -1 +1 @@',
				line: 12,
				inReplyToId: GitHubId.make(40),
			}),
		]),
})

const read = (input: {
	readonly discussion: GitHubIssue | GitHubPullRequest
	readonly seen?: DiscussionSeen
	readonly mentionedIn?: MentionedIn
}) => {
	const context = readDiscussionContext({
		discussion: input.discussion,
		seen: Option.fromNullishOr(input.seen),
		botUserId: BOT_USER_ID,
		mentionedIn: input.mentionedIn,
	})
	return Effect.provide(context, api)
}

describe('readDiscussionContext', () => {
	it.effect('shows the whole issue the first time, without the bot or the mention', ({ expect }) =>
		Effect.gen(function* () {
			const context = yield* read({
				discussion: issue,
				mentionedIn: MentionedIn.cases.Comment.make({ id: GitHubId.make(13) }),
			})

			expect(context.text).toBe(
				[
					'<github-discussion>',
					'Issue #42 "App crashes" (open), opened by @alice:',
					'It crashes when I open it.',
					'',
					'Comments:',
					'',
					'@alice commented:',
					'It crashes on start.',
					'',
					'@bob commented:',
					'Same here, on Linux.',
					'</github-discussion>',
					'',
					'',
				].join('\n'),
			)
			expect(context.seen).toEqual({ comment: 13, review: 0, reviewComment: 0 })
		}),
	)

	it.effect('shows only what is new since the last turn', ({ expect }) =>
		Effect.gen(function* () {
			const context = yield* read({ discussion: issue, seen: { comment: 11, review: 0, reviewComment: 0 } })

			expect(context.text).toBe(
				[
					'<github-discussion>',
					'New since your last turn:',
					'',
					'Comments:',
					'',
					'@bob commented:',
					'Same here, on Linux.',
					'',
					'@alice commented:',
					'@agent please fix it',
					'</github-discussion>',
					'',
					'',
				].join('\n'),
			)
			expect(context.seen).toEqual({ comment: 13, review: 0, reviewComment: 0 })
		}),
	)

	it.effect('shows nothing when nothing is new', ({ expect }) =>
		Effect.gen(function* () {
			const seen = { comment: 13, review: 0, reviewComment: 0 }
			expect(yield* read({ discussion: issue, seen })).toEqual({ text: '', seen })
		}),
	)

	it.effect('shows a pull request with its reviews and line comments', ({ expect }) =>
		Effect.gen(function* () {
			const context = yield* read({
				discussion: pullRequest,
				mentionedIn: MentionedIn.cases.ReviewComment.make({ id: GitHubId.make(41) }),
			})

			expect(context.text).toBe(
				[
					'<github-discussion>',
					'Pull request #43 "Fix the crash" (open), opened by @alice:',
					'(no description)',
					'',
					'Reviews:',
					'',
					'@bob reviewed (changes requested):',
					'Needs a test.',
					'',
					'Line comments:',
					'',
					'@bob commented on src/app.ts:12:',
					'This can be null.',
					'</github-discussion>',
					'',
					'',
				].join('\n'),
			)
			expect(context.seen).toEqual({ comment: 0, review: 30, reviewComment: 41 })
		}),
	)
})
