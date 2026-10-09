import { describe, it } from '@effect/vitest'
import {
	GitHubEventId,
	GitHubId,
	GitHubIssueComment,
	GitHubParticipant,
	GitHubPrCommentCreated,
	GitHubPrMentioned,
	GitHubPrReviewCommentCreated,
	GitHubPullRequest,
	GitHubPullRequestRef,
	GitHubReviewComment,
} from '@humanlayer/channels-github'

import { gitHubRequest } from '../src/GithubBot'

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
const alice = GitHubParticipant.make({ id: GitHubId.make(1), login: 'alice', type: 'User' })

const lineComment = (id: number, line: number, body: string, inReplyTo?: number) =>
	GitHubPrReviewCommentCreated.make({
		pullRequest,
		actor: alice,
		eventId: GitHubEventId.make(`line-comment-${id}`),
		comment: GitHubReviewComment.make({
			ref: { pullRequest: pullRequest.ref, id: GitHubId.make(id) },
			nodeId: `review-comment-${id}`,
			body,
			url: `https://github.com/humanlayer/effect-channels/pull/43#discussion_r${id}`,
			author: alice,
			reviewId: GitHubId.make(600),
			path: 'src/app.ts',
			commitId: 'abc',
			originalCommitId: 'abc',
			diffHunk: `@@ -${line} +${line} @@`,
			line,
			inReplyToId: inReplyTo === undefined ? null : GitHubId.make(inReplyTo),
		}),
	})

describe('gitHubRequest', () => {
	it('lists every line comment in the batch, each with the thread to answer it in', ({ expect }) => {
		const first = lineComment(701, 12, '@agent why is this null?')
		const second = lineComment(702, 30, '@agent and rename this', 650)
		const event = GitHubPrMentioned.make({ pullRequest, trigger: first, events: [second] })

		expect(gitHubRequest(event)).toBe(
			[
				'<system-information>These comments were posted together, and they are the request. Answer each line comment in its own review thread with github_post_comment, setting reply_to to its thread. Your final answer is posted on the pull request, so keep it to a short summary.</system-information>',
				'@alice commented on src/app.ts:12 (line comment 701, thread 701):',
				'```diff',
				'@@ -12 +12 @@',
				'```',
				'@agent why is this null?',
				'',
				'---',
				'',
				'@alice commented on src/app.ts:30 (line comment 702, thread 650):',
				'```diff',
				'@@ -30 +30 @@',
				'```',
				'@agent and rename this',
			].join('\n'),
		)
	})

	it('keeps a lone comment as it is', ({ expect }) => {
		const comment = GitHubPrCommentCreated.make({
			pullRequest,
			actor: alice,
			eventId: GitHubEventId.make('comment-800'),
			comment: GitHubIssueComment.make({
				ref: { discussion: { _tag: 'PullRequest', ref: pullRequest.ref }, id: GitHubId.make(800) },
				body: '@agent what does this do?',
				url: 'https://github.com/humanlayer/effect-channels/pull/43#issuecomment-800',
				author: alice,
			}),
		})
		expect(gitHubRequest(GitHubPrMentioned.make({ pullRequest, trigger: comment, events: [] }))).toBe(
			'@agent what does this do?',
		)
	})
})
