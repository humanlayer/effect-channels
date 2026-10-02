import { describe, it } from '@effect/vitest'
import {
	DeliveryContext,
	DeliveryReceipt,
	type DeliveryAdmission,
	MailboxSubscriptions,
	ProviderEventHandled,
} from '@humanlayer/channels-delivery-next'
import { Deferred, Effect, Layer } from 'effect'
import { vi } from 'vite-plus/test'

import { GitHubApi } from '../src/GitHubApi'
import { GitHubCallbacks, type GitHubCallbackHandlers } from '../src/GitHubCallbacks'
import { GitHubBotConfiguration, makeGitHubEventProcessor } from '../src/GitHubEventProcessor'
import { GitHubId } from '../src/GitHubIdentity'
import { githubEmulatorEndpoints, makeGitHubEmulatorFixture, makeInMemoryMailboxFixture } from './fixtures'

type Handler<K extends keyof GitHubCallbackHandlers<never, never>> = NonNullable<
	GitHubCallbackHandlers<never, never>[K]
>

describe('GitHub webhook routing', () => {
	it.effect('processes an emulator issue through its keyed mailbox and calls onIssueCreated', ({ expect }) =>
		Effect.gen(function* () {
			const onIssueCreated = vi.fn<Handler<'onIssueCreated'>>(() => Effect.void)
			const processor = makeGitHubEventProcessor({
				namespace: 'github-emulator-test',
				bot: GitHubBotConfiguration.make({ mentionNames: ['agent'], botUserId: GitHubId.make(999) }),
			})
			const mailbox = yield* makeInMemoryMailboxFixture([processor])
			const github = yield* makeGitHubEmulatorFixture({ mailboxDelivery: mailbox.mailboxDelivery })

			const { number: issueNumber } = yield* github.request(githubEmulatorEndpoints.createIssue, {
				params: undefined,
				token: github.aliceToken,
				body: { title: 'Process this issue', body: 'Issue body' },
			})
			const mailboxKey = yield* mailbox.awaitMailboxKey

			expect(onIssueCreated).not.toHaveBeenCalled()
			const processingLayer = Layer.mergeAll(
				GitHubCallbacks.layer({ onIssueCreated }),
				Layer.mock(GitHubApi, {}),
				Layer.mock(MailboxSubscriptions, { isSubscribed: () => Effect.succeed(false) }),
			)
			expect(yield* mailbox.processNext(mailboxKey).pipe(Effect.provide(processingLayer))).toEqual(
				ProviderEventHandled.make({}),
			)
			expect(onIssueCreated).toHaveBeenCalledOnce()
			expect(onIssueCreated).toHaveBeenCalledWith(
				expect.objectContaining({
					_tag: 'GitHubIssueCreated',
					trigger: expect.objectContaining({ _tag: 'GitHubIssueOpened', title: 'Process this issue' }),
					issue: expect.objectContaining({ ref: expect.objectContaining({ number: issueNumber }) }),
				}),
				expect.any(DeliveryContext),
			)
		}),
	)

	it.live('delivers every webhook the emulator can produce through normal REST mutations', ({ expect }) =>
		Effect.gen(function* () {
			const admissions: Array<DeliveryAdmission> = []
			const allAdmissionsDelivered = yield* Deferred.make<void>()
			const fixture = yield* makeGitHubEmulatorFixture({
				mailboxDelivery: {
					deliver: (admission) =>
						Effect.gen(function* () {
							admissions.push(admission)
							if (admissions.length === 21) yield* Deferred.succeed(allAdmissionsDelivered, undefined)
							return DeliveryReceipt.make({ mailboxKey: admission.resourceId, accepted: true })
						}),
				},
			})
			const endpoints = githubEmulatorEndpoints
			const alice = fixture.aliceToken
			const reviewer = fixture.reviewerToken
			const { number: issueNumber } = yield* fixture.request(endpoints.createIssue, {
				params: undefined,
				token: alice,
				body: { title: 'Emulator issue', body: 'Issue body' },
			})
			const issue = { issue: issueNumber }
			yield* fixture.send(endpoints.editIssueTitle, {
				params: issue,
				token: alice,
				body: { title: 'Edited emulator issue' },
			})
			yield* fixture.send(endpoints.closeIssue, {
				params: issue,
				token: alice,
				body: { state: 'closed', state_reason: 'completed' },
			})
			yield* fixture.send(endpoints.reopenIssue, {
				params: issue,
				token: alice,
				body: { state: 'open', state_reason: 'reopened' },
			})
			yield* fixture.send(endpoints.addAssignees, {
				params: issue,
				token: alice,
				body: { assignees: ['reviewer'] },
			})
			yield* fixture.send(endpoints.removeAssignees, {
				params: issue,
				token: alice,
				body: { assignees: ['reviewer'] },
			})
			yield* fixture.send(endpoints.createLabel, {
				params: undefined,
				token: alice,
				body: { name: 'bug', color: 'ff0000' },
			})
			yield* fixture.send(endpoints.addIssueLabels, { params: issue, token: alice, body: { labels: ['bug'] } })
			yield* fixture.send(endpoints.removeIssueLabel, {
				params: { ...issue, label: 'bug' },
				token: alice,
				body: undefined,
			})

			const issueComment = yield* fixture.request(endpoints.postIssueComment, {
				params: issue,
				token: alice,
				body: { body: '@agent please review' },
			})
			yield* fixture.send(endpoints.updateIssueComment, {
				params: { comment: issueComment.id },
				token: alice,
				body: { body: '@agent please review this edit' },
			})
			yield* fixture.send(endpoints.deleteIssueComment, {
				params: { comment: issueComment.id },
				token: alice,
				body: undefined,
			})

			const pull = yield* fixture.request(endpoints.createPullRequest, {
				params: undefined,
				token: alice,
				body: { title: 'Emulator pull request', body: 'Pull request body', head: 'feature', base: 'main' },
			})
			const pullRequest = { pullRequest: pull.number }
			yield* fixture.send(endpoints.editPullRequestTitle, {
				params: pullRequest,
				token: alice,
				body: { title: 'Edited emulator pull request' },
			})
			yield* fixture.send(endpoints.closePullRequest, {
				params: pullRequest,
				token: alice,
				body: { state: 'closed' },
			})
			yield* fixture.send(endpoints.reopenPullRequest, {
				params: pullRequest,
				token: alice,
				body: { state: 'open' },
			})
			yield* fixture.send(endpoints.requestReviewers, {
				params: pullRequest,
				token: alice,
				body: { reviewers: ['reviewer'] },
			})

			const review = yield* fixture.request(endpoints.createReview, {
				params: pullRequest,
				token: reviewer,
				body: { body: 'Looks good', event: 'COMMENT' },
			})
			yield* fixture.send(endpoints.dismissReview, {
				params: { ...pullRequest, review: review.id },
				token: alice,
				body: { message: 'No longer current' },
			})

			const reviewComment = yield* fixture.request(endpoints.postReviewComment, {
				params: pullRequest,
				token: reviewer,
				body: {
					body: 'Please change this line',
					commit_id: pull.head.sha,
					path: 'README.md',
					line: 1,
					side: 'RIGHT',
				},
			})
			yield* fixture.send(endpoints.updateReviewComment, {
				params: { comment: reviewComment.id },
				token: reviewer,
				body: { body: 'Please change this edited line' },
			})
			yield* fixture.send(endpoints.deleteReviewComment, {
				params: { comment: reviewComment.id },
				token: reviewer,
				body: undefined,
			})

			yield* Deferred.await(allAdmissionsDelivered)
			expect(admissions).toHaveLength(21)
			const issueResourceId = `github:v1:${fixture.repositoryId}:issue:${issueNumber}`
			const pullRequestResourceId = `github:v1:${fixture.repositoryId}:pull-request:${pull.number}`
			expect(admissions.filter((admission) => admission.resourceId === issueResourceId)).toHaveLength(11)
			expect(admissions.filter((admission) => admission.resourceId === pullRequestResourceId)).toHaveLength(10)
		}),
	)
})
