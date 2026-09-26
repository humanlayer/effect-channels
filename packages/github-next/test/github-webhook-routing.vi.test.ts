import { describe, it } from '@effect/vitest'
import {
	DeliveryReceipt,
	type DeliveryAdmission,
	MailboxSubscriptions,
	ProviderEventHandled,
} from '@humanlayer/channels-delivery-next'
import { Deferred, Effect, Layer } from 'effect'
import { vi } from 'vite-plus/test'

import { GitHubApi } from '../src/GitHubApi'
import { GitHubCallbacks } from '../src/GitHubCallbacks'
import { GitHubBotConfiguration, makeGitHubEventProcessor } from '../src/GitHubEventProcessor'
import { GitHubId } from '../src/GitHubIdentity'
import {
	decodeGitHubIdentifiedResponse,
	decodeGitHubNumberedResponse,
	githubEmulatorRequest as request,
	makeGitHubEmulatorFixture,
	makeInMemoryMailboxFixture,
} from './fixtures'

describe('GitHub webhook routing', () => {
	it.effect('processes an emulator issue through its keyed mailbox and calls onIssueCreated', ({ expect }) =>
		Effect.gen(function* () {
			const onIssueCreated = vi.fn(() => Effect.void)
			const processor = makeGitHubEventProcessor({
				namespace: 'github-emulator-test',
				bot: GitHubBotConfiguration.make({ mentionNames: ['agent'], botUserId: GitHubId.make(999) }),
			})
			const mailbox = yield* makeInMemoryMailboxFixture([processor])
			const github = yield* makeGitHubEmulatorFixture({ mailboxDelivery: mailbox.mailboxDelivery })

			const issueResponse = yield* request(github.url, '/repos/alice/project/issues', github.aliceToken, {
				body: { title: 'Process this issue', body: 'Issue body' },
			})
			const issueNumber = (yield* decodeGitHubNumberedResponse(issueResponse)).number
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
			const repo = '/repos/alice/project'
			const issueResponse = yield* request(fixture.url, `${repo}/issues`, fixture.aliceToken, {
				body: { title: 'Emulator issue', body: 'Issue body' },
			})
			const issueNumber = String((yield* decodeGitHubNumberedResponse(issueResponse)).number)
			const issuePath = `${repo}/issues/${issueNumber}`

			yield* request(fixture.url, issuePath, fixture.aliceToken, {
				method: 'PATCH',
				body: { title: 'Edited emulator issue' },
			})
			yield* request(fixture.url, issuePath, fixture.aliceToken, {
				method: 'PATCH',
				body: { state: 'closed' },
			})
			yield* request(fixture.url, issuePath, fixture.aliceToken, {
				method: 'PATCH',
				body: { state: 'open' },
			})
			yield* request(fixture.url, `${issuePath}/assignees`, fixture.aliceToken, {
				body: { assignees: ['reviewer'] },
			})
			yield* request(fixture.url, `${issuePath}/assignees`, fixture.aliceToken, {
				method: 'DELETE',
				body: { assignees: ['reviewer'] },
			})
			yield* request(fixture.url, `${repo}/labels`, fixture.aliceToken, {
				body: { name: 'bug', color: 'ff0000' },
			})
			yield* request(fixture.url, `${issuePath}/labels`, fixture.aliceToken, {
				body: { labels: ['bug'] },
			})
			yield* request(fixture.url, `${issuePath}/labels/bug`, fixture.aliceToken, { method: 'DELETE' })

			const issueCommentResponse = yield* request(fixture.url, `${issuePath}/comments`, fixture.aliceToken, {
				body: { body: '@agent please review' },
			})
			const issueCommentPath = `${repo}/issues/comments/${String((yield* decodeGitHubIdentifiedResponse(issueCommentResponse)).id)}`
			yield* request(fixture.url, issueCommentPath, fixture.aliceToken, {
				method: 'PATCH',
				body: { body: '@agent please review this edit' },
			})
			yield* request(fixture.url, issueCommentPath, fixture.aliceToken, { method: 'DELETE' })

			const pullRequestResponse = yield* request(fixture.url, `${repo}/pulls`, fixture.aliceToken, {
				body: {
					title: 'Emulator pull request',
					body: 'Pull request body',
					head: 'feature',
					base: 'main',
				},
			})
			const pullNumber = String((yield* decodeGitHubNumberedResponse(pullRequestResponse)).number)
			const pullPath = `${repo}/pulls/${pullNumber}`
			yield* request(fixture.url, pullPath, fixture.aliceToken, {
				method: 'PATCH',
				body: { title: 'Edited emulator pull request' },
			})
			yield* request(fixture.url, pullPath, fixture.aliceToken, {
				method: 'PATCH',
				body: { state: 'closed' },
			})
			yield* request(fixture.url, pullPath, fixture.aliceToken, {
				method: 'PATCH',
				body: { state: 'open' },
			})
			yield* request(fixture.url, `${pullPath}/requested_reviewers`, fixture.aliceToken, {
				body: { reviewers: ['reviewer'] },
			})

			const reviewResponse = yield* request(fixture.url, `${pullPath}/reviews`, fixture.reviewerToken, {
				body: { body: 'Looks good', event: 'COMMENT' },
			})
			yield* request(
				fixture.url,
				`${pullPath}/reviews/${String((yield* decodeGitHubIdentifiedResponse(reviewResponse)).id)}/dismissals`,
				fixture.aliceToken,
				{
					method: 'PUT',
					body: { message: 'No longer current' },
				},
			)

			const reviewCommentResponse = yield* request(fixture.url, `${pullPath}/comments`, fixture.reviewerToken, {
				body: { body: 'Please change this line', path: 'README.md', line: 1, side: 'RIGHT' },
			})
			const reviewCommentPath = `${repo}/pulls/comments/${String((yield* decodeGitHubIdentifiedResponse(reviewCommentResponse)).id)}`
			yield* request(fixture.url, reviewCommentPath, fixture.reviewerToken, {
				method: 'PATCH',
				body: { body: 'Please change this edited line' },
			})
			yield* request(fixture.url, reviewCommentPath, fixture.reviewerToken, { method: 'DELETE' })

			yield* Deferred.await(allAdmissionsDelivered)
			expect(admissions).toHaveLength(21)
			const issueResourceId = `github:v1:${fixture.repositoryId}:issue:${issueNumber}`
			const pullRequestResourceId = `github:v1:${fixture.repositoryId}:pull-request:${pullNumber}`
			expect(admissions.filter((admission) => admission.resourceId === issueResourceId)).toHaveLength(11)
			expect(admissions.filter((admission) => admission.resourceId === pullRequestResourceId)).toHaveLength(10)
		}),
	)
})
