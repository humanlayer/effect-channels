import { describe, it } from '@effect/vitest'
import { MailboxSubscriptionCreatedResult, MailboxSubscriptions } from '@humanlayer/channels-delivery'
import {
	type GitHubAccessLevel,
	GitHubApi,
	GitHubApiError,
	GitHubCallbacks,
	GitHubContent,
	GitHubDiscussionRef,
	GitHubEventId,
	GitHubId,
	GitHubIssue,
	GitHubIssueComment,
	GitHubIssueCommentCreated,
	GitHubIssueMentioned,
	GitHubIssueOpened,
	GitHubIssueRef,
	GitHubParticipant,
	GitHubPrCommentCreated,
	GitHubPrMentioned,
	GitHubPrOpened,
	GitHubPrReviewCommentCreated,
	GitHubPullRequest,
	GitHubPullRequestRef,
	GitHubReactionTarget,
	GitHubReviewComment,
	type GitHubReactionRequest,
	type GitHubUserAccessRequest,
} from '@humanlayer/channels-github'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { ConfigProvider, Effect, Layer, Ref } from 'effect'

import { makeTestDeliveryExecution } from '../../../packages/delivery/test/delivery-execution'
import { AgentSessions } from '../src/AgentSessionDO'
import { AutoLabel } from '../src/AutoLabel'
import { AgentSessionMessage } from '../src/DeliveryTurn'
import { githubHandlers, maintainerOnlyNotice, respondToMentionAccess } from '../src/GithubBot'

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
const issueDiscussion = GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref })
const prDiscussion = GitHubDiscussionRef.cases.PullRequest.make({ ref: pullRequest.ref })
const person = (id: number) => GitHubParticipant.make({ id: GitHubId.make(id), login: `user-${id}`, type: 'User' })
const botAuthor = GitHubParticipant.make({ id: GitHubId.make(999), login: 'agent[bot]', type: 'Bot' })
const discussionComment = (
	discussion: GitHubDiscussionRef,
	author: GitHubParticipant | null,
	body = maintainerOnlyNotice,
	id = 500,
) =>
	GitHubIssueComment.make({
		ref: { discussion, id: GitHubId.make(id) },
		body,
		url: 'https://github.com/humanlayer/effect-channels/issues/42#issuecomment-500',
		author,
	})

const fixtures = (actor = person(1)) => {
	const eventId = GitHubEventId.make(`mention-by-${actor.login}`)
	const issueOpened = GitHubIssueOpened.make({ issue, actor, eventId, title: '@agent help', body: null })
	const prOpened = GitHubPrOpened.make({ pullRequest, actor, eventId, title: 'Help', body: '@agent help' })
	const issueComment = GitHubIssueCommentCreated.make({
		issue,
		actor,
		eventId,
		comment: discussionComment(issueDiscussion, actor, '@agent help', actor.id),
	})
	const prComment = GitHubPrCommentCreated.make({
		pullRequest,
		actor,
		eventId,
		comment: discussionComment(prDiscussion, actor, '@agent help', actor.id),
	})
	const reviewComment = GitHubPrReviewCommentCreated.make({
		pullRequest,
		actor,
		eventId,
		comment: GitHubReviewComment.make({
			ref: { pullRequest: pullRequest.ref, id: GitHubId.make(700 + actor.id) },
			nodeId: 'review-comment-node',
			body: '@agent help',
			url: 'https://github.com/humanlayer/effect-channels/pull/43#discussion_r701',
			author: actor,
			reviewId: GitHubId.make(600),
			path: 'src/index.ts',
			commitId: 'head-sha',
			originalCommitId: 'original-sha',
			diffHunk: '@@ -1 +1 @@',
		}),
	})
	return [
		{
			name: 'issue opening',
			event: GitHubIssueMentioned.make({ issue, trigger: issueOpened, events: [issueOpened] }),
			discussion: issueDiscussion,
			target: GitHubReactionTarget.cases.Discussion.make({ discussion: issueDiscussion }),
		},
		{
			name: 'PR opening',
			prompt: 'Help\n@agent help',
			event: GitHubPrMentioned.make({ pullRequest, trigger: prOpened, events: [prOpened] }),
			discussion: prDiscussion,
			target: GitHubReactionTarget.cases.Discussion.make({ discussion: prDiscussion }),
		},
		{
			name: 'issue comment',
			mentionedIn: { _tag: 'Comment', id: issueComment.comment.ref.id },
			event: GitHubIssueMentioned.make({ issue, trigger: issueComment, events: [issueComment] }),
			discussion: issueDiscussion,
			target: GitHubReactionTarget.cases.Comment.make({ comment: issueComment.comment.ref }),
		},
		{
			name: 'PR comment',
			mentionedIn: { _tag: 'Comment', id: prComment.comment.ref.id },
			event: GitHubPrMentioned.make({ pullRequest, trigger: prComment, events: [prComment] }),
			discussion: prDiscussion,
			target: GitHubReactionTarget.cases.Comment.make({ comment: prComment.comment.ref }),
		},
		{
			name: 'review comment',
			prompt: [
				'<system-information>This request is line comment 701 on src/index.ts, in the review thread of line comment 701. To reply in that thread, use github_post_comment with reply_to 701. The diff around it:',
				'```diff',
				'@@ -1 +1 @@',
				'```</system-information>',
				'',
				'@agent help',
			].join('\n'),
			mentionedIn: { _tag: 'ReviewComment', id: reviewComment.comment.ref.id },
			event: GitHubPrMentioned.make({ pullRequest, trigger: reviewComment, events: [reviewComment] }),
			discussion: prDiscussion,
			target: GitHubReactionTarget.cases.Comment.make({ comment: reviewComment.comment.ref }),
		},
	]
}

interface ApiState {
	readonly lookups: ReadonlyArray<GitHubUserAccessRequest>
	readonly reactions: ReadonlyArray<GitHubReactionRequest>
	readonly lists: ReadonlyArray<GitHubDiscussionRef>
	readonly posts: ReadonlyArray<{ readonly discussion: GitHubDiscussionRef; readonly content: GitHubContent }>
	readonly comments: ReadonlyArray<GitHubIssueComment>
}

const makeApi = (
	access: Effect.Effect<GitHubAccessLevel, GitHubApiError>,
	options: {
		readonly comments?: ReadonlyArray<GitHubIssueComment>
		readonly lookupFailure?: GitHubApiError
		readonly ambiguousPostFailure?: GitHubApiError
	} = {},
) =>
	Effect.gen(function* () {
		const state = yield* Ref.make<ApiState>({
			lookups: [],
			reactions: [],
			lists: [],
			posts: [],
			comments: options.comments ?? [],
		})
		const list = (discussion: GitHubDiscussionRef) =>
			Effect.gen(function* () {
				yield* Ref.update(state, (current) => ({ ...current, lists: [...current.lists, discussion] }))
				if (options.lookupFailure !== undefined) return yield* Effect.fail(options.lookupFailure)
				return (yield* Ref.get(state)).comments
			})
		const post = (discussion: GitHubDiscussionRef, content: GitHubContent) =>
			Effect.gen(function* () {
				const comment = discussionComment(discussion, botAuthor, content.markdown)
				const current = yield* Ref.updateAndGet(state, (previous) => ({
					...previous,
					posts: [...previous.posts, { discussion, content }],
					comments: [...previous.comments, comment],
				}))
				if (options.ambiguousPostFailure !== undefined && current.posts.length === 1) {
					return yield* Effect.fail(options.ambiguousPostFailure)
				}
				return comment
			})
		const layer = Layer.merge(
			ConfigProvider.layer(ConfigProvider.fromUnknown({ GITHUB_BOT_USER_ID: '999' })),
			Layer.mock(GitHubApi, {
				fetchUserAccess: (input) =>
					Ref.update(state, (current) => ({ ...current, lookups: [...current.lookups, input] })).pipe(
						Effect.andThen(access),
					),
				addReaction: (input) =>
					Ref.update(state, (current) => ({ ...current, reactions: [...current.reactions, input] })),
				listIssueComments: ({ issue }) => list(GitHubDiscussionRef.cases.Issue.make({ ref: issue })),
				listPullRequestComments: ({ pullRequest }) =>
					list(GitHubDiscussionRef.cases.PullRequest.make({ ref: pullRequest })),
				postIssueComment: ({ issue, content }) =>
					post(GitHubDiscussionRef.cases.Issue.make({ ref: issue }), content),
				postPullRequestComment: ({ pullRequest, content }) =>
					post(GitHubDiscussionRef.cases.PullRequest.make({ ref: pullRequest }), content),
			}),
		)
		return { state, layer }
	})

describe('respondToMentionAccess', () => {
	for (const fixture of fixtures()) {
		for (const access of ['write', 'maintain', 'admin'] as const) {
			it.effect(`allows ${access} on ${fixture.name}, reacting only on the trigger`, ({ expect }) =>
				Effect.gen(function* () {
					const api = yield* makeApi(Effect.succeed(access))
					expect(yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer))).toBe(true)
					expect(yield* Ref.get(api.state)).toEqual({
						lookups: [{ repository: fixture.discussion.ref, login: fixture.event.trigger.actor.login }],
						reactions: [{ target: fixture.target, reaction: 'eyes' }],
						lists: [],
						posts: [],
						comments: [],
					})
				}),
			)
		}
		for (const access of ['none', 'read', 'triage'] as const) {
			it.effect(`denies ${access} on ${fixture.name}, reacting and posting one discussion notice`, ({ expect }) =>
				Effect.gen(function* () {
					const api = yield* makeApi(Effect.succeed(access))
					expect(yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer))).toBe(false)
					const state = yield* Ref.get(api.state)
					expect(state.lookups).toEqual([
						{ repository: fixture.discussion.ref, login: fixture.event.trigger.actor.login },
					])
					expect(state.reactions).toEqual([{ target: fixture.target, reaction: '-1' }])
					expect(state.lists).toEqual([fixture.discussion])
					expect(state.posts).toEqual([
						{ discussion: fixture.discussion, content: { markdown: maintainerOnlyNotice } },
					])
					expect(state.comments).toEqual([discussionComment(fixture.discussion, botAuthor)])
				}),
			)
		}

		for (const author of [person(123), null, botAuthor]) {
			it.effect(
				`${fixture.name}: ${author?.login ?? 'null'}-authored matching notice ${author === botAuthor ? 'suppresses' : 'does not suppress'} posting`,
				({ expect }) =>
					Effect.gen(function* () {
						const existing = discussionComment(fixture.discussion, author)
						const api = yield* makeApi(Effect.succeed('read'), { comments: [existing] })
						expect(yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer))).toBe(false)
						const state = yield* Ref.get(api.state)
						expect(state.reactions).toEqual([{ target: fixture.target, reaction: '-1' }])
						expect(state.lists).toEqual([fixture.discussion])
						expect(state.posts).toEqual(
							author === botAuthor
								? []
								: [{ discussion: fixture.discussion, content: { markdown: maintainerOnlyNotice } }],
						)
						expect(state.comments).toEqual(
							author === botAuthor
								? [existing]
								: [existing, discussionComment(fixture.discussion, botAuthor)],
						)
					}),
			)
		}

		it.effect(`${fixture.name}: treats a permission 404 as none`, ({ expect }) =>
			Effect.gen(function* () {
				const error = GitHubApiError.make({
					operation: 'fetch_user_access',
					reason: 'not_found',
					status: 404,
					retryable: false,
				})
				const api = yield* makeApi(Effect.fail(error))
				expect(yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer))).toBe(false)
				const state = yield* Ref.get(api.state)
				expect(state.reactions).toEqual([{ target: fixture.target, reaction: '-1' }])
				expect(state.lists).toEqual([fixture.discussion])
				expect(state.posts).toEqual([
					{ discussion: fixture.discussion, content: { markdown: maintainerOnlyNotice } },
				])
			}),
		)

		for (const reason of [
			'forbidden',
			'authentication',
			'rate_limited',
			'unavailable',
			'invalid_response',
		] as const) {
			it.effect(`${fixture.name}: propagates ${reason} permission failures without writes`, ({ expect }) =>
				Effect.gen(function* () {
					const error = GitHubApiError.make({ operation: 'fetch_user_access', reason, retryable: true })
					const api = yield* makeApi(Effect.fail(error))
					expect(
						yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer), Effect.flip),
					).toEqual(error)
					expect(yield* Ref.get(api.state)).toEqual({
						lookups: [{ repository: fixture.discussion.ref, login: fixture.event.trigger.actor.login }],
						reactions: [],
						lists: [],
						posts: [],
						comments: [],
					})
				}),
			)
		}

		it.effect(`${fixture.name}: propagates comment lookup failure without posting`, ({ expect }) =>
			Effect.gen(function* () {
				const error = GitHubApiError.make({
					operation:
						fixture.discussion === issueDiscussion ? 'list_issue_comments' : 'list_pull_request_comments',
					reason: 'unavailable',
					retryable: true,
				})
				const api = yield* makeApi(Effect.succeed('none'), { lookupFailure: error })
				expect(
					yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer), Effect.flip),
				).toEqual(error)
				const state = yield* Ref.get(api.state)
				expect(state.reactions).toEqual([{ target: fixture.target, reaction: '-1' }])
				expect(state.lists).toEqual([fixture.discussion])
				expect(state.posts).toEqual([])
				expect(state.comments).toEqual([])
			}),
		)

		it.effect(
			`${fixture.name}: retry discovers a remotely posted notice after an ambiguous failure`,
			({ expect }) =>
				Effect.gen(function* () {
					const error = GitHubApiError.make({
						operation:
							fixture.discussion === issueDiscussion ? 'post_issue_comment' : 'post_pull_request_comment',
						reason: 'unavailable',
						retryable: true,
					})
					const api = yield* makeApi(Effect.succeed('none'), { ambiguousPostFailure: error })
					const failures = yield* Ref.make(0)
					const result = yield* respondToMentionAccess(fixture.event).pipe(
						Effect.tapError((failure) =>
							Effect.gen(function* () {
								yield* Ref.update(failures, (previous) => previous + 1)
								expect(failure).toEqual(error)
								expect((yield* Ref.get(api.state)).comments).toEqual([
									discussionComment(fixture.discussion, botAuthor),
								])
							}),
						),
						Effect.retry({ times: 1 }),
						Effect.provide(api.layer),
					)
					expect(result).toBe(false)
					expect(yield* Ref.get(failures)).toBe(1)
					const state = yield* Ref.get(api.state)
					expect(state.lookups).toHaveLength(2)
					expect(state.reactions).toEqual(Array(2).fill({ target: fixture.target, reaction: '-1' }))
					expect(state.lists).toEqual([fixture.discussion, fixture.discussion])
					expect(state.posts).toEqual([
						{ discussion: fixture.discussion, content: { markdown: maintainerOnlyNotice } },
					])
					expect(state.comments).toEqual([discussionComment(fixture.discussion, botAuthor)])
				}),
		)
	}

	for (const discussion of [issueDiscussion, prDiscussion]) {
		it.effect(
			`${discussion._tag}: new actors and trigger shapes do not post a second denial notice`,
			({ expect }) =>
				Effect.gen(function* () {
					const api = yield* makeApi(Effect.succeed('read'))
					const mentions = fixtures().filter((fixture) => fixture.discussion === discussion)
					const laterMentions = fixtures(person(2)).filter((fixture) => fixture.discussion === discussion)
					for (const fixture of [...mentions, ...laterMentions]) {
						expect(yield* respondToMentionAccess(fixture.event).pipe(Effect.provide(api.layer))).toBe(false)
					}
					const all = [...mentions, ...laterMentions]
					const state = yield* Ref.get(api.state)
					expect(state.lookups).toEqual(
						all.map(({ event }) => ({ repository: discussion.ref, login: event.trigger.actor.login })),
					)
					expect(state.reactions).toEqual(all.map(({ target }) => ({ target, reaction: '-1' })))
					expect(state.lists).toEqual(all.map(() => discussion))
					expect(state.posts).toEqual([{ discussion, content: { markdown: maintainerOnlyNotice } }])
					expect(state.comments).toEqual([discussionComment(discussion, botAuthor)])
				}),
		)
	}
})

interface CallbackState {
	readonly subscribed: ReadonlyArray<string>
	readonly sent: ReadonlyArray<{ readonly mailboxKey: string; readonly message: typeof AgentSessionMessage.Encoded }>
	readonly handoffsBeforeSend: ReadonlyArray<number>
}

const runOnMentioned = (
	fixture: ReturnType<typeof fixtures>[number],
	access: GitHubAccessLevel,
	options: { readonly send?: Effect.Effect<void> } = {},
) =>
	Effect.gen(function* () {
		const api = yield* makeApi(Effect.succeed(access))
		const delivery = yield* makeTestDeliveryExecution(
			fixture.discussion === issueDiscussion ? issue.mailboxKey : pullRequest.mailboxKey,
		)
		const state = yield* Ref.make<CallbackState>({ subscribed: [], sent: [], handoffsBeforeSend: [] })
		const agentSessions = AgentSessions.of({
			getByName: (mailboxKey: string) => ({
				send: (message: typeof AgentSessionMessage.Encoded) =>
					Effect.gen(function* () {
						const handoffs = yield* Ref.get(delivery.handoffs)
						yield* Ref.update(state, (current) => ({
							...current,
							sent: [...current.sent, { mailboxKey, message }],
							handoffsBeforeSend: [...current.handoffsBeforeSend, handoffs.length],
						}))
						yield* options.send ?? Effect.void
					}),
			}),
		})
		const exit = yield* Effect.exit(
			githubHandlers.onMentioned(fixture.event, delivery.execution.context).pipe(
				Effect.provide(
					Layer.mergeAll(
						Layer.succeed(AgentSessions, agentSessions),
						Layer.mock(MailboxSubscriptions, {
							subscribe: ({ mailboxKey }) =>
								Ref.update(state, (current) => ({
									...current,
									subscribed: [...current.subscribed, mailboxKey],
								})).pipe(Effect.as(MailboxSubscriptionCreatedResult.make({}))),
						}),
						Layer.mock(AutoLabel, {}),
						Layer.mock(RuntimeContext, { Type: 'Worker', id: 'mention-access-test', env: {} }),
						api.layer,
					),
				),
			),
		)
		return {
			exit,
			api: yield* Ref.get(api.state),
			callback: yield* Ref.get(state),
			handoffs: yield* Ref.get(delivery.handoffs),
			deliveryId: delivery.execution.context.deliveryId,
		}
	})

describe('the registered GitHub onMentioned handler', () => {
	for (const fixture of fixtures()) {
		const mailboxKey = fixture.discussion === issueDiscussion ? issue.mailboxKey : pullRequest.mailboxKey

		it.effect(`${fixture.name}: reacts, subscribes, sends to the AgentSession, then hands off`, ({ expect }) =>
			Effect.gen(function* () {
				const result = yield* runOnMentioned(fixture, 'write')
				expect(result.exit._tag).toBe('Success')
				expect(result.api.reactions).toEqual([{ target: fixture.target, reaction: 'eyes' }])
				expect(result.api.posts).toEqual([])
				expect(result.callback.subscribed).toEqual([mailboxKey])
				expect(result.callback.sent).toHaveLength(1)
				const [sent] = result.callback.sent
				expect(sent?.mailboxKey).toBe(mailboxKey)
				expect(sent?.message).toMatchObject({
					prompt: fixture.prompt ?? '@agent help',
					deliveryId: result.deliveryId,
					accessToken: 'test-access-token',
					githubDiscussion: { mailboxKey },
				})
				expect(sent?.message.mentionedIn).toEqual(fixture.mentionedIn)
				expect(result.callback.handoffsBeforeSend).toEqual([0])
				expect(result.handoffs).toEqual([undefined])
			}),
		)

		it.effect(`${fixture.name}: a denied mention never subscribes, sends, or hands off`, ({ expect }) =>
			Effect.gen(function* () {
				const result = yield* runOnMentioned(fixture, 'read')
				expect(result.exit._tag).toBe('Success')
				expect(result.api.reactions).toEqual([{ target: fixture.target, reaction: '-1' }])
				expect(result.api.posts).toEqual([
					{ discussion: fixture.discussion, content: { markdown: maintainerOnlyNotice } },
				])
				expect(result.callback).toEqual({ subscribed: [], sent: [], handoffsBeforeSend: [] })
				expect(result.handoffs).toEqual([])
			}),
		)

		it.effect(`${fixture.name}: a failed send does not hand off`, ({ expect }) =>
			Effect.gen(function* () {
				const result = yield* runOnMentioned(fixture, 'write', { send: Effect.die('rpc failed') })
				expect(result.exit._tag).toBe('Failure')
				expect(result.callback.sent).toHaveLength(1)
				expect(result.handoffs).toEqual([])
			}),
		)
	}
})
