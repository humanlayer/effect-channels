import { describe, it } from '@effect/vitest'
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
import { ConfigProvider, Effect, Layer, Logger, Ref } from 'effect'

import { makeTestDeliveryExecution } from '../../../packages/delivery/test/delivery-execution'
import { AutoLabel } from '../src/AutoLabel'
import { maintainerOnlyNotice, makeOnMentioned, respondToMentionAccess } from '../src/GithubBot'

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
			event: GitHubPrMentioned.make({ pullRequest, trigger: prOpened, events: [prOpened] }),
			discussion: prDiscussion,
			target: GitHubReactionTarget.cases.Discussion.make({ discussion: prDiscussion }),
		},
		{
			name: 'issue comment',
			event: GitHubIssueMentioned.make({ issue, trigger: issueComment, events: [issueComment] }),
			discussion: issueDiscussion,
			target: GitHubReactionTarget.cases.Comment.make({ comment: issueComment.comment.ref }),
		},
		{
			name: 'PR comment',
			event: GitHubPrMentioned.make({ pullRequest, trigger: prComment, events: [prComment] }),
			discussion: prDiscussion,
			target: GitHubReactionTarget.cases.Comment.make({ comment: prComment.comment.ref }),
		},
		{
			name: 'review comment',
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

describe('the registered GitHub onMentioned handler', () => {
	for (const fixture of fixtures()) {
		for (const access of ['write', 'read'] as const) {
			it.effect(
				`${fixture.name}: the handler applies ${access} access before entering its branch`,
				({ expect }) =>
					Effect.gen(function* () {
						const api = yield* makeApi(Effect.succeed(access))
						const logs: Array<string> = []
						const logger = Logger.layer([
							Logger.make((entry) => logs.push(JSON.stringify(Logger.formatStructured.log(entry)))),
						])
						yield* Effect.gen(function* () {
							const registered = yield* GitHubCallbacks
							if (registered.onMentioned === undefined)
								return yield* Effect.die('onMentioned is not registered')
							const { execution } = yield* makeTestDeliveryExecution()
							yield* registered.onMentioned(fixture.event, execution.context)
							if (access === 'read') yield* registered.onMentioned(fixture.event, execution.context)
						}).pipe(
							Effect.provide(
								GitHubCallbacks.layer({ onMentioned: makeOnMentioned(() => Effect.succeed(null)) }),
							),
							Effect.provide(Layer.mock(AutoLabel, {})),
							Effect.provide(
								Layer.mock(RuntimeContext, { Type: 'Worker', id: 'mention-access-test', env: {} }),
							),
							Effect.provide(api.layer),
							Effect.provide(logger),
						)
						const state = yield* Ref.get(api.state)
						const branchLog =
							fixture.discussion === issueDiscussion
								? 'Authorized issue mention received'
								: 'Authorized PR mention received'
						if (access === 'write') {
							expect(state.reactions).toEqual([{ target: fixture.target, reaction: 'eyes' }])
							expect(state.posts).toEqual([])
							expect(logs.some((entry) => entry.includes(branchLog))).toBe(true)
						} else {
							expect(state.reactions).toEqual([
								{ target: fixture.target, reaction: '-1' },
								{ target: fixture.target, reaction: '-1' },
							])
							expect(state.posts).toEqual([
								{ discussion: fixture.discussion, content: { markdown: maintainerOnlyNotice } },
							])
							expect(logs.some((entry) => entry.includes(branchLog))).toBe(false)
						}
					}),
			)
		}
	}
})
