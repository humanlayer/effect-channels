/**
 * GitHub's output half: each saved operation becomes the right GitHub call, or none, over a recording
 * `GitHubApi`, and GitHub failures come back as retryable or not.
 */
import { describe, it } from '@effect/vitest'
import {
	BatchId,
	CreateMessage,
	DeliveryActivity,
	DeliveryOperationId,
	DeliveryOutcome,
	PreparedDeliveryCallback,
	ProviderOutputAttempt,
	ProviderPresentOutcome,
	ProviderReactionTarget,
	ProviderRenderPlan,
	ProviderSetMessageReaction,
	RenderedDeliveryPlan,
	DeliveryPlan,
	DeliveryPlanItem,
	DeliveryPlanItemId,
	DeliveryPlanItemState,
	SetActivity,
	makeDeliveryId,
	MessageId,
	type ProviderOutputOperation,
} from '@humanlayer/channels-delivery'
import { Effect, Layer, Match, Option, Ref, Result, Schema } from 'effect'

import {
	commentOutputScenarios,
	planCommentScenarios,
	type CommentOutputFault,
	type CommentOutputHarness,
} from '../../delivery/test/comment-output-scenarios'
import {
	GitHubActivationTarget,
	GitHubActivationTargetJson,
	GitHubApi,
	GitHubApiError,
	GitHubDeliveryDestination,
	GitHubDeliveryDestinationJson,
	GitHubId,
	GitHubIssueComment,
	GitHubIssueCommentRef,
	GitHubIssueRef,
	GitHubOutputReceipt,
	GitHubOutputReceiptJson,
	GitHubPullRequestRef,
	GitHubReviewCommentRef,
	type GitHubReactionTarget,
	gitHubPresentationVersion,
	gitHubReactionTargets,
	gitHubSupportedOperations,
	makeGitHubOutputProcessor,
	type GitHubApiErrorReason,
} from '../src'

const issue = GitHubIssueRef.make({
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'alice',
	repository: 'project',
	number: GitHubId.make(42),
})
const pullRequest = GitHubPullRequestRef.make({ ...issue, number: GitHubId.make(43) })
const mentionComment = GitHubIssueCommentRef.make({ discussion: { _tag: 'Issue', ref: issue }, id: GitHubId.make(500) })

const issueDestination = GitHubDeliveryDestination.cases.GitHubIssue.make({ issue })
const pullRequestDestination = GitHubDeliveryDestination.cases.GitHubPullRequest.make({ pullRequest })

const prepared = (input: {
	readonly destination?: GitHubDeliveryDestination
	readonly activationTarget?: GitHubActivationTarget
	readonly presentationVersion?: number
}) => {
	const target = Option.fromUndefinedOr(input.activationTarget)
	return PreparedDeliveryCallback.make({
		name: 'onMentioned',
		presentationVersion: input.presentationVersion ?? gitHubPresentationVersion,
		destination: Schema.encodeSync(GitHubDeliveryDestinationJson)(input.destination ?? issueDestination),
		...Option.match(target, {
			onNone: () => ({}),
			onSome: (value) => ({ activationTarget: Schema.encodeSync(GitHubActivationTargetJson)(value) }),
		}),
		supportedOperations: gitHubSupportedOperations(target),
	})
}

const attempt = (operation: ProviderOutputOperation, invocation: PreparedDeliveryCallback) =>
	ProviderOutputAttempt.make({
		deliveryId: makeDeliveryId({ mailboxKey: 'github:v1:mailbox', batchId: BatchId.make('batch-1') }),
		operationId: DeliveryOperationId.make('outcome'),
		attempt: 1,
		hadAmbiguousAttempt: false,
		idempotencyKey: '00000000-0000-4000-8000-000000000001',
		prepared: invocation,
		operation,
	})

/** How GitHub answers for each shared fault. */
const sharedFaultReasons = {
	retryable: 'unavailable',
	final: 'forbidden',
	gone: 'not_found',
} as const satisfies Record<CommentOutputFault, GitHubApiErrorReason>

/**
 * Run one operation through the GitHub output processor over a recording `GitHubApi`. Every call is
 * recorded as a line such as `post issue 42: text` or `react + eyes on issue-comment 500`, and fails
 * with a `GitHubApiError` of reason `failWith` when one is given (retryable only for `unavailable`).
 * Any other `GitHubApi` method dies.
 */
const run = (
	operation: ProviderOutputOperation,
	invocation: PreparedDeliveryCallback,
	failWith?: GitHubApiErrorReason,
) =>
	Effect.gen(function* () {
		const calls = yield* Ref.make<ReadonlyArray<string>>([])
		const failure = Option.map(Option.fromUndefinedOr(failWith), (reason) =>
			GitHubApiError.make({ operation: 'post_issue_comment', reason, retryable: reason === 'unavailable' }),
		)
		const record = (line: string) =>
			Ref.update(calls, (all) => [...all, line]).pipe(
				Effect.andThen(Option.match(failure, { onNone: () => Effect.void, onSome: Effect.fail })),
			)
		const commentOn = (discussion: GitHubIssueCommentRef['discussion'], markdown: string) =>
			GitHubIssueComment.make({
				ref: { discussion, id: GitHubId.make(900) },
				body: markdown,
				url: 'https://github.com/alice/project/issues/42#issuecomment-900',
				author: null,
			})
		const commentTarget = (ref: GitHubIssueCommentRef | GitHubReviewCommentRef) =>
			Schema.is(GitHubReviewCommentRef)(ref) ? `review-comment ${ref.id}` : `issue-comment ${ref.id}`
		const reactionTarget = (target: GitHubReactionTarget) =>
			Match.value(target).pipe(
				Match.tagsExhaustive({
					Comment: ({ comment }) => commentTarget(comment),
					Discussion: ({ discussion }) => `${discussion._tag} ${discussion.ref.number}`,
				}),
			)
		const api = Layer.mock(GitHubApi, {
			postIssueComment: ({ issue: target, content }) =>
				record(`post issue ${target.number}: ${content.markdown}`).pipe(
					Effect.as(commentOn({ _tag: 'Issue', ref: target }, content.markdown)),
				),
			postPullRequestComment: ({ pullRequest: target, content }) =>
				record(`post pull-request ${target.number}: ${content.markdown}`).pipe(
					Effect.as(commentOn({ _tag: 'PullRequest', ref: target }, content.markdown)),
				),
			updateComment: ({ comment, content }) =>
				record(`edit ${commentTarget(comment)}: ${content.markdown}`).pipe(
					Effect.as(commentOn(mentionComment.discussion, content.markdown)),
				),
			deleteComment: ({ comment }) => record(`delete ${commentTarget(comment)}`),
			addReaction: ({ target, reaction }) => record(`react + ${reaction} on ${reactionTarget(target)}`),
			removeReaction: ({ target, reaction }) => record(`react - ${reaction} on ${reactionTarget(target)}`),
		})
		const processor = yield* makeGitHubOutputProcessor({ namespace: 'github-output-test' }).pipe(
			Effect.provide(api),
		)
		const result = yield* processor.process(attempt(operation, invocation)).pipe(Effect.result)
		return { result, calls: yield* Ref.get(calls) }
	})

const gitHubIssueHarness: CommentOutputHarness = {
	provider: 'GitHub issue',
	optionBullet: '- ',
	run: (operation, options = {}) =>
		run(
			operation,
			prepared({
				presentationVersion: options.futureVersion === true ? gitHubPresentationVersion + 1 : undefined,
			}),
			options.fault === undefined ? undefined : sharedFaultReasons[options.fault],
		).pipe(
			Effect.map(({ result, calls }) => ({
				result,
				shown: calls.map((line) =>
					line
						.replace(/^post issue 42: /, 'post: ')
						.replace(/^edit issue-comment 900: /, 'edit: ')
						.replace(/^delete issue-comment 900$/, 'delete'),
				),
			})),
		),
}

commentOutputScenarios(gitHubIssueHarness)
planCommentScenarios(gitHubIssueHarness)

const working = SetActivity.make({ activity: DeliveryActivity.cases.Working.make({ message: 'Running tests' }) })
const idle = SetActivity.make({ activity: DeliveryActivity.cases.Idle.make({}) })
const completed = DeliveryOutcome.cases.Completed.make({})

describe('GitHub delivery output', () => {
	it.effect('comments the plan again when its comment was deleted, and keeps the new comment', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<string>>([])
			const api = Layer.mock(GitHubApi, {
				updateComment: ({ comment }) =>
					Ref.update(calls, (all) => [...all, `edit ${comment.id}`]).pipe(
						Effect.andThen(
							Effect.fail(
								GitHubApiError.make({
									operation: 'update_comment',
									reason: 'not_found',
									retryable: false,
								}),
							),
						),
					),
				postIssueComment: ({ issue: target, content }) =>
					Ref.update(calls, (all) => [...all, 'post']).pipe(
						Effect.as(
							GitHubIssueComment.make({
								ref: { discussion: { _tag: 'Issue', ref: target }, id: GitHubId.make(901) },
								body: content.markdown,
								url: 'https://github.com/alice/project/issues/42#issuecomment-901',
								author: null,
							}),
						),
					),
			})
			const plan = DeliveryPlan.make({
				items: [
					DeliveryPlanItem.make({
						id: DeliveryPlanItemId.make('a'),
						title: 'Step a',
						state: DeliveryPlanItemState.cases.Pending.make({}),
					}),
				],
			})
			const deleted = Schema.encodeSync(GitHubOutputReceiptJson)(
				GitHubOutputReceipt.make({ comment: mentionComment }),
			)
			const processor = yield* makeGitHubOutputProcessor({ namespace: 'github-output-test' }).pipe(
				Effect.provide(api),
			)
			const applied = yield* processor.process(
				attempt(
					ProviderRenderPlan.make({
						revision: 2,
						plan,
						rendered: RenderedDeliveryPlan.make({
							revision: 1,
							plan: DeliveryPlan.make({ items: [] }),
							presentation: deleted,
						}),
					}),
					prepared({}),
				),
			)
			expect(yield* Ref.get(calls)).toEqual(['edit 500', 'post'])
			const receipt = yield* Schema.decodeUnknownEffect(GitHubOutputReceiptJson)(applied.receipt)
			expect(receipt.comment.id).toEqual(GitHubId.make(901))
		}),
	)

	it.effect('comments on a pull request, and keeps the comment as an opaque receipt', ({ expect }) =>
		Effect.gen(function* () {
			const { result, calls } = yield* run(
				CreateMessage.make({ messageId: MessageId.make('summary'), markdown: 'Summary' }),
				prepared({ destination: pullRequestDestination }),
			)
			expect(calls).toEqual(['post pull-request 43: Summary'])
			if (!Result.isSuccess(result)) return expect.unreachable()
			const receipt = yield* Schema.decodeUnknownEffect(GitHubOutputReceiptJson)(result.success.receipt)
			expect(receipt).toEqual(
				GitHubOutputReceipt.make({
					comment: { discussion: { _tag: 'PullRequest', ref: pullRequest }, id: GitHubId.make(900) },
				}),
			)
		}),
	)

	it.effect('shows Working as eyes on the activation target, and removes it for Idle', ({ expect }) =>
		Effect.gen(function* () {
			const targets = [
				[
					GitHubActivationTarget.cases.GitHubIssueComment.make({ comment: mentionComment }),
					'issue-comment 500',
				],
				[
					GitHubActivationTarget.cases.GitHubReviewComment.make({
						comment: GitHubReviewCommentRef.make({ pullRequest, id: GitHubId.make(700) }),
					}),
					'review-comment 700',
				],
				[GitHubActivationTarget.cases.GitHubIssue.make({ issue }), 'Issue 42'],
				[GitHubActivationTarget.cases.GitHubPullRequest.make({ pullRequest }), 'PullRequest 43'],
			] as const
			for (const [activationTarget, shown] of targets) {
				const invocation = prepared({ activationTarget })
				const set = yield* run(working, invocation)
				expect(Result.isSuccess(set.result)).toBe(true)
				expect(set.calls).toEqual([`react + eyes on ${shown}`])
				const cleared = yield* run(idle, invocation)
				expect(Result.isSuccess(cleared.result)).toBe(true)
				expect(cleared.calls).toEqual([`react - eyes on ${shown}`])
			}
		}),
	)

	it.effect('a result clears eyes before it comments, and only when the worker was working', ({ expect }) =>
		Effect.gen(function* () {
			const invocation = prepared({
				activationTarget: GitHubActivationTarget.cases.GitHubIssueComment.make({ comment: mentionComment }),
			})
			const posted = yield* run(
				ProviderPresentOutcome.make({ clearActivity: true, outcome: completed, markdown: 'Done.' }),
				invocation,
			)
			expect(posted.calls).toEqual(['react - eyes on issue-comment 500', 'post issue 42: Done.'])
			const silent = yield* run(
				ProviderPresentOutcome.make({ clearActivity: true, outcome: completed }),
				invocation,
			)
			expect(silent.calls).toEqual(['react - eyes on issue-comment 500'])
			expect(Result.isSuccess(silent.result)).toBe(true)
			const neverWorking = yield* run(
				ProviderPresentOutcome.make({ clearActivity: false, outcome: completed }),
				invocation,
			)
			expect(neverWorking.calls).toEqual([])
		}),
	)

	it.effect('counts eyes already removed, or a target already gone, as cleared', ({ expect }) =>
		Effect.gen(function* () {
			const invocation = prepared({ activationTarget: GitHubActivationTarget.cases.GitHubIssue.make({ issue }) })
			const cleared = yield* run(idle, invocation, 'not_found')
			expect(Result.isSuccess(cleared.result)).toBe(true)
			expect(cleared.calls).toEqual(['react - eyes on Issue 42'])
			const result = yield* run(
				ProviderPresentOutcome.make({ clearActivity: true, outcome: completed }),
				invocation,
				'not_found',
			)
			expect(Result.isSuccess(result.result)).toBe(true)
			expect(result.calls).toEqual(['react - eyes on Issue 42'])
		}),
	)

	it.effect('sorts reaction failures into retryable and final', ({ expect }) =>
		Effect.gen(function* () {
			const invocation = prepared({ activationTarget: GitHubActivationTarget.cases.GitHubIssue.make({ issue }) })
			const outage = yield* run(working, invocation, 'unavailable')
			const forbidden = yield* run(working, invocation, 'forbidden')
			const gone = yield* run(working, invocation, 'not_found')
			if (
				!Result.isFailure(outage.result) ||
				!Result.isFailure(forbidden.result) ||
				!Result.isFailure(gone.result)
			) {
				return expect.unreachable()
			}
			expect(outage.result.failure).toMatchObject({ retryable: true, safeCode: 'github_reaction_failed' })
			expect(forbidden.result.failure).toMatchObject({ retryable: false, safeCode: 'github_reaction_failed' })
			expect(gone.result.failure).toMatchObject({ retryable: false, safeCode: 'github_reaction_failed' })
		}),
	)

	it.effect('lists SetActivity only with an activation target, and refuses activity without one', ({ expect }) =>
		Effect.gen(function* () {
			expect(gitHubSupportedOperations(Option.none())).not.toContain('SetActivity')
			expect(
				gitHubSupportedOperations(Option.some(GitHubActivationTarget.cases.GitHubIssue.make({ issue }))),
			).toContain('SetActivity')
			const refused = yield* run(working, prepared({}))
			if (!Result.isFailure(refused.result)) return expect.unreachable()
			expect(refused.result.failure).toMatchObject({ retryable: false, safeCode: 'activation_target_missing' })
			expect(refused.calls).toEqual([])
		}),
	)

	describe('portable reactions', () => {
		const onActivation = ProviderReactionTarget.cases.ActivationTarget.make({})
		const react = (
			target: ProviderReactionTarget,
			reaction: ProviderSetMessageReaction['reaction'],
			active: boolean,
		) => ProviderSetMessageReaction.make({ target, reaction, active })

		it.effect(
			"adds and removes GitHub's reaction on the issue, pull request, or comment that started the delivery",
			({ expect }) =>
				Effect.gen(function* () {
					const targets = [
						[
							GitHubActivationTarget.cases.GitHubIssueComment.make({ comment: mentionComment }),
							'issue-comment 500',
						],
						[
							GitHubActivationTarget.cases.GitHubReviewComment.make({
								comment: GitHubReviewCommentRef.make({ pullRequest, id: GitHubId.make(700) }),
							}),
							'review-comment 700',
						],
						[GitHubActivationTarget.cases.GitHubIssue.make({ issue }), 'Issue 42'],
						[GitHubActivationTarget.cases.GitHubPullRequest.make({ pullRequest }), 'PullRequest 43'],
					] as const
					for (const [activationTarget, shown] of targets) {
						const invocation = prepared({ activationTarget })
						const added = yield* run(react(onActivation, 'thumbs_up', true), invocation)
						const removed = yield* run(react(onActivation, 'thumbs_down', false), invocation)
						expect(Result.isSuccess(added.result) && Result.isSuccess(removed.result)).toBe(true)
						expect([...added.calls, ...removed.calls]).toEqual([
							`react + +1 on ${shown}`,
							`react - -1 on ${shown}`,
						])
					}
				}),
		)

		it.effect('reacts on a comment the delivery posted, through its receipt', ({ expect }) =>
			Effect.gen(function* () {
				const posted = Schema.encodeSync(GitHubOutputReceiptJson)(
					GitHubOutputReceipt.make({
						comment: GitHubIssueCommentRef.make({
							discussion: { _tag: 'PullRequest', ref: pullRequest },
							id: GitHubId.make(900),
						}),
					}),
				)
				const target = ProviderReactionTarget.cases.MessageTarget.make({
					messageId: MessageId.make('summary'),
					reference: posted,
				})
				const { result, calls } = yield* run(
					react(target, 'rocket', true),
					prepared({ destination: pullRequestDestination }),
				)
				expect(Result.isSuccess(result)).toBe(true)
				expect(calls).toEqual(['react + rocket on issue-comment 900'])
			}),
		)

		it.effect('counts a target already gone as removed, and sorts other failures', ({ expect }) =>
			Effect.gen(function* () {
				const invocation = prepared({
					activationTarget: GitHubActivationTarget.cases.GitHubIssue.make({ issue }),
				})
				const removed = yield* run(react(onActivation, 'heart', false), invocation, 'not_found')
				expect(Result.isSuccess(removed.result)).toBe(true)
				const outage = yield* run(react(onActivation, 'heart', true), invocation, 'unavailable')
				const forbidden = yield* run(react(onActivation, 'heart', true), invocation, 'forbidden')
				const noTarget = yield* run(react(onActivation, 'heart', true), prepared({}))
				if (
					!Result.isFailure(outage.result) ||
					!Result.isFailure(forbidden.result) ||
					!Result.isFailure(noTarget.result)
				) {
					return expect.unreachable()
				}
				expect(outage.result.failure).toMatchObject({ retryable: true, safeCode: 'github_reaction_failed' })
				expect(forbidden.result.failure).toMatchObject({ retryable: false, safeCode: 'github_reaction_failed' })
				expect(noTarget.result.failure).toMatchObject({
					retryable: false,
					safeCode: 'activation_target_missing',
				})
				expect(noTarget.calls).toEqual([])
			}),
		)

		it('reacts on what started the delivery only when there is one, and always on its comments', ({ expect }) => {
			expect(gitHubReactionTargets(Option.none())).toEqual(['MessageTarget'])
			expect(
				gitHubReactionTargets(Option.some(GitHubActivationTarget.cases.GitHubIssue.make({ issue }))),
			).toEqual(['ActivationTarget', 'MessageTarget'])
		})
	})
})
