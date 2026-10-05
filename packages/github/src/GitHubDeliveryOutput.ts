/**
 * This file sends a GitHub delivery's saved output to GitHub.
 *
 * It reads the issue or pull request the delivery saved before its callback ran, and what started the
 * delivery (its activation target), and turns each operation into a GitHub call:
 *
 * - `PresentOutcome` comments the Markdown on the issue or pull request, with any `awaitingInput`
 *   options listed after it. Without Markdown it makes no comment. When the remote worker's last activity
 *   was `Working` it first removes the bot's `eyes` reaction.
 * - `SetActivity` shows `Working` as the bot's `eyes` reaction on the activation target, and removes it for
 *   `Idle`. The text of `Working` is not shown. A reaction already there, or already gone, counts as done.
 * - `CreateMessage` comments. Its receipt is the comment, which later updates and deletions of the same
 *   message receive back as their reference.
 * - `UpdateMessage` edits that comment; `DeleteMessage` removes it. A comment already gone counts as
 *   deleted.
 * - `AddExternalLink` is applied without a call; GitHub has nowhere to show a link for now.
 * - `SetMessageReaction` adds or removes the bot's reaction on what started the delivery, or on a comment
 *   the delivery posted. A reaction already there, or already gone, counts as done. The activity's `eyes`
 *   and a portable `eyes` on the activation target are the same reaction, so `Idle` removes both.
 * - `RenderPlan` keeps one plan comment: the first plan comments it, and each later plan edits it. A plan
 *   comment someone deleted is commented again.
 *
 * A comment is at-least-once: when GitHub accepts it but the attempt dies before the store saves the
 * result, the next attempt comments again, and the attempt carries `hadAmbiguousAttempt`. Reactions,
 * edits, and deletions are safe to repeat.
 */
import {
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	type DeliveryOutcome,
	type PortableReaction,
	type ProviderOutputAttempt,
	type ProviderOutputProcessor,
	type ProviderPresentOutcome,
	ProviderReactionTarget,
	type ProviderRenderPlan,
	type ProviderSetMessageReaction,
	deliveryPlanMarkdown,
	sameDeliveryPlan,
} from '@humanlayer/channels-delivery'
import { Array as Arr, Effect, Match, Option, Predicate, Schema } from 'effect'

import { GitHubApi, type GitHubApiError } from './GitHubApi'
import {
	type GitHubActivationTarget,
	GitHubActivationTargetJson,
	type GitHubDeliveryDestination,
	GitHubDeliveryDestinationJson,
	gitHubPresentationVersion,
} from './GitHubDeliveryDestination'
import {
	GitHubContent,
	GitHubDiscussionRef,
	GitHubIssueCommentRef,
	GitHubReaction,
	GitHubReactionTarget,
} from './GitHubModels'

/** What GitHub made for an operation: the comment it posted. Saved by the store, read only here. */
export const GitHubOutputReceipt = Schema.TaggedStruct('GitHubComment', { comment: GitHubIssueCommentRef })
export type GitHubOutputReceipt = typeof GitHubOutputReceipt.Type
export const GitHubOutputReceiptJson = Schema.toCodecJson(GitHubOutputReceipt)

/** GitHub's reaction for each portable reaction. */
export const gitHubPortableReactions = {
	thumbs_up: '+1',
	thumbs_down: '-1',
	laugh: 'laugh',
	confused: 'confused',
	heart: 'heart',
	hooray: 'hooray',
	rocket: 'rocket',
	eyes: 'eyes',
} as const satisfies Record<PortableReaction, GitHubReaction>

/** The reaction that shows a remote worker is working. */
export const gitHubActivityReaction = GitHubReaction.make('eyes')

const failed = (safeCode: string, retryable: boolean, retryAfterMs?: number) =>
	new DeliveryOutputFailed(
		Predicate.isUndefined(retryAfterMs)
			? { provider: 'github', retryable, safeCode }
			: { provider: 'github', retryable, safeCode, retryAfterMs },
	)

const applied = DeliveryOutputApplied.make({})

/** Log a GitHub failure with its cause, then report it as retryable or not. */
const reportGitHubFailure =
	(message: string, safeCode: string) =>
	<A, R>(effect: Effect.Effect<A, GitHubApiError, R>) =>
		effect.pipe(
			Effect.tapError((error) => Effect.logWarning(message, error)),
			Effect.mapError((error) => failed(safeCode, error.retryable, error.retryAfterMs)),
		)

/** Log saved data this processor cannot read or write, with its cause, then report it as final. */
const reportUnreadable =
	(message: string, safeCode: string) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.tapError((error) => Effect.logWarning(message, error)),
			Effect.mapError(() => failed(safeCode, false)),
		)

/**
 * Something GitHub no longer has, such as a deleted comment or the comment a reaction was on, counts as
 * done for a removal. The error is logged; any other failure passes through.
 */
const countNotFoundAsDone =
	(message: string) =>
	<R>(effect: Effect.Effect<void, GitHubApiError, R>) =>
		effect.pipe(
			Effect.catchIf(
				(error) => error.reason === 'not_found',
				(error) => Effect.logInfo(message, error),
			),
		)

/** The comment a result posts: its Markdown, then any options as a list. */
const outcomeMarkdown = (outcome: DeliveryOutcome, markdown: string) =>
	Match.value(outcome).pipe(
		Match.tag('AwaitingInput', ({ options }) =>
			Arr.match(options ?? [], {
				onEmpty: () => markdown,
				onNonEmpty: (listed) => `${markdown}\n\n${listed.map((option) => `- ${option}`).join('\n')}`,
			}),
		),
		Match.orElse(() => markdown),
	)

/** The issue or pull request a destination names. */
const destinationDiscussion = (destination: GitHubDeliveryDestination) =>
	Match.value(destination).pipe(
		Match.tagsExhaustive({
			GitHubIssue: ({ issue }) => GitHubDiscussionRef.cases.Issue.make({ ref: issue }),
			GitHubPullRequest: ({ pullRequest }) => GitHubDiscussionRef.cases.PullRequest.make({ ref: pullRequest }),
		}),
	)

/** What the activity reaction goes on: the mentioning comment, or the issue or pull request itself. */
const activationReactionTarget = (target: GitHubActivationTarget) =>
	Match.value(target).pipe(
		Match.withReturnType<GitHubReactionTarget>(),
		Match.tagsExhaustive({
			GitHubIssue: ({ issue }) =>
				GitHubReactionTarget.cases.Discussion.make({
					discussion: GitHubDiscussionRef.cases.Issue.make({ ref: issue }),
				}),
			GitHubPullRequest: ({ pullRequest }) =>
				GitHubReactionTarget.cases.Discussion.make({
					discussion: GitHubDiscussionRef.cases.PullRequest.make({ ref: pullRequest }),
				}),
			GitHubIssueComment: ({ comment }) => GitHubReactionTarget.cases.Comment.make({ comment }),
			GitHubReviewComment: ({ comment }) => GitHubReactionTarget.cases.Comment.make({ comment }),
		}),
	)

/** Comment on the issue or pull request; the receipt is the comment. */
const postComment = Effect.fn('github.output.post_comment')(function* (
	destination: GitHubDeliveryDestination,
	markdown: string,
) {
	const gitHubApi = yield* GitHubApi
	const content = GitHubContent.make({ markdown })
	const posted = yield* Match.value(destination).pipe(
		Match.tagsExhaustive({
			GitHubIssue: ({ issue }) => gitHubApi.postIssueComment({ issue, content }),
			GitHubPullRequest: ({ pullRequest }) => gitHubApi.postPullRequestComment({ pullRequest, content }),
		}),
		reportGitHubFailure('GitHub output comment failed', 'github_comment_failed'),
	)
	const receipt = yield* Schema.encodeEffect(GitHubOutputReceiptJson)(
		GitHubOutputReceipt.make({ comment: posted.ref }),
	).pipe(reportUnreadable('GitHub output receipt could not be encoded', 'receipt_unencodable'))
	return DeliveryOutputApplied.make({ receipt })
})

/** The comment a saved reference names. */
const postedComment = (reference: Schema.Json) =>
	Schema.decodeEffect(GitHubOutputReceiptJson)(reference).pipe(
		Effect.map((receipt) => receipt.comment),
		reportUnreadable('GitHub output message reference could not be read', 'message_reference_invalid'),
	)

/**
 * Show the plan in one comment: comment it the first time, then edit that comment, whose receipt is the
 * plan's presentation. An unchanged plan makes no call. A plan comment someone deleted is commented again.
 */
const renderPlan = Effect.fn('github.output.render_plan')(function* (
	destination: GitHubDeliveryDestination,
	{ plan, rendered }: ProviderRenderPlan,
) {
	const gitHubApi = yield* GitHubApi
	const markdown = deliveryPlanMarkdown(plan)
	const presentation = rendered?.presentation
	if (Predicate.isUndefined(rendered) || Predicate.isUndefined(presentation)) return yield* postComment(destination, markdown)
	if (sameDeliveryPlan(rendered.plan, plan)) return DeliveryOutputApplied.make({ receipt: presentation })
	const comment = yield* postedComment(presentation)
	const edited = yield* gitHubApi.updateComment({ comment, content: GitHubContent.make({ markdown }) }).pipe(
		Effect.as(true),
		Effect.catchIf(
			(error) => error.reason === 'not_found',
			(error) => Effect.logInfo('GitHub plan comment is gone; commenting the plan again', error).pipe(Effect.as(false)),
		),
		reportGitHubFailure('GitHub plan comment update failed', 'github_comment_update_failed'),
	)
	return edited ? DeliveryOutputApplied.make({ receipt: presentation }) : yield* postComment(destination, markdown)
})

/** Edit the comment a message's create posted. */
const updateComment = Effect.fn('github.output.update_comment')(function* (reference: Schema.Json, markdown: string) {
	const gitHubApi = yield* GitHubApi
	const comment = yield* postedComment(reference)
	yield* gitHubApi
		.updateComment({ comment, content: GitHubContent.make({ markdown }) })
		.pipe(reportGitHubFailure('GitHub output comment update failed', 'github_comment_update_failed'))
	return applied
})

/** Delete the comment a message's create posted. A comment already gone counts as deleted. */
const deleteComment = Effect.fn('github.output.delete_comment')(function* (reference: Schema.Json) {
	const gitHubApi = yield* GitHubApi
	const comment = yield* postedComment(reference)
	yield* gitHubApi.deleteComment({ comment }).pipe(
		countNotFoundAsDone('GitHub comment was already gone; counting it as deleted'),
		reportGitHubFailure('GitHub output comment delete failed', 'github_comment_delete_failed'),
	)
	return applied
})

/** Add the `eyes` reaction to what started the delivery. GitHub answers 200 when it is already there. */
const addActivityReaction = Effect.fn('github.output.add_activity_reaction')(function* (
	target: GitHubActivationTarget,
) {
	const gitHubApi = yield* GitHubApi
	yield* gitHubApi
		.addReaction({ target: activationReactionTarget(target), reaction: gitHubActivityReaction })
		.pipe(reportGitHubFailure('GitHub activity reaction failed', 'github_reaction_failed'))
})

/**
 * Remove the bot's `eyes` reaction from what started the delivery. No reaction to remove, or a target
 * already gone, counts as removed.
 */
const removeActivityReaction = Effect.fn('github.output.remove_activity_reaction')(function* (
	target: GitHubActivationTarget,
) {
	const gitHubApi = yield* GitHubApi
	yield* gitHubApi.removeReaction({ target: activationReactionTarget(target), reaction: gitHubActivityReaction }).pipe(
		countNotFoundAsDone('GitHub reaction target was already gone; counting the reaction as removed'),
		reportGitHubFailure('GitHub activity reaction removal failed', 'github_reaction_failed'),
	)
})

/** Show `Working` or `Idle` on the activation target. A delivery without one never lists `SetActivity`. */
const setActivityReaction = Effect.fn('github.output.set_activity_reaction')(function* (
	activationTarget: Option.Option<GitHubActivationTarget>,
	working: boolean,
) {
	const target = yield* Effect.fromOption(activationTarget).pipe(
		Effect.mapError(() => failed('activation_target_missing', false)),
	)
	yield* working ? addActivityReaction(target) : removeActivityReaction(target)
	return applied
})

/** What a portable reaction goes on: what started the delivery, or a comment the delivery posted. */
const portableReactionTarget = (
	activationTarget: Option.Option<GitHubActivationTarget>,
	target: ProviderReactionTarget,
) =>
	ProviderReactionTarget.match(target, {
		ActivationTarget: () =>
			Effect.fromOption(activationTarget).pipe(
				Effect.mapError(() => failed('activation_target_missing', false)),
				Effect.map(activationReactionTarget),
			),
		MessageTarget: ({ reference }) =>
			postedComment(reference).pipe(Effect.map((comment) => GitHubReactionTarget.cases.Comment.make({ comment }))),
	})

/**
 * Add or remove the bot's reaction. GitHub answers 200 when it is already there, and removal finds
 * nothing to delete when it is gone; a target already gone counts as removed.
 */
const setPortableReaction = Effect.fn('github.output.set_reaction')(function* (
	activationTarget: Option.Option<GitHubActivationTarget>,
	{ target, reaction, active }: ProviderSetMessageReaction,
) {
	const gitHubApi = yield* GitHubApi
	const request = {
		target: yield* portableReactionTarget(activationTarget, target),
		reaction: gitHubPortableReactions[reaction],
	}
	yield* active
		? gitHubApi
				.addReaction(request)
				.pipe(reportGitHubFailure('GitHub output reaction failed', 'github_reaction_failed'))
		: gitHubApi.removeReaction(request).pipe(
				countNotFoundAsDone('GitHub reaction target was already gone; counting the reaction as removed'),
				reportGitHubFailure('GitHub output reaction removal failed', 'github_reaction_failed'),
			)
	return applied
})

/**
 * Present the result: clear `eyes` first when the worker's last activity was `Working`, so a retry after
 * a posted comment cannot leave it behind, then comment any Markdown.
 */
const presentOutcome = Effect.fn('github.output.present_outcome')(function* (
	destination: GitHubDeliveryDestination,
	activationTarget: Option.Option<GitHubActivationTarget>,
	operation: ProviderPresentOutcome,
) {
	if (operation.clearActivity) {
		yield* Option.match(activationTarget, { onNone: () => Effect.void, onSome: removeActivityReaction })
	}
	if (Predicate.isUndefined(operation.markdown)) return applied
	return yield* postComment(destination, outcomeMarkdown(operation.outcome, operation.markdown))
})

/** Build GitHub's output half. Its `GitHubApi` is the bot's own. */
export const makeGitHubOutputProcessor = Effect.fn('github.make_output_processor')(function* (input: {
	readonly namespace: string
}) {
	const gitHubApi = yield* GitHubApi

	const process = Effect.fn('github.process_delivery_output')(function* (attempt: ProviderOutputAttempt) {
		if (attempt.prepared.presentationVersion !== gitHubPresentationVersion) {
			return yield* failed('unsupported_presentation_version', false)
		}
		const destination = yield* Schema.decodeEffect(GitHubDeliveryDestinationJson)(
			attempt.prepared.destination,
		).pipe(reportUnreadable('GitHub output destination could not be read', 'destination_invalid'))
		const activationTarget = yield* Effect.transposeOption(
			Option.map(
				Option.fromUndefinedOr(attempt.prepared.activationTarget),
				Schema.decodeEffect(GitHubActivationTargetJson),
			),
		).pipe(reportUnreadable('GitHub output activation target could not be read', 'activation_target_invalid'))
		if (attempt.hadAmbiguousAttempt) {
			yield* Effect.logWarning('GitHub output retried after an attempt that may have been applied')
		}

		const discussion = destinationDiscussion(destination)
		yield* Effect.annotateCurrentSpan({
			'github.repository_id': discussion.ref.repositoryId,
			'github.discussion_number': discussion.ref.number,
		})

		return yield* Match.value(attempt.operation).pipe(
			Match.tagsExhaustive({
				AddExternalLink: () => Effect.succeed(applied),
				SetMessageReaction: (operation) => setPortableReaction(activationTarget, operation),
				RenderPlan: (operation) => renderPlan(destination, operation),
				PresentOutcome: (operation) => presentOutcome(destination, activationTarget, operation),
				SetActivity: ({ activity }) =>
					Match.value(activity).pipe(
						Match.tagsExhaustive({
							Working: () => setActivityReaction(activationTarget, true),
							Idle: () => setActivityReaction(activationTarget, false),
						}),
					),
				CreateMessage: ({ markdown }) => postComment(destination, markdown),
				UpdateMessage: ({ markdown, reference }) => updateComment(reference, markdown),
				DeleteMessage: ({ reference }) => deleteComment(reference),
			}),
		)
	}, (effect, attempt) =>
		effect.pipe(
			Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
			Effect.provideService(GitHubApi, gitHubApi),
		),
	)

	return { namespace: input.namespace, providerName: 'github', process } satisfies ProviderOutputProcessor
})
