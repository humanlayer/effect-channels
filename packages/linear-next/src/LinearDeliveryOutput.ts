/**
 * This file sends a Linear delivery's saved output to Linear.
 *
 * It reads the destination the delivery saved before its callback ran. A session callback's output goes
 * to its Agent Session as Agent Activities; an issue callback's output goes to the issue as comments.
 *
 * Agent Session:
 *
 * - `PresentOutcome` posts the one final activity Linear expects: `response` for `Completed`, `error`
 *   for `Failed`, and `elicitation` for `AwaitingInput`, with the options as choices. Without Markdown
 *   it posts a short default text. The final activity replaces any ephemeral thought, so there is no
 *   separate step to clear activity.
 * - `SetActivity` shows `Working` as an ephemeral thought, which Linear replaces with the next activity.
 *   `Idle` is applied without a call: Linear sets the session's state from its last activity.
 * - `CreateMessage` posts a lasting (non-ephemeral) thought. Activities cannot be edited or removed, so
 *   the session does not list `UpdateMessage` or `DeleteMessage`.
 * - `AddExternalLink` adds the link to the session (`agentSessionUpdate` with `addedExternalUrls`).
 * - `SetMessageReaction` reacts on the comment that started the session, else the issue. Its messages
 *   are activities, which take no reactions, so it has no message target.
 * - `RenderPlan` replaces the session's whole Agent Plan (`agentSessionUpdate` with `plan`); a failed
 *   item is `canceled`, with the reason in its text. The Agent Plan API is a technology preview: when
 *   Linear refuses the plan as invalid, the session shows the item in progress as an ephemeral thought
 *   instead, and keeps doing so for that delivery. A thought is posted only when that item changes.
 *
 * Every activity carries the operation's idempotency key as its ID. Linear refuses a second activity
 * with the same ID, so an attempt that finds the activity already made counts as applied, and session
 * output is exactly-once.
 *
 * Issue:
 *
 * - `PresentOutcome` comments the Markdown, with any options listed after it; without Markdown it is
 *   applied without a call.
 * - `CreateMessage` comments; `UpdateMessage` edits and `DeleteMessage` removes that comment. A comment
 *   already gone counts as deleted.
 * - `AddExternalLink` is applied without a call.
 * - `SetMessageReaction` reacts on the mentioning comment, else the issue, or on a comment the delivery
 *   posted.
 * - `RenderPlan` keeps one plan comment: the first plan comments it, and each later plan edits it. A plan
 *   comment someone deleted is commented again.
 *
 * Reactions, both kinds: an add sends the operation's idempotency key as the reaction's ID. Linear
 * answers a repeat with the reaction it already has (seen live), so a retry never reacts twice; an
 * `already_exists` answer also counts as applied. Its receipt is the reaction. Linear removes a
 * reaction by its ID, so a removal deletes the reaction this delivery's last add made, and one already
 * gone counts as removed. With no add to undo there is nothing this delivery can find to remove, and the
 * removal is applied without a call.
 *
 * A comment is at-least-once: when Linear accepts it but the attempt dies before the store saves the
 * result, the next attempt comments again, and the attempt carries `hadAmbiguousAttempt`.
 */
import {
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	type DeliveryOutcome,
	type PortableReaction,
	type ProviderOutputAttempt,
	type ProviderOutputProcessor,
	ProviderReactionTarget,
	type ProviderRenderPlan,
	type ProviderSetMessageReaction,
	type DeliveryPlan,
	type DeliveryPlanItem,
	deliveryPlanMarkdown,
	sameDeliveryPlan,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Option, Predicate, Schema } from 'effect'

import { LinearApi, type LinearApiError, LinearReactionTarget } from './LinearApi'
import type { LinearBotConfiguration } from './LinearBot'
import {
	LinearActivationTarget,
	LinearDeliveryDestination,
	LinearDeliveryPresentationVersion,
	type LinearAgentSessionDestination,
	type LinearIssueDestination,
} from './LinearDeliveryDestination'
import { LinearAgentActivityId, LinearReactionId } from './LinearIdentity'
import {
	LinearActivityContent,
	LinearAgentPlanStep,
	LinearAgentSessionExternalUrl,
	LinearCommentRef,
	LinearContent,
	LinearCreateAgentActivityRequest,
	LinearIssueRef,
	LinearUpdateAgentSessionRequest,
} from './LinearModels'

/** The Agent Activity Linear made for an operation. */
export const LinearActivityReceipt = Schema.TaggedStruct('LinearAgentActivity', { activityId: LinearAgentActivityId })

/** The issue comment Linear made for an operation. Later updates and deletions of the message receive it back. */
export const LinearCommentReceipt = Schema.TaggedStruct('LinearIssueComment', { comment: LinearCommentRef })

/** The reaction Linear made for an add. A later removal of the same reaction receives it back. */
export const LinearReactionReceipt = Schema.TaggedStruct('LinearReaction', {
	issue: LinearIssueRef,
	reactionId: LinearReactionId,
})
export type LinearReactionReceipt = typeof LinearReactionReceipt.Type

/** The session shows the plan as its Agent Plan. */
export const LinearAgentPlanReceipt = Schema.TaggedStruct('LinearAgentPlan', {})

/**
 * Linear refused the Agent Plan, so the session shows the plan's item in progress as a thought.
 *
 * @property text - the thought last posted, if any; a plan whose item in progress reads the same posts nothing
 */
export const LinearPlanThoughtReceipt = Schema.TaggedStruct('LinearPlanThought', {
	text: Schema.optionalKey(Schema.NonEmptyString),
})
export type LinearPlanThoughtReceipt = typeof LinearPlanThoughtReceipt.Type

/** What Linear made for an operation. Saved by the store, read only here. */
export const LinearOutputReceipt = Schema.Union([
	LinearActivityReceipt,
	LinearCommentReceipt,
	LinearReactionReceipt,
	LinearAgentPlanReceipt,
	LinearPlanThoughtReceipt,
])
export type LinearOutputReceipt = typeof LinearOutputReceipt.Type
const LinearOutputReceiptJson = Schema.toCodecJson(LinearOutputReceipt)
const LinearDeliveryDestinationJson = Schema.toCodecJson(LinearDeliveryDestination)
const LinearActivationTargetJson = Schema.toCodecJson(LinearActivationTarget)

/** Linear's emoji name for each portable reaction. */
export const linearPortableReactions = {
	thumbs_up: '+1',
	thumbs_down: '-1',
	laugh: 'laughing',
	confused: 'confused',
	heart: 'heart',
	hooray: 'tada',
	rocket: 'rocket',
	eyes: 'eyes',
} as const satisfies Record<PortableReaction, string>

/** The text of a session's final activity when the remote worker sent no Markdown. */
export const linearDefaultOutcomeText = {
	Completed: 'Done.',
	Failed: 'The work could not be finished.',
	AwaitingInput: 'Waiting for your reply.',
} as const

const failed = (safeCode: string, retryable: boolean, retryAfterMs?: number) =>
	new DeliveryOutputFailed(
		Predicate.isUndefined(retryAfterMs)
			? { provider: 'linear', retryable, safeCode }
			: { provider: 'linear', retryable, safeCode, retryAfterMs },
	)

/** An item's text with its note, if it has one. */
const withNote = (title: string, note: string | undefined) => (Predicate.isUndefined(note) ? title : `${title}: ${note}`)

/** The plan as Agent Plan steps. Linear has no failed status, so a failed item is `canceled` and says why. */
export const linearAgentPlanSteps = (plan: DeliveryPlan) =>
	plan.items.map((item) =>
		Match.value(item.state).pipe(
			Match.tagsExhaustive({
				Pending: () => LinearAgentPlanStep.make({ content: item.title, status: 'pending' }),
				InProgress: ({ details }) => LinearAgentPlanStep.make({ content: withNote(item.title, details), status: 'inProgress' }),
				Completed: ({ result }) => LinearAgentPlanStep.make({ content: withNote(item.title, result), status: 'completed' }),
				Failed: ({ reason }) =>
					LinearAgentPlanStep.make({ content: `${item.title} (failed${Predicate.isUndefined(reason) ? '' : `: ${reason}`})`, status: 'canceled' }),
			}),
		),
	)

/** The thought that stands in for the Agent Plan: the first item in progress, if any. */
const planThoughtText = (plan: DeliveryPlan) => {
	const current = plan.items.find((item: DeliveryPlanItem) => Predicate.isTagged(item.state, 'InProgress'))
	if (Predicate.isUndefined(current) || !Predicate.isTagged(current.state, 'InProgress')) return undefined
	return withNote(current.title, current.state.details)
}

/** The comment a result posts on an issue: its Markdown, then any options as a list. */
const issueOutcomeMarkdown = (outcome: DeliveryOutcome, markdown: string) =>
	Match.value(outcome).pipe(
		Match.tag('AwaitingInput', ({ options }) =>
			Arr.match(options ?? [], {
				onEmpty: () => markdown,
				onNonEmpty: (listed) => `${markdown}\n\n${listed.map((option) => `- ${option}`).join('\n')}`,
			}),
		),
		Match.orElse(() => markdown),
	)

/** The final activity of a session turn. */
const sessionOutcomeContent = (outcome: DeliveryOutcome, markdown: string | undefined) =>
	Match.value(outcome).pipe(
		Match.withReturnType<LinearActivityContent>(),
		Match.tagsExhaustive({
			Completed: () =>
				LinearActivityContent.cases.Response.make({ body: markdown ?? linearDefaultOutcomeText.Completed }),
			Failed: () => LinearActivityContent.cases.Error.make({ body: markdown ?? linearDefaultOutcomeText.Failed }),
			AwaitingInput: ({ options }) => {
				const body = markdown ?? linearDefaultOutcomeText.AwaitingInput
				return LinearActivityContent.cases.Elicitation.make(
					Predicate.isUndefined(options) ? { body } : { body, options },
				)
			},
		}),
	)

/** Build Linear's output half. Its `LinearApi` is the bot's own. */
export const makeLinearOutputProcessor = Effect.fn('linear.make_output_processor')(function* (input: {
	readonly namespace: string
	readonly bot: LinearBotConfiguration
}) {
	const linearApi = yield* LinearApi

	const process = Effect.fn('linear.process_delivery_output')(function* (attempt: ProviderOutputAttempt) {
		const annotations = { delivery_id: attempt.deliveryId, operation_id: attempt.operationId }
		if (attempt.prepared.presentationVersion !== LinearDeliveryPresentationVersion) {
			return yield* failed('unsupported_presentation_version', false)
		}
		const destination = yield* Schema.decodeEffect(LinearDeliveryDestinationJson)(
			attempt.prepared.destination,
		).pipe(Effect.mapError(() => failed('destination_invalid', false)))

		/** Log a Linear failure with the operation it belongs to, then report it as retryable or not. */
		const reportLinearFailure =
			(message: string, safeCode: string) =>
			<A>(effect: Effect.Effect<A, LinearApiError>) =>
				effect.pipe(
					Effect.tapError((error) => Effect.logWarning(message, error).pipe(Effect.annotateLogs(annotations))),
					Effect.mapError((error) => failed(safeCode, error.retryable, error.retryAfterMs)),
				)

		const encodeReceipt = (receipt: LinearOutputReceipt) =>
			Schema.encodeEffect(LinearOutputReceiptJson)(receipt).pipe(
				Effect.mapError(() => failed('receipt_unencodable', false)),
				Effect.map((encoded) => DeliveryOutputApplied.make({ receipt: encoded })),
			)

		const applied = DeliveryOutputApplied.make({})
		const unsupported = Effect.fail(failed('unsupported_operation', false))

		/** Log saved data this processor cannot read, then report it as final. */
		const reportUnreadable =
			(message: string, safeCode: string) =>
			<A, E>(effect: Effect.Effect<A, E>) =>
				effect.pipe(
					Effect.tapError((error) => Effect.logWarning(message, error).pipe(Effect.annotateLogs(annotations))),
					Effect.mapError(() => failed(safeCode, false)),
				)

		/** A saved receipt, read back. */
		const decodeReceipt = (reference: Schema.Json, safeCode: string) =>
			Schema.decodeEffect(LinearOutputReceiptJson)(reference).pipe(
				reportUnreadable('Linear output receipt could not be read', safeCode),
			)

		/** The comment a reference names. */
		const postedComment = (reference: Schema.Json) =>
			decodeReceipt(reference, 'message_reference_invalid').pipe(
				Effect.flatMap((receipt) =>
					Match.value(receipt).pipe(
						Match.withReturnType<Effect.Effect<LinearCommentRef, DeliveryOutputFailed>>(),
						Match.tagsExhaustive({
							LinearIssueComment: ({ comment: posted }) => Effect.succeed(posted),
							LinearAgentActivity: () => Effect.fail(failed('message_reference_invalid', false)),
							LinearReaction: () => Effect.fail(failed('message_reference_invalid', false)),
							LinearAgentPlan: () => Effect.fail(failed('message_reference_invalid', false)),
							LinearPlanThought: () => Effect.fail(failed('message_reference_invalid', false)),
						}),
					),
				),
			)

		/** The comment or issue that started the delivery. */
		const activationReactionTarget = Effect.fromOption(
			Option.fromUndefinedOr(attempt.prepared.activationTarget),
		).pipe(
			Effect.mapError(() => failed('activation_target_missing', false)),
			Effect.flatMap((encoded) =>
				Schema.decodeEffect(LinearActivationTargetJson)(encoded).pipe(
					reportUnreadable('Linear output activation target could not be read', 'activation_target_invalid'),
				),
			),
			Effect.filterOrFail(
				(target) => target.organizationId === input.bot.organizationId,
				() => failed('destination_identity_mismatch', false),
			),
			Effect.map((target) =>
				Match.value(target).pipe(
					Match.withReturnType<LinearReactionTarget>(),
					Match.tagsExhaustive({
						LinearIssueActivationTarget: ({ organizationId, issueId }) =>
							LinearReactionTarget.cases.Issue.make({
								issue: LinearIssueRef.make({ organizationId, teamId: null, issueId }),
							}),
						LinearCommentActivationTarget: ({ organizationId, issueId, commentId }) =>
							LinearReactionTarget.cases.Comment.make({
								comment: LinearCommentRef.make({ organizationId, teamId: null, issueId, commentId }),
							}),
					}),
				),
			),
		)

		/** Add the bot's reaction under the operation's idempotency key; the receipt is the reaction. */
		const addReaction = (target: LinearReactionTarget, reaction: PortableReaction) => {
			const reactionId = LinearReactionId.make(attempt.idempotencyKey)
			const issue = Match.value(target).pipe(
				Match.tagsExhaustive({
					Issue: ({ issue: reacted }) => reacted,
					Comment: ({ comment }) =>
						LinearIssueRef.make({ organizationId: comment.organizationId, teamId: null, issueId: comment.issueId }),
				}),
			)
			return linearApi.createReaction({ target, emoji: linearPortableReactions[reaction], reactionId }).pipe(
				Effect.asVoid,
				Effect.catchIf(
					(error) => error.reason === 'already_exists',
					() =>
						Effect.logInfo('Linear reaction already exists; an earlier attempt made it').pipe(
							Effect.annotateLogs(annotations),
						),
				),
				reportLinearFailure('Linear reaction failed', 'linear_reaction_failed'),
				Effect.flatMap(() => encodeReceipt(LinearReactionReceipt.make({ issue, reactionId }))),
				Effect.withSpan('linear.output.add_reaction', { attributes: { reaction } }),
			)
		}

		/** Delete the reaction this delivery's last add made. One already gone counts as removed. */
		const removeReaction = (addedReference: Schema.Json | undefined) => {
			if (Predicate.isUndefined(addedReference)) {
				return Effect.logInfo('Linear reaction removal has no add to undo; nothing to remove').pipe(
					Effect.annotateLogs(annotations),
					Effect.as(applied),
				)
			}
			const notAReaction = () =>
				Effect.logWarning('Linear reaction removal got a receipt that is not a reaction').pipe(
					Effect.annotateLogs(annotations),
					Effect.andThen(Effect.fail(failed('reaction_reference_invalid', false))),
				)
			return decodeReceipt(addedReference, 'reaction_reference_invalid').pipe(
				Effect.flatMap((receipt) =>
					Match.value(receipt).pipe(
						Match.withReturnType<Effect.Effect<LinearReactionReceipt, DeliveryOutputFailed>>(),
						Match.tagsExhaustive({
							LinearReaction: (added) => Effect.succeed(added),
							LinearIssueComment: notAReaction,
							LinearAgentActivity: notAReaction,
							LinearAgentPlan: notAReaction,
							LinearPlanThought: notAReaction,
						}),
					),
				),
				Effect.flatMap(({ issue, reactionId }) =>
					linearApi.deleteReaction({ issue, reactionId }).pipe(
						Effect.catchIf(
							(error) => error.reason === 'not_found',
							() =>
								Effect.logInfo('Linear reaction was already gone; counting it as removed').pipe(
									Effect.annotateLogs(annotations),
								),
						),
						reportLinearFailure('Linear reaction removal failed', 'linear_reaction_failed'),
					),
				),
				Effect.as(applied),
				Effect.withSpan('linear.output.remove_reaction'),
			)
		}

		/** Add or remove the bot's reaction on the activation target, or on a comment the delivery posted. */
		const setReaction = ({ target, reaction, active, addedReference }: ProviderSetMessageReaction) => {
			if (!active) return removeReaction(addedReference)
			return ProviderReactionTarget.match(target, {
				ActivationTarget: () => activationReactionTarget,
				MessageTarget: ({ reference }) =>
					postedComment(reference).pipe(
						Effect.map((comment) => LinearReactionTarget.cases.Comment.make({ comment })),
					),
			}).pipe(Effect.flatMap((reactionTarget) => addReaction(reactionTarget, reaction)))
		}

		const toSession = (session: LinearAgentSessionDestination) => {
			if (session.organizationId !== input.bot.organizationId || session.appUserId !== input.bot.appUserId) {
				return Effect.fail(failed('destination_identity_mismatch', false))
			}
			const activityId = LinearAgentActivityId.make(attempt.idempotencyKey)

			/**
			 * Post one activity under the operation's idempotency key. Linear refuses a second activity with
			 * the same ID, so finding it already made means an earlier attempt made it.
			 */
			const sendActivity = (content: LinearActivityContent, ephemeral: boolean) =>
				linearApi
					.createAgentActivity(
						LinearCreateAgentActivityRequest.make({
							organizationId: session.organizationId,
							sessionId: session.sessionId,
							content,
							ephemeral,
							activityId,
						}),
					)
					.pipe(
						Effect.map((receipt) => receipt.activityId),
						Effect.catchIf(
							(error) => error.reason === 'already_exists',
							() =>
								Effect.logInfo('Linear activity already exists; an earlier attempt posted it').pipe(
									Effect.annotateLogs(annotations),
									Effect.as(activityId),
								),
						),
						reportLinearFailure('Linear session activity failed', 'linear_activity_failed'),
					)

			const postActivity = (content: LinearActivityContent, ephemeral: boolean) =>
				sendActivity(content, ephemeral).pipe(
					Effect.flatMap((made) =>
						encodeReceipt(LinearActivityReceipt.make({ activityId: LinearAgentActivityId.make(made) })),
					),
				)

			/**
			 * The Agent Plan's stand-in: post the item in progress as an ephemeral thought, unless the last
			 * thought already said the same.
			 */
			const planThought = (plan: DeliveryPlan, previous: LinearPlanThoughtReceipt | undefined) => {
				const text = planThoughtText(plan)
				if (Predicate.isUndefined(text) || text === previous?.text) {
					return encodeReceipt(previous ?? LinearPlanThoughtReceipt.make({}))
				}
				return sendActivity(LinearActivityContent.cases.Thought.make({ body: text }), true).pipe(
					Effect.flatMap(() => encodeReceipt(LinearPlanThoughtReceipt.make({ text }))),
				)
			}

			/** How the session showed the plan last time, read from its presentation. Unreadable counts as never shown. */
			const shownPlan = (presentation: Schema.Json | undefined) =>
				Predicate.isUndefined(presentation)
					? Effect.succeed(undefined)
					: Schema.decodeEffect(LinearOutputReceiptJson)(presentation).pipe(
							Effect.catchTag('SchemaError', (error) =>
								Effect.logWarning('Linear plan presentation could not be read; sending the Agent Plan', error).pipe(
									Effect.annotateLogs(annotations),
									Effect.as(undefined),
								),
							),
						)

			/**
			 * Replace the session's whole Agent Plan. A session already showing the thought stand-in keeps it;
			 * one whose Agent Plan Linear refuses as invalid switches to it.
			 */
			const renderPlan = ({ plan, rendered }: ProviderRenderPlan) =>
				Effect.gen(function* () {
					const shown = yield* shownPlan(rendered?.presentation)
					if (Predicate.isNotUndefined(shown) && Predicate.isTagged(shown, 'LinearPlanThought')) {
						return yield* planThought(plan, shown)
					}
					const replaced = yield* linearApi
						.updateAgentSession(
							LinearUpdateAgentSessionRequest.make({
								organizationId: session.organizationId,
								sessionId: session.sessionId,
								plan: linearAgentPlanSteps(plan),
							}),
						)
						.pipe(
							Effect.as(true),
							Effect.catchIf(
								(error) => error.reason === 'validation' || error.reason === 'rejected',
								(error) =>
									Effect.logWarning('Linear refused the Agent Plan; showing the item in progress as a thought', error).pipe(
										Effect.annotateLogs(annotations),
										Effect.as(false),
									),
							),
							reportLinearFailure('Linear Agent Plan update failed', 'linear_plan_failed'),
						)
					return replaced ? yield* encodeReceipt(LinearAgentPlanReceipt.make({})) : yield* planThought(plan, undefined)
				}).pipe(Effect.withSpan('linear.output.render_session_plan', { attributes: { item_count: plan.items.length } }))

			return Match.value(attempt.operation).pipe(
				Match.tagsExhaustive({
					PresentOutcome: ({ outcome, markdown }) => postActivity(sessionOutcomeContent(outcome, markdown), false),
					SetActivity: ({ activity }) =>
						Match.value(activity).pipe(
							Match.tagsExhaustive({
								Working: ({ message }) =>
									postActivity(LinearActivityContent.cases.Thought.make({ body: message }), true),
								Idle: () => Effect.succeed(applied),
							}),
						),
					CreateMessage: ({ markdown }) =>
						postActivity(LinearActivityContent.cases.Thought.make({ body: markdown }), false),
					UpdateMessage: () => unsupported,
					DeleteMessage: () => unsupported,
					SetMessageReaction: (operation) =>
						ProviderReactionTarget.match(operation.target, {
							ActivationTarget: () => setReaction(operation),
							MessageTarget: () => unsupported,
						}),
					RenderPlan: renderPlan,
					AddExternalLink: ({ link }) =>
						linearApi
							.updateAgentSession(
								LinearUpdateAgentSessionRequest.make({
									organizationId: session.organizationId,
									sessionId: session.sessionId,
									addedExternalUrls: [LinearAgentSessionExternalUrl.make({ label: link.label, url: link.url })],
								}),
							)
							.pipe(
								reportLinearFailure('Linear session link update failed', 'linear_session_update_failed'),
								Effect.as(applied),
							),
				}),
			)
		}

		const toIssue = (target: LinearIssueDestination) => {
			if (target.organizationId !== input.bot.organizationId) {
				return Effect.fail(failed('destination_identity_mismatch', false))
			}
			const issue = LinearIssueRef.make({ organizationId: target.organizationId, teamId: null, issueId: target.issueId })

			/** Comment on the issue; the receipt is the comment. */
			const comment = (markdown: string) =>
				linearApi.createComment({ issue, content: LinearContent.make({ markdown }) }).pipe(
					reportLinearFailure('Linear issue comment failed', 'linear_comment_failed'),
					Effect.flatMap((created) => encodeReceipt(LinearCommentReceipt.make({ comment: created.ref }))),
				)

			/**
			 * Show the plan in one comment: comment it the first time, then edit that comment, whose receipt
			 * is the plan's presentation. An unchanged plan makes no call; a deleted one is commented again.
			 */
			const renderPlan = ({ plan, rendered }: ProviderRenderPlan) =>
				Effect.gen(function* () {
					const markdown = deliveryPlanMarkdown(plan)
					const presentation = rendered?.presentation
					if (Predicate.isUndefined(rendered) || Predicate.isUndefined(presentation)) return yield* comment(markdown)
					if (sameDeliveryPlan(rendered.plan, plan)) return DeliveryOutputApplied.make({ receipt: presentation })
					const posted = yield* postedComment(presentation)
					const edited = yield* linearApi
						.updateComment({ comment: posted, content: LinearContent.make({ markdown }) })
						.pipe(
							Effect.as(true),
							Effect.catchIf(
								(error) => error.reason === 'not_found',
								(error) =>
									Effect.logInfo('Linear plan comment is gone; commenting the plan again', error).pipe(
										Effect.annotateLogs(annotations),
										Effect.as(false),
									),
							),
							reportLinearFailure('Linear plan comment update failed', 'linear_comment_update_failed'),
						)
					return edited ? DeliveryOutputApplied.make({ receipt: presentation }) : yield* comment(markdown)
				}).pipe(Effect.withSpan('linear.output.render_issue_plan'))

			return Match.value(attempt.operation).pipe(
				Match.tagsExhaustive({
					RenderPlan: renderPlan,
					PresentOutcome: ({ outcome, markdown }) =>
						Predicate.isUndefined(markdown)
							? Effect.succeed(applied)
							: comment(issueOutcomeMarkdown(outcome, markdown)),
					SetActivity: () => unsupported,
					CreateMessage: ({ markdown }) => comment(markdown),
					UpdateMessage: ({ markdown, reference }) =>
						postedComment(reference).pipe(
							Effect.flatMap((posted) =>
								linearApi
									.updateComment({ comment: posted, content: LinearContent.make({ markdown }) })
									.pipe(reportLinearFailure('Linear issue comment update failed', 'linear_comment_update_failed')),
							),
							Effect.as(applied),
						),
					DeleteMessage: ({ reference }) =>
						postedComment(reference).pipe(
							Effect.flatMap((posted) =>
								linearApi.deleteComment({ comment: posted }).pipe(
									Effect.catchIf(
										(error) => error.reason === 'not_found',
										() => Effect.logInfo('Linear comment was already gone; counting it as deleted'),
									),
									reportLinearFailure('Linear issue comment delete failed', 'linear_comment_delete_failed'),
								),
							),
							Effect.as(applied),
						),
					SetMessageReaction: setReaction,
					AddExternalLink: () => Effect.succeed(applied),
				}),
			)
		}

		if (attempt.hadAmbiguousAttempt) {
			yield* Effect.logWarning('Linear output retried after an attempt that may have been applied').pipe(
				Effect.annotateLogs(annotations),
			)
		}
		return yield* Match.value(destination).pipe(
			Match.tagsExhaustive({
				LinearAgentSessionDestination: toSession,
				LinearIssueDestination: toIssue,
			}),
		)
	})

	return { namespace: input.namespace, providerName: 'linear', process } satisfies ProviderOutputProcessor
})
