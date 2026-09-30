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
 *
 * A comment is at-least-once: when Linear accepts it but the attempt dies before the store saves the
 * result, the next attempt comments again, and the attempt carries `hadAmbiguousAttempt`.
 */
import {
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	type DeliveryOutcome,
	type ProviderOutputAttempt,
	type ProviderOutputProcessor,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Predicate, Schema } from 'effect'

import { LinearApi, type LinearApiError } from './LinearApi'
import type { LinearBotConfiguration } from './LinearBot'
import {
	LinearDeliveryDestination,
	LinearDeliveryPresentationVersion,
	type LinearAgentSessionDestination,
	type LinearIssueDestination,
} from './LinearDeliveryDestination'
import { LinearAgentActivityId } from './LinearIdentity'
import {
	LinearActivityContent,
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

/** What Linear made for an operation. Saved by the store, read only here. */
export const LinearOutputReceipt = Schema.Union([LinearActivityReceipt, LinearCommentReceipt])
export type LinearOutputReceipt = typeof LinearOutputReceipt.Type
const LinearOutputReceiptJson = Schema.toCodecJson(LinearOutputReceipt)
const LinearDeliveryDestinationJson = Schema.toCodecJson(LinearDeliveryDestination)

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

		const toSession = (session: LinearAgentSessionDestination) => {
			if (session.organizationId !== input.bot.organizationId || session.appUserId !== input.bot.appUserId) {
				return Effect.fail(failed('destination_identity_mismatch', false))
			}
			const activityId = LinearAgentActivityId.make(attempt.idempotencyKey)

			/**
			 * Post one activity under the operation's idempotency key. Linear refuses a second activity with
			 * the same ID, so finding it already made means an earlier attempt made it.
			 */
			const postActivity = (content: LinearActivityContent, ephemeral: boolean) =>
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
						Effect.flatMap((made) =>
							encodeReceipt(LinearActivityReceipt.make({ activityId: LinearAgentActivityId.make(made) })),
						),
					)

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

			/** The comment a reference names. */
			const postedComment = (reference: Schema.Json) =>
				Schema.decodeEffect(LinearOutputReceiptJson)(reference).pipe(
					Effect.mapError(() => failed('message_reference_invalid', false)),
					Effect.flatMap((receipt) =>
						Match.value(receipt).pipe(
							Match.tagsExhaustive({
								LinearIssueComment: ({ comment: posted }) => Effect.succeed(posted),
								LinearAgentActivity: () => Effect.fail(failed('message_reference_invalid', false)),
							}),
						),
					),
				)

			return Match.value(attempt.operation).pipe(
				Match.tagsExhaustive({
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
