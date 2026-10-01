/**
 * This file sends a Slack delivery's saved output to Slack.
 *
 * It reads the thread the delivery saved before its callback ran, and turns each operation into a
 * Slack call:
 *
 * - `PresentOutcome` posts the Markdown to the thread, with any `awaitingInput` options listed after it.
 *   A post also clears the thread's status line. With no Markdown it only clears the status line, and
 *   only when the remote worker had set one (`clearActivity`); otherwise it is applied without a call.
 * - `SetActivity` shows `Working` as the thread's status line (`assistant.threads.setStatus`), and
 *   clears it for `Idle`.
 * - `CreateMessage` posts to the thread. Its receipt is the posted message, which later updates and
 *   deletions of the same message receive back as their reference.
 * - `UpdateMessage` edits that message with `chat.update`; `DeleteMessage` removes it with `chat.delete`.
 *   A message already gone counts as deleted.
 * - `AddExternalLink` is applied without a call; Slack has nowhere to show a link for now.
 * - `SetMessageReaction` adds or removes the bot's reaction (`reactions.add`, `reactions.remove`) on the
 *   message that started the delivery, or on a message the delivery posted. A reaction already there,
 *   or already gone, counts as done.
 *
 * A post is at-least-once: when Slack accepts it but the attempt dies before the store saves the
 * result, the next attempt posts again, and the first post stays in the thread. The attempt then
 * carries `hadAmbiguousAttempt`, which delivery status shows. Edits, deletions, and reactions are safe to
 * repeat.
 */
import {
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	type DeliveryOutcome,
	type PortableReaction,
	type ProviderOutputAttempt,
	type ProviderOutputProcessor,
	ProviderReactionTarget,
	type ProviderSetMessageReaction,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Option, Predicate, Schema } from 'effect'

import { SlackApi, type SlackApiError } from './SlackApi'
import {
	SlackActivationTargetJson,
	SlackDeliveryDestinationJson,
	slackPresentationVersion,
} from './SlackDeliveryDestination'
import { SlackMarkdownContent, SlackMessageRef, SlackReaction } from './SlackModels'

/** What Slack made for an operation: the message it posted. Saved by the store, read only here. */
export const SlackOutputReceipt = Schema.TaggedStruct('SlackPostedMessage', {
	message: SlackMessageRef,
})
export type SlackOutputReceipt = typeof SlackOutputReceipt.Type
export const SlackOutputReceiptJson = Schema.toCodecJson(SlackOutputReceipt)

/**
 * Slack errors that another attempt cannot fix: the channel or thread is gone, the bot may not post
 * there, the token is bad, or the message itself is refused. Anything else, such as a network failure,
 * a 5xx, or a rate limit that outlasted its retries, is worth another attempt.
 */
const permanentSlackErrors: ReadonlySet<string> = new Set([
	'account_inactive',
	'as_user_not_supported',
	'cant_delete_message',
	'cant_update_message',
	'channel_not_found',
	'compliance_exports_prevent_deletion',
	'edit_window_closed',
	'invalid_auth',
	'invalid_blocks',
	'invalid_thread_ts',
	'is_archived',
	'message_not_found',
	'missing_scope',
	'msg_too_long',
	'no_text',
	'not_allowed_token_type',
	'not_authed',
	'not_in_channel',
	'restricted_action',
	'restricted_action_non_threadable_channel',
	'restricted_action_read_only_channel',
	'restricted_action_thread_locked',
	'restricted_action_thread_only_channel',
	'team_access_not_granted',
	'thread_locked',
	'token_expired',
	'token_revoked',
])

/** Whether another attempt at a failed Slack call might succeed. */
export const isRetryableSlackApiError = (error: SlackApiError) => !permanentSlackErrors.has(error.message)

/** Slack's emoji name for each portable reaction. */
export const slackPortableReactions = {
	thumbs_up: SlackReaction.make('thumbsup'),
	thumbs_down: SlackReaction.make('thumbsdown'),
	laugh: SlackReaction.make('laughing'),
	confused: SlackReaction.make('confused'),
	heart: SlackReaction.make('heart'),
	hooray: SlackReaction.make('tada'),
	rocket: SlackReaction.make('rocket'),
	eyes: SlackReaction.make('eyes'),
} satisfies Record<PortableReaction, SlackReaction>

const failed = (safeCode: string, retryable: boolean) =>
	new DeliveryOutputFailed({ provider: 'slack', retryable, safeCode })

/** The message a result posts: its Markdown, then any options as a list. */
const outcomeMarkdown = (outcome: DeliveryOutcome, markdown: string) =>
	Match.value(outcome).pipe(
		Match.tag('AwaitingInput', ({ options }) =>
			Arr.match(options ?? [], {
				onEmpty: () => markdown,
				onNonEmpty: (listed) => `${markdown}\n\n${listed.map((option) => `• ${option}`).join('\n')}`,
			}),
		),
		Match.orElse(() => markdown),
	)

/** Build Slack's output half. Its `SlackApi` is the bot's own. */
export const makeSlackOutputProcessor = Effect.fn('slack.make_output_processor')(function* (input: {
	readonly namespace: string
}) {
	const slackApi = yield* SlackApi

	const process = Effect.fn('slack.process_delivery_output')(function* (attempt: ProviderOutputAttempt) {
		if (attempt.prepared.presentationVersion !== slackPresentationVersion) {
			return yield* failed('unsupported_presentation_version', false)
		}
		const destination = yield* Schema.decodeUnknownEffect(SlackDeliveryDestinationJson)(
			attempt.prepared.destination,
		).pipe(Effect.mapError(() => failed('destination_invalid', false)))
		if (attempt.hadAmbiguousAttempt) {
			yield* Effect.logWarning('Slack output retried after an attempt that may have posted').pipe(
				Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
			)
		}

		/** Log a Slack failure with the operation it belongs to, then report it as retryable or not. */
		const reportSlackFailure =
			(message: string, safeCode: string) =>
			<A>(effect: Effect.Effect<A, SlackApiError>) =>
				effect.pipe(
					Effect.tapError((error) =>
						Effect.logWarning(message, error).pipe(
							Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
						),
					),
					Effect.mapError((error) => failed(safeCode, isRetryableSlackApiError(error))),
				)

		/** Post Markdown to the thread; the receipt is the posted message. */
		const post = (markdown: string) =>
			slackApi
				.postToThread({ thread: destination.thread, content: SlackMarkdownContent.make({ markdown }) })
				.pipe(
					reportSlackFailure('Slack output post failed', 'slack_post_failed'),
					Effect.flatMap(({ ref }) =>
						Schema.encodeEffect(SlackOutputReceiptJson)(SlackOutputReceipt.make({ message: ref })).pipe(
							Effect.mapError(() => failed('receipt_unencodable', false)),
						),
					),
					Effect.map((receipt) => DeliveryOutputApplied.make({ receipt })),
				)

		/** The posted message a reference names. */
		const postedMessage = (reference: Schema.Json) =>
			Schema.decodeUnknownEffect(SlackOutputReceiptJson)(reference).pipe(
				Effect.map(({ message }) => message),
				Effect.mapError(() => failed('message_reference_invalid', false)),
			)

		const applied = DeliveryOutputApplied.make({})

		/** Clear the thread's status line. */
		const clearStatus = slackApi
			.clearThreadStatus({ thread: destination.thread })
			.pipe(reportSlackFailure('Slack thread status clear failed', 'slack_status_failed'), Effect.as(applied))

		/** The message a reaction goes on: the one that started the delivery, or one the delivery posted. */
		const reactionMessage = (target: ProviderReactionTarget) =>
			ProviderReactionTarget.match(target, {
				ActivationTarget: () =>
					Effect.fromOption(Option.fromUndefinedOr(attempt.prepared.activationTarget)).pipe(
						Effect.mapError(() => failed('activation_target_missing', false)),
						Effect.flatMap((encoded) =>
							Schema.decodeUnknownEffect(SlackActivationTargetJson)(encoded).pipe(
								Effect.tapError((error) =>
									Effect.logWarning('Slack output activation target could not be read', error).pipe(
										Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
									),
								),
								Effect.mapError(() => failed('activation_target_invalid', false)),
							),
						),
						Effect.map(({ message }) => message),
					),
				MessageTarget: ({ reference }) => postedMessage(reference),
			})

		/**
		 * Add or remove the bot's reaction. Slack answers `already_reacted` and `no_reaction` when the
		 * reaction is already as asked, and `message_not_found` when a removal's message is gone.
		 */
		const setReaction = ({ target, reaction, active }: ProviderSetMessageReaction) =>
			reactionMessage(target).pipe(
				Effect.flatMap((message) => {
					const request = { message, reaction: slackPortableReactions[reaction] }
					const done: ReadonlySet<string> = active
						? new Set(['already_reacted'])
						: new Set(['no_reaction', 'message_not_found'])
					return (active ? slackApi.addReaction(request) : slackApi.removeReaction(request)).pipe(
						Effect.catchIf(
							(error) => done.has(error.message),
							(error) =>
								Effect.logInfo('Slack reaction was already as asked; counting it as done').pipe(
									Effect.annotateLogs({ slack_error: error.message }),
								),
						),
						reportSlackFailure('Slack output reaction failed', 'slack_reaction_failed'),
					)
				}),
				Effect.as(applied),
				Effect.withSpan('slack.output.set_reaction', { attributes: { reaction, active } }),
			)

		return yield* Match.value(attempt.operation).pipe(
			Match.tagsExhaustive({
				AddExternalLink: () => Effect.succeed(applied),
				SetMessageReaction: setReaction,
				PresentOutcome: ({ outcome, markdown, clearActivity }) =>
					Predicate.isNotUndefined(markdown)
						? post(outcomeMarkdown(outcome, markdown))
						: clearActivity
							? clearStatus
							: Effect.succeed(applied),
				SetActivity: ({ activity }) =>
					Match.value(activity).pipe(
						Match.tagsExhaustive({
							Working: ({ message }) =>
								slackApi
									.setThreadStatus({ thread: destination.thread, status: message })
									.pipe(
										reportSlackFailure('Slack thread status update failed', 'slack_status_failed'),
										Effect.as(applied),
									),
							Idle: () => clearStatus,
						}),
					),
				CreateMessage: ({ markdown }) => post(markdown),
				UpdateMessage: ({ markdown, reference }) =>
					postedMessage(reference).pipe(
						Effect.flatMap((message) =>
							slackApi
								.updateMessage({ message, content: SlackMarkdownContent.make({ markdown }) })
								.pipe(reportSlackFailure('Slack output update failed', 'slack_update_failed')),
						),
						Effect.as(applied),
					),
				DeleteMessage: ({ reference }) =>
					postedMessage(reference).pipe(
						Effect.flatMap((message) =>
							slackApi.deleteMessage({ message }).pipe(
								Effect.catchIf(
									(error) => error.message === 'message_not_found',
									() => Effect.logInfo('Slack message was already gone; counting it as deleted'),
								),
								reportSlackFailure('Slack output delete failed', 'slack_delete_failed'),
							),
						),
						Effect.as(applied),
					),
			}),
		)
	})

	return { namespace: input.namespace, providerName: 'slack', process } satisfies ProviderOutputProcessor
})
