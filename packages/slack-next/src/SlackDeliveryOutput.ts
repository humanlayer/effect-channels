/**
 * This file sends a Slack delivery's saved output to Slack.
 *
 * It reads the thread the delivery saved before its callback ran, and turns each operation into a
 * Slack call:
 *
 * - `PresentOutcome` posts the Markdown to the thread, with any `awaitingInput` options listed after it.
 *   With no Markdown there is nothing to show, so it is applied without a call.
 * - `AddExternalLink` is applied without a call; Slack has nowhere to show a link for now.
 *
 * A post is at-least-once: when Slack accepts it but the attempt dies before the store saves the
 * result, the next attempt posts again. The attempt then carries `hadAmbiguousAttempt`.
 */
import {
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	type DeliveryOutcome,
	type ProviderOutputAttempt,
	type ProviderOutputProcessor,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Predicate, Schema } from 'effect'

import { SlackApi, type SlackApiError } from './SlackApi'
import { SlackDeliveryDestinationJson, slackPresentationVersion } from './SlackDeliveryDestination'
import { SlackMarkdownContent, SlackMessageRef } from './SlackModels'

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
	'channel_not_found',
	'invalid_auth',
	'invalid_blocks',
	'invalid_thread_ts',
	'is_archived',
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

		return yield* Match.value(attempt.operation).pipe(
			Match.tagsExhaustive({
				AddExternalLink: () => Effect.succeed(DeliveryOutputApplied.make({})),
				PresentOutcome: ({ outcome, markdown }) =>
					Predicate.isUndefined(markdown)
						? Effect.succeed(DeliveryOutputApplied.make({}))
						: slackApi
								.postToThread({
									thread: destination.thread,
									content: SlackMarkdownContent.make({
										markdown: outcomeMarkdown(outcome, markdown),
									}),
								})
								.pipe(
									Effect.tapError((error) =>
										Effect.logWarning('Slack output post failed', error).pipe(
											Effect.annotateLogs({
												delivery_id: attempt.deliveryId,
												operation_id: attempt.operationId,
											}),
										),
									),
									Effect.mapError((error) =>
										failed('slack_post_failed', isRetryableSlackApiError(error)),
									),
									Effect.flatMap(({ ref }) =>
										Schema.encodeEffect(SlackOutputReceiptJson)(
											SlackOutputReceipt.make({ message: ref }),
										).pipe(Effect.mapError(() => failed('receipt_unencodable', false))),
									),
									Effect.map((receipt) => DeliveryOutputApplied.make({ receipt })),
								),
			}),
		)
	})

	return { namespace: input.namespace, providerName: 'slack', process } satisfies ProviderOutputProcessor
})
