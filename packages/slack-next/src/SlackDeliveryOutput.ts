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
 * - `RenderPlan` shows the plan as a stream in plan mode, which stays open. The first plan starts it with
 *   every task; a later one appends only what changed (see `SlackDeliveryPlan.ts`). A change no chunk can
 *   express, or a stream Slack has already closed, starts a new stream with the whole plan, then stops and
 *   deletes the old one as best it can. `PresentOutcome` stops the stream before it posts.
 *
 * A post is at-least-once: when Slack accepts it but the attempt dies before the store saves the
 * result, the next attempt posts again, and the first post stays in the thread. The attempt then
 * carries `hadAmbiguousAttempt`, which delivery status shows. Edits, deletions, and reactions are safe to
 * repeat. So is appending a plan: each task chunk carries the task's whole state. Starting a plan stream
 * is at-least-once, like a post.
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
	type DeliveryPlan,
	type RenderedDeliveryPlan,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Option, Predicate, Schema } from 'effect'

import { SlackApi, type SlackApiError } from './SlackApi'
import {
	SlackActivationTargetJson,
	SlackDeliveryDestinationJson,
	slackPresentationVersion,
} from './SlackDeliveryDestination'
import {
	SlackPlanChange,
	SlackPlanPresentation,
	SlackPlanPresentationJson,
	slackPlanChange,
	slackPlanStartChunks,
} from './SlackDeliveryPlan'
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

/** Slack's answers to an append on a stream that is no longer open; the plan then needs a new stream. */
const closedStreamErrors: ReadonlySet<string> = new Set(['message_not_in_streaming_state', 'stopped_by_user', 'message_not_found'])

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

		/** The plan's stream, when a saved presentation names one. One that cannot be read is logged and treated as none. */
		const planStream = (presentation: Schema.Json | undefined) =>
			Predicate.isUndefined(presentation)
				? Effect.succeedNone
				: Schema.decodeUnknownEffect(SlackPlanPresentationJson)(presentation).pipe(
						Effect.map(({ message }) => Option.some(message)),
						Effect.catchTag('SchemaError', (error) =>
							Effect.logWarning('Slack plan presentation could not be read; starting a new stream', error).pipe(
								Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
								Effect.as(Option.none()),
							),
						),
					)

		/** The plan's presentation as the operation's receipt. */
		const planReceipt = (message: SlackMessageRef) =>
			Schema.encodeEffect(SlackPlanPresentationJson)(SlackPlanPresentation.make({ message })).pipe(
				Effect.mapError(() => failed('receipt_unencodable', false)),
				Effect.map((receipt) => DeliveryOutputApplied.make({ receipt })),
			)

		/** Stop a plan stream. One Slack already closed, or that is gone, counts as stopped. */
		const stopPlanStream = (message: SlackMessageRef) =>
			slackApi.stopStream({ message }).pipe(
				Effect.catchIf(
					(error) => closedStreamErrors.has(error.message),
					(error) =>
						Effect.logInfo('Slack plan stream was already closed').pipe(
							Effect.annotateLogs({ slack_error: error.message }),
						),
				),
			)

		/** Stop and delete a plan stream a new one replaced. A failure is logged; the new stream stands. */
		const removeReplacedStream = (message: SlackMessageRef) =>
			stopPlanStream(message).pipe(
				Effect.andThen(
					slackApi
						.deleteMessage({ message })
						.pipe(Effect.catchIf((error) => error.message === 'message_not_found', () => Effect.void)),
				),
				Effect.catchTag('SlackApiError', (error) =>
					Effect.logWarning('Slack could not remove a replaced plan stream; it stays in the thread', error).pipe(
						Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
					),
				),
				Effect.withSpan('slack.output.remove_replaced_plan_stream'),
			)

		/** Start a new stream with the whole plan, then remove the stream it replaces, if any. */
		const startPlan = (plan: DeliveryPlan, replaced: Option.Option<SlackMessageRef>) =>
			slackApi.startPlanStream({ thread: destination.thread, chunks: slackPlanStartChunks(plan) }).pipe(
				reportSlackFailure('Slack plan stream start failed', 'slack_plan_failed'),
				Effect.tap(() => Option.match(replaced, { onNone: () => Effect.void, onSome: removeReplacedStream })),
				Effect.flatMap(planReceipt),
			)

		/** Bring the stream from the plan it shows to `plan`: append what changed, or replace the stream. */
		const updatePlan = (plan: DeliveryPlan, shown: RenderedDeliveryPlan, message: SlackMessageRef) =>
			SlackPlanChange.$match(slackPlanChange(shown.plan, plan), {
				Replace: () => startPlan(plan, Option.some(message)),
				Append: ({ chunks }) =>
					Arr.isReadonlyArrayNonEmpty(chunks)
						? slackApi.appendStream({ message, chunks }).pipe(
								Effect.as(true),
								Effect.catchIf(
									(error) => closedStreamErrors.has(error.message),
									(error) =>
										Effect.logInfo('Slack plan stream is closed; starting a new one').pipe(
											Effect.annotateLogs({ slack_error: error.message }),
											Effect.as(false),
										),
								),
								reportSlackFailure('Slack plan stream append failed', 'slack_plan_failed'),
								Effect.flatMap((appended) => (appended ? planReceipt(message) : startPlan(plan, Option.some(message)))),
							)
						: planReceipt(message),
			})

		const renderPlan = ({ plan, rendered }: ProviderRenderPlan) =>
			Effect.gen(function* () {
				const stream = yield* planStream(rendered?.presentation)
				if (Predicate.isUndefined(rendered) || Option.isNone(stream)) return yield* startPlan(plan, Option.none())
				return yield* updatePlan(plan, rendered, stream.value)
			}).pipe(Effect.withSpan('slack.output.render_plan', { attributes: { item_count: plan.items.length } }))

		/**
		 * Present the result: stop the plan stream first, then post the Markdown, or clear the status line.
		 * A stop Slack refuses for good is logged and the result still posts.
		 */
		const presentOutcome = ({ outcome, markdown, clearActivity, planPresentation }: ProviderPresentOutcome) =>
			Effect.gen(function* () {
				const stream = yield* planStream(planPresentation)
				if (Option.isSome(stream)) {
					yield* stopPlanStream(stream.value).pipe(
						Effect.catchIf(
							(error) => !isRetryableSlackApiError(error),
							(error) =>
								Effect.logWarning('Slack plan stream could not be stopped; presenting the result anyway', error).pipe(
									Effect.annotateLogs({ delivery_id: attempt.deliveryId, operation_id: attempt.operationId }),
								),
						),
						reportSlackFailure('Slack plan stream stop failed', 'slack_plan_failed'),
					)
				}
				if (Predicate.isNotUndefined(markdown)) return yield* post(outcomeMarkdown(outcome, markdown))
				return clearActivity ? yield* clearStatus : applied
			})

		return yield* Match.value(attempt.operation).pipe(
			Match.tagsExhaustive({
				AddExternalLink: () => Effect.succeed(applied),
				SetMessageReaction: setReaction,
				RenderPlan: renderPlan,
				PresentOutcome: presentOutcome,
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
