/** Slack post-admission batch normalization and callback routing. */
import {
	deliveryMailboxKey,
	type DeliveryAdmission,
	type DeliveryAdmissionBatch,
	type DeliveryCallbackResult,
	type DeliveryContext,
	MailboxSubscriptions,
	PreparedDeliveryInvocation,
	type ProviderDeliveryExecution,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Data, Effect, Match, Option, Predicate, Schema } from 'effect'

import { SlackApi } from './SlackApi'
import {
	SlackConversationStopped,
	SlackEventId,
	SlackMessageDeleted,
	SlackMessageReceived,
	SlackMessageUpdated,
	SlackNewMention,
	SlackNonEmptyThreadEvents,
	SlackReactionAdded,
	SlackReactionRemoved,
	SlackSubscribedThreadEvents,
	type SlackThreadEvent,
} from './SlackCallbackEvents'
import { type SlackCallbackError, SlackCallbackName, SlackCallbacks } from './SlackCallbacks'
import {
	SlackActivationTarget,
	SlackActivationTargetJson,
	SlackDeliveryDestination,
	SlackDeliveryDestinationJson,
	slackPresentationVersion,
	slackThreadSupportedOperations,
} from './SlackDeliveryDestination'
import { SlackChannelId, SlackMessageTs, SlackTeamId, slackThreadResourceId } from './SlackIdentity'
import {
	slackFileFromMetadata,
	SlackMarkdownContent,
	SlackMessage,
	SlackMessageRef,
	type SlackParticipant,
	SlackReaction,
	SlackThreadRef,
} from './SlackModels'
import { SlackThread } from './SlackThread'
import type { SlackMessageSnapshot } from './SlackWebhookEventSchemas'
import {
	SlackAgentSessionStoppedEnvelope,
	SlackAppMentionEnvelope,
	SlackEventEnvelope,
	SlackMessageDeletedEnvelope,
	SlackMessageEnvelope,
	SlackMessageUpdatedEnvelope,
	SlackReactionAddedEnvelope,
	SlackReactionRemovedEnvelope,
} from './SlackWebhookSchemas'

export type SlackEventProcessorOptions = {
	readonly namespace: string
}

const SlackResourceAddress = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
})
type SlackResourceAddress = typeof SlackResourceAddress.Type

type SlackEnvelope =
	| SlackAppMentionEnvelope
	| SlackMessageEnvelope
	| SlackMessageUpdatedEnvelope
	| SlackMessageDeletedEnvelope
	| SlackReactionAddedEnvelope
	| SlackReactionRemovedEnvelope
	| SlackAgentSessionStoppedEnvelope

type SlackParticipantLookup = {
	readonly teamId: SlackTeamId
	readonly user?: string
	readonly botId?: string
}

type SlackParticipantLookupRequest = {
	teamId: SlackTeamId
	userId?: string
	botId?: string
}

type SlackMessageDeletedFields = {
	eventId: SlackEventId
	messageRef: SlackMessageRef
	previousMessage?: SlackMessage
	actor?: SlackParticipant
}

type NormalizedEvent = {
	readonly event: SlackThreadEvent
	readonly activation: Option.Option<SlackMessage>
}

const invalidPayload = () => ProviderEventInvalid.make({ provider: 'slack', reason: 'invalid_payload' })
const identityMismatch = () => ProviderEventInvalid.make({ provider: 'slack', reason: 'identity_mismatch' })

const providerFailure = (safeCode: string) =>
	ProviderEventExecutionFailed.make({ provider: 'slack', retryable: true, safeCode })

const nonRetryableFailure = (safeCode: string) =>
	ProviderEventExecutionFailed.make({ provider: 'slack', retryable: false, safeCode })

const decodeEnvelope = (admission: DeliveryAdmission) =>
	Schema.decodeUnknownEffect(SlackEventEnvelope)(admission.payload, { onExcessProperty: 'preserve' }).pipe(
		Effect.flatMap((envelope) =>
			Match.value(envelope.event.type).pipe(
				Match.when('app_mention', () =>
					Schema.decodeUnknownEffect(SlackAppMentionEnvelope)(admission.payload, {
						onExcessProperty: 'preserve',
					}),
				),
				Match.when('message', () =>
					Schema.decodeUnknownEffect(SlackMessageEnvelope)(admission.payload, {
						onExcessProperty: 'preserve',
					}).pipe(
						Effect.flatMap((messageEnvelope) =>
							Match.value(messageEnvelope.event.subtype).pipe(
								Match.when('message_changed', () =>
									Schema.decodeUnknownEffect(SlackMessageUpdatedEnvelope)(admission.payload, {
										onExcessProperty: 'preserve',
									}),
								),
								Match.when('message_deleted', () =>
									Schema.decodeUnknownEffect(SlackMessageDeletedEnvelope)(admission.payload, {
										onExcessProperty: 'preserve',
									}),
								),
								Match.whenOr(undefined, 'file_share', () => Effect.succeed(messageEnvelope)),
								Match.orElse(() => Effect.fail(invalidPayload())),
							),
						),
					),
				),
				Match.when('reaction_added', () =>
					Schema.decodeUnknownEffect(SlackReactionAddedEnvelope)(admission.payload, {
						onExcessProperty: 'preserve',
					}),
				),
				Match.when('reaction_removed', () =>
					Schema.decodeUnknownEffect(SlackReactionRemovedEnvelope)(admission.payload, {
						onExcessProperty: 'preserve',
					}),
				),
				Match.when('agent_session_stopped', () =>
					Schema.decodeUnknownEffect(SlackAgentSessionStoppedEnvelope)(admission.payload, {
						onExcessProperty: 'preserve',
					}),
				),
				Match.orElse(() => Effect.fail(invalidPayload())),
			),
		),
		Effect.tapError((error) => Effect.logError('Stored Slack event could not be decoded', error)),
		Effect.mapError(() => invalidPayload()),
	)

const parseResourceAddress = (resourceId: string) =>
	Effect.gen(function* () {
		const parts = resourceId.split(':')
		if (parts.length !== 5 || parts[0] !== 'slack' || parts[1] !== 'v1') return yield* identityMismatch()
		const encodedTeam = parts[2]
		const encodedChannel = parts[3]
		const encodedThread = parts[4]
		if (encodedTeam === undefined || encodedChannel === undefined || encodedThread === undefined) {
			return yield* identityMismatch()
		}
		const decoded = yield* Effect.try({
			try: () => ({
				teamId: decodeURIComponent(encodedTeam),
				channelId: decodeURIComponent(encodedChannel),
				threadTs: decodeURIComponent(encodedThread),
			}),
			catch: identityMismatch,
		})
		return yield* Schema.decodeEffect(SlackResourceAddress)(decoded).pipe(Effect.mapError(identityMismatch))
	})

const envelopeChannelId = (envelope: SlackEnvelope) =>
	Match.value(envelope.event).pipe(
		Match.discriminatorsExhaustive('type')({
			app_mention: (event) => event.channel,
			message: (event) => event.channel,
			reaction_added: (event) => event.item.channel,
			reaction_removed: (event) => event.item.channel,
			agent_session_stopped: (event) => event.channel,
		}),
	)

const envelopeRoot = (envelope: SlackEnvelope, address: SlackResourceAddress) =>
	Match.value(envelope.event).pipe(
		Match.discriminatorsExhaustive('type')({
			app_mention: (event) => event.thread_ts ?? event.ts,
			message: (event) =>
				Match.value(event.subtype).pipe(
					Match.when('message_changed', () => event.message?.thread_ts ?? event.message?.ts ?? event.ts),
					Match.when(
						'message_deleted',
						() =>
							event.previous_message?.thread_ts ??
							event.deleted_ts ??
							event.previous_message?.ts ??
							event.ts,
					),
					Match.orElse(() => event.thread_ts ?? event.ts),
				),
			reaction_added: () => address.threadTs,
			reaction_removed: () => address.threadTs,
			agent_session_stopped: (event) => event.thread_ts,
		}),
	)

const isDirectMessageEnvelope = (envelope: SlackEnvelope) =>
	Match.value(envelope.event).pipe(
		Match.discriminatorsExhaustive('type')({
			app_mention: () => false,
			message: (event) => event.channel_type === 'im' || event.channel_type === 'mpim',
			reaction_added: () => false,
			reaction_removed: () => false,
			agent_session_stopped: () => false,
		}),
	)

const resolveParticipant = (input: SlackParticipantLookup) =>
	Effect.flatMap(SlackApi, (api) => {
		const request: SlackParticipantLookupRequest = { teamId: input.teamId }
		if (input.user !== undefined) request.userId = input.user
		if (input.botId !== undefined) request.botId = input.botId
		return api.resolveParticipant(request)
	})

const makeMessage = (input: {
	readonly teamId: SlackTeamId
	readonly thread: SlackThreadRef
	readonly eventId: string
	readonly eventTime: number
	readonly snapshot: SlackMessageSnapshot
}) =>
	Effect.gen(function* () {
		const author = yield* resolveParticipant({
			teamId: input.teamId,
			user: input.snapshot.user,
			botId: input.snapshot.bot_id,
		})
		const ref = SlackMessageRef.make({
			teamId: input.teamId,
			channelId: input.thread.channelId,
			messageTs: input.snapshot.ts,
		})
		return SlackMessage.make({
			ref,
			thread: input.thread,
			author,
			content: SlackMarkdownContent.make({ markdown: input.snapshot.text ?? '' }),
			files: (input.snapshot.files ?? []).map((file) => slackFileFromMetadata(input.teamId, file)),
			metadata: { eventId: input.eventId, eventTime: input.eventTime },
		})
	})

const normalizeEnvelope = (envelope: SlackEnvelope, thread: SlackThreadRef) =>
	Effect.gen(function* () {
		const eventId = SlackEventId.make(envelope.event_id)
		return yield* Match.value(envelope.event).pipe(
			Match.discriminatorsExhaustive('type')({
				app_mention: (event) =>
					Effect.gen(function* () {
						const message = yield* makeMessage({
							teamId: envelope.team_id,
							thread,
							eventId,
							eventTime: envelope.event_time,
							snapshot: event,
						})
						if (message.author.isMe) return Option.none<NormalizedEvent>()
						return Option.some<NormalizedEvent>({
							event: SlackMessageReceived.make({ eventId, message }),
							activation: Option.some(message),
						})
					}),
				message: (event) =>
					Match.value(event.subtype).pipe(
						Match.when('message_changed', () =>
							Effect.gen(function* () {
								const snapshot = event.message
								if (snapshot === undefined) return yield* invalidPayload()
								const message = yield* makeMessage({
									teamId: envelope.team_id,
									thread,
									eventId,
									eventTime: envelope.event_time,
									snapshot,
								})
								const previousSnapshot = event.previous_message
								const previousMessage =
									previousSnapshot === undefined
										? undefined
										: yield* makeMessage({
												teamId: envelope.team_id,
												thread,
												eventId,
												eventTime: envelope.event_time,
												snapshot: previousSnapshot,
											})
								if (message.author.isMe || previousMessage?.author.isMe === true) {
									return Option.none<NormalizedEvent>()
								}
								return Option.some<NormalizedEvent>({
									event:
										previousMessage === undefined
											? SlackMessageUpdated.make({ eventId, message })
											: SlackMessageUpdated.make({ eventId, message, previousMessage }),
									activation: Option.none(),
								})
							}),
						),
						Match.when('message_deleted', () =>
							Effect.gen(function* () {
								const messageTs = event.deleted_ts ?? event.previous_message?.ts ?? event.ts
								const messageRef = SlackMessageRef.make({
									teamId: envelope.team_id,
									channelId: event.channel,
									messageTs,
								})
								const fields: SlackMessageDeletedFields = { eventId, messageRef }
								const previousSnapshot = event.previous_message
								if (previousSnapshot !== undefined) {
									const previousMessage = yield* makeMessage({
										teamId: envelope.team_id,
										thread,
										eventId,
										eventTime: envelope.event_time,
										snapshot: previousSnapshot,
									})
									if (previousMessage.author.isMe) return Option.none<NormalizedEvent>()
									fields.previousMessage = previousMessage
								}
								if (event.user !== undefined || event.bot_id !== undefined) {
									fields.actor = yield* resolveParticipant({
										teamId: envelope.team_id,
										user: event.user,
										botId: event.bot_id,
									})
								}
								return Option.some<NormalizedEvent>({
									event: SlackMessageDeleted.make(fields),
									activation: Option.none(),
								})
							}),
						),
						Match.whenOr(undefined, 'file_share', () =>
							Effect.gen(function* () {
								const message = yield* makeMessage({
									teamId: envelope.team_id,
									thread,
									eventId,
									eventTime: envelope.event_time,
									snapshot: event,
								})
								if (message.author.isMe) return Option.none<NormalizedEvent>()
								const topLevelDm =
									event.thread_ts === undefined &&
									(event.channel_type === 'im' || event.channel_type === 'mpim')
								return Option.some<NormalizedEvent>({
									event: SlackMessageReceived.make({ eventId, message }),
									activation: topLevelDm ? Option.some(message) : Option.none(),
								})
							}),
						),
						Match.orElse(() => Effect.fail(invalidPayload())),
					),
				reaction_added: (event) =>
					Effect.gen(function* () {
						const actor = yield* resolveParticipant({ teamId: envelope.team_id, user: event.user })
						if (actor.isMe) return Option.none<NormalizedEvent>()
						const messageRef = SlackMessageRef.make({
							teamId: envelope.team_id,
							channelId: event.item.channel,
							messageTs: event.item.ts,
						})
						const message = yield* Effect.flatMap(SlackApi, (api) =>
							api.getMessage({ thread, message: messageRef }),
						)
						return Option.some<NormalizedEvent>({
							event: SlackReactionAdded.make({
								eventId,
								message,
								actor,
								reaction: SlackReaction.make(event.reaction),
							}),
							activation: Option.none(),
						})
					}),
				reaction_removed: (event) =>
					Effect.gen(function* () {
						const actor = yield* resolveParticipant({ teamId: envelope.team_id, user: event.user })
						if (actor.isMe) return Option.none<NormalizedEvent>()
						const messageRef = SlackMessageRef.make({
							teamId: envelope.team_id,
							channelId: event.item.channel,
							messageTs: event.item.ts,
						})
						const message = yield* Effect.flatMap(SlackApi, (api) =>
							api.getMessage({ thread, message: messageRef }),
						)
						return Option.some<NormalizedEvent>({
							event: SlackReactionRemoved.make({
								eventId,
								message,
								actor,
								reaction: SlackReaction.make(event.reaction),
							}),
							activation: Option.none(),
						})
					}),
				agent_session_stopped: (event) =>
					Effect.gen(function* () {
						const actor = yield* resolveParticipant({ teamId: envelope.team_id, user: event.user })
						return Option.some<NormalizedEvent>({
							event: SlackConversationStopped.make({
								eventId,
								actor,
								streamingMessages: event.streaming_message_ts.map((messageTs) =>
									SlackMessageRef.make({
										teamId: envelope.team_id,
										channelId: event.channel,
										messageTs,
									}),
								),
							}),
							activation: Option.none(),
						})
					}),
			}),
		)
	})

const runCallback = (effect: Effect.Effect<DeliveryCallbackResult, { readonly retryable: boolean }>) =>
	effect.pipe(
		Effect.mapError((error) =>
			ProviderEventExecutionFailed.make({
				provider: 'slack',
				retryable: error.retryable,
				safeCode: 'callback_failed',
			}),
		),
		Effect.as(ProviderEventHandled.make({})),
	)

const belongsToBatch = (
	options: SlackEventProcessorOptions,
	first: DeliveryAdmission,
	address: SlackResourceAddress,
	admission: DeliveryAdmission,
	envelope: SlackEnvelope,
) =>
	admission.namespace === options.namespace &&
	admission.provider === 'slack' &&
	admission.installationId === first.installationId &&
	admission.resourceId === first.resourceId &&
	envelope.team_id === address.teamId &&
	envelope.team_id === admission.installationId &&
	envelopeChannelId(envelope) === address.channelId &&
	envelopeRoot(envelope, address) === address.threadTs &&
	slackThreadResourceId(address) === admission.resourceId

/** The callback a batch runs, with the event value it receives. */
type SlackInvocation = Data.TaggedEnum<{
	NewMention: { readonly event: SlackNewMention }
	SubscribedThreadEvents: { readonly event: SlackSubscribedThreadEvents }
}>
const SlackInvocation = Data.taggedEnum<SlackInvocation>()

const invocationCallbackName = SlackInvocation.$match({
	NewMention: (): SlackCallbackName => 'onNewMention',
	SubscribedThreadEvents: (): SlackCallbackName => 'onSubscribedThreadEvents',
})

/** A new mention starts at the first activating message; relevant events before it are dropped. */
const buildNewMention = (thread: SlackThread, normalized: ReadonlyArray<NormalizedEvent>) => {
	const activationIndex = normalized.findIndex(({ activation }) => Option.isSome(activation))
	const activating = normalized[activationIndex]
	if (activating === undefined || Option.isNone(activating.activation)) return Option.none<SlackInvocation>()
	return Option.some(
		SlackInvocation.NewMention({
			event: SlackNewMention.make({
				thread,
				trigger: activating.activation.value,
				events: normalized.slice(activationIndex + 1).map(({ event }) => event),
			}),
		}),
	)
}

const buildSubscribedThreadEvents = (thread: SlackThread, normalized: ReadonlyArray<NormalizedEvent>) => {
	const first = normalized[0]
	if (first === undefined) return Option.none<SlackInvocation>()
	const events = SlackNonEmptyThreadEvents.make([first.event, ...normalized.slice(1).map(({ event }) => event)])
	return Option.some(
		SlackInvocation.SubscribedThreadEvents({ event: SlackSubscribedThreadEvents.make({ thread, events }) }),
	)
}

/** Rebuild the event value for a callback an earlier attempt chose. */
const buildInvocation = (
	callback: SlackCallbackName,
	thread: SlackThread,
	normalized: ReadonlyArray<NormalizedEvent>,
): Option.Option<SlackInvocation> =>
	Match.value(callback).pipe(
		Match.when('onNewMention', () => buildNewMention(thread, normalized)),
		Match.when('onSubscribedThreadEvents', () => buildSubscribedThreadEvents(thread, normalized)),
		Match.exhaustive,
	)

/** Which callback a new batch runs, or why it runs none. Ignoring a batch is a normal outcome, not an error. */
type CallbackSelection = Data.TaggedEnum<{
	Selected: { readonly invocation: SlackInvocation }
	Ignored: { readonly reason: string }
}>
const CallbackSelection = Data.taggedEnum<CallbackSelection>()

/** Choose the callback for a batch no attempt has prepared yet. */
const selectInvocation = (input: {
	readonly callbacks: typeof SlackCallbacks.Service
	readonly thread: SlackThread
	readonly normalized: ReadonlyArray<NormalizedEvent>
	readonly subscribed: boolean
}): CallbackSelection => {
	if (input.subscribed) {
		if (Predicate.isUndefined(input.callbacks.onSubscribedThreadEvents)) {
			return CallbackSelection.Ignored({ reason: 'callback_not_configured' })
		}
		return Option.match(buildSubscribedThreadEvents(input.thread, input.normalized), {
			onNone: () => CallbackSelection.Ignored({ reason: 'no_relevant_event' }),
			onSome: (invocation) => CallbackSelection.Selected({ invocation }),
		})
	}
	const mention = buildNewMention(input.thread, input.normalized)
	if (Option.isNone(mention)) return CallbackSelection.Ignored({ reason: 'no_activation_event' })
	if (Predicate.isUndefined(input.callbacks.onNewMention)) {
		return CallbackSelection.Ignored({ reason: 'callback_not_configured' })
	}
	return CallbackSelection.Selected({ invocation: mention.value })
}

const isCallbackConfigured = (callbacks: typeof SlackCallbacks.Service, callback: SlackCallbackName) =>
	Predicate.isNotUndefined(callbacks[callback])

/** A new mention's trigger message; a subscribed batch has no single message that started it. */
const invocationActivationTarget = SlackInvocation.$match({
	NewMention: ({ event }) => Option.some(SlackActivationTarget.make({ message: event.trigger.ref })),
	SubscribedThreadEvents: () => Option.none<SlackActivationTarget>(),
})

/** The saved form of an invocation: callback name, thread, and the activating message when there is one. */
const preparedInvocation = Effect.fn('slack.prepared_invocation')(function* (invocation: SlackInvocation) {
	const destination = yield* Schema.encodeEffect(SlackDeliveryDestinationJson)(
		SlackDeliveryDestination.make({ thread: invocation.event.thread.ref }),
	)
	const activationTarget = yield* Effect.transposeOption(
		Option.map(invocationActivationTarget(invocation), Schema.encodeEffect(SlackActivationTargetJson)),
	)
	return PreparedDeliveryInvocation.make({
		callback: invocationCallbackName(invocation),
		presentationVersion: slackPresentationVersion,
		destination,
		...Option.match(activationTarget, { onNone: () => ({}), onSome: (target) => ({ activationTarget: target }) }),
		supportedOperations: slackThreadSupportedOperations,
	})
})

/** Save the callback choice before any application code runs, so every retry runs the same callback. */
const prepareInvocation = Effect.fn('slack.prepare_delivery')(function* (
	execution: ProviderDeliveryExecution,
	invocation: SlackInvocation,
) {
	const prepared = yield* preparedInvocation(invocation).pipe(
		Effect.tapError((error) => Effect.logError('Slack delivery destination could not be encoded', error)),
		Effect.mapError(() => nonRetryableFailure('delivery_destination_unencodable')),
	)
	yield* execution.prepare(prepared).pipe(
		Effect.tapError((error) =>
			Effect.logError('Slack delivery preparation failed', error).pipe(
				Effect.annotateLogs({ deliveryId: execution.deliveryId, callback: prepared.callback }),
			),
		),
		Effect.catchTags({
			DeliveryPreparationUnavailable: () => Effect.fail(providerFailure('delivery_prepare_unavailable')),
			DeliveryPreparationConflict: () => Effect.fail(nonRetryableFailure('delivery_prepare_conflict')),
		}),
	)
})

const invokeCallback = (
	callbacks: typeof SlackCallbacks.Service,
	invocation: SlackInvocation,
	delivery: DeliveryContext,
): Option.Option<Effect.Effect<DeliveryCallbackResult, SlackCallbackError>> =>
	SlackInvocation.$match(invocation, {
		NewMention: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onNewMention), (callback) => callback(event, delivery)),
		SubscribedThreadEvents: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onSubscribedThreadEvents), (callback) =>
				callback(event, delivery),
			),
	})

const runInvocation = (
	callbacks: typeof SlackCallbacks.Service,
	invocation: SlackInvocation,
	delivery: DeliveryContext,
) =>
	Option.match(invokeCallback(callbacks, invocation, delivery), {
		onNone: () => Effect.fail(nonRetryableFailure('prepared_callback_missing')),
		onSome: runCallback,
	})

/** Run the callback an earlier attempt saved, without choosing again. */
const runPreparedInvocation = Effect.fn('slack.run_prepared_invocation')(function* (input: {
	readonly callbacks: typeof SlackCallbacks.Service
	readonly prepared: PreparedDeliveryInvocation
	readonly thread: SlackThread
	readonly normalized: ReadonlyArray<NormalizedEvent>
	readonly delivery: DeliveryContext
}) {
	const annotations = { deliveryId: input.delivery.deliveryId, callback: input.prepared.callback }
	const callback = yield* Schema.decodeUnknownEffect(SlackCallbackName)(input.prepared.callback).pipe(
		Effect.tapError((error) =>
			Effect.logError('Prepared Slack callback is unknown', error).pipe(Effect.annotateLogs(annotations)),
		),
		Effect.mapError(() => nonRetryableFailure('prepared_callback_missing')),
	)
	if (!isCallbackConfigured(input.callbacks, callback)) {
		yield* Effect.logError('Prepared Slack callback is no longer configured').pipe(Effect.annotateLogs(annotations))
		return yield* nonRetryableFailure('prepared_callback_missing')
	}
	const invocation = buildInvocation(callback, input.thread, input.normalized)
	if (Option.isNone(invocation)) {
		yield* Effect.logError('Prepared Slack callback cannot be rebuilt from its batch').pipe(
			Effect.annotateLogs(annotations),
		)
		return yield* nonRetryableFailure('prepared_callback_unbuildable')
	}
	return yield* runInvocation(input.callbacks, invocation.value, input.delivery)
})

const processSlackBatch = (options: SlackEventProcessorOptions) =>
	Effect.fn('slack.process_event_batch')(function* (
		admissions: DeliveryAdmissionBatch,
		execution: ProviderDeliveryExecution,
	) {
		const callbacks = yield* SlackCallbacks
		const first = admissions[0]
		const envelopes = yield* Effect.forEach(admissions, decodeEnvelope)
		const address = yield* parseResourceAddress(first.resourceId)

		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const envelope = envelopes[index]
			if (admission === undefined || envelope === undefined) return yield* identityMismatch()
			if (!belongsToBatch(options, first, address, admission, envelope)) return yield* identityMismatch()
		}

		const isDm = address.channelId.startsWith('D') || envelopes.some(isDirectMessageEnvelope)
		const threadRef = SlackThreadRef.make({ ...address, isDm })
		const mailboxKey = deliveryMailboxKey(first)
		const thread = SlackThread.make({ ref: threadRef, mailboxKey })
		const normalizedOptions = yield* Effect.forEach(envelopes, (envelope) =>
			normalizeEnvelope(envelope, threadRef),
		).pipe(
			Effect.tapError((error) => Effect.logError('Slack event normalization failed', error)),
			Effect.mapError((error) =>
				Schema.is(ProviderEventInvalid)(error) ? error : providerFailure('slack_api_failed'),
			),
		)
		const normalized = normalizedOptions.flatMap(Option.toArray)

		if (Option.isSome(execution.prepared)) {
			return yield* runPreparedInvocation({
				callbacks,
				prepared: execution.prepared.value,
				thread,
				normalized,
				delivery: execution.context,
			})
		}
		if (Arr.isReadonlyArrayEmpty(normalized)) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })

		const subscribed = yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey }),
		).pipe(
			Effect.tapError((error) => Effect.logError('Slack subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)

		return yield* CallbackSelection.$match(selectInvocation({ callbacks, thread, normalized, subscribed }), {
			Ignored: ({ reason }) => Effect.succeed(ProviderEventIgnored.make({ reason })),
			Selected: ({ invocation }) =>
				prepareInvocation(execution, invocation).pipe(
					Effect.andThen(runInvocation(callbacks, invocation, execution.context)),
				),
		})
	})

export const makeSlackEventProcessor = (
	options: SlackEventProcessorOptions,
): ProviderEventProcessor<SlackCallbacks | SlackApi | MailboxSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'slack',
	process: processSlackBatch(options),
})
