/** Slack post-admission batch normalization and callback routing. */
import {
	type DeliveryAdmission,
	type DeliveryAdmissionBatch,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery-next'
import { Effect, Match, Option, Predicate, Schema } from 'effect'

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
import { SlackChannelId, SlackMessageTs, SlackTeamId, slackThreadResourceId } from './SlackIdentity'
import { SlackMarkdownContent, SlackMessage, SlackMessageRef, SlackReaction, SlackThreadRef } from './SlackModels'
import { SlackSubscriptions } from './SlackSubscriptions'
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

type SlackEventHandler<A, E, R> = (event: A) => Effect.Effect<void, E, R>

export const RetryabilityMetadata = Schema.Struct({
	retryability: Schema.Literals(['retryable', 'non_retryable']),
})
export type RetryabilityMetadata = typeof RetryabilityMetadata.Type

export type SlackEventProcessorOptions<E, R> = {
	readonly namespace: string
	readonly handlers: {
		readonly onNewMention?: SlackEventHandler<SlackNewMention, E, R>
		readonly onSubscribedThreadEvents?: SlackEventHandler<SlackSubscribedThreadEvents, E, R>
	}
}

const SlackResourceAddress = Schema.Struct({
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	threadTs: SlackMessageTs,
})
type SlackResourceAddress = typeof SlackResourceAddress.Type

type SlackEnvelope =
	| typeof SlackAppMentionEnvelope.Type
	| typeof SlackMessageEnvelope.Type
	| typeof SlackMessageUpdatedEnvelope.Type
	| typeof SlackMessageDeletedEnvelope.Type
	| typeof SlackReactionAddedEnvelope.Type
	| typeof SlackReactionRemovedEnvelope.Type
	| typeof SlackAgentSessionStoppedEnvelope.Type

type NormalizedEvent = {
	readonly event: SlackThreadEvent
	readonly activation: Option.Option<SlackMessage>
}

const invalidPayload = () => ProviderEventInvalid.make({ provider: 'slack', reason: 'invalid_payload' })
const identityMismatch = () => ProviderEventInvalid.make({ provider: 'slack', reason: 'identity_mismatch' })

const providerFailure = (safeCode: string) =>
	ProviderEventExecutionFailed.make({ provider: 'slack', retryable: true, safeCode })

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
								Match.when(undefined, () => Effect.succeed(messageEnvelope)),
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
		return yield* Schema.decodeUnknownEffect(SlackResourceAddress)(decoded).pipe(Effect.mapError(identityMismatch))
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

const resolveParticipant = (input: { readonly teamId: SlackTeamId; readonly user?: string; readonly botId?: string }) =>
	Effect.flatMap(SlackApi, (api) =>
		api.resolveParticipant({
			teamId: input.teamId,
			...(input.user === undefined ? {} : { userId: input.user }),
			...(input.botId === undefined ? {} : { botId: input.botId }),
		}),
	)

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
			metadata: {
				eventId: input.eventId,
				eventTime: input.eventTime,
				...(input.snapshot.files === undefined ? {} : { files: input.snapshot.files }),
			},
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
								const previousMessage = yield* event.previous_message === undefined
									? Effect.succeed(undefined)
									: makeMessage({
											teamId: envelope.team_id,
											thread,
											eventId,
											eventTime: envelope.event_time,
											snapshot: event.previous_message,
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
								const previousMessage = yield* event.previous_message === undefined
									? Effect.succeed(undefined)
									: makeMessage({
											teamId: envelope.team_id,
											thread,
											eventId,
											eventTime: envelope.event_time,
											snapshot: event.previous_message,
										})
								if (previousMessage?.author.isMe === true) return Option.none<NormalizedEvent>()
								const actor = yield* event.user === undefined && event.bot_id === undefined
									? Effect.succeed(undefined)
									: resolveParticipant({
											teamId: envelope.team_id,
											user: event.user,
											botId: event.bot_id,
										})
								return Option.some<NormalizedEvent>({
									event: SlackMessageDeleted.make({
										eventId,
										messageRef,
										...(previousMessage === undefined ? {} : { previousMessage }),
										...(actor === undefined ? {} : { actor }),
									}),
									activation: Option.none(),
								})
							}),
						),
						Match.when(undefined, () =>
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

const runHandler = <A, E, R>(name: string, handler: SlackEventHandler<A, E, R>, event: A) =>
	handler(event).pipe(
		Effect.tapError((error) => Effect.logError(`Slack ${name} callback failed`, error)),
		Effect.mapError((error) =>
			ProviderEventExecutionFailed.make({
				provider: 'slack',
				retryable: !Schema.is(RetryabilityMetadata)(error) || error.retryability === 'retryable',
				safeCode: 'callback_failed',
			}),
		),
		Effect.as(ProviderEventHandled.make({})),
	)

const processSlackBatch = <E, R>(options: SlackEventProcessorOptions<E, R>) =>
	Effect.fn('slack.process_event_batch')(function* (admissions: DeliveryAdmissionBatch) {
		const first = admissions[0]
		const envelopes = yield* Effect.forEach(admissions, decodeEnvelope)
		const address = yield* parseResourceAddress(first.resourceId)

		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const envelope = envelopes[index]
			if (admission === undefined || envelope === undefined) return yield* identityMismatch()
			if (
				admission.namespace !== options.namespace ||
				admission.provider !== 'slack' ||
				admission.installationId !== first.installationId ||
				admission.resourceId !== first.resourceId ||
				envelope.team_id !== address.teamId ||
				envelope.team_id !== admission.installationId ||
				envelopeChannelId(envelope) !== address.channelId ||
				envelopeRoot(envelope, address) !== address.threadTs ||
				slackThreadResourceId(address) !== admission.resourceId
			) {
				return yield* identityMismatch()
			}
		}

		const isDm = address.channelId.startsWith('D') || envelopes.some(isDirectMessageEnvelope)
		const threadRef = SlackThreadRef.make({ ...address, isDm })
		const thread = SlackThread.make({ ref: threadRef })
		const normalizedOptions = yield* Effect.forEach(envelopes, (envelope) =>
			normalizeEnvelope(envelope, threadRef),
		).pipe(
			Effect.tapError((error) => Effect.logError('Slack event normalization failed', error)),
			Effect.mapError((error) =>
				Schema.is(ProviderEventInvalid)(error) ? error : providerFailure('slack_api_failed'),
			),
		)
		const normalized = normalizedOptions.flatMap(Option.toArray)
		if (normalized.length === 0) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })

		const subscribed = yield* Effect.flatMap(SlackSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ thread: threadRef }),
		).pipe(
			Effect.tapError((error) => Effect.logError('Slack subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)

		if (subscribed) {
			if (Predicate.isUndefined(options.handlers.onSubscribedThreadEvents)) {
				return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
			}
			const firstNormalized = normalized[0]
			if (firstNormalized === undefined) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })
			const events = SlackNonEmptyThreadEvents.make([
				firstNormalized.event,
				...normalized.slice(1).map(({ event }) => event),
			])
			return yield* runHandler(
				'onSubscribedThreadEvents',
				options.handlers.onSubscribedThreadEvents,
				SlackSubscribedThreadEvents.make({ thread, events }),
			)
		}

		const activationIndex = normalized.findIndex(({ activation }) => Option.isSome(activation))
		if (activationIndex < 0) return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		const activating = normalized[activationIndex]
		if (activating === undefined || Option.isNone(activating.activation)) {
			return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		}
		if (Predicate.isUndefined(options.handlers.onNewMention)) {
			return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
		}
		return yield* runHandler(
			'onNewMention',
			options.handlers.onNewMention,
			SlackNewMention.make({
				thread,
				trigger: activating.activation.value,
				events: normalized.slice(activationIndex + 1).map(({ event }) => event),
			}),
		)
	})

export const makeSlackEventProcessor = <E, R>(
	options: SlackEventProcessorOptions<E, R>,
): ProviderEventProcessor<R | SlackApi | SlackSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'slack',
	process: processSlackBatch(options),
})
