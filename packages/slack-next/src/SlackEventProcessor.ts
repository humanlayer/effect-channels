/**
 * Slack's post-admission event processor. It re-parses stored payloads, selects
 * the configured Slack callback, and narrows callback failures for delivery.
 */
import {
	type DeliveryAdmission,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
	type ProviderEventResult,
} from '@humanlayer/channels-delivery-next'
import { Effect, Match, Predicate, Schema } from 'effect'

import type {
	SlackAgentSessionStoppedEvent,
	SlackAppMentionEvent,
	SlackMessageDeletedEvent,
	SlackMessageEvent,
	SlackMessageUpdatedEvent,
	SlackReactionAddedEvent,
	SlackReactionRemovedEvent,
} from './SlackWebhookEventSchemas'
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

export type SlackReactionEvent = SlackReactionAddedEvent | SlackReactionRemovedEvent

type SlackEventHandler<A, E, R> = (event: A) => Effect.Effect<void, E, R>

export const RetryabilityMetadata = Schema.Struct({
	retryability: Schema.Literals(['retryable', 'non_retryable']),
})
export type RetryabilityMetadata = typeof RetryabilityMetadata.Type

export type SlackEventProcessorOptions<E, R> = {
	readonly namespace: string
	readonly handlers: {
		readonly onNewMention?: SlackEventHandler<SlackAppMentionEvent, E, R>
		readonly onSubscribedMessage?: SlackEventHandler<SlackMessageEvent, E, R>
		readonly onDirectMessage?: SlackEventHandler<SlackMessageEvent, E, R>
		readonly onMessageUpdated?: SlackEventHandler<SlackMessageUpdatedEvent, E, R>
		readonly onMessageDeleted?: SlackEventHandler<SlackMessageDeletedEvent, E, R>
		readonly onReaction?: SlackEventHandler<SlackReactionEvent, E, R>
		readonly onConversationStopped?: SlackEventHandler<SlackAgentSessionStoppedEvent, E, R>
	}
}

const invalidPayload = () => ProviderEventInvalid.make({ provider: 'slack', reason: 'invalid_payload' })

const makeEnvelopeDecoder =
	<S extends Schema.Top>(schema: S, eventName: string) =>
	(admission: DeliveryAdmission) =>
		Schema.decodeUnknownEffect(schema)(admission.payload, { onExcessProperty: 'preserve' }).pipe(
			Effect.tapError((error) => Effect.logError(`Stored Slack ${eventName} could not be decoded`, error)),
			Effect.mapError(invalidPayload),
		)

const decodeSlackEnvelope = makeEnvelopeDecoder(SlackEventEnvelope, 'event envelope')
const decodeAppMention = makeEnvelopeDecoder(SlackAppMentionEnvelope, 'app mention')
const decodeMessage = makeEnvelopeDecoder(SlackMessageEnvelope, 'message')
const decodeMessageUpdated = makeEnvelopeDecoder(SlackMessageUpdatedEnvelope, 'message update')
const decodeMessageDeleted = makeEnvelopeDecoder(SlackMessageDeletedEnvelope, 'message deletion')
const decodeReactionAdded = makeEnvelopeDecoder(SlackReactionAddedEnvelope, 'reaction addition')
const decodeReactionRemoved = makeEnvelopeDecoder(SlackReactionRemovedEnvelope, 'reaction removal')
const decodeConversationStopped = makeEnvelopeDecoder(SlackAgentSessionStoppedEnvelope, 'conversation stop')

const runHandler = <A, E, R>(
	name: string,
	handler: SlackEventHandler<A, E, R> | undefined,
	event: A,
): Effect.Effect<ProviderEventResult, ProviderEventExecutionFailed, R> => {
	if (Predicate.isUndefined(handler)) {
		return Effect.succeed(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
	}

	return handler(event).pipe(
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
}

const processDecodedEvent = <A, E, R>(input: {
	readonly admission: DeliveryAdmission
	readonly decode: (admission: DeliveryAdmission) => Effect.Effect<{ readonly event: A }, ProviderEventInvalid>
	readonly handlerName: string
	readonly handler: SlackEventHandler<A, E, R> | undefined
	readonly span: string
}) =>
	input.decode(input.admission).pipe(
		Effect.flatMap((envelope) => runHandler(input.handlerName, input.handler, envelope.event)),
		Effect.withSpan(input.span),
	)

const processAppMention = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	processDecodedEvent({
		admission,
		decode: decodeAppMention,
		handlerName: 'onNewMention',
		handler: options.handlers.onNewMention,
		span: 'slack.process_app_mention',
	})

const processMessageUpdated = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	processDecodedEvent({
		admission,
		decode: decodeMessageUpdated,
		handlerName: 'onMessageUpdated',
		handler: options.handlers.onMessageUpdated,
		span: 'slack.process_message_updated',
	})

const processMessageDeleted = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	processDecodedEvent({
		admission,
		decode: decodeMessageDeleted,
		handlerName: 'onMessageDeleted',
		handler: options.handlers.onMessageDeleted,
		span: 'slack.process_message_deleted',
	})

const processMessage = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	decodeMessage(admission).pipe(
		Effect.flatMap((envelope) =>
			Match.value(envelope.event.subtype).pipe(
				Match.when('message_changed', () => processMessageUpdated(options, admission)),
				Match.when('message_deleted', () => processMessageDeleted(options, admission)),
				Match.when(undefined, () =>
					envelope.event.channel_type === 'im' || envelope.event.channel_type === 'mpim'
						? runHandler('onDirectMessage', options.handlers.onDirectMessage, envelope.event)
						: runHandler('onSubscribedMessage', options.handlers.onSubscribedMessage, envelope.event),
				),
				Match.orElse(() =>
					Effect.succeed(ProviderEventIgnored.make({ reason: 'unsupported_message_subtype' })),
				),
			),
		),
		Effect.withSpan('slack.process_message'),
	)

const processReactionAdded = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	processDecodedEvent({
		admission,
		decode: decodeReactionAdded,
		handlerName: 'onReaction',
		handler: options.handlers.onReaction,
		span: 'slack.process_reaction_added',
	})

const processReactionRemoved = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	processDecodedEvent({
		admission,
		decode: decodeReactionRemoved,
		handlerName: 'onReaction',
		handler: options.handlers.onReaction,
		span: 'slack.process_reaction_removed',
	})

const processConversationStopped = <E, R>(options: SlackEventProcessorOptions<E, R>, admission: DeliveryAdmission) =>
	processDecodedEvent({
		admission,
		decode: decodeConversationStopped,
		handlerName: 'onConversationStopped',
		handler: options.handlers.onConversationStopped,
		span: 'slack.process_conversation_stopped',
	})

const processSlackEvent = <E, R>(options: SlackEventProcessorOptions<E, R>) =>
	Effect.fn('slack.process_event')(function* (admission: DeliveryAdmission) {
		const envelope = yield* decodeSlackEnvelope(admission)

		return yield* Match.value(envelope.event.type).pipe(
			Match.when('app_mention', () => processAppMention(options, admission)),
			Match.when('message', () => processMessage(options, admission)),
			Match.when('reaction_added', () => processReactionAdded(options, admission)),
			Match.when('reaction_removed', () => processReactionRemoved(options, admission)),
			Match.when('agent_session_stopped', () => processConversationStopped(options, admission)),
			Match.orElse(() => Effect.succeed(ProviderEventIgnored.make({ reason: 'unsupported_slack_event' }))),
		)
	})

export const makeSlackEventProcessor = <E, R>(
	options: SlackEventProcessorOptions<E, R>,
): ProviderEventProcessor<R> => ({
	namespace: options.namespace,
	providerName: 'slack',
	process: processSlackEvent(options),
})
