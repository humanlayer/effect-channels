import { Context, Effect, Layer, Option } from 'effect'

import { ChannelsGate } from './ChannelsGate.ts'
import { ChannelsObserver } from './ChannelsObserver.ts'
import { ConversationCoordinator } from './ConversationCoordinator.ts'
import { IngressError, type ObserverError } from './Errors.ts'
import type {
	InboundEvent,
	NormalizedConversationStopped,
	NormalizedMessage,
	NormalizedMessageDeleted,
	NormalizedMessageUpdated,
	NormalizedReaction,
} from './Events.ts'
import {
	ConversationStoppedEvent,
	MessageDeletedEvent,
	MessageEvent,
	MessageUpdatedEvent,
	NewMentionDelivery,
	ReactionEvent,
	SubscribedMessageDelivery,
} from './Events.ts'
import type { IngressResult } from './Operations.ts'
import { IngressAccepted, IngressDropped } from './Operations.ts'
import { Organizations } from './Organizations.ts'
import type { OrgId, ProviderName, TenantId } from './Schema.ts'
import { Subscriptions } from './Subscriptions.ts'

const observerBestEffort = (effect: Effect.Effect<void, ObserverError>) =>
	effect.pipe(
		Effect.catchCause((cause) => Effect.logError('channels observer failed', cause)),
		Effect.asVoid,
	)

export class Ingress extends Context.Service<
	Ingress,
	{
		readonly acceptMessage: (message: NormalizedMessage) => Effect.Effect<IngressResult, IngressError>
		readonly acceptMessageUpdated: (event: NormalizedMessageUpdated) => Effect.Effect<IngressResult, IngressError>
		readonly acceptMessageDeleted: (event: NormalizedMessageDeleted) => Effect.Effect<IngressResult, IngressError>
		readonly acceptReaction: (event: NormalizedReaction) => Effect.Effect<IngressResult, IngressError>
		readonly acceptConversationStopped: (
			event: NormalizedConversationStopped,
		) => Effect.Effect<IngressResult, IngressError>
	}
>()('channels/Ingress') {
	static readonly layer = Layer.effect(
		Ingress,
		Effect.gen(function* () {
			const coordinator = yield* ConversationCoordinator
			const organizations = yield* Organizations
			const gate = yield* ChannelsGate
			const observer = yield* ChannelsObserver
			const subscriptions = yield* Subscriptions

			const acceptMessage = Effect.fn('channels.ingress')(function* (message: NormalizedMessage) {
				const attributes = {
					provider: message.provider,
					tenant: message.tenant,
					thread_id: message.thread.ref.id,
					idempotency_key: message.idempotencyKey,
				}
				yield* Effect.annotateCurrentSpan(attributes)
				const organization = yield* organizations
					.resolve({ source: message.provider, tenant: message.tenant })
					.pipe(
						Effect.tapError((error) => Effect.logError('organization lookup failed', error)),
						Effect.mapError(() =>
							IngressError.make({
								operation: 'Organizations.resolve',
								provider: message.provider,
								message: 'organization lookup failed',
							}),
						),
					)
				if (Option.isNone(organization)) {
					yield* Effect.logWarning('dropping event for unknown organization').pipe(
						Effect.annotateLogs(attributes),
					)
					return IngressDropped.make({ reason: 'unknown_organization' })
				}
				yield* Effect.annotateCurrentSpan({ org_id: organization.value })
				const allowed = yield* gate
					.allowed({ orgId: organization.value, source: message.provider, tenant: message.tenant })
					.pipe(
						Effect.tapError((error) => Effect.logError('channels gate failed', error)),
						Effect.mapError(() =>
							IngressError.make({
								operation: 'ChannelsGate.allowed',
								provider: message.provider,
								message: 'gate lookup failed',
							}),
						),
					)
				if (!allowed) {
					return IngressDropped.make({ reason: 'tenant_disabled' })
				}
				if (message.message.author.isMe) {
					return IngressDropped.make({ reason: 'bot' })
				}
				const subscribed = yield* subscriptions.isSubscribed({ threadId: message.thread.ref.id }).pipe(
					Effect.tapError((error) => Effect.logError('subscription lookup failed', error)),
					Effect.mapError(() =>
						IngressError.make({
							operation: 'Subscriptions.isSubscribed',
							provider: message.provider,
							message: 'subscription lookup failed',
						}),
					),
				)
				if (!subscribed && !message.mentioned) {
					return IngressDropped.make({ reason: 'irrelevant' })
				}
				const delivery = subscribed
					? SubscribedMessageDelivery.make({})
					: NewMentionDelivery.make({ location: message.thread.ref.isNew ? 'channel_root' : 'thread' })
				const event = MessageEvent.make({
					orgId: organization.value,
					provider: message.provider,
					tenant: message.tenant,
					idempotencyKey: message.idempotencyKey,
					thread: message.thread,
					message: message.message,
					delivery,
					raw: message.raw,
				})
				yield* observerBestEffort(observer.eventReceived(event))
				yield* coordinator.submit(event).pipe(
					Effect.tapError((error) => Effect.logError('conversation admission failed', error)),
					Effect.mapError(() =>
						IngressError.make({
							operation: 'ConversationCoordinator.submit',
							provider: message.provider,
							message: 'conversation admission failed',
						}),
					),
				)
				if (subscribed) {
					yield* subscriptions.subscribe({ threadId: message.thread.ref.id }).pipe(
						Effect.tapError((error) =>
							Effect.logWarning('subscription renewal failed', error).pipe(
								Effect.annotateLogs(attributes),
							),
						),
						Effect.ignore,
					)
				}
				return IngressAccepted.make({ idempotencyKey: message.idempotencyKey })
			})

			const acceptLifecycle = Effect.fn('channels.ingress')(function* (input: {
				readonly provider: ProviderName
				readonly tenant: TenantId
				readonly idempotencyKey: NormalizedMessage['idempotencyKey']
				readonly isOwn: boolean
				readonly makeEvent: (orgId: OrgId) => InboundEvent
			}) {
				const organization = yield* organizations
					.resolve({ source: input.provider, tenant: input.tenant })
					.pipe(
						Effect.mapError(() =>
							IngressError.make({
								operation: 'Organizations.resolve',
								provider: input.provider,
								message: 'organization lookup failed',
							}),
						),
					)
				if (Option.isNone(organization)) return IngressDropped.make({ reason: 'unknown_organization' })
				const allowed = yield* gate
					.allowed({ orgId: organization.value, source: input.provider, tenant: input.tenant })
					.pipe(
						Effect.mapError(() =>
							IngressError.make({
								operation: 'ChannelsGate.allowed',
								provider: input.provider,
								message: 'gate lookup failed',
							}),
						),
					)
				if (!allowed) return IngressDropped.make({ reason: 'tenant_disabled' })
				if (input.isOwn) return IngressDropped.make({ reason: 'bot' })
				const event = input.makeEvent(organization.value)
				yield* observerBestEffort(observer.eventReceived(event))
				yield* coordinator.submit(event).pipe(
					Effect.mapError(() =>
						IngressError.make({
							operation: 'ConversationCoordinator.submit',
							provider: input.provider,
							message: 'conversation admission failed',
						}),
					),
				)
				return IngressAccepted.make({ idempotencyKey: input.idempotencyKey })
			})

			const acceptMessageUpdated = (event: NormalizedMessageUpdated) =>
				acceptLifecycle({
					...event,
					isOwn: event.message.author.isMe,
					makeEvent: (orgId) => MessageUpdatedEvent.make({ orgId, ...event }),
				})

			const acceptMessageDeleted = (event: NormalizedMessageDeleted) =>
				acceptLifecycle({
					...event,
					isOwn: event.previousMessage?.author.isMe ?? false,
					makeEvent: (orgId) => MessageDeletedEvent.make({ orgId, ...event }),
				})

			const acceptReaction = (event: NormalizedReaction) =>
				acceptLifecycle({
					...event,
					isOwn: event.actor.isMe,
					makeEvent: (orgId) => ReactionEvent.make({ orgId, ...event }),
				})

			const acceptConversationStopped = Effect.fn('channels.ingress.conversation_stopped')(function* (
				event: NormalizedConversationStopped,
			) {
				const organization = yield* organizations
					.resolve({ source: event.provider, tenant: event.tenant })
					.pipe(
						Effect.tapError((error) =>
							Effect.logError('organization lookup failed for provider stop', error),
						),
						Effect.mapError(() =>
							IngressError.make({
								operation: 'Organizations.resolve',
								provider: event.provider,
								message: 'organization lookup failed',
							}),
						),
					)
				if (Option.isNone(organization)) return IngressDropped.make({ reason: 'unknown_organization' })
				const allowed = yield* gate
					.allowed({ orgId: organization.value, source: event.provider, tenant: event.tenant })
					.pipe(
						Effect.mapError(() =>
							IngressError.make({
								operation: 'ChannelsGate.allowed',
								provider: event.provider,
								message: 'gate lookup failed',
							}),
						),
					)
				if (!allowed) return IngressDropped.make({ reason: 'tenant_disabled' })
				const stopped = ConversationStoppedEvent.make({ orgId: organization.value, ...event })
				const accepted = yield* coordinator.submitCancellation(stopped).pipe(
					Effect.mapError(() =>
						IngressError.make({
							operation: 'ConversationCoordinator.submitCancellation',
							provider: event.provider,
							message: 'conversation stop admission failed',
						}),
					),
				)
				if (!accepted) return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
				yield* observerBestEffort(observer.eventReceived(stopped))
				return IngressAccepted.make({ idempotencyKey: event.idempotencyKey })
			})

			return Ingress.of({
				acceptMessage,
				acceptMessageUpdated,
				acceptMessageDeleted,
				acceptReaction,
				acceptConversationStopped,
			})
		}),
	)
}
