import { DeliveryPolicy, layerMailboxStoreServices } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import { Effect, Layer, Predicate, Schema } from 'effect'

import { SlackIngressError } from './DomainErrors'
import { Slack } from './Slack'
import { SlackIngress, type SlackHandlerRegistration, type SlackIngressHandlers } from './SlackIngress'
import { SlackSubscriptions } from './SlackSubscriptions'

/** Bounded initial policy, not a production capacity recommendation or an exactly-once guarantee. */
const defaultPolicy = DeliveryPolicy.make({
	mode: 'queue',
	maxPayloadBytes: 256_000,
	maxEnvelopes: 1_000,
	maxOutcomes: 10_000,
	retentionMs: 86_400_000,
	maxAttempts: 5,
	retryBaseMs: 100,
	retryMaxMs: 30_000,
	leaseMs: 30_000,
	heartbeatMs: 5_000,
	conflictRetries: 10,
})

export type SlackBotHandlers<E, R> = {
	readonly [K in keyof SlackIngressHandlers<E, R>]?:
		| NonNullable<SlackIngressHandlers<E, R>[K]>
		| NonNullable<SlackIngressHandlers<E, R>[K]>[number]['handler']
}

const registrations = <A, E, R>(
	id: string,
	handler:
		| SlackHandlerRegistration<A, E, R>['handler']
		| ReadonlyArray<SlackHandlerRegistration<A, E, R>>
		| undefined,
): ReadonlyArray<SlackHandlerRegistration<A, E, R>> =>
	handler === undefined ? [] : Predicate.isFunction(handler) ? [{ id, handler }] : handler

export type SlackBotOptions<E, R> = {
	readonly namespace: string
	readonly handlers: SlackBotHandlers<E, R>
	readonly policy?: Partial<DeliveryPolicy>
}
export type SlackMemoryBotOptions<E, R> = SlackBotOptions<E, R> & { readonly maxMailboxes?: number }

/**
 * Slack-only composition: native operations, subscriptions and admission.
 * Hosts supply storage and HTTP; handlers keep their ambient requirements.
 * Construction is lazy and does not register routes or start a delivery worker. Hosts compose SlackRoutes and
 * explicitly run ingress processing when applicable.
 */
export const SlackBot = {
	make: <E = never, R = never>(options: SlackBotOptions<E, R>) => {
		const services = Layer.unwrap(
			Schema.decodeUnknownEffect(DeliveryPolicy)({ ...defaultPolicy, ...options.policy }).pipe(
				Effect.mapError(() => SlackIngressError.make({ operation: 'configuration' })),
				Effect.map((policy) =>
					SlackIngress.layer({
						namespace: options.namespace,
						policy,
						handlers: {
							onNewMention: registrations('mention', options.handlers.onNewMention),
							onSubscribedMessage: registrations('subscribed', options.handlers.onSubscribedMessage),
							onDirectMessage: registrations('dm', options.handlers.onDirectMessage),
							onMessageUpdated: registrations('edited', options.handlers.onMessageUpdated),
							onMessageDeleted: registrations('deleted', options.handlers.onMessageDeleted),
							onReaction: registrations('reaction', options.handlers.onReaction),
							onConversationStopped: registrations('stopped', options.handlers.onConversationStopped),
						},
					}),
				),
			),
		).pipe(Layer.provideMerge(Slack.layerFromStore))
		return services
	},
	/** Memory-backed preset; connections and HTTP remain required services. */
	memory: <E = never, R = never>(options: SlackMemoryBotOptions<E, R>) => {
		const bot = SlackBot.make(options)
		const deliveryStorage = memory({ maxMailboxes: options.maxMailboxes ?? 10_000 })
		const storage = Layer.merge(
			SlackSubscriptions.layerMemory(),
			layerMailboxStoreServices.pipe(Layer.provide(deliveryStorage)),
		)
		return bot.pipe(Layer.provideMerge(storage))
	},
}
