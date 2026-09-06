import { DeliveryPolicy, type RunnerOptions } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import { Crypto, Effect, Layer, Predicate } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { Slack } from './Slack.ts'
import { SlackIngress, type SlackHandlerRegistration, type SlackIngressHandlers } from './SlackIngress.ts'
import { SlackRoutes } from './SlackRoutes.ts'
import { SlackSubscriptions } from './SlackSubscriptions.ts'

/** Bounded, single-process defaults for the memory bot preset, not a durability guarantee. */
const memoryPolicy = DeliveryPolicy.make({
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

export type SlackMemoryBotOptions<E, R> = {
	readonly namespace: string
	readonly handlers: SlackBotHandlers<E, R>
	readonly policy?: Partial<DeliveryPolicy>
	readonly runner?: Partial<RunnerOptions>
	readonly maxMailboxes?: number
}

/**
 * Slack-only composition: native operations, subscriptions, admission and a scoped worker.
 * Hosts supply SlackClient, SlackTenantCredentials and Crypto; handlers keep their ambient requirements.
 * Construction is lazy. Routes alone admit work without starting a worker; layer runs both in one scope.
 */
export const SlackBot = {
	memory: <E = never, R = never>(options: SlackMemoryBotOptions<E, R>) => {
		const services = SlackIngress.layer({
			namespace: options.namespace,
			policy: { ...memoryPolicy, ...options.policy },
			handlers: {
				onNewMention: registrations('mention', options.handlers.onNewMention),
				onSubscribedMessage: registrations('subscribed', options.handlers.onSubscribedMessage),
				onDirectMessage: registrations('dm', options.handlers.onDirectMessage),
				onMessageUpdated: registrations('edited', options.handlers.onMessageUpdated),
				onMessageDeleted: registrations('deleted', options.handlers.onMessageDeleted),
				onReaction: registrations('reaction', options.handlers.onReaction),
				onConversationStopped: registrations('stopped', options.handlers.onConversationStopped),
			},
		}).pipe(
			Layer.provideMerge(
				Layer.mergeAll(
					Slack.layer,
					SlackSubscriptions.layerMemory(),
					memory({ maxMailboxes: options.maxMailboxes ?? 10_000 }),
				),
			),
		)
		const worker = Layer.effectDiscard(
			Effect.flatMap(SlackIngress, (ingress) =>
				ingress.run({ scanLimit: 100, concurrency: 8, pollMs: 25, ...options.runner }),
			).pipe(Effect.forkScoped),
		).pipe(Layer.provide(services))
		const routes = SlackRoutes.layer.pipe(
			HttpRouter.provideRequest(Layer.merge(services, Layer.effectContext(Effect.context<Crypto.Crypto>()))),
			Layer.provide(services),
		)
		return { services, routes, worker, layer: Layer.merge(routes, worker) }
	},
}
