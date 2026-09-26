import {
	bind,
	DeliveryPolicy,
	DeliveryQueue,
	parseMailboxAddress,
	type DeliveryQueueError,
	DeliveryOutputError,
	HandlerFailure,
	IngressAttributionStore,
	DeliveryInterruption,
	MailboxStore,
	MailboxReadiness,
	DeliveryError,
	type DeliveryDefinitionMismatch,
	type NativeResolvedDelivery,
	resolveDeliveryFor,
	type ResolvedDelivery,
	type MailboxStoreError,
	type EventDefinition,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Data, Effect, Layer, Match, Option, Schema } from 'effect'

import { RetryabilityMetadata, SlackIngressError } from './DomainErrors'
import { ThreadId } from './Model'
import { Slack } from './Slack'
import { SlackAuthors } from './SlackAuthors'
import { deliverSlackFinalMessage } from './SlackDeliveryOutput'
import {
	ConversationStoppedEvent,
	MessageDeletedEvent,
	MessageEvent,
	MessageUpdatedEvent,
	ReactionEvent,
} from './SlackEvents'
import type { SlackHandlerRegistration, SlackIngressOptions } from './SlackIngress'
import { resolveSlackIngressAttribution } from './SlackIngressAttribution'
import { SlackOrganizations } from './SlackOrganizations'

export const SlackDeliveryResource = Schema.Struct({ threadId: ThreadId })
export type SlackDeliveryResource = typeof SlackDeliveryResource.Type

const resourceKey = (resource: SlackDeliveryResource) => String(resource.threadId.length) + ':' + resource.threadId

export const messageDefinition = {
	provider: 'slack',
	name: 'slack.message',
	version: '1',
	event: MessageEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: MessageEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.thread.ref.id }),
	}),
} satisfies EventDefinition<typeof MessageEvent, typeof SlackDeliveryResource, 'slack'>

export const updatedDefinition = {
	provider: 'slack',
	name: 'slack.message_updated',
	version: '1',
	event: MessageUpdatedEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: MessageUpdatedEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.thread.ref.id }),
	}),
} satisfies EventDefinition<typeof MessageUpdatedEvent, typeof SlackDeliveryResource, 'slack'>

export const deletedDefinition = {
	provider: 'slack',
	name: 'slack.message_deleted',
	version: '1',
	event: MessageDeletedEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: MessageDeletedEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.threadRef.id }),
	}),
} satisfies EventDefinition<typeof MessageDeletedEvent, typeof SlackDeliveryResource, 'slack'>

export const reactionDefinition = {
	provider: 'slack',
	name: 'slack.reaction',
	version: '1',
	event: ReactionEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: ReactionEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.thread.ref.id }),
	}),
} satisfies EventDefinition<typeof ReactionEvent, typeof SlackDeliveryResource, 'slack'>

export const stoppedDefinition = {
	provider: 'slack',
	name: 'slack.conversation_stopped',
	version: '1',
	event: ConversationStoppedEvent,
	resource: SlackDeliveryResource,
	resourceKey,
	identify: (event: ConversationStoppedEvent) => ({
		installation: event.tenant,
		eventId: event.idempotencyKey,
		resource: SlackDeliveryResource.make({ threadId: event.threadRef.id }),
	}),
} satisfies EventDefinition<typeof ConversationStoppedEvent, typeof SlackDeliveryResource, 'slack'>

export const slackDeliveryDefinitions = [
	messageDefinition,
	updatedDefinition,
	deletedDefinition,
	reactionDefinition,
	stoppedDefinition,
] as const

/** Decode delivery-control identity into the correlated native Slack event and resource. */
export type SlackResolvedDelivery =
	| NativeResolvedDelivery<MessageEvent, SlackDeliveryResource, 'slack'>
	| NativeResolvedDelivery<MessageUpdatedEvent, SlackDeliveryResource, 'slack'>
	| NativeResolvedDelivery<MessageDeletedEvent, SlackDeliveryResource, 'slack'>
	| NativeResolvedDelivery<ReactionEvent, SlackDeliveryResource, 'slack'>
	| NativeResolvedDelivery<ConversationStoppedEvent, SlackDeliveryResource, 'slack'>

export const resolveSlackDelivery = (
	delivery: ResolvedDelivery,
): Effect.Effect<SlackResolvedDelivery, DeliveryDefinitionMismatch | Schema.SchemaError> => {
	switch (delivery.definition) {
		case updatedDefinition.name:
			return resolveDeliveryFor(delivery, updatedDefinition)
		case deletedDefinition.name:
			return resolveDeliveryFor(delivery, deletedDefinition)
		case reactionDefinition.name:
			return resolveDeliveryFor(delivery, reactionDefinition)
		case stoppedDefinition.name:
			return resolveDeliveryFor(delivery, stoppedDefinition)
		default:
			return resolveDeliveryFor(delivery, messageDefinition)
	}
}

const mapIngressError =
	(operation: string) =>
	<E, A, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.tapError(Effect.logError),
			Effect.mapError((error) =>
				Schema.is(SlackIngressError)(error) ? error : SlackIngressError.make({ operation }),
			),
		)

const handlerFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError(Effect.logError),
		Effect.mapError((error) =>
			HandlerFailure.make({
				retryable: !Schema.is(RetryabilityMetadata)(error) || error.retryability === 'retryable',
			}),
		),
	)

export type BindingError = DeliveryError | DeliveryQueueError | MailboxStoreError

type DeliveryBinding<A> = {
	readonly admit: (input: {
		readonly event: A
		readonly organizationId: string
	}) => Effect.Effect<{ readonly key: string; readonly accepted: boolean }, BindingError, DeliveryQueue>
	readonly keyForResource: (input: {
		readonly installation: string
		readonly resource: SlackDeliveryResource
	}) => Effect.Effect<string, DeliveryError>
	readonly cancelActive: (input: {
		readonly key: string
		readonly controlId: string
	}) => Effect.Effect<boolean, BindingError, DeliveryInterruption>
	readonly awaitCancellation: (input: {
		readonly key: string
		readonly controlId: string
	}) => Effect.Effect<void, BindingError, MailboxStore>
	readonly processMailbox: (input: {
		readonly key: string
	}) => Effect.Effect<
		boolean,
		BindingError | DeliveryOutputError | HandlerFailure,
		MailboxStore | SlackAuthors | SlackIngressBindings
	>
	readonly run: (
		input: RunnerOptions,
	) => Effect.Effect<void, BindingError, MailboxStore | MailboxReadiness | SlackAuthors | SlackIngressBindings>
}

export const resolveMessage = (event: MessageEvent): Effect.Effect<MessageEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		return yield* Effect.map(
			authors.resolveDelivery({ thread: event.thread, message: event.message }),
			({ thread, message }) => MessageEvent.make({ ...event, thread, message }),
		)
	}).pipe(Effect.withSpan('slack.ingress.resolve_message'))

export const resolveUpdated = (event: MessageUpdatedEvent): Effect.Effect<MessageUpdatedEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		const delivery = yield* authors.resolveDelivery({
			thread: event.thread,
			message: event.message,
		})
		const previousMessage =
			event.previousMessage === undefined ? undefined : yield* authors.resolveMessage(event.previousMessage)
		return previousMessage === undefined
			? MessageUpdatedEvent.make({ ...event, thread: delivery.thread, message: delivery.message })
			: MessageUpdatedEvent.make({
					...event,
					thread: delivery.thread,
					message: delivery.message,
					previousMessage,
				})
	}).pipe(Effect.withSpan('slack.ingress.resolve_updated'))

export const resolveDeleted = (event: MessageDeletedEvent): Effect.Effect<MessageDeletedEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		return yield* event.previousMessage === undefined
			? Effect.succeed(event)
			: Effect.map(authors.resolveMessage(event.previousMessage), (previousMessage) =>
					MessageDeletedEvent.make({ ...event, previousMessage }),
				)
	}).pipe(Effect.withSpan('slack.ingress.resolve_deleted'))

export const resolveReaction = (event: ReactionEvent): Effect.Effect<ReactionEvent, never, SlackAuthors> =>
	Effect.gen(function* () {
		const authors = yield* SlackAuthors
		const actor = yield* authors.resolveAuthor({ tenant: event.tenant, author: event.actor })
		const message = event.message === undefined ? undefined : yield* authors.resolveMessage(event.message)
		return message === undefined
			? ReactionEvent.make({ ...event, actor })
			: ReactionEvent.make({ ...event, actor, message })
	}).pipe(Effect.withSpan('slack.ingress.resolve_reaction'))

export type AdmitInput = Data.TaggedEnum<{
	Message: { readonly event: MessageEvent }
	Updated: { readonly event: MessageUpdatedEvent }
	Deleted: { readonly event: MessageDeletedEvent }
	Reaction: { readonly event: ReactionEvent }
}>
export const AdmitInput = Data.taggedEnum<AdmitInput>()
export interface StopInput {
	readonly event: ConversationStoppedEvent
}

/**
 * Configured registration topology. Ordinary execution dependencies remain ambient.
 * @effect-expect-leaking DeliveryQueue MailboxStore
 */
export class SlackIngressBindings extends Context.Service<
	SlackIngressBindings,
	{
		readonly admit: (
			input: AdmitInput,
		) => Effect.Effect<void, SlackIngressError, DeliveryQueue | IngressAttributionStore>
		readonly stopConversation: (
			input: StopInput,
		) => Effect.Effect<void, SlackIngressError, DeliveryQueue | DeliveryInterruption | IngressAttributionStore>
		readonly awaitStoppedTargets: (input: StopInput) => Effect.Effect<void, BindingError, MailboxStore>
		readonly processMailbox: (input: {
			readonly key: string
		}) => Effect.Effect<void, SlackIngressError, MailboxStore | SlackAuthors | SlackIngressBindings>
		readonly run: (
			input: RunnerOptions,
		) => Effect.Effect<
			void,
			SlackIngressError,
			MailboxStore | MailboxReadiness | SlackAuthors | SlackIngressBindings
		>
	}
>()('slack/IngressBindings') {
	static readonly layer = <E, R>(options: SlackIngressOptions<E, R>) =>
		Layer.effect(SlackIngressBindings, makeBindings(options))
}

/** A Stop handler cannot start until all targeted message handlers have finalized. */
export const awaitStoppedTargets = Effect.fn('slack.ingress.stop_barrier')(function* ({ event }: StopInput) {
	const bindings = yield* SlackIngressBindings
	yield* bindings.awaitStoppedTargets({ event })
})

const makeBindings = <E, R>(options: SlackIngressOptions<E, R>) =>
	Effect.gen(function* () {
		yield* DeliveryPolicy.makeEffect(options.policy).pipe(
			Effect.mapError(() => SlackIngressError.make({ operation: 'configuration' })),
		)
		yield* Schema.NonEmptyString.makeEffect(options.namespace).pipe(
			Effect.mapError(() => SlackIngressError.make({ operation: 'configuration' })),
		)
		const registrations = [
			...(options.handlers.onNewMention ?? []),
			...(options.handlers.onSubscribedMessage ?? []),
			...(options.handlers.onDirectMessage ?? []),
			...(options.handlers.onMessageUpdated ?? []),
			...(options.handlers.onMessageDeleted ?? []),
			...(options.handlers.onReaction ?? []),
			...(options.handlers.onConversationStopped ?? []),
		]
		const ids = registrations.map((registration) => registration.id)
		if (new Set(ids).size !== ids.length || ids.some((id) => id.length === 0)) {
			return yield* SlackIngressError.make({ operation: 'duplicate_or_empty_handler_id' })
		}
		const handlerContext = yield* Effect.context<R>()
		const configuredSlack = yield* Effect.serviceOption(Slack)
		const configuredOrganizations = yield* Effect.serviceOption(SlackOrganizations)
		const organizations = Option.getOrElse(configuredOrganizations, () =>
			SlackOrganizations.of({
				legacyOrganizationId: 'default',
				resolve: Effect.fn('slack.organizations.default')(() => Effect.succeed({ organizationId: 'default' })),
			}),
		)
		const organizationFor = (event: { readonly tenant: string; readonly idempotencyKey: string }) =>
			resolveSlackIngressAttribution({
				namespace: options.namespace,
				workspaceId: event.tenant,
				eventId: event.idempotencyKey,
			}).pipe(Effect.provideService(SlackOrganizations, organizations))

		const bindRegistration = <A, I>(
			definition: EventDefinition<Schema.Codec<A, I>, typeof SlackDeliveryResource>,
			registration: SlackHandlerRegistration<A, E, R>,
			resolve: (event: A) => Effect.Effect<A, never, SlackAuthors>,
		): DeliveryBinding<A> => {
			const delivery = bind({
				namespace: options.namespace,
				handlerId: registration.id,
				legacyOrganizationId: organizations.legacyOrganizationId ?? null,
				definition,
				policy:
					definition.name === messageDefinition.name ? options.policy : { ...options.policy, mode: 'serial' },
				handler: (event, context) =>
					Effect.gen(function* () {
						const resolved = yield* resolve(event)
						const skipped = yield* Effect.forEach(context.skipped, resolve)
						return yield* registration
							.handler(resolved, { ...context, skipped })
							.pipe(Effect.scoped, Effect.provide(handlerContext))
					}).pipe(handlerFailure),
				deliverFinalMessage: (operation) =>
					Option.match(configuredSlack, {
						onNone: () =>
							Effect.fail(
								DeliveryOutputError.make({ retryable: false, safeCode: 'provider_unavailable' }),
							),
						onSome: (slack) =>
							deliverSlackFinalMessage(operation).pipe(Effect.provideService(Slack, slack)),
					}),
			})
			return delivery
		}

		const newMention = (options.handlers.onNewMention ?? []).map((registration) =>
			bindRegistration(messageDefinition, registration, resolveMessage),
		)
		const subscribedMessage = (options.handlers.onSubscribedMessage ?? []).map((registration) =>
			bindRegistration(messageDefinition, registration, resolveMessage),
		)
		const directMessage = (options.handlers.onDirectMessage ?? []).map((registration) =>
			bindRegistration(messageDefinition, registration, resolveMessage),
		)
		const messageBindings = [...newMention, ...subscribedMessage, ...directMessage]
		const updated = (options.handlers.onMessageUpdated ?? []).map((registration) =>
			bindRegistration(updatedDefinition, registration, resolveUpdated),
		)
		const deleted = (options.handlers.onMessageDeleted ?? []).map((registration) =>
			bindRegistration(deletedDefinition, registration, resolveDeleted),
		)
		const reactions = (options.handlers.onReaction ?? []).map((registration) =>
			bindRegistration(
				reactionDefinition,
				{
					...registration,
					handler: (event, context) =>
						registration.emojis === undefined ||
						registration.emojis.some((emoji) => emoji.name === event.emoji.name)
							? registration.handler(event, context)
							: Effect.void,
				},
				resolveReaction,
			),
		)

		const stopped = (options.handlers.onConversationStopped ?? []).map((registration) =>
			bind({
				namespace: options.namespace,
				handlerId: registration.id,
				definition: stoppedDefinition,
				legacyOrganizationId: organizations.legacyOrganizationId ?? null,
				policy: { ...options.policy, mode: 'serial' },
				handler: (event, context) =>
					awaitStoppedTargets({ event }).pipe(
						Effect.andThen(() =>
							registration.handler(event, context).pipe(Effect.scoped, Effect.provide(handlerContext)),
						),
						handlerFailure,
						Effect.withSpan('slack.ingress.execute_stopped'),
					),
				deliverFinalMessage: (operation) =>
					Option.match(configuredSlack, {
						onNone: () =>
							Effect.fail(
								DeliveryOutputError.make({ retryable: false, safeCode: 'provider_unavailable' }),
							),
						onSome: (slack) =>
							deliverSlackFinalMessage(operation).pipe(Effect.provideService(Slack, slack)),
					}),
			}),
		)
		const allBindings = [...messageBindings, ...updated, ...deleted, ...reactions, ...stopped]
		const keyedBindings = registrations.map((registration, index) => ({
			id: registration.id,
			binding: allBindings[index],
		}))

		const admit = <A extends { readonly tenant: string; readonly idempotencyKey: string }>(
			bindings: ReadonlyArray<DeliveryBinding<A>>,
			event: A,
		) =>
			Effect.gen(function* () {
				if (bindings.length === 0) return
				const organization = yield* organizationFor(event)
				if (organization === null) return
				yield* Effect.forEach(
					bindings,
					(binding) => binding.admit({ event, organizationId: organization.organizationId }),
					{ discard: true },
				)
			}).pipe(mapIngressError('delivery_admit'))
		const targets = (event: ConversationStoppedEvent) =>
			Effect.forEach(messageBindings, (binding) =>
				binding
					.keyForResource({ installation: event.tenant, resource: { threadId: event.threadRef.id } })
					.pipe(Effect.map((key) => ({ binding, key, controlId: event.idempotencyKey }))),
			)
		return SlackIngressBindings.of({
			admit: (input) =>
				Match.value(input).pipe(
					Match.tagsExhaustive({
						Message: ({ event }) =>
							Match.value(event.delivery).pipe(
								Match.tagsExhaustive({
									DirectMessageDelivery: () => admit(directMessage, event),
									SubscribedMessageDelivery: () => admit(subscribedMessage, event),
									NewMentionDelivery: () => admit(newMention, event),
									PatternMessageDelivery: () => Effect.void,
								}),
							),
						Updated: ({ event }) => admit(updated, event),
						Deleted: ({ event }) => admit(deleted, event),
						Reaction: ({ event }) => admit(reactions, event),
					}),
					Effect.withSpan('slack.ingress.admit'),
				),
			stopConversation: Effect.fn('slack.ingress.stop_conversation')(function* ({ event }: StopInput) {
				const organization = yield* organizationFor(event)
				if (organization === null) return
				const selected = yield* targets(event).pipe(mapIngressError('cancel_active'))
				yield* Effect.forEach(
					selected,
					({ binding, key, controlId }) => binding.cancelActive({ key, controlId }),
					{ discard: true },
				).pipe(mapIngressError('cancel_active'))
				yield* admit(stopped, event)
			}),
			awaitStoppedTargets: Effect.fn('slack.ingress.await_stopped_targets')(function* ({ event }: StopInput) {
				const selected = yield* targets(event)
				yield* Effect.forEach(
					selected,
					({ binding, key, controlId }) => binding.awaitCancellation({ key, controlId }),
					{ discard: true },
				)
			}),
			processMailbox: Effect.fn('slack.ingress.process_mailbox')(function* ({ key }) {
				const address = parseMailboxAddress(key)
				const entry = keyedBindings.find(({ id }) => id === address?.handlerId)
				if (
					address === undefined ||
					address.namespace !== options.namespace ||
					address.provider !== 'slack' ||
					entry?.binding === undefined
				)
					return yield* SlackIngressError.make({ operation: 'configuration' })
				yield* entry.binding.processMailbox({ key }).pipe(mapIngressError('delivery_run'), Effect.asVoid)
			}),
			run: (input) =>
				(allBindings.length === 0
					? Effect.never
					: Effect.forEach(allBindings, (binding) => binding.run(input), {
							concurrency: 'unbounded',
							discard: true,
						})
				).pipe(mapIngressError('delivery_run'), Effect.withSpan('slack.ingress.bindings_run')),
		})
	})
