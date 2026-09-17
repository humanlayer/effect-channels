import {
	bind,
	type DeliveryHandoff,
	DeliveryOutputError,
	DeliveryPolicy,
	DeliveryQueue,
	HandlerFailure,
	IngressAttributionStore,
	MailboxReadiness,
	MailboxStore,
	parseMailboxAddress,
	type HandlerContext,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Layer, Logger, Match, Option, Schema } from 'effect'

import { GitHub } from './GitHub'
import { GitHubActivityEvent, GitHubCreationEvent, GitHubMentionEvent, activityEventDefinition } from './GitHubActivity'
import { deliverGitHubFinalMessage } from './GitHubDeliveryOutput'
import { GitHubError, GitHubIngressError } from './GitHubErrors'
import { resolveGitHubIngressAttribution } from './GitHubIngressAttribution'
import { GitHubOrganizations } from './GitHubOrganizations'
import { GitHubDiscussionRef } from './GitHubResource'
import { GitHubSubscriptionStore } from './GitHubSubscriptions'

export interface GitHubHandlerRegistration<E, R> {
	readonly id: string
	readonly onCreation?: (
		event: GitHubCreationEvent,
		context: HandlerContext<GitHubActivityEvent>,
	) => Effect.Effect<void | DeliveryHandoff, E, R>
	readonly onMention?: (
		event: GitHubMentionEvent,
		context: HandlerContext<GitHubActivityEvent>,
	) => Effect.Effect<void | DeliveryHandoff, E, R>
	readonly onSubscribedEvent?: (
		event: GitHubActivityEvent,
		context: HandlerContext<GitHubActivityEvent>,
	) => Effect.Effect<void | DeliveryHandoff, E, R>
}

export interface GitHubIngressOptions<E, R> {
	readonly namespace: string
	readonly policy: DeliveryPolicy
	readonly handlers: ReadonlyArray<GitHubHandlerRegistration<E, R>>
}
export class GitHubIngress extends Context.Service<
	GitHubIngress,
	{
		readonly acceptActivity: (input: {
			readonly event: GitHubActivityEvent
			readonly mentioned: boolean
			readonly own: boolean
		}) => Effect.Effect<void, GitHubIngressError>
		readonly processActivity: (input: {
			readonly event: GitHubActivityEvent
		}) => Effect.Effect<void, GitHubIngressError, MailboxStore>
		readonly processMailbox: (input: { readonly key: string }) => Effect.Effect<void, GitHubIngressError, MailboxStore>
		readonly run: (input: RunnerOptions) => Effect.Effect<void, GitHubIngressError, MailboxStore | MailboxReadiness>
	}
>()('github/GitHubIngress') {
	static readonly layer = <E = never, R = never>(options: GitHubIngressOptions<E, R>) =>
		Layer.effect(
			GitHubIngress,
			Effect.gen(function* () {
				const subscriptions = yield* GitHubSubscriptionStore
				const handlerContext = yield* Effect.context<R | DeliveryQueue | IngressAttributionStore>()
				const queue = yield* DeliveryQueue
				const attribution = yield* IngressAttributionStore
				const loggers = yield* Logger.CurrentLoggers
				const configuredGitHub = yield* Effect.serviceOption(GitHub)
				const configuredOrganizations = yield* Effect.serviceOption(GitHubOrganizations)
				const organizations = Option.getOrElse(configuredOrganizations, () =>
					GitHubOrganizations.of({
						legacyOrganizationId: 'default',
						resolve: Effect.fn('github.organizations.default')(() =>
							Effect.succeed({ organizationId: 'default' }),
						),
					}),
				)
				const organizationFor = (event: GitHubActivityEvent) =>
					resolveGitHubIngressAttribution({
						namespace: options.namespace,
						eventId: event.deliveryId,
						installationId: event.resource.repository.installationId,
					}).pipe(Effect.provideService(GitHubOrganizations, organizations))
				const policy = yield* DeliveryPolicy.makeEffect(options.policy).pipe(
					Effect.mapError(() => GitHubIngressError.make({ operation: 'configuration' })),
				)
				yield* Schema.NonEmptyString.makeEffect(options.namespace).pipe(
					Effect.mapError(() => GitHubIngressError.make({ operation: 'configuration' })),
				)
				const ids = new Set<string>()
				const bindings: Array<{
					readonly id: string
					readonly binding: ReturnType<
						typeof bind<typeof GitHubActivityEvent, typeof GitHubDiscussionRef, never>
					>
				}> = []
				for (const registration of options.handlers) {
					if (registration.id.length === 0 || ids.has(registration.id))
						return yield* GitHubIngressError.make({ operation: 'configuration' })
					ids.add(registration.id)
					for (const route of ['creation', 'mention', 'subscribed'] as const) {
						const configured = Match.value(route).pipe(
							Match.when('creation', () => registration.onCreation !== undefined),
							Match.when('mention', () => registration.onMention !== undefined),
							Match.when('subscribed', () => registration.onSubscribedEvent !== undefined),
							Match.exhaustive,
						)
						if (!configured) continue
						const id = JSON.stringify([registration.id, route])
						const binding = bind({
							namespace: options.namespace,
							handlerId: id,
							definition: activityEventDefinition,
							legacyOrganizationId: organizations.legacyOrganizationId ?? null,
							policy: { ...policy, mode: 'serial' },
							handler: (event, context) => {
								const effect = Match.value(route).pipe(
									Match.when('creation', () =>
										Schema.is(GitHubCreationEvent)(event)
											? registration.onCreation?.(event, context)
											: undefined,
									),
									Match.when('mention', () =>
										Schema.is(GitHubMentionEvent)(event)
											? registration.onMention?.(event, context)
											: undefined,
									),
									Match.when('subscribed', () => registration.onSubscribedEvent?.(event, context)),
									Match.exhaustive,
								)
								if (effect === undefined) return Effect.fail(HandlerFailure.make({ retryable: false }))
								return effect.pipe(
									Effect.scoped,
									Effect.provide(handlerContext),
									Effect.tapError(() =>
										Effect.logError('GitHub activity handler failed', { handler: registration.id }),
									),
									Effect.tapError((error) =>
										Schema.is(GitHubError)(error) &&
										error.reason === 'unavailable' &&
										error.retryAfterMs !== undefined
											? Effect.sleep(error.retryAfterMs)
											: Effect.void,
									),
									Effect.mapError((error) =>
										HandlerFailure.make({
											retryable: Schema.is(GitHubError)(error)
												? error.reason === 'unavailable'
												: true,
										}),
									),
								) as Effect.Effect<void, HandlerFailure, never>
							},
							deliverFinalMessage: (operation) =>
								Option.match(configuredGitHub, {
									onNone: () =>
										Effect.fail(
											DeliveryOutputError.make({
												retryable: false,
												safeCode: 'provider_unavailable',
											}),
										),
									onSome: (github) =>
										deliverGitHubFinalMessage(operation).pipe(
											Effect.provideService(GitHub, github),
										),
								}),
						})
						bindings.push({ id, binding })
					}
				}
				const provide = <A, E, R2>(operation: 'admit' | 'process' | 'run', effect: Effect.Effect<A, E, R2>) =>
					effect.pipe(
						Effect.provideService(Logger.CurrentLoggers, loggers),
						Effect.tapError(Effect.logError),
						Effect.mapError((error) =>
							Schema.is(GitHubIngressError)(error) ? error : GitHubIngressError.make({ operation }),
						),
						Effect.asVoid,
					)
				return GitHubIngress.of({
					acceptActivity: Effect.fn('github.ingress.accept_activity')((input) =>
						provide(
							'admit',
							Effect.gen(function* () {
								const event = yield* GitHubActivityEvent.makeEffect(input.event)
								const direct: Array<string> = []
								const followed: Array<string> = []
								if (!input.own)
									for (const registration of options.handlers) {
										if (
											registration.onCreation !== undefined &&
											Schema.is(GitHubCreationEvent)(event)
										)
											direct.push(JSON.stringify([registration.id, 'creation']))
										if (input.mentioned && registration.onMention !== undefined)
											direct.push(JSON.stringify([registration.id, 'mention']))
										else if (registration.onSubscribedEvent !== undefined)
											followed.push(JSON.stringify([registration.id, 'subscribed']))
									}
								const decision = yield* subscriptions.resolveRoute({
									namespace: options.namespace,
									resource: event.resource,
									deliveryId: event.deliveryId,
									direct,
									followed,
								})
								if (decision.targets.length === 0) return
								const organization = yield* organizationFor(event)
								if (organization === null) return
								for (const target of decision.targets) {
									const entry = bindings.find((entry) => entry.id === target)
									if (entry === undefined)
										return yield* GitHubIngressError.make({ operation: 'configuration' })
									yield* entry.binding.admit({ event, organizationId: organization.organizationId })
								}
							}).pipe(
								Effect.provideService(DeliveryQueue, queue),
								Effect.provideService(IngressAttributionStore, attribution),
							),
						),
					),
					processActivity: Effect.fn('github.ingress.process_activity')((input) =>
						provide(
							'process',
							Effect.forEach(
								bindings,
								({ binding }) =>
									binding
										.keyFor(input)
										.pipe(Effect.flatMap((key) => binding.processMailbox({ key }))),
								{ discard: true },
							),
						),
					),
					processMailbox: Effect.fn('github.ingress.process_mailbox')(({ key }) =>
						provide(
							'process',
							Effect.gen(function* () {
								const address = parseMailboxAddress(key)
								if (
									address === undefined ||
									address.namespace !== options.namespace ||
									address.provider !== 'github'
								)
									return yield* GitHubIngressError.make({ operation: 'configuration' })
								const entry = bindings.find(({ id }) => id === address.handlerId)
								if (entry === undefined)
									return yield* GitHubIngressError.make({ operation: 'configuration' })
								yield* entry.binding.processMailbox({ key })
							}),
						),
					),
					run: Effect.fn('github.ingress.run')((input) =>
						provide(
							'run',
							Effect.forEach(
								bindings.map((entry) => entry.binding),
								(binding) => binding.run(input),
								{
									concurrency: Math.max(1, bindings.length),
									discard: true,
								},
							),
						),
					),
				})
			}),
		)
}

/** Explicit long-running polling program for server hosts. Constructing GitHubBot does not start it. */
export const runDeliveryPolling = (input: RunnerOptions) =>
	Effect.flatMap(GitHubIngress, (ingress) => ingress.run(input)).pipe(Effect.withSpan('github.delivery.polling'))
