import {
	bind,
	DeliveryPolicy,
	HandlerFailure,
	MailboxReadiness,
	MailboxStore,
	type HandlerContext,
	type RunnerOptions,
} from '@humanlayer/channels-delivery'
import { Context, Effect, Layer, Match, Schema } from 'effect'

import {
	GitHubActivityEvent,
	GitHubCreationEvent,
	GitHubMentionEvent,
	activityEventDefinition,
} from './GitHubActivity.js'
import { GitHubError, GitHubIngressError } from './GitHubErrors.js'
import { GitHubIssueEvent, issueEventDefinition } from './GitHubEvents.js'
import { GitHubDiscussionRef } from './GitHubResource.js'
import { GitHubSubscriptionStore } from './GitHubSubscriptions.js'

export interface GitHubActivityRegistration<E, R> {
	readonly id: string
	readonly onCreation?: (
		event: GitHubCreationEvent,
		context: HandlerContext<GitHubActivityEvent>,
	) => Effect.Effect<void, E, R>
	readonly onMention?: (
		event: GitHubMentionEvent,
		context: HandlerContext<GitHubActivityEvent>,
	) => Effect.Effect<void, E, R>
	readonly onSubscribedEvent?: (
		event: GitHubActivityEvent,
		context: HandlerContext<GitHubActivityEvent>,
	) => Effect.Effect<void, E, R>
}

export interface GitHubHandlerRegistration<E, R> {
	readonly id: string
	readonly handler: (event: GitHubIssueEvent, context: HandlerContext<GitHubIssueEvent>) => Effect.Effect<void, E, R>
}
export interface GitHubIngressOptions<E, R> {
	readonly namespace: string
	readonly policy: DeliveryPolicy
	readonly handlers?: ReadonlyArray<GitHubHandlerRegistration<E, R>>
	readonly activityHandlers?: ReadonlyArray<GitHubActivityRegistration<E, R>>
}
export class GitHubIngress extends Context.Service<
	GitHubIngress,
	{
		readonly activityEnabled: boolean
		readonly accept: (input: { readonly event: GitHubIssueEvent }) => Effect.Effect<void, GitHubIngressError>
		readonly acceptActivity: (input: {
			readonly event: GitHubActivityEvent
			readonly mentioned: boolean
			readonly own: boolean
		}) => Effect.Effect<void, GitHubIngressError>
		readonly processActivity: (input: {
			readonly event: GitHubActivityEvent
		}) => Effect.Effect<void, GitHubIngressError>
		readonly process: (input: { readonly event: GitHubIssueEvent }) => Effect.Effect<void, GitHubIngressError>
		readonly run: (input: RunnerOptions) => Effect.Effect<void, GitHubIngressError>
	}
>()('github/GitHubIngress') {
	static readonly layer = <E, R>(options: GitHubIngressOptions<E, R>) =>
		Layer.effect(
			GitHubIngress,
			Effect.gen(function* () {
				const subscriptions = yield* GitHubSubscriptionStore
				if (options.activityHandlers !== undefined && (options.handlers?.length ?? 0) > 0)
					return yield* GitHubIngressError.make({ operation: 'configuration' })
				const context = yield* Effect.context<R | MailboxStore | MailboxReadiness>()
				const policy = yield* DeliveryPolicy.makeEffect(options.policy).pipe(
					Effect.mapError(() => GitHubIngressError.make({ operation: 'configuration' })),
				)
				yield* Schema.NonEmptyString.makeEffect(options.namespace).pipe(
					Effect.mapError(() => GitHubIngressError.make({ operation: 'configuration' })),
				)
				const ids = new Set<string>()
				for (const registration of options.handlers ?? []) {
					if (registration.id.length === 0 || ids.has(registration.id))
						return yield* GitHubIngressError.make({ operation: 'configuration' })
					ids.add(registration.id)
				}
				const bindings = (options.handlers ?? []).map((registration) =>
					bind({
						namespace: options.namespace,
						handlerId: registration.id,
						definition: issueEventDefinition,
						policy,
						handler: (event, context) =>
							registration.handler(event, context).pipe(
								Effect.scoped,
								Effect.tapError((error) =>
									Effect.logError('GitHub application handler failed', {
										reason: Schema.is(GitHubError)(error) ? error.reason : 'application',
									}).pipe(Effect.annotateLogs({ handler: registration.id })),
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
							),
					}),
				)
				const activityIds = new Set<string>()
				const activityBindings: Array<{
					readonly id: string
					readonly binding: ReturnType<typeof bind<typeof GitHubActivityEvent, typeof GitHubDiscussionRef, R>>
				}> = []
				for (const registration of options.activityHandlers ?? []) {
					if (registration.id.length === 0 || activityIds.has(registration.id))
						return yield* GitHubIngressError.make({ operation: 'configuration' })
					activityIds.add(registration.id)
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
								)
							},
						})
						activityBindings.push({ id, binding })
					}
				}
				const provide = <A, E>(
					operation: 'admit' | 'process' | 'run',
					effect: Effect.Effect<A, E, R | MailboxStore | MailboxReadiness>,
				) =>
					effect.pipe(
						Effect.provide(context),
						Effect.tapError(Effect.logError),
						Effect.mapError(() => GitHubIngressError.make({ operation })),
						Effect.asVoid,
					)
				return GitHubIngress.of({
					activityEnabled: options.activityHandlers !== undefined,
					acceptActivity: Effect.fn('github.ingress.accept_activity')((input) =>
						provide(
							'admit',
							Effect.gen(function* () {
								const event = yield* GitHubActivityEvent.makeEffect(input.event)
								const direct: Array<string> = []
								const followed: Array<string> = []
								if (!input.own)
									for (const registration of options.activityHandlers ?? []) {
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
								for (const target of decision.targets) {
									const entry = activityBindings.find((entry) => entry.id === target)
									if (entry === undefined)
										return yield* GitHubIngressError.make({ operation: 'configuration' })
									yield* entry.binding.admit({ event })
								}
							}),
						),
					),
					processActivity: Effect.fn('github.ingress.process_activity')((input) =>
						provide(
							'process',
							Effect.forEach(
								activityBindings,
								({ binding }) =>
									binding
										.keyFor(input)
										.pipe(Effect.flatMap((key) => binding.processMailbox({ key }))),
								{ discard: true },
							),
						),
					),
					accept: Effect.fn('github.ingress.accept')((input) =>
						provide(
							'admit',
							Effect.forEach(bindings, (binding) => binding.admit(input), { discard: true }),
						),
					),
					process: Effect.fn('github.ingress.process')((input) =>
						provide(
							'process',
							Effect.forEach(
								bindings,
								(binding) =>
									binding
										.keyFor(input)
										.pipe(Effect.flatMap((key) => binding.processMailbox({ key }))),
								{ discard: true },
							),
						),
					),
					run: Effect.fn('github.ingress.run')((input) =>
						provide(
							'run',
							Effect.forEach(
								[...bindings, ...activityBindings.map((entry) => entry.binding)],
								(binding) => binding.run(input),
								{
									concurrency: Math.max(1, bindings.length + activityBindings.length),
									discard: true,
								},
							),
						),
					),
				})
			}),
		)
}
