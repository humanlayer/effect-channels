import { describe, it } from '@effect/vitest'
import {
	DeliveryPreparationConflict,
	DeliveryPreparationUnavailable,
	MailboxSubscriptions,
	MailboxSubscriptionsMemory,
	PreparedDeliveryInvocation,
	ProviderDeliveryExecution,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	deliveryMailboxKey,
	type DeliveryAdmission,
	type DeliveryContext,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Match, Option, Ref, type Schema } from 'effect'

import { makeTestDeliveryExecution } from '../../delivery-next/test/delivery-execution'
import { LinearApi } from '../src/LinearApi'
import { LinearCallbacks, type LinearCallbackHandlers } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import { LinearAgentActivityId } from '../src/LinearIdentity'
import { LinearAgentActivityReceipt, type LinearCreateAgentActivityRequest } from '../src/LinearModels'
import {
	agentSessionPayloads,
	appUserNotificationPayloads,
	linearAgentSessionAdmission,
	linearAppUserId,
	linearIssueCreateAdmission,
	linearNotificationAdmission,
	linearOauthClientId,
	linearOrganizationId,
	makeLinearTestProvider,
	signedLinearInput,
} from './fixtures'

const namespace = 'linear-processing-test'
const processor = makeLinearEventProcessor({
	namespace,
	bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
	oauthClientId: linearOauthClientId,
})

const organizationId = '6c5940f1-4f77-4f0b-8517-45b58f3c7d21'
const issueId = 'b33fb278-fbe0-45e4-b4eb-94b0839f51b9'
const sessionId = '71000000-0000-4000-8000-000000000001'

const sessionOperations = [
	'PresentOutcome',
	'CreateMessage',
	'SetMessageReaction',
	'SetActivity',
	'RenderPlan',
	'AddExternalLink',
]
const issueOperations = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'RenderPlan',
	'AddExternalLink',
]

const receipt = (request: LinearCreateAgentActivityRequest) =>
	LinearAgentActivityReceipt.make({
		activityId: LinearAgentActivityId.make('generated-by-provider'),
		sessionId: request.sessionId,
	})

/** Records the order of preparation, provider calls, and callbacks in one log. */
const makeOrderLog = Ref.make<ReadonlyArray<string>>([])

const recordingExecution = (execution: ProviderDeliveryExecution, order: Ref.Ref<ReadonlyArray<string>>) =>
	new ProviderDeliveryExecution({
		deliveryId: execution.deliveryId,
		idempotencyKey: execution.idempotencyKey,
		prepared: execution.prepared,
		context: execution.context,
		prepare: (invocation) =>
			Ref.update(order, (values) => [...values, 'prepare']).pipe(Effect.andThen(execution.prepare(invocation))),
	})

const failingExecution = (
	execution: ProviderDeliveryExecution,
	error: DeliveryPreparationConflict | DeliveryPreparationUnavailable,
) =>
	new ProviderDeliveryExecution({
		deliveryId: execution.deliveryId,
		idempotencyKey: execution.idempotencyKey,
		prepared: execution.prepared,
		context: execution.context,
		prepare: () => Effect.fail(error),
	})

const withPrepared = (execution: ProviderDeliveryExecution, prepared: PreparedDeliveryInvocation) =>
	new ProviderDeliveryExecution({ ...execution, prepared: Option.some(prepared) })

const layer = <E>(
	handlers: LinearCallbackHandlers<E, MailboxSubscriptions>,
	createAgentActivity: (
		request: LinearCreateAgentActivityRequest,
	) => Effect.Effect<LinearAgentActivityReceipt> = () => Effect.die('no activity expected'),
) =>
	Layer.mergeAll(
		LinearCallbacks.layer(handlers).pipe(Layer.provideMerge(MailboxSubscriptionsMemory)),
		Layer.mock(LinearApi, { createAgentActivity }),
	)

const admittedEvent = (payload: Schema.Json, deliveryId: string) =>
	Effect.gen(function* () {
		const outcome = yield* makeLinearTestProvider(namespace).handle(
			signedLinearInput(payload, 'AgentSessionEvent', deliveryId),
		)
		return yield* Match.value(outcome).pipe(
			Match.tagsExhaustive({
				Event: ({ event }): Effect.Effect<DeliveryAdmission> => Effect.succeed(event),
				Events: () => Effect.die('Expected one Linear admission'),
				Ignored: () => Effect.die('Expected admitted Linear webhook'),
				Response: () => Effect.die('Expected admitted Linear webhook'),
			}),
		)
	})

const stopPromptPayload = {
	...agentSessionPayloads[1],
	agentActivity: { ...agentSessionPayloads[1].agentActivity, signal: 'stop' },
}

describe('Linear delivery preparation', () => {
	it.effect('prepares a session destination once, before the automatic thought and the callback', ({ expect }) =>
		Effect.gen(function* () {
			const order = yield* makeOrderLog
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process(
					[yield* linearAgentSessionAdmission(agentSessionPayloads[0])],
					recordingExecution(test.execution, order),
				)
				.pipe(
					Effect.provide(
						layer(
							{ onAgentSessionCreated: () => Ref.update(order, (values) => [...values, 'callback']) },
							(request) =>
								Ref.update(order, (values) => [...values, 'thought']).pipe(Effect.as(receipt(request))),
						),
					),
				)
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(order)).toEqual(['prepare', 'thought', 'callback'])
			expect(yield* Ref.get(test.preparations)).toEqual([
				{
					callback: 'onAgentSessionCreated',
					presentationVersion: 1,
					destination: {
						_tag: 'LinearAgentSessionDestination',
						organizationId,
						appUserId: linearAppUserId,
						sessionId,
						issueId,
					},
					activationTarget: { _tag: 'LinearIssueActivationTarget', organizationId, issueId },
					supportedOperations: sessionOperations,
				},
			])
		}),
	)

	it.effect("uses the session's source comment as its activation target", ({ expect }) =>
		Effect.gen(function* () {
			const payload = agentSessionPayloads[1]
			const sourceCommentId = '72000000-0000-4000-8000-000000000009'
			const admission = yield* admittedEvent(
				{ ...payload, agentSession: { ...payload.agentSession, sourceCommentId } },
				'delivery-source-comment',
			)
			const test = yield* makeTestDeliveryExecution()
			yield* processor
				.process([admission], test.execution)
				.pipe(Effect.provide(layer({ onAgentSessionPrompted: () => Effect.void })))
			const [prepared] = yield* Ref.get(test.preparations)
			expect(prepared?.callback).toBe('onAgentSessionPrompted')
			expect(prepared?.activationTarget).toEqual({
				_tag: 'LinearCommentActivationTarget',
				organizationId,
				issueId,
				commentId: sourceCommentId,
			})
		}),
	)

	it.effect('prepares an issue destination for a comment mention', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process([linearNotificationAdmission(appUserNotificationPayloads[1])], test.execution)
				.pipe(Effect.provide(layer({ onMentioned: () => Effect.void })))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(test.preparations)).toEqual([
				{
					callback: 'onMentioned',
					presentationVersion: 1,
					destination: { _tag: 'LinearIssueDestination', organizationId, issueId },
					activationTarget: {
						_tag: 'LinearCommentActivationTarget',
						organizationId,
						issueId,
						commentId: 'dd45e8fb-4444-4555-8666-001122334455',
					},
					supportedOperations: issueOperations,
				},
			])
		}),
	)

	it.effect('prepares an issue destination with the issue as activation for an assignment', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			yield* processor
				.process([linearNotificationAdmission(appUserNotificationPayloads[2])], test.execution)
				.pipe(Effect.provide(layer({ onAssigned: () => Effect.void })))
			const [prepared] = yield* Ref.get(test.preparations)
			expect(prepared?.callback).toBe('onAssigned')
			expect(prepared?.destination).toEqual({ _tag: 'LinearIssueDestination', organizationId, issueId })
			expect(prepared?.activationTarget).toEqual({ _tag: 'LinearIssueActivationTarget', organizationId, issueId })
		}),
	)

	it.effect('does not prepare a batch it ignores', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process([linearNotificationAdmission(appUserNotificationPayloads[0])], test.execution)
				.pipe(
					Effect.provide(
						layer({
							onAgentSessionCreated: () => Effect.void,
							onMentioned: () => Effect.die('must not run'),
						}),
					),
				)
			expect(result).toEqual(ProviderEventIgnored.make({ reason: 'supplemental_signal' }))
			expect(yield* Ref.get(test.preparations)).toEqual([])
		}),
	)

	it.effect('passes the delivery context to the callback', ({ expect }) =>
		Effect.gen(function* () {
			const seen = yield* Ref.make(Option.none<DeliveryContext>())
			const test = yield* makeTestDeliveryExecution()
			yield* processor.process([linearIssueCreateAdmission(namespace)], test.execution).pipe(
				Effect.provide(
					layer({
						onIssueCreated: (_event, delivery) =>
							Ref.set(seen, Option.some(delivery)).pipe(Effect.andThen(delivery.handoff())),
					}),
				),
			)
			const delivery = Option.getOrThrow(yield* Ref.get(seen))
			expect(delivery.deliveryId).toBe(test.execution.deliveryId)
			expect(yield* Ref.get(test.handoffs)).toHaveLength(1)
		}),
	)

	it.effect('reruns the saved callback on retry after the first attempt subscribed the issue', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<string>>([])
			const services = layer({
				onIssueCreated: (event) =>
					Effect.gen(function* () {
						const attempt = (yield* Ref.updateAndGet(calls, (values) => [...values, 'created'])).length
						yield* event.issue.subscribe()
						yield* attempt === 1 ? Effect.fail({ retryable: true }) : Effect.void
					}),
				onSubscribedEvent: () => Ref.update(calls, (values) => [...values, 'subscribed']),
			})
			const admission = linearIssueCreateAdmission(namespace)
			const first = yield* makeTestDeliveryExecution()

			const outcome = yield* Effect.gen(function* () {
				const failure = yield* processor.process([admission], first.execution).pipe(Effect.flip)
				const retry = yield* first.retry
				const result = yield* processor.process([admission], retry.execution)
				const subscriptions = yield* MailboxSubscriptions
				const subscribed = yield* subscriptions.isSubscribed({ mailboxKey: deliveryMailboxKey(admission) })
				return { failure, retry, result, subscribed }
			}).pipe(Effect.provide(services))

			expect(outcome.failure).toEqual(
				ProviderEventExecutionFailed.make({ provider: 'linear', retryable: true, safeCode: 'callback_failed' }),
			)
			expect(Option.map(outcome.retry.execution.prepared, (prepared) => prepared.callback)).toEqual(
				Option.some('onIssueCreated'),
			)
			expect(outcome.result).toEqual(ProviderEventHandled.make({}))
			expect(outcome.subscribed).toBe(true)
			expect(yield* Ref.get(calls)).toEqual(['created', 'created'])
			expect(yield* Ref.get(first.preparations)).toHaveLength(1)
			expect(yield* Ref.get(outcome.retry.preparations)).toEqual([])
		}),
	)

	it.effect('fails a retry whose saved callback is unknown, unconfigured, or unbuildable', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const prepared = (callback: string) =>
				withPrepared(
					test.execution,
					PreparedDeliveryInvocation.make({
						callback,
						presentationVersion: 1,
						destination: { _tag: 'LinearIssueDestination', organizationId, issueId },
						supportedOperations: [],
					}),
				)
			const run = (callback: string) =>
				processor
					.process([linearIssueCreateAdmission(namespace)], prepared(callback))
					.pipe(
						Effect.provide(
							layer({ onIssueCreated: () => Effect.die('must not run'), onMentioned: () => Effect.void }),
						),
						Effect.flip,
					)
			const failed = (safeCode: string) =>
				ProviderEventExecutionFailed.make({ provider: 'linear', retryable: false, safeCode })
			expect(yield* run('onNewMention')).toEqual(failed('prepared_callback_missing'))
			expect(yield* run('onAssigned')).toEqual(failed('prepared_callback_missing'))
			expect(yield* run('onMentioned')).toEqual(failed('prepared_callback_unbuildable'))
		}),
	)

	it.effect('maps preparation failures before any thought or callback runs', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const admission = yield* linearAgentSessionAdmission(agentSessionPayloads[0])
			const run = (error: DeliveryPreparationConflict | DeliveryPreparationUnavailable) =>
				processor
					.process([admission], failingExecution(test.execution, error))
					.pipe(
						Effect.provide(
							layer({ onAgentSessionCreated: () => Effect.die('must not run') }, () =>
								Effect.die('must not create a thought'),
							),
						),
						Effect.flip,
					)
			expect(yield* run(DeliveryPreparationUnavailable.make({ reason: 'store offline' }))).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'linear',
					retryable: true,
					safeCode: 'delivery_prepare_unavailable',
				}),
			)
			expect(yield* run(DeliveryPreparationConflict.make({ deliveryId: test.execution.deliveryId }))).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'linear',
					retryable: false,
					safeCode: 'delivery_prepare_conflict',
				}),
			)
		}),
	)
})

describe('Linear stop prompts', () => {
	it.effect('admits a stop prompt as an interrupt and passes its signal to the callback', ({ expect }) =>
		Effect.gen(function* () {
			const stop = yield* admittedEvent(stopPromptPayload, 'delivery-stop')
			const normal = yield* admittedEvent(agentSessionPayloads[1], 'delivery-prompted')
			expect(stop.interrupt).toBe(true)
			expect(normal).not.toHaveProperty('interrupt')

			const signals = yield* Ref.make<ReadonlyArray<string | null>>([])
			const services = layer({
				onAgentSessionPrompted: (event) => Ref.update(signals, (values) => [...values, event.prompt.signal]),
			})
			for (const admission of [stop, normal]) {
				const test = yield* makeTestDeliveryExecution()
				yield* processor.process([admission], test.execution).pipe(Effect.provide(services))
			}
			expect(yield* Ref.get(signals)).toEqual(['stop', null])
		}),
	)

	it.effect('does not mark a created session event as an interrupt', ({ expect }) =>
		Effect.gen(function* () {
			const created = yield* admittedEvent(agentSessionPayloads[0], 'delivery-created')
			expect(created).not.toHaveProperty('interrupt')
		}),
	)
})
