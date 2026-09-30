import { describe, it } from '@effect/vitest'
import {
	MailboxSubscriptions,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Ref } from 'effect'

import { makeTestDeliveryExecution } from '../../delivery-next/test/delivery-execution'
import { LinearApi } from '../src/LinearApi'
import { LinearCallbacks, type LinearCallbackHandlers } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import { LinearAgentActivityId } from '../src/LinearIdentity'
import { LinearActivityContent, LinearAgentActivityReceipt } from '../src/LinearModels'
import type { LinearCreateAgentActivityRequest } from '../src/LinearModels'
import {
	firstAttempt,
	agentSessionPayloads,
	appUserNotificationPayloads,
	linearAgentSessionAdmission,
	linearAppUserId,
	linearNotificationAdmission,
	linearOauthClientId,
	linearOrganizationId,
} from './fixtures'

const namespace = 'linear-processing-test'
const processor = makeLinearEventProcessor({
	namespace,
	bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
	oauthClientId: linearOauthClientId,
})

const makeLayer = <E>(
	handlers: LinearCallbackHandlers<E, never>,
	createAgentActivity: (request: LinearCreateAgentActivityRequest) => Effect.Effect<LinearAgentActivityReceipt>,
) =>
	Layer.mergeAll(
		LinearCallbacks.layer(handlers),
		Layer.mock(LinearApi, { createAgentActivity }),
		Layer.mock(MailboxSubscriptions, {
			isSubscribed: () => Effect.succeed(false),
			subscribe: () => Effect.die('not used'),
			unsubscribe: () => Effect.die('not used'),
		}),
	)

describe('Linear Agent Session processing', () => {
	it.effect('emits one ephemeral thought before the created callback', ({ expect }) =>
		Effect.gen(function* () {
			const order = yield* Ref.make<ReadonlyArray<string>>([])
			const requests = yield* Ref.make<ReadonlyArray<LinearCreateAgentActivityRequest>>([])
			const layer = makeLayer(
				{
					onAgentSessionCreated: (event) =>
						Ref.update(order, (values) => [...values, 'callback']).pipe(
							Effect.tap(() =>
								Effect.sync(() => {
									expect(event.session.mailboxKey).toContain(
										'linear:v1:agent-session:71000000-0000-4000-8000-000000000001',
									)
									expect(event.issue.mailboxKey).toContain(
										'linear:v1:issue:b33fb278-fbe0-45e4-b4eb-94b0839f51b9',
									)
									expect(event.previousComments).toHaveLength(1)
									expect(event.guidance).toHaveLength(1)
								}),
							),
						),
				},
				(request) =>
					Ref.update(requests, (values) => [...values, request]).pipe(
						Effect.andThen(Ref.update(order, (values) => [...values, 'thought'])),
						Effect.as(
							LinearAgentActivityReceipt.make({
								activityId: LinearAgentActivityId.make('generated-by-provider'),
								sessionId: request.sessionId,
							}),
						),
					),
			)
			const result = yield* processor
				.process([yield* linearAgentSessionAdmission(agentSessionPayloads[0])], yield* firstAttempt())
				.pipe(Effect.provide(layer))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(order)).toEqual(['thought', 'callback'])
			const captured = yield* Ref.get(requests)
			expect(captured).toHaveLength(1)
			expect(captured[0]).toMatchObject({ ephemeral: true })
			expect(captured[0]?.content).toEqual(LinearActivityContent.cases.Thought.make({ body: 'Working on this…' }))
			expect(captured[0]?.deliveryId).toBe('delivery-created')
		}),
	)

	it.effect('can retry a callback after a successful thought without reusing a caller activity ID', ({ expect }) =>
		Effect.gen(function* () {
			const attempts = yield* Ref.make(0)
			const requests = yield* Ref.make<ReadonlyArray<LinearCreateAgentActivityRequest>>([])
			const layer = makeLayer(
				{
					onAgentSessionCreated: () =>
						Ref.updateAndGet(attempts, (value) => value + 1).pipe(
							Effect.flatMap((attempt) =>
								attempt === 1 ? Effect.fail({ retryable: true }) : Effect.void,
							),
						),
				},
				(request) =>
					Ref.update(requests, (values) => [...values, request]).pipe(
						Effect.as(
							LinearAgentActivityReceipt.make({
								activityId: LinearAgentActivityId.make('generated-by-provider'),
								sessionId: request.sessionId,
							}),
						),
					),
			)
			const admission = yield* linearAgentSessionAdmission(agentSessionPayloads[0])
			const first = yield* makeTestDeliveryExecution()
			yield* processor.process([admission], first.execution).pipe(Effect.provide(layer), Effect.flip)
			const retry = yield* first.retry
			const result = yield* processor.process([admission], retry.execution).pipe(Effect.provide(layer))
			expect(result).toEqual(ProviderEventHandled.make({}))
			const captured = yield* Ref.get(requests)
			expect(captured).toHaveLength(2)
			for (const capturedRequest of captured) expect(capturedRequest).not.toHaveProperty('activityId')
		}),
	)

	it.effect('routes prompted events without an automatic thought', ({ expect }) =>
		Effect.gen(function* () {
			const prompts = yield* Ref.make<ReadonlyArray<string>>([])
			const layer = makeLayer(
				{
					onAgentSessionPrompted: (event) => Ref.update(prompts, (values) => [...values, event.prompt.body]),
				},
				() => Effect.die('prompted must not create an automatic thought'),
			)
			const result = yield* processor
				.process([yield* linearAgentSessionAdmission(agentSessionPayloads[1])], yield* firstAttempt())
				.pipe(Effect.provide(layer))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(prompts)).toEqual(['Please include a regression test.'])
		}),
	)

	it.effect('treats notification entry signals as supplemental when session callbacks are enabled', ({ expect }) =>
		Effect.gen(function* () {
			const layer = makeLayer(
				{ onAgentSessionCreated: () => Effect.void, onMentioned: () => Effect.die('must not run') },
				() => Effect.die('must not create an activity'),
			)
			const result = yield* processor
				.process([linearNotificationAdmission(appUserNotificationPayloads[0])], yield* firstAttempt())
				.pipe(Effect.provide(layer))
			expect(result).toEqual(ProviderEventIgnored.make({ reason: 'supplemental_signal' }))
		}),
	)

	it.effect('rejects a stored session identity mismatch before activity or callback work', ({ expect }) =>
		Effect.gen(function* () {
			const payload = structuredClone(agentSessionPayloads[0])
			payload.agentSession.organizationId = 'different-organization'
			const layer = makeLayer({ onAgentSessionCreated: () => Effect.die('must not run') }, () =>
				Effect.die('must not create an activity'),
			)
			const result = yield* processor
				.process([yield* linearAgentSessionAdmission(payload)], yield* firstAttempt())
				.pipe(Effect.provide(layer), Effect.flip)
			expect(result).toEqual(ProviderEventInvalid.make({ provider: 'linear', reason: 'identity_mismatch' }))
		}),
	)
})
