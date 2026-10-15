import { describe, it } from '@effect/vitest'
import { DeliveryHandoff, type DeliveryContext } from '@humanlayer/channels-delivery'
import { Cause, Context, Effect, Exit, Layer, Ref, Schema } from 'effect'

import { makeTestDeliveryExecution } from '../../delivery/test/delivery-execution'
import {
	SlackCallbackError,
	type SlackCallbackOperations,
	SlackChannelId,
	SlackEventId,
	SlackMessage,
	SlackMessageReceived,
	SlackMessageRef,
	SlackMessageTs,
	SlackNewMention,
	SlackParticipant,
	SlackPlainTextContent,
	SlackSubscribedThreadEvents,
	SlackTeamId,
	SlackThread,
	SlackThreadRef,
	SlackUserId,
	makeSlackCallbacks,
} from '../src'

class CallbackDependency extends Context.Service<CallbackDependency, { readonly value: string }>()(
	'@humanlayer/channels-slack/test/CallbackDependency',
) {}

class ApplicationFailure extends Schema.TaggedError<ApplicationFailure>()('ApplicationFailure', {}) {}

const dependencyLayer = Layer.succeed(CallbackDependency, CallbackDependency.of({ value: 'captured' }))
const teamId = SlackTeamId.make('T_CALLBACKS')
const channelId = SlackChannelId.make('C_CALLBACKS')
const threadTs = SlackMessageTs.make('1700000000.000001')
const threadRef = SlackThreadRef.make({ teamId, channelId, threadTs, isDm: false })
const thread = SlackThread.make({ ref: threadRef, mailboxKey: 'mailbox:callbacks' })
const trigger = SlackMessage.make({
	ref: SlackMessageRef.make({ teamId, channelId, messageTs: threadTs }),
	thread: threadRef,
	author: SlackParticipant.make({
		userId: SlackUserId.make('U_ALICE'),
		userName: 'alice',
		fullName: 'Alice Example',
		isBot: false,
		isMe: false,
	}),
	content: SlackPlainTextContent.make({ text: 'hello' }),
	files: [],
	metadata: {},
})
const newMention = SlackNewMention.make({ thread, trigger, events: [] })
const subscribedThreadEvents = SlackSubscribedThreadEvents.make({
	thread,
	events: [SlackMessageReceived.make({ eventId: SlackEventId.make('Ev_CALLBACKS'), message: trigger })],
})

/** Run a wrapped callback with a fresh test delivery. */
const withDelivery =
	<A, B, E, R>(
		callback: ((event: A, delivery: DeliveryContext) => Effect.Effect<B, E, R>) | undefined,
		name: string,
	) =>
	(event: A) =>
		callback === undefined
			? Effect.die(new Error(`Expected ${name} callback`))
			: Effect.flatMap(makeTestDeliveryExecution(), ({ execution }) => callback(event, execution.context))

const requireNewMention = <R>(callbacks: SlackCallbackOperations<R>) =>
	withDelivery(callbacks.onNewMention, 'onNewMention')

const requireSubscribed = <R>(callbacks: SlackCallbackOperations<R>) =>
	withDelivery(callbacks.onSubscribedThreadEvents, 'onSubscribedThreadEvents')

describe('makeSlackCallbacks', () => {
	it.effect('reads the application services where the callback runs', ({ expect }) =>
		Effect.gen(function* () {
			let captured: string | undefined
			const callbacks = makeSlackCallbacks({
				onNewMention: () =>
					Effect.flatMap(CallbackDependency, ({ value }) =>
						Effect.sync(() => {
							captured = value
						}),
					),
			})

			yield* requireNewMention(callbacks)(newMention).pipe(Effect.provide(dependencyLayer))
			expect(captured).toBe('captured')
		}),
	)

	it.effect('turns typed failures into retryable callback errors', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = makeSlackCallbacks({
				onNewMention: () => Effect.fail(ApplicationFailure.make({})),
			})
			const error = yield* requireNewMention(callbacks)(newMention).pipe(Effect.flip)
			expect(error).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'failed', retryable: true }),
			)
		}),
	)

	it.effect('honors retryability metadata and boolean flags', ({ expect }) =>
		Effect.gen(function* () {
			const metadataCallbacks = makeSlackCallbacks({
				onNewMention: () => Effect.fail({ retryability: 'non_retryable' as const }),
			})
			const metadataError = yield* requireNewMention(metadataCallbacks)(newMention).pipe(Effect.flip)
			expect(metadataError).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'failed', retryable: false }),
			)

			const flagCallbacks = makeSlackCallbacks({
				onSubscribedThreadEvents: () => Effect.fail({ retryable: false }),
			})
			const flagError = yield* requireSubscribed(flagCallbacks)(subscribedThreadEvents).pipe(Effect.flip)
			expect(flagError).toEqual(
				SlackCallbackError.make({ callback: 'onSubscribedThreadEvents', reason: 'failed', retryable: false }),
			)
		}),
	)

	it.effect('classifies defects as unexpected retryable callback errors', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = makeSlackCallbacks({
				onNewMention: () => Effect.die(new Error('callback defect')),
			})
			const error = yield* requireNewMention(callbacks)(newMention).pipe(Effect.flip)
			expect(error).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'unexpected', retryable: true }),
			)
		}),
	)

	it.effect('preserves interruption instead of converting it to a callback error', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = makeSlackCallbacks({ onNewMention: () => Effect.interrupt })
			const exit = yield* requireNewMention(callbacks)(newMention).pipe(Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)

	it.effect('passes the delivery context through and returns a handoff unchanged', ({ expect }) =>
		Effect.gen(function* () {
			const { execution, handoffs } = yield* makeTestDeliveryExecution()
			const callbacks = makeSlackCallbacks({
				onNewMention: (_event, delivery) => delivery.handoff(),
			})
			const onNewMention =
				callbacks.onNewMention ?? (() => Effect.die(new Error('Expected onNewMention callback')))
			const result = yield* onNewMention(newMention, execution.context)
			expect(result).toEqual(DeliveryHandoff.make({ deliveryId: execution.deliveryId }))
			expect(yield* Ref.get(handoffs)).toHaveLength(1)
		}),
	)
})
