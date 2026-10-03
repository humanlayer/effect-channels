import { describe, it } from '@effect/vitest'
import { DeliveryHandoff, type DeliveryContext } from '@humanlayer/channels-delivery'
import { Cause, Context, Effect, Exit, Layer, Ref, Schema } from 'effect'

import { makeTestDeliveryExecution } from '../../delivery/test/delivery-execution'
import {
	SlackCallbackError,
	SlackCallbacks,
	type SlackCallbackHandlers,
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

const buildCallbacks = <E, R>(handlers: SlackCallbackHandlers<E, R>) =>
	SlackCallbacks.pipe(Effect.provide(SlackCallbacks.layer(handlers)))

/** Run a wrapped callback with a fresh test delivery. */
const withDelivery =
	<A, B, E>(callback: ((event: A, delivery: DeliveryContext) => Effect.Effect<B, E>) | undefined, name: string) =>
	(event: A) =>
		callback === undefined
			? Effect.die(new Error(`Expected ${name} callback`))
			: Effect.flatMap(makeTestDeliveryExecution(), ({ execution }) => callback(event, execution.context))

const requireNewMention = (callbacks: SlackCallbacks['Service']) => withDelivery(callbacks.onNewMention, 'onNewMention')

const requireSubscribed = (callbacks: SlackCallbacks['Service']) =>
	withDelivery(callbacks.onSubscribedThreadEvents, 'onSubscribedThreadEvents')

describe('SlackCallbacks', () => {
	it.effect('captures the application context when its layer is built', ({ expect }) =>
		Effect.gen(function* () {
			let captured: string | undefined
			const callbacks = yield* buildCallbacks({
				onNewMention: () =>
					Effect.flatMap(CallbackDependency, ({ value }) =>
						Effect.sync(() => {
							captured = value
						}),
					),
			}).pipe(Effect.provide(dependencyLayer))

			yield* requireNewMention(callbacks)(newMention)
			expect(captured).toBe('captured')
		}),
	)

	it.effect('turns typed failures into retryable callback errors', ({ expect }) =>
		Effect.gen(function* () {
			const callbacks = yield* buildCallbacks({
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
			const metadataCallbacks = yield* buildCallbacks({
				onNewMention: () => Effect.fail({ retryability: 'non_retryable' as const }),
			})
			const metadataError = yield* requireNewMention(metadataCallbacks)(newMention).pipe(Effect.flip)
			expect(metadataError).toEqual(
				SlackCallbackError.make({ callback: 'onNewMention', reason: 'failed', retryable: false }),
			)

			const flagCallbacks = yield* buildCallbacks({
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
			const callbacks = yield* buildCallbacks({
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
			const callbacks = yield* buildCallbacks({ onNewMention: () => Effect.interrupt })
			const exit = yield* requireNewMention(callbacks)(newMention).pipe(Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)

	it.effect('passes the delivery context through and returns a handoff unchanged', ({ expect }) =>
		Effect.gen(function* () {
			const { execution, handoffs } = yield* makeTestDeliveryExecution()
			const callbacks = yield* buildCallbacks({
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
