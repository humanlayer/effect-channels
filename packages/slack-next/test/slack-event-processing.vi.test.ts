import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	processProviderEvent,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Effect, Match, Option, Redacted, Ref, Schema } from 'effect'

import { makeSlackEventProcessor } from '../src/SlackEventProcessor'
import { SlackReactionThreadResolver } from '../src/SlackReactionThreadResolver'
import type { SlackAppMentionEvent } from '../src/SlackWebhookEventSchemas'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import { signedSlackInput } from './fixtures'

const signingSecret = 'processing-test-secret'
const payload = {
	type: 'event_callback',
	team_id: 'T_TEST',
	event_id: 'Ev_MENTION',
	event_time: 1_700_000_000,
	event: {
		type: 'app_mention',
		user: 'U_TEST',
		text: '<@U_BOT> hello',
		ts: '1700000001.000001',
		channel: 'C_TEST',
	},
}

const admitStoredMention = Effect.gen(function* () {
	const outcome = yield* makeSlackWebhookProvider({
		namespace: 'mention-test',
		signingSecret: Redacted.make(signingSecret),
	})
		.handle(signedSlackInput(signingSecret, payload))
		.pipe(
			Effect.provideService(SlackReactionThreadResolver, {
				resolve: () => Effect.die(new Error('App mentions must not resolve reaction threads')),
			}),
			Effect.provide(NodeCrypto.layer),
		)

	const admission = yield* Match.value(outcome).pipe(
		Match.tagsExhaustive({
			Event: ({ event }) => Effect.succeed(event),
			Ignored: () => Effect.die(new Error('Expected Slack app mention to be admitted, but it was ignored')),
			Response: () => Effect.die(new Error('Expected Slack app mention to be admitted, but received a response')),
		}),
	)
	const codec = Schema.fromJsonString(DeliveryAdmission)
	const encoded = yield* Schema.encodeEffect(codec)(admission)
	return yield* Schema.decodeEffect(codec)(encoded)
})

const storeAdmission = (eventId: string, eventPayload: DeliveryAdmission['payload']) =>
	Effect.gen(function* () {
		const codec = Schema.fromJsonString(DeliveryAdmission)
		const encoded = yield* Schema.encodeEffect(codec)(
			DeliveryAdmission.make({
				namespace: 'mention-test',
				provider: 'slack',
				installationId: 'T_TEST',
				resourceId: 'slack:v1:T_TEST:C_TEST:1700000000.000001',
				eventId,
				payload: eventPayload,
			}),
		)
		return yield* Schema.decodeEffect(codec)(encoded)
	})

describe('Slack event processing', () => {
	it.effect('decodes a stored app mention and runs onNewMention', ({ expect }) =>
		Effect.gen(function* () {
			const storedAdmission = yield* admitStoredMention
			const received = yield* Ref.make<Option.Option<SlackAppMentionEvent>>(Option.none())
			const processor = makeSlackEventProcessor({
				namespace: 'mention-test',
				handlers: {
					onNewMention: (event) => Ref.set(received, Option.some(event)),
				},
			})

			const result = yield* processProviderEvent([processor])(storedAdmission)

			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(received)).toEqual(Option.some(payload.event))
		}),
	)

	it.effect('ignores an app mention when onNewMention is not configured', ({ expect }) =>
		Effect.gen(function* () {
			const storedAdmission = yield* admitStoredMention
			const processor = makeSlackEventProcessor<never, never>({
				namespace: 'mention-test',
				handlers: {},
			})

			const result = yield* processProviderEvent([processor])(storedAdmission)

			expect(result).toEqual(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
		}),
	)

	it.effect('translates an onNewMention failure into a provider execution failure', ({ expect }) =>
		Effect.gen(function* () {
			const storedAdmission = yield* admitStoredMention
			const processor = makeSlackEventProcessor({
				namespace: 'mention-test',
				handlers: {
					onNewMention: () => Effect.fail('callback failed'),
				},
			})

			const error = yield* processProviderEvent([processor])(storedAdmission).pipe(Effect.flip)

			expect(error).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'slack',
					retryable: true,
					safeCode: 'callback_failed',
				}),
			)
		}),
	)

	it.effect('rejects an invalid stored Slack payload', ({ expect }) =>
		Effect.gen(function* () {
			const storedAdmission = yield* admitStoredMention
			const invalidAdmission = DeliveryAdmission.make({
				...storedAdmission,
				payload: { type: 'event_callback' },
			})
			const processor = makeSlackEventProcessor<never, never>({
				namespace: 'mention-test',
				handlers: {},
			})

			const error = yield* processProviderEvent([processor])(invalidAdmission).pipe(Effect.flip)

			expect(error).toEqual(
				ProviderEventInvalid.make({
					provider: 'slack',
					reason: 'invalid_payload',
				}),
			)
		}),
	)

	it.effect('routes messages, lifecycle events, reactions, and stopped events', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<string>>([])
			const record = (name: string) => Ref.update(calls, (current) => [...current, name])
			const processor = makeSlackEventProcessor({
				namespace: 'mention-test',
				handlers: {
					onSubscribedMessage: () => record('subscribed'),
					onDirectMessage: () => record('direct'),
					onMessageUpdated: () => record('updated'),
					onMessageDeleted: () => record('deleted'),
					onReaction: (event) => record(event.type),
					onConversationStopped: () => record('stopped'),
				},
			})
			const envelope = (eventId: string, event: DeliveryAdmission['payload']) => ({
				type: 'event_callback',
				team_id: 'T_TEST',
				event_id: eventId,
				event_time: 1_700_000_000,
				event,
			})
			const messageFields = {
				type: 'message',
				user: 'U_TEST',
				text: 'hello',
				ts: '1700000001.000001',
				channel: 'C_TEST',
			}
			const events = [
				envelope('Ev_CHANNEL', { ...messageFields, channel_type: 'channel' }),
				envelope('Ev_DM', { ...messageFields, channel: 'D_TEST', channel_type: 'im' }),
				envelope('Ev_UPDATED', {
					...messageFields,
					subtype: 'message_changed',
					message: { ...messageFields, thread_ts: '1700000000.000001' },
				}),
				envelope('Ev_DELETED', {
					...messageFields,
					subtype: 'message_deleted',
					deleted_ts: '1700000001.000001',
					previous_message: { ...messageFields, thread_ts: '1700000000.000001' },
				}),
				envelope('Ev_REACTION_ADDED', {
					type: 'reaction_added',
					user: 'U_TEST',
					reaction: 'thumbsup',
					item: { type: 'message', channel: 'C_TEST', ts: '1700000001.000001' },
					event_ts: '1700000002.000001',
				}),
				envelope('Ev_REACTION_REMOVED', {
					type: 'reaction_removed',
					user: 'U_TEST',
					reaction: 'thumbsup',
					item: { type: 'message', channel: 'C_TEST', ts: '1700000001.000001' },
					event_ts: '1700000003.000001',
				}),
				envelope('Ev_STOPPED', {
					type: 'agent_session_stopped',
					channel: 'C_TEST',
					thread_ts: '1700000000.000001',
					user: 'U_TEST',
					event_ts: '1700000004.000001',
					streaming_message_ts: [],
				}),
			]

			for (const event of events) {
				const admission = yield* storeAdmission(event.event_id, event)
				expect(yield* processProviderEvent([processor])(admission)).toEqual(ProviderEventHandled.make({}))
			}

			expect(yield* Ref.get(calls)).toEqual([
				'subscribed',
				'direct',
				'updated',
				'deleted',
				'reaction_added',
				'reaction_removed',
				'stopped',
			])
		}),
	)
})
