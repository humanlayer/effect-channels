import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	processProviderEvent,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer } from 'effect'
import { vi } from 'vitest'

import { SlackApi } from '../src/SlackApi'
import {
	SlackMessageReceived,
	SlackReactionAdded,
	type SlackNewMention,
	type SlackSubscribedThreadEvents,
} from '../src/SlackCallbackEvents'
import { makeSlackEventProcessor } from '../src/SlackEventProcessor'
import { SlackChannelId, SlackMessageTs, SlackTeamId, slackThreadResourceId } from '../src/SlackIdentity'
import { SlackMarkdownContent, SlackMessage, SlackParticipant, SlackUserId } from '../src/SlackModels'
import { SlackSubscriptions } from '../src/SlackSubscriptions'
import {
	SlackAgentSessionStoppedEnvelope,
	SlackAppMentionEnvelope,
	SlackMessageDeletedEnvelope,
	SlackMessageEnvelope,
	SlackMessageUpdatedEnvelope,
	SlackReactionAddedEnvelope,
	SlackReactionRemovedEnvelope,
} from '../src/SlackWebhookSchemas'
import { makeInMemoryMailboxFixture } from './fixtures'

const teamId = SlackTeamId.make('T_TEST')
const channelId = SlackChannelId.make('C_TEST')
const rootTs = SlackMessageTs.make('1700000000.000001')
const resourceId = slackThreadResourceId({ teamId, channelId, threadTs: rootTs })

const alice = SlackParticipant.make({
	userId: SlackUserId.make('U_ALICE'),
	userName: 'alice',
	fullName: 'Alice Example',
	isBot: false,
	isMe: false,
})
const agent = SlackParticipant.make({
	userId: SlackUserId.make('U_AGENT'),
	userName: 'agent',
	fullName: 'Agent Bot',
	isBot: true,
	isMe: true,
})

const admission = (eventId: string, payload: DeliveryAdmission['payload'], resource = resourceId) =>
	DeliveryAdmission.make({
		namespace: 'mention-test',
		provider: 'slack',
		installationId: teamId,
		resourceId: resource,
		eventId,
		payload,
	})

const mention = (eventId = 'Ev_MENTION') =>
	SlackAppMentionEnvelope.make({
		type: 'event_callback',
		team_id: teamId,
		event_id: eventId,
		event_time: 1_700_000_000,
		event: {
			type: 'app_mention',
			user: alice.userId,
			text: '<@U_AGENT> hello',
			ts: rootTs,
			channel: channelId,
		},
	})

const reply = (eventId = 'Ev_REPLY', user = alice.userId) =>
	SlackMessageEnvelope.make({
		type: 'event_callback',
		team_id: teamId,
		event_id: eventId,
		event_time: 1_700_000_001,
		event: {
			type: 'message',
			user,
			text: 'thread reply',
			ts: SlackMessageTs.make('1700000001.000001'),
			thread_ts: rootTs,
			channel: channelId,
			channel_type: 'channel',
		},
	})

const reaction = (eventId = 'Ev_REACTION') =>
	SlackReactionAddedEnvelope.make({
		type: 'event_callback',
		team_id: teamId,
		event_id: eventId,
		event_time: 1_700_000_002,
		event: {
			type: 'reaction_added',
			user: alice.userId,
			reaction: 'eyes',
			item: { type: 'message', channel: channelId, ts: SlackMessageTs.make('1700000001.000001') },
			event_ts: SlackMessageTs.make('1700000002.000001'),
		},
	})

const apiLayer = Layer.mock(SlackApi, {
	resolveParticipant: (request) => Effect.succeed(request.userId === agent.userId ? agent : alice),
	getMessage: (request) =>
		Effect.succeed(
			SlackMessage.make({
				ref: request.message,
				thread: request.thread,
				author: alice,
				content: SlackMarkdownContent.make({ markdown: 'thread reply' }),
				metadata: { source: 'test-provider' },
			}),
		),
})

const runtimeLayer = Layer.merge(apiLayer, SlackSubscriptions.layerMemory)
const subscribedRuntimeLayer = Layer.merge(
	apiLayer,
	Layer.mock(SlackSubscriptions, { isSubscribed: () => Effect.succeed(true) }),
)

const process = <E, R>(
	handlers: Parameters<typeof makeSlackEventProcessor<E, R>>[0]['handlers'],
	batch: DeliveryAdmissionBatch,
	layer = runtimeLayer,
) =>
	processProviderEvent([makeSlackEventProcessor({ namespace: 'mention-test', handlers })])(batch).pipe(
		Effect.provide(layer),
	)

describe('Slack event batch processing', () => {
	it.effect('delivers mention plus later message and reaction in one onNewMention call', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn((_event: SlackNewMention) => Effect.void)
			const admissions = DeliveryAdmissionBatch.make([
				admission('Ev_MENTION', mention()),
				admission('Ev_REPLY', reply()),
				admission('Ev_REACTION', reaction()),
			])
			const processor = makeSlackEventProcessor({ namespace: 'mention-test', handlers: { onNewMention } })
			const mailbox = yield* makeInMemoryMailboxFixture([processor])
			for (const item of admissions) yield* mailbox.mailboxDelivery.deliver(item)
			const mailboxKey = (yield* mailbox.mailboxKeys)[0]
			if (mailboxKey === undefined) return yield* Effect.die(new Error('Expected one in-memory mailbox'))

			expect(yield* mailbox.processBatch(mailboxKey).pipe(Effect.provide(runtimeLayer))).toEqual(
				ProviderEventHandled.make({}),
			)
			expect(onNewMention).toHaveBeenCalledOnce()
			const callback = onNewMention.mock.calls[0]?.[0]
			expect(callback?.trigger.ref.messageTs).toBe(rootTs)
			expect(callback?.events.map((event) => event._tag)).toEqual(['SlackMessageReceived', 'SlackReactionAdded'])
			expect(callback?.events[0]).toBeInstanceOf(SlackMessageReceived)
			expect(callback?.events[1]).toBeInstanceOf(SlackReactionAdded)
		}),
	)

	it.effect('delivers every relevant event in one subscribed callback', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn((_event: SlackNewMention) => Effect.void)
			const onSubscribedThreadEvents = vi.fn((_event: SlackSubscribedThreadEvents) => Effect.void)
			const batch = DeliveryAdmissionBatch.make([
				admission('Ev_MENTION', mention()),
				admission('Ev_REPLY', reply()),
				admission('Ev_REACTION', reaction()),
			])

			expect(yield* process({ onNewMention, onSubscribedThreadEvents }, batch, subscribedRuntimeLayer)).toEqual(
				ProviderEventHandled.make({}),
			)
			expect(onNewMention).not.toHaveBeenCalled()
			expect(onSubscribedThreadEvents).toHaveBeenCalledOnce()
			expect(onSubscribedThreadEvents.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual([
				'SlackMessageReceived',
				'SlackMessageReceived',
				'SlackReactionAdded',
			])
		}),
	)

	it.effect('normalizes lifecycle variants into the subscribed event union in order', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedThreadEvents = vi.fn((_event: SlackSubscribedThreadEvents) => Effect.void)
			const snapshot = {
				user: alice.userId,
				text: 'edited',
				ts: SlackMessageTs.make('1700000001.000001'),
				thread_ts: rootTs,
			}
			const updated = SlackMessageUpdatedEnvelope.make({
				type: 'event_callback',
				team_id: teamId,
				event_id: 'Ev_UPDATED',
				event_time: 1_700_000_003,
				event: {
					type: 'message',
					subtype: 'message_changed',
					user: alice.userId,
					text: 'edited',
					ts: snapshot.ts,
					thread_ts: rootTs,
					channel: channelId,
					message: snapshot,
					previous_message: { ...snapshot, text: 'before' },
				},
			})
			const deleted = SlackMessageDeletedEnvelope.make({
				type: 'event_callback',
				team_id: teamId,
				event_id: 'Ev_DELETED',
				event_time: 1_700_000_004,
				event: {
					type: 'message',
					subtype: 'message_deleted',
					user: alice.userId,
					text: '',
					ts: snapshot.ts,
					thread_ts: rootTs,
					channel: channelId,
					deleted_ts: snapshot.ts,
					previous_message: snapshot,
				},
			})
			const removed = SlackReactionRemovedEnvelope.make({
				...reaction('Ev_REMOVED'),
				event_id: 'Ev_REMOVED',
				event: { ...reaction('Ev_REMOVED').event, type: 'reaction_removed' },
			})
			const stopped = SlackAgentSessionStoppedEnvelope.make({
				type: 'event_callback',
				team_id: teamId,
				event_id: 'Ev_STOPPED',
				event_time: 1_700_000_005,
				event: {
					type: 'agent_session_stopped',
					channel: channelId,
					thread_ts: rootTs,
					user: alice.userId,
					event_ts: SlackMessageTs.make('1700000005.000001'),
					streaming_message_ts: [snapshot.ts],
				},
			})
			const batch = DeliveryAdmissionBatch.make([
				admission('Ev_UPDATED', updated),
				admission('Ev_DELETED', deleted),
				admission('Ev_REMOVED', removed),
				admission('Ev_STOPPED', stopped),
			])

			yield* process({ onSubscribedThreadEvents }, batch, subscribedRuntimeLayer)
			expect(onSubscribedThreadEvents.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual([
				'SlackMessageUpdated',
				'SlackMessageDeleted',
				'SlackReactionRemoved',
				'SlackConversationStopped',
			])
		}),
	)

	it.effect('ignores relevant events before the first activation', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn((_event: SlackNewMention) => Effect.void)
			const batch = DeliveryAdmissionBatch.make([
				admission('Ev_BEFORE', reply('Ev_BEFORE')),
				admission('Ev_MENTION', mention()),
				admission('Ev_SELF', reply('Ev_SELF', agent.userId)),
				admission('Ev_AFTER', reply('Ev_AFTER')),
			])

			yield* process({ onNewMention }, batch)
			const callback = onNewMention.mock.calls[0]?.[0]
			expect(callback?.trigger.ref.messageTs).toBe(rootTs)
			expect(callback?.events).toHaveLength(1)
			expect(callback?.events[0]?._tag).toBe('SlackMessageReceived')
		}),
	)

	it.effect('activates each top-level direct message independently', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn((_event: SlackNewMention) => Effect.void)
			const dmChannel = SlackChannelId.make('D_DIRECT')
			const firstTs = SlackMessageTs.make('1700000100.000001')
			const secondTs = SlackMessageTs.make('1700000200.000001')
			const dm = (eventId: string, ts: typeof firstTs) =>
				SlackMessageEnvelope.make({
					type: 'event_callback',
					team_id: teamId,
					event_id: eventId,
					event_time: 1_700_000_100,
					event: {
						type: 'message',
						user: alice.userId,
						text: 'direct',
						ts,
						channel: dmChannel,
						channel_type: 'im',
					},
				})
			for (const [eventId, ts] of [
				['Ev_DM_ONE', firstTs],
				['Ev_DM_TWO', secondTs],
			] as const) {
				const resource = slackThreadResourceId({ teamId, channelId: dmChannel, threadTs: ts })
				yield* process(
					{ onNewMention },
					DeliveryAdmissionBatch.make([admission(eventId, dm(eventId, ts), resource)]),
				)
			}

			expect(onNewMention).toHaveBeenCalledTimes(2)
			expect(onNewMention.mock.calls.map(([event]) => event.thread.ref.threadTs)).toEqual([firstTs, secondTs])
		}),
	)

	it.effect('does not activate an unsubscribed DM thread reply', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn((_event: SlackNewMention) => Effect.void)
			const dmChannel = SlackChannelId.make('D_DIRECT')
			const dmResource = slackThreadResourceId({ teamId, channelId: dmChannel, threadTs: rootTs })
			const payload = SlackMessageEnvelope.make({
				...reply('Ev_DM_REPLY'),
				event: { ...reply('Ev_DM_REPLY').event, channel: dmChannel, channel_type: 'im' },
			})

			expect(
				yield* process(
					{ onNewMention },
					DeliveryAdmissionBatch.make([admission('Ev_DM_REPLY', payload, dmResource)]),
				),
			).toEqual(ProviderEventIgnored.make({ reason: 'no_activation_event' }))
			expect(onNewMention).not.toHaveBeenCalled()
		}),
	)

	it.effect('preserves callback failure retryability and rejects mixed resource identity', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* process<{ readonly retryability: 'non_retryable' }, never>(
				{ onNewMention: () => Effect.fail({ retryability: 'non_retryable' as const }) },
				DeliveryAdmissionBatch.make([admission('Ev_MENTION', mention())]),
			).pipe(Effect.flip)
			expect(failure).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'slack',
					retryable: false,
					safeCode: 'callback_failed',
				}),
			)

			const mismatch = DeliveryAdmissionBatch.make([
				admission('Ev_MENTION', mention()),
				DeliveryAdmission.make({
					...admission('Ev_REPLY', reply()),
					resourceId: `${resourceId}-other`,
				}),
			])
			expect(yield* process<never, never>({}, mismatch).pipe(Effect.flip)).toEqual(
				ProviderEventInvalid.make({ provider: 'slack', reason: 'identity_mismatch' }),
			)
		}),
	)
})
