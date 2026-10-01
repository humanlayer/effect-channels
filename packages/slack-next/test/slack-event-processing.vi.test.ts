import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	DeliveryAdmissionBatch,
	type DeliveryOperationKind,
	DeliveryPreparationConflict,
	DeliveryPreparationUnavailable,
	MailboxSubscriptions,
	PreparedDeliveryInvocation,
	ProviderDeliveryExecution,
	processProviderEvent,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	ProviderWebhookEvent,
	MailboxSubscriptionsMemory,
} from '@humanlayer/channels-delivery-next'
import { Cause, Effect, Exit, Layer, Option, Redacted, Ref, Schema } from 'effect'
import { vi } from 'vite-plus/test'

import { makeTestDeliveryExecution } from '../../delivery-next/test/delivery-execution'
import { SlackApi, SlackApiError } from '../src/SlackApi'
import {
	SlackConversationStopped,
	SlackMessageDeleted,
	SlackMessageReceived,
	SlackMessageUpdated,
	SlackReactionAdded,
	SlackReactionRemoved,
	type SlackNewMention,
	type SlackSubscribedThreadEvents,
} from '../src/SlackCallbackEvents'
import { SlackCallbacks, type SlackCallbackHandler, type SlackCallbackHandlers } from '../src/SlackCallbacks'
import { makeSlackEventProcessor } from '../src/SlackEventProcessor'
import { SlackChannelId, SlackMessageTs, SlackTeamId, slackThreadResourceId } from '../src/SlackIdentity'
import {
	slackFileFromMetadata,
	SlackMarkdownContent,
	SlackMessage,
	SlackParticipant,
	SlackUserId,
} from '../src/SlackModels'
import { makeSlackWebhookProvider } from '../src/SlackWebhookProvider'
import {
	SlackAgentSessionStoppedEnvelope,
	SlackAppMentionEnvelope,
	SlackMessageDeletedEnvelope,
	SlackMessageEnvelope,
	SlackMessageUpdatedEnvelope,
	SlackReactionAddedEnvelope,
	SlackReactionRemovedEnvelope,
} from '../src/SlackWebhookSchemas'
import { makeInMemoryMailboxFixture, signedSlackInput } from './fixtures'
import { slackFileMetadata, slackFileObject } from './slack-file-fixtures'

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

type MentionCallback = (event: SlackNewMention) => Effect.Effect<void>
type SubscribedCallback = (event: SlackSubscribedThreadEvents) => Effect.Effect<void>

const apiLayer = Layer.mock(SlackApi, {
	resolveParticipant: (request) => Effect.succeed(request.userId === agent.userId ? agent : alice),
	getMessage: (request) =>
		Effect.succeed(
			SlackMessage.make({
				ref: request.message,
				thread: request.thread,
				author: alice,
				content: SlackMarkdownContent.make({ markdown: 'thread reply' }),
				files: [],
				metadata: { source: 'test-provider' },
			}),
		),
})

const runtimeLayer = Layer.merge(apiLayer, MailboxSubscriptionsMemory)
const subscribedRuntimeLayer = Layer.merge(
	apiLayer,
	Layer.mock(MailboxSubscriptions, { isSubscribed: () => Effect.succeed(true) }),
)

const process = <E, R>(handlers: SlackCallbackHandlers<E, R>, batch: DeliveryAdmissionBatch, layer = runtimeLayer) =>
	Effect.flatMap(makeTestDeliveryExecution(), ({ execution }) =>
		processProviderEvent([makeSlackEventProcessor({ namespace: 'mention-test' })])(batch, execution),
	).pipe(Effect.provide(Layer.merge(SlackCallbacks.layer(handlers), layer)))

describe('Slack event batch processing', () => {
	it.effect('carries the author workspace from user_team, and leaves it out when Slack omits it', ({ expect }) =>
		Effect.gen(function* () {
			const received = yield* Ref.make(Option.none<SlackNewMention>())
			const otherTeam = SlackTeamId.make('T_OTHER')
			const connectMention = mention()
			const connectReply = reply('Ev_CONNECT_REPLY')
			const batch = DeliveryAdmissionBatch.make([
				admission('Ev_MENTION', {
					...connectMention,
					event: { ...connectMention.event, user_team: otherTeam },
				}),
				admission('Ev_REPLY', reply()),
				admission('Ev_CONNECT_REPLY', { ...connectReply, event: { ...connectReply.event, user_team: teamId } }),
			])

			expect(yield* process({ onNewMention: (event) => Ref.set(received, Option.some(event)) }, batch)).toEqual(
				ProviderEventHandled.make({}),
			)
			const event = Option.getOrThrow(yield* Ref.get(received))
			expect(event.trigger.authorTeamId).toBe('T_OTHER')
			expect(event.events.map((threadEvent) => threadEvent._tag)).toEqual([
				'SlackMessageReceived',
				'SlackMessageReceived',
			])
			const [plain, connect] = event.events.filter(Schema.is(SlackMessageReceived))
			expect(plain?.message).not.toHaveProperty('authorTeamId')
			expect(connect?.message.authorTeamId).toBe('T_TEST')
		}),
	)

	it.effect('delivers mention plus later message and reaction in one onNewMention call', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
			const admissions = DeliveryAdmissionBatch.make([
				admission('Ev_MENTION', mention()),
				admission('Ev_REPLY', reply()),
				admission('Ev_REACTION', reaction()),
			])
			const processor = makeSlackEventProcessor({ namespace: 'mention-test' })
			const mailbox = yield* makeInMemoryMailboxFixture([processor])
			for (const item of admissions) yield* mailbox.mailboxDelivery.deliver(item)
			const mailboxKey = (yield* mailbox.mailboxKeys)[0]
			if (mailboxKey === undefined) return yield* Effect.die(new Error('Expected one in-memory mailbox'))

			expect(
				yield* mailbox
					.processBatch(mailboxKey)
					.pipe(Effect.provide(Layer.merge(SlackCallbacks.layer({ onNewMention }), runtimeLayer))),
			).toEqual(ProviderEventHandled.make({}))
			expect(onNewMention).toHaveBeenCalledOnce()
			const callback = onNewMention.mock.calls[0]?.[0]
			expect(callback?.trigger.ref.messageTs).toBe(rootTs)
			expect(callback?.events).toHaveLength(2)
			expect(callback?.events[0]).toBeInstanceOf(SlackMessageReceived)
			expect(callback?.events[1]).toBeInstanceOf(SlackReactionAdded)
		}),
	)

	it.effect('delivers every relevant event in one subscribed callback', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
			const onSubscribedThreadEvents = vi.fn<SubscribedCallback>(() => Effect.void)
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
			const events = onSubscribedThreadEvents.mock.calls[0]?.[0].events
			expect(events).toHaveLength(3)
			expect(events?.[0]).toBeInstanceOf(SlackMessageReceived)
			expect(events?.[1]).toBeInstanceOf(SlackMessageReceived)
			expect(events?.[2]).toBeInstanceOf(SlackReactionAdded)
		}),
	)

	it.effect('normalizes lifecycle variants into the subscribed event union in order', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedThreadEvents = vi.fn<SubscribedCallback>(() => Effect.void)
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
			const events = onSubscribedThreadEvents.mock.calls[0]?.[0].events
			expect(events).toHaveLength(4)
			expect(events?.[0]).toBeInstanceOf(SlackMessageUpdated)
			expect(events?.[1]).toBeInstanceOf(SlackMessageDeleted)
			expect(events?.[2]).toBeInstanceOf(SlackReactionRemoved)
			expect(events?.[3]).toBeInstanceOf(SlackConversationStopped)
		}),
	)

	it.effect('ignores relevant events before the first activation', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
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
			expect(callback?.events[0]).toBeInstanceOf(SlackMessageReceived)
		}),
	)

	it.effect('activates each top-level direct message independently', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
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
			const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
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

	it.effect('fails an event at once when Slack refuses the bot token, and retries other Slack failures', ({ expect }) =>
		Effect.gen(function* () {
			const failingWith = (message: string) =>
				Layer.merge(
					Layer.mock(SlackApi, {
						resolveParticipant: () => Effect.fail(SlackApiError.make({ operation: 'resolve_participant', message })),
					}),
					MailboxSubscriptionsMemory,
				)
			const run = (message: string) =>
				process({ onNewMention: () => Effect.void }, mentionBatch(), failingWith(message)).pipe(Effect.flip)
			expect(yield* run('invalid_auth')).toEqual(
				ProviderEventExecutionFailed.make({ provider: 'slack', retryable: false, safeCode: 'slack_token_rejected' }),
			)
			expect(yield* run('Could not reach Slack')).toEqual(
				ProviderEventExecutionFailed.make({ provider: 'slack', retryable: true, safeCode: 'slack_api_failed' }),
			)
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

	it.effect('preserves callback interruption through event processing', ({ expect }) =>
		Effect.gen(function* () {
			const exit = yield* process(
				{ onNewMention: () => Effect.interrupt },
				DeliveryAdmissionBatch.make([admission('Ev_MENTION', mention())]),
			).pipe(Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)

	it.effect('exposes mention files as SlackFile values instead of raw metadata', ({ expect }) =>
		Effect.gen(function* () {
			const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
			const withFiles = SlackAppMentionEnvelope.make({
				...mention(),
				event: { ...mention().event, files: [slackFileMetadata] },
			})

			yield* process({ onNewMention }, DeliveryAdmissionBatch.make([admission('Ev_MENTION', withFiles)]))
			const trigger = onNewMention.mock.calls[0]?.[0].trigger
			expect(trigger?.files).toEqual([slackFileFromMetadata(teamId, slackFileMetadata)])
			expect(trigger?.metadata).toEqual({ eventId: 'Ev_MENTION', eventTime: 1_700_000_000 })
		}),
	)

	it.effect('admits and delivers a signed file_share follow-up in a subscribed thread', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedThreadEvents = vi.fn<SubscribedCallback>(() => Effect.void)
			const fileShareTs = '1700000003.000001'
			const payload = {
				type: 'event_callback',
				team_id: teamId,
				api_app_id: 'A0123APP',
				event_id: 'Ev_FILE_SHARE',
				event_time: 1_700_000_003,
				event: {
					type: 'message',
					subtype: 'file_share',
					text: 'here is the log',
					files: [slackFileObject],
					upload: false,
					user: alice.userId,
					display_as_bot: false,
					ts: fileShareTs,
					thread_ts: rootTs,
					client_msg_id: '8e7c2d1a-0000-4000-8000-000000000001',
					channel: channelId,
					event_ts: fileShareTs,
					channel_type: 'channel',
				},
			}
			const provider = makeSlackWebhookProvider({
				namespace: 'mention-test',
				signingSecret: Redacted.make('file-share-secret'),
			})
			const outcome = yield* provider
				.handle(yield* signedSlackInput('file-share-secret', payload))
				.pipe(Effect.provide(Layer.merge(NodeCrypto.layer, apiLayer)))
			if (!Schema.is(ProviderWebhookEvent)(outcome)) return yield* Effect.die('expected an admitted event')
			expect(outcome.event.resourceId).toBe(resourceId)

			expect(
				yield* process(
					{ onSubscribedThreadEvents },
					DeliveryAdmissionBatch.make([outcome.event]),
					subscribedRuntimeLayer,
				),
			).toEqual(ProviderEventHandled.make({}))
			const received = onSubscribedThreadEvents.mock.calls[0]?.[0].events[0]
			expect(received).toBeInstanceOf(SlackMessageReceived)
			expect(received).toMatchObject({
				message: {
					ref: { messageTs: fileShareTs },
					files: [slackFileFromMetadata(teamId, slackFileMetadata)],
					metadata: { eventId: 'Ev_FILE_SHARE', eventTime: 1_700_000_003 },
				},
			})
		}),
	)
})

const processor = makeSlackEventProcessor({ namespace: 'mention-test' })
const mentionBatch = () => DeliveryAdmissionBatch.make([admission('Ev_MENTION', mention())])

/** Callbacks and the processor share one in-memory subscription store, so a callback can subscribe the thread. */
const sharedRuntimeLayer = <E, R>(handlers: SlackCallbackHandlers<E, R>) =>
	Layer.merge(Layer.provideMerge(SlackCallbacks.layer(handlers), MailboxSubscriptionsMemory), apiLayer)

const threadSupportedOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'SetActivity',
	'RenderPlan',
	'AddExternalLink',
]

describe('Slack delivery preparation', () => {
	it.effect('prepares the callback, thread, and trigger message once before the callback runs', ({ expect }) => {
		const preparedBeforeCallback: Array<number> = []
		const deliveryIds: Array<string> = []
		return Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const onNewMention: SlackCallbackHandler<SlackNewMention, never, never> = (_event, delivery) =>
				Ref.get(test.preparations).pipe(
					Effect.map((preparations) => {
						preparedBeforeCallback.push(preparations.length)
						deliveryIds.push(delivery.deliveryId)
					}),
				)
			const result = yield* processor
				.process(mentionBatch(), test.execution)
				.pipe(Effect.provide(sharedRuntimeLayer({ onNewMention })))

			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(preparedBeforeCallback).toEqual([1])
			expect(deliveryIds).toEqual([test.execution.deliveryId])
			expect(yield* Ref.get(test.preparations)).toEqual([
				PreparedDeliveryInvocation.make({
					callback: 'onNewMention',
					presentationVersion: 1,
					destination: {
						_tag: 'SlackThread',
						thread: { teamId, channelId, threadTs: rootTs, isDm: false },
					},
					activationTarget: { _tag: 'SlackMessage', message: { teamId, channelId, messageTs: rootTs } },
					supportedOperations: threadSupportedOperations,
					reactionTargets: ['ActivationTarget', 'MessageTarget'],
				}),
			])
		})
	})

	it.effect('omits the activation target for a subscribed batch', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			yield* processor
				.process(DeliveryAdmissionBatch.make([admission('Ev_REPLY', reply())]), test.execution)
				.pipe(
					Effect.provide(
						Layer.merge(
							SlackCallbacks.layer({ onSubscribedThreadEvents: () => Effect.void }),
							subscribedRuntimeLayer,
						),
					),
				)
			expect(yield* Ref.get(test.preparations)).toEqual([
				PreparedDeliveryInvocation.make({
					callback: 'onSubscribedThreadEvents',
					presentationVersion: 1,
					destination: {
						_tag: 'SlackThread',
						thread: { teamId, channelId, threadTs: rootTs, isDm: false },
					},
					supportedOperations: threadSupportedOperations,
					reactionTargets: ['MessageTarget'],
				}),
			])
		}),
	)

	it.effect('does not prepare a batch it ignores', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process(DeliveryAdmissionBatch.make([admission('Ev_REPLY', reply())]), test.execution)
				.pipe(Effect.provide(sharedRuntimeLayer({ onNewMention: () => Effect.void })))
			expect(result).toEqual(ProviderEventIgnored.make({ reason: 'no_activation_event' }))
			expect(yield* Ref.get(test.preparations)).toEqual([])
		}),
	)

	it.effect('retries the saved callback after the first attempt subscribed the thread', ({ expect }) => {
		const calls: Array<string> = []
		const onNewMention: SlackCallbackHandler<
			SlackNewMention,
			{ readonly retryable: boolean },
			MailboxSubscriptions
		> = (event) =>
			Effect.suspend(() => {
				calls.push('onNewMention')
				return calls.length === 1
					? event.thread.subscribe().pipe(Effect.orDie, Effect.andThen(Effect.fail({ retryable: true })))
					: Effect.void
			})
		const onSubscribedThreadEvents = () =>
			Effect.sync(() => {
				calls.push('onSubscribedThreadEvents')
			})
		return Effect.gen(function* () {
			const first = yield* makeTestDeliveryExecution()
			expect(yield* processor.process(mentionBatch(), first.execution).pipe(Effect.flip)).toEqual(
				ProviderEventExecutionFailed.make({ provider: 'slack', retryable: true, safeCode: 'callback_failed' }),
			)

			const second = yield* first.retry
			expect(Option.map(second.execution.prepared, ({ callback }) => callback)).toEqual(
				Option.some('onNewMention'),
			)
			expect(yield* processor.process(mentionBatch(), second.execution)).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(second.preparations)).toEqual([])
			expect(calls).toEqual(['onNewMention', 'onNewMention'])

			const fresh = yield* makeTestDeliveryExecution()
			yield* processor.process(mentionBatch(), fresh.execution)
			expect(calls).toEqual(['onNewMention', 'onNewMention', 'onSubscribedThreadEvents'])
		}).pipe(Effect.provide(sharedRuntimeLayer({ onNewMention, onSubscribedThreadEvents })))
	})

	it.effect('records a handoff the callback returns and reports the batch handled', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process(mentionBatch(), test.execution)
				.pipe(Effect.provide(sharedRuntimeLayer({ onNewMention: (_event, delivery) => delivery.handoff() })))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(test.handoffs)).toHaveLength(1)
		}),
	)

	it.effect('maps preparation failures without running the callback', ({ expect }) => {
		const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
		return Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const withPrepare = (prepare: ProviderDeliveryExecution['prepare']) =>
				new ProviderDeliveryExecution({ ...test.execution, prepare })

			const conflict = yield* processor
				.process(
					mentionBatch(),
					withPrepare(() =>
						Effect.fail(DeliveryPreparationConflict.make({ deliveryId: test.execution.deliveryId })),
					),
				)
				.pipe(Effect.flip)
			expect(conflict).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'slack',
					retryable: false,
					safeCode: 'delivery_prepare_conflict',
				}),
			)

			const unavailable = yield* processor
				.process(
					mentionBatch(),
					withPrepare(() => Effect.fail(DeliveryPreparationUnavailable.make({ reason: 'store offline' }))),
				)
				.pipe(Effect.flip)
			expect(unavailable).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'slack',
					retryable: true,
					safeCode: 'delivery_prepare_unavailable',
				}),
			)
			expect(onNewMention).not.toHaveBeenCalled()
		}).pipe(Effect.provide(sharedRuntimeLayer({ onNewMention })))
	})

	it.effect('fails a saved callback that is unknown or no longer configured', ({ expect }) => {
		const onNewMention = vi.fn<MentionCallback>(() => Effect.void)
		return Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const withPrepared = (callback: string) =>
				new ProviderDeliveryExecution({
					...test.execution,
					prepared: Option.some(
						PreparedDeliveryInvocation.make({
							callback,
							presentationVersion: 1,
							destination: null,
							supportedOperations: [],
						}),
					),
				})
			const missing = ProviderEventExecutionFailed.make({
				provider: 'slack',
				retryable: false,
				safeCode: 'prepared_callback_missing',
			})

			expect(
				yield* processor.process(mentionBatch(), withPrepared('onSubscribedThreadEvents')).pipe(Effect.flip),
			).toEqual(missing)
			expect(yield* processor.process(mentionBatch(), withPrepared('onRemoved')).pipe(Effect.flip)).toEqual(
				missing,
			)
			expect(onNewMention).not.toHaveBeenCalled()
			expect(yield* Ref.get(test.preparations)).toEqual([])
		}).pipe(Effect.provide(sharedRuntimeLayer({ onNewMention })))
	})

	it.effect('fails a saved callback whose event cannot be rebuilt from the batch', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const execution = new ProviderDeliveryExecution({
				...test.execution,
				prepared: Option.some(
					PreparedDeliveryInvocation.make({
						callback: 'onNewMention',
						presentationVersion: 1,
						destination: null,
						supportedOperations: [],
					}),
				),
			})
			const error = yield* processor
				.process(DeliveryAdmissionBatch.make([admission('Ev_REPLY', reply())]), execution)
				.pipe(Effect.provide(sharedRuntimeLayer({ onNewMention: () => Effect.void })), Effect.flip)
			expect(error).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'slack',
					retryable: false,
					safeCode: 'prepared_callback_unbuildable',
				}),
			)
		}),
	)
})
