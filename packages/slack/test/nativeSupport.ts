import { DeliveryPolicy, layerMailboxStoreServices, mailboxKey, MailboxStore } from '@humanlayer/channels-delivery'
import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { DateTime, Effect, Layer, Predicate } from 'effect'

import {
	Author,
	IdempotencyKey,
	Message,
	MessageRef,
	NormalizedMessage,
	Slack,
	SlackIngress,
	SlackSubscriptions,
	Thread,
	UserId,
	UserProfile,
	type SlackIngressHandlers,
} from '../src/index'
import type { SlackClient } from '../src/SlackClient'
import { slackThreadRef } from '../src/SlackThreadId'
import { stubSlackClientLayer, testChannelId, testRootTs, testTeamId } from './support'

export const expectTaggedFailure =
	<K extends string>(tag: K) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<E & { readonly _tag: K }, A, R> =>
		Effect.flatMap(Effect.flip(effect), (error) =>
			Predicate.isTagged(tag)(error) ? Effect.succeed(error) : Effect.die(new Error(`expected a ${tag} failure`)),
		)

export const testThreadRef = slackThreadRef(
	{ teamId: testTeamId, channelId: testChannelId, threadTs: testRootTs },
	true,
)

export const testAuthor = Author.make({
	userId: UserId.make('U_TEST'),
	userName: 'tester',
	fullName: 'Test User',
	isBot: false,
	isMe: false,
})

export const testMessage = Message.make({
	ref: MessageRef.make('100.1'),
	threadRef: testThreadRef,
	text: 'hello',
	markdown: 'hello',
	author: testAuthor,
	metadata: { sentAt: DateTime.makeUnsafe('2026-08-29T12:00:00Z') },
	attachments: [],
	raw: { type: 'app_mention' },
})

export const testThread = Thread.make({
	ref: testThreadRef,
	currentMessage: testMessage,
	recentMessages: [testMessage],
})

export const nativeMessage = (seed: string) => {
	const message = Message.make({ ...testMessage, text: seed, markdown: seed })
	return NormalizedMessage.make({
		provider: 'slack',
		tenant: testThreadRef.channel.tenant,
		idempotencyKey: IdempotencyKey.make(`evt_${seed.repeat(32)}`),
		thread: Thread.make({ ref: testThreadRef, currentMessage: message, recentMessages: [message] }),
		message,
		mentioned: true,
		raw: { seed },
	})
}

export const nativePolicy = DeliveryPolicy.make({
	mode: 'queue',
	maxPayloadBytes: 65_536,
	maxEnvelopes: 16,
	maxOutcomes: 64,
	retentionMs: 60_000,
	maxAttempts: 3,
	retryBaseMs: 100,
	retryMaxMs: 1_000,
	leaseMs: 1_000,
	heartbeatMs: 100,
	conflictRetries: 8,
})

export const nativeRunner = { scanLimit: 16, concurrency: 2, pollMs: 10 }

export const nativeIngressLayer = <E, R>(
	handlers: SlackIngressHandlers<E, R>,
	storage = deliveryMemory({ maxMailboxes: 100 }),
	policy: DeliveryPolicy = nativePolicy,
	clientOverrides: Partial<SlackClient['Service']> = {},
) => {
	const storageWithQueue = Layer.merge(storage, layerMailboxStoreServices.pipe(Layer.provide(storage)))
	const slack = Slack.layer.pipe(
		Layer.provide(
			stubSlackClientLayer({
				getUser: (input) =>
					Effect.succeed(
						UserProfile.make({
							author: Author.make({ ...testAuthor, userId: input.userId, fullName: 'Hydrated User' }),
						}),
					),
				...clientOverrides,
			}),
		),
	)
	return SlackIngress.layer({ namespace: 'slack-native-test', policy, handlers }).pipe(
		Layer.provideMerge(Layer.mergeAll(storageWithQueue, SlackSubscriptions.layerMemory(), slack)),
	)
}

export const nativeMailbox = (handlerId: string, event: NormalizedMessage) =>
	Effect.flatMap(MailboxStore, (store) =>
		store.loadMailbox({
			key: mailboxKey({
				namespace: 'slack-native-test',
				handlerId,
				provider: 'slack',
				installation: event.tenant,
				resourceKey: `${event.thread.ref.id.length}:${event.thread.ref.id}`,
			}),
		}),
	)
