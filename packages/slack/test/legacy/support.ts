import { DeliveryPolicy } from '@humanlayer/channels-delivery'
import { layerMailboxStoreServices } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import { DateTime, Effect, Layer, Predicate } from 'effect'

import {
	ChannelId,
	IdempotencyKey,
	Message,
	MessageEvent,
	MessageRef,
	NewMentionDelivery,
	NormalizedMessage,
	ProviderName,
	TenantId,
	Thread,
	ThreadId,
	UserId,
	Slack,
	SlackApiError,
	SlackIngress,
	SlackSubscriptions,
	type SlackClient,
	type Author,
	type ThreadRef,
	type SlackIngressHandlers,
} from '../../src/index'
import { stubSlackClientLayer } from '../support'

export const testChannelRef = {
	id: ChannelId.make('slack:v1:T_TEST:C_TEST'),
	provider: ProviderName.make('slack'),
	tenant: TenantId.make('T_TEST'),
	isDm: false,
}

export const testThreadRef = {
	id: ThreadId.make('slack:v1:T_TEST:C_TEST:100.1'),
	channel: testChannelRef,
	isNew: true,
}

export const testAuthor = {
	userId: UserId.make('U_TEST'),
	userName: 'tester',
	fullName: 'Test User',
	isBot: false,
	isMe: false,
}

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

export const makeTestMessageEvent = (idempotencyKey: string) =>
	MessageEvent.make({
		provider: 'slack',
		tenant: TenantId.make('T_TEST'),
		idempotencyKey: IdempotencyKey.make(idempotencyKey),
		thread: testThread,
		message: testMessage,
		delivery: NewMentionDelivery.make({ location: 'channel_root' }),
		raw: { type: 'app_mention' },
	})

export const testMessageEvent = makeTestMessageEvent(`evt_${'0'.repeat(32)}`)

export const makeTestAuthor = (input: {
	readonly userId: string
	readonly isBot?: boolean | 'unknown'
	readonly isMe?: boolean
}): Author => ({
	userId: UserId.make(input.userId),
	userName: input.userId,
	fullName: input.userId,
	isBot: input.isBot ?? false,
	isMe: input.isMe ?? false,
})

export const testIdempotencyKey = (seed: string) =>
	IdempotencyKey.make(
		`evt_${seed
			.replace(/[^a-f0-9]/g, '')
			.padEnd(32, '0')
			.slice(0, 32)}`,
	)

export const testThreadRefFor = (rootTs: string, isNew: boolean): ThreadRef => ({
	id: ThreadId.make(`slack:v1:T_TEST:C_TEST:${rootTs}`),
	channel: testChannelRef,
	isNew,
})

export const makeTestMessage = (input: {
	readonly messageTs: string
	readonly threadRef?: ThreadRef
	readonly author?: Author
	readonly text?: string
}) => {
	const text = input.text ?? `message ${input.messageTs}`
	return Message.make({
		ref: MessageRef.make(input.messageTs),
		threadRef: input.threadRef ?? testThreadRef,
		text,
		markdown: text,
		author: input.author ?? testAuthor,
		metadata: { sentAt: DateTime.makeUnsafe({ epochMilliseconds: Math.round(Number(input.messageTs) * 1000) }) },
		attachments: [],
		raw: { ts: input.messageTs },
	})
}

export const makeTestNormalizedMessage = (input: {
	readonly messageTs: string
	readonly rootTs?: string
	readonly mentioned?: boolean
	readonly author?: Author
	readonly text?: string
}): NormalizedMessage => {
	const threadRef = testThreadRefFor(input.rootTs ?? input.messageTs, input.rootTs === undefined)
	const message = makeTestMessage({
		messageTs: input.messageTs,
		threadRef,
		author: input.author ?? testAuthor,
		text: input.text ?? `message ${input.messageTs}`,
	})
	return NormalizedMessage.make({
		provider: 'slack',
		tenant: TenantId.make('T_TEST'),
		idempotencyKey: testIdempotencyKey(input.messageTs),
		thread: Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] }),
		message,
		mentioned: input.mentioned ?? false,
		raw: { ts: input.messageTs },
	})
}

export const expectTaggedFailure =
	<K extends string>(tag: K) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<E & { readonly _tag: K }, A, R> =>
		Effect.filterOrElse(
			Effect.flip(effect),
			(error): error is E & { readonly _tag: K } => Predicate.isTagged(error, tag),
			() => Effect.die(new Error(`expected a ${tag} failure`)),
		)

export const policy = DeliveryPolicy.make({
	mode: 'serial',
	maxPayloadBytes: 100_000,
	maxEnvelopes: 100,
	maxOutcomes: 100,
	retentionMs: 86_400_000,
	maxAttempts: 6,
	retryBaseMs: 100,
	retryMaxMs: 400,
	leaseMs: 1_000,
	heartbeatMs: 20,
	conflictRetries: 8,
})
export const runnerOptions = { pollMs: 10, scanLimit: 100, concurrency: 8 }

export const nativeSlackLayer = (client: Partial<SlackClient['Service']> = {}) =>
	Slack.layer.pipe(
		Layer.provideMerge(
			stubSlackClientLayer({
				getUser: () => Effect.fail(SlackApiError.make({ operation: 'users.info', code: 'user_not_found' })),
				...client,
			}),
		),
	)

export const ingressLayer = <E = never, R = never>(
	handlers: SlackIngressHandlers<E, R>,
	client: Partial<SlackClient['Service']> = {},
) => {
	const slack = nativeSlackLayer(client)
	const storage = memory({ maxMailboxes: 100 })
	const dependencies = Layer.mergeAll(
		slack,
		SlackSubscriptions.layerMemory(),
		layerMailboxStoreServices.pipe(Layer.provide(storage)),
	)
	return SlackIngress.layer({ namespace: 'legacy-regression', policy, handlers }).pipe(
		Layer.provideMerge(dependencies),
	)
}
