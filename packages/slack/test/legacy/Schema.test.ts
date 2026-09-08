import { assert, expect, it } from '@effect/vitest'
import { DateTime, Effect, Schema } from 'effect'

import {
	Channel,
	ChannelId,
	Emoji,
	Message,
	MessageRef,
	ProviderName,
	TenantId,
	Thread,
	ThreadId,
	UserId,
} from '../../src/index.js'

const channelRef = {
	id: ChannelId.make('slack:v1:T_TEST:C_TEST'),
	provider: ProviderName.make('slack'),
	tenant: TenantId.make('T_TEST'),
	isDm: false,
}

const threadRef = {
	id: ThreadId.make('slack:v1:T_TEST:C_TEST:100.1'),
	channel: channelRef,
	isNew: true,
}

const message = Message.make({
	ref: MessageRef.make('100.1'),
	threadRef,
	text: 'hello',
	markdown: 'hello',
	author: {
		userId: UserId.make('U_TEST'),
		userName: 'tester',
		fullName: 'Test User',
		isBot: false,
		isMe: false,
	},
	metadata: { sentAt: DateTime.makeUnsafe('2026-08-29T12:00:00Z') },
	attachments: [],
	raw: { type: 'app_mention' },
})

it.effect('round-trips schema classes and reconstructs behavior', () =>
	Effect.gen(function* () {
		const thread = Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] })
		const encoded = yield* Schema.encodeEffect(Thread)(thread)
		const decoded = yield* Schema.decodeEffect(Thread)(encoded)

		assert.ok(Schema.is(Thread)(decoded))
		assert.deepStrictEqual(decoded.ref, threadRef)
		assert.ok(decoded.currentMessage !== undefined && Schema.is(Message)(decoded.currentMessage))
		assert.ok(Schema.is(Channel)(decoded.channel))
	}),
)

it('constructs canonical class values with make', () => {
	const channel = Channel.fromRef(channelRef)
	const emoji = Emoji.custom('party_parrot')

	expect(channel).toBeInstanceOf(Channel)
	expect(emoji).toBeInstanceOf(Emoji)
	expect(Emoji.ThumbsUp.name).toBe('thumbs_up')
})
