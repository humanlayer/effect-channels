import { assert, it } from '@effect/vitest'
import { Cause, Effect, Exit, Stream } from 'effect'

import {
	ChannelId,
	Channels,
	MarkdownContent,
	MessageRef,
	TenantId,
	ThreadId,
	UserId,
	unimplemented,
} from '../src/index.ts'
import { ChannelsLayer } from './support.ts'

const threadId = ThreadId.make('slack:v1:T_TEST:C_TEST:100.1')
const channel = {
	id: ChannelId.make('slack:v1:T_TEST:C_TEST'),
	provider: 'slack' as const,
	tenant: TenantId.make('T_TEST'),
	isDm: false,
}
const content = MarkdownContent.make({ markdown: 'hello' })

const expectDefect = <A, E>(operation: string, effect: Effect.Effect<A, E>) =>
	Effect.gen(function* () {
		const exit = yield* Effect.exit(effect)
		assert.strictEqual(Exit.isFailure(exit), true)
		if (Exit.isFailure(exit)) {
			assert.ok(Cause.pretty(exit.cause).includes(`${operation} is intentionally unimplemented`))
		}
	})

it.effect('names every Phase 1 core placeholder', () =>
	Effect.gen(function* () {
		const channels = yield* Channels
		yield* expectDefect(
			'Channels.onSubscribedMessage',
			channels.onSubscribedMessage(() => Effect.void),
		)
		yield* expectDefect(
			'Channels.onNewMessage',
			channels.onNewMessage(/^hello/, () => Effect.void),
		)
		yield* expectDefect(
			'Channels.onDirectMessage',
			channels.onDirectMessage(() => Effect.void),
		)
		yield* expectDefect('Channels.postToChannel', channels.postToChannel({ channel, content }))
		yield* expectDefect('Channels.edit', channels.edit({ threadId, messageRef: MessageRef.make('100.2'), content }))
		yield* expectDefect('Channels.delete', channels.delete({ threadId, messageRef: MessageRef.make('100.2') }))
		yield* expectDefect('Channels.stream', channels.stream({ threadId }, Stream.empty))
		yield* expectDefect('Channels.startThreadTyping', channels.startThreadTyping({ threadId }))
		yield* expectDefect('Channels.startChannelTyping', channels.startChannelTyping({ channel }))
		yield* expectDefect('Channels.messages', channels.messages({ threadId }))
		yield* expectDefect('Channels.messageStream', Stream.runDrain(channels.messageStream({ threadId })))
		yield* expectDefect('Channels.info', channels.info({ threadId }))
		yield* expectDefect(
			'Channels.getUser',
			channels.getUser({ provider: 'slack', tenant: TenantId.make('T_TEST'), userId: UserId.make('U_TEST') }),
		)
	}).pipe(Effect.provide(ChannelsLayer)),
)

it.effect('includes the exact operation name in shared defects', () =>
	expectDefect('Example.operation', unimplemented('Example.operation')),
)
