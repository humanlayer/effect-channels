import { PlainTextContent, type MessageEvent } from '@humanlayer/channels-slack'
import { Effect } from 'effect'

const reply = Effect.fn('example.storage.reply')(function* ({ thread, message }: MessageEvent) {
	yield* thread.post(PlainTextContent.make({ text: `Durable echo: ${message.text}` }))
})

export const handlers = {
	onNewMention: Effect.fn('example.storage.onNewMention')(function* (event: MessageEvent) {
		yield* event.thread.subscribe()
		yield* reply(event)
	}),
	onSubscribedMessage: reply,
	onDirectMessage: reply,
}
