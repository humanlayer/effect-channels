import {
	Slack,
	Emoji,
	FileUpload,
	MarkdownContent,
	MarkdownTextChunk,
	PlanUpdateChunk,
	TaskUpdateChunk,
	type Thread,
	type Message,
	type SentMessage,
} from '@humanlayer/channels-slack'
import { Effect, Stream } from 'effect'

const imageRequest = /\bimage\b/i
const reactionRequest = /\breact(?:ion)?\b/i
const editRequest = /\bedit\b/i
const deleteRequest = /\bdelete\b/i
const streamRequest = /\bstream\b/i
const encoder = new TextEncoder()

const exampleImage = () =>
	FileUpload.make({
		filename: 'channels-example.svg',
		mimeType: 'image/svg+xml',
		data: encoder.encode(
			'<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360" viewBox="0 0 640 360"><rect width="640" height="360" rx="32" fill="#4a154b"/><text x="320" y="180" fill="white" font-family="sans-serif" font-size="42" text-anchor="middle" dominant-baseline="middle">Hello from Slack</text></svg>',
		),
	})

const echoContent = (prefix: string, text: string) => {
	const markdown = `${prefix}: ${text}`
	return imageRequest.test(text)
		? MarkdownContent.make({ markdown, files: [exampleImage()] })
		: MarkdownContent.make({ markdown })
}

export const demonstrateLifecycle = Effect.fn('example.echo.lifecycle')(function* ({
	thread,
	message,
	sent,
}: {
	readonly thread: Thread
	readonly message: Message
	readonly sent: SentMessage
}) {
	if (reactionRequest.test(message.text)) {
		const slack = yield* Slack
		yield* slack.addReaction({ threadId: thread.ref.id, messageRef: message.ref, emoji: Emoji.Check })
	}
	if (editRequest.test(message.text)) {
		yield* sent.edit(MarkdownContent.make({ markdown: `Edited echo: ${message.text}` }))
	}
	if (deleteRequest.test(message.text)) yield* sent.delete()
})

export const respond = Effect.fn('example.echo.respond')(
	({ thread, prefix, text }: { readonly thread: Thread; readonly prefix: string; readonly text: string }) =>
		streamRequest.test(text)
			? thread.stream(
					Stream.make(
						PlanUpdateChunk.make({ title: 'Streaming an Effect response' }),
						TaskUpdateChunk.make({ id: 'compose', title: 'Compose reply', status: 'in_progress' }),
						MarkdownTextChunk.make({ text: `${prefix}: ` }),
						MarkdownTextChunk.make({ text }),
						TaskUpdateChunk.make({ id: 'compose', title: 'Compose reply', status: 'complete' }),
					).pipe(Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk)))),
				)
			: thread.post(echoContent(prefix, text)),
)
