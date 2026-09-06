import { MarkdownContent, MarkdownTextChunk, type Thread } from '@humanlayer/channels-slack'
import { Effect, Stream } from 'effect'

const streamRequest = /\bstream\b/i

export const respond = Effect.fn('example.workspace_echo.respond')(({
	thread,
	text,
	mention = false,
}: {
	readonly thread: Thread
	readonly text: string
	readonly mention?: boolean
}) => {
	if (!streamRequest.test(text))
		return thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${text}` }))
	const chunks = mention
		? Stream.make(
				MarkdownTextChunk.make({ text: 'Workspace-aware streaming echo: ' }),
				MarkdownTextChunk.make({ text }),
			)
		: Stream.make(MarkdownTextChunk.make({ text: `Workspace-aware stream: ${text}` }))
	return thread.stream(chunks.pipe(Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk)))))
})
