import { Channels, Emoji, MarkdownContent, MarkdownTextChunk } from '@humanlayer/channels'
import { ChannelsStorage, createChannelsApp, slack } from '@humanlayer/channels-app'
import { Effect, Stream } from 'effect'

import { loadSlackConnection } from './store.ts'

const reactionRequest = /\breact(?:ion)?\b/i
const streamRequest = /\bstream\b/i

export const app = createChannelsApp({
	providers: [slack({ loadConnection: ({ workspaceId }) => loadSlackConnection({ workspaceId }) })],
	storage: ChannelsStorage.postgres({ pool: 'shared' }),
	handlers: {
		onNewMention: (thread, message) =>
			Effect.gen(function* () {
				yield* Effect.logInfo(`Received a mention from ${message.author.fullName}`).pipe(
					Effect.annotateLogs({ tenant: thread.ref.channel.tenant }),
				)
				yield* thread.subscribe()
				if (streamRequest.test(message.text)) {
					yield* thread.stream(
						Stream.make(
							MarkdownTextChunk.make({ text: 'Workspace-aware streaming echo: ' }),
							MarkdownTextChunk.make({ text: message.text }),
						).pipe(Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk)))),
					)
				} else {
					yield* thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
				}
				if (reactionRequest.test(message.text)) {
					const channels = yield* Channels
					yield* channels.addReaction({
						threadId: thread.ref.id,
						messageRef: message.ref,
						emoji: Emoji.Check,
					})
				}
			}),
		onSubscribedMessage: (thread, message) =>
			(streamRequest.test(message.text)
				? thread.stream(
						Stream.make(MarkdownTextChunk.make({ text: `Workspace-aware stream: ${message.text}` })).pipe(
							Stream.mapEffect((chunk) => Effect.sleep(600).pipe(Effect.as(chunk))),
						),
					)
				: thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
			).pipe(Effect.asVoid),
		onMessageUpdated: (event) =>
			Effect.logInfo(`Workspace message ${event.message.ref} was edited`).pipe(
				Effect.annotateLogs({ tenant: event.tenant }),
			),
		onMessageDeleted: (event) =>
			Effect.logInfo(`Workspace message ${event.messageRef} was deleted`).pipe(
				Effect.annotateLogs({ tenant: event.tenant }),
			),
		onAnyReaction: (event) =>
			Effect.logInfo(`Workspace reaction ${event.rawEmoji}`).pipe(Effect.annotateLogs({ tenant: event.tenant })),
		onConversationStopped: (event) =>
			Effect.logInfo(`Workspace stream stopped in ${event.threadRef.id}`).pipe(
				Effect.annotateLogs({ tenant: event.tenant }),
			),
	},
})

export const handle = app.handle
export const routes = app.routes
