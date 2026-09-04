import { MarkdownContent } from '@humanlayer/channels'
import { ChannelsStorage, createChannelsApp, slack } from '@humanlayer/channels-app'
import { Effect } from 'effect'

import { loadSlackConnection } from './store.ts'

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
				yield* thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
			}),
		onSubscribedMessage: (thread, message) =>
			thread
				.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
				.pipe(Effect.asVoid),
	},
})

export const handle = app.handle
export const routes = app.routes
