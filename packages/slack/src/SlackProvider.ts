import {
	Capabilities,
	ChannelProvider,
	Message,
	MessageRef,
	PostFailed,
	SentMessage,
	SentRef,
	UserId,
	unimplemented,
} from '@humanlayer/channels'
import type { Content, InlineContentNode } from '@humanlayer/channels'
import { Clock, Context, DateTime, Effect, Layer, Match, Stream } from 'effect'

import { SlackPostMessageInput } from './Schema.ts'
import { SlackClient } from './SlackClient.ts'
import { decodeSlackThreadId, slackThreadRef } from './SlackThreadId.ts'

const SlackCapabilities = Capabilities.make({
	threadPost: true,
	channelPost: false,
	edit: false,
	delete: false,
	streaming: 'unsupported',
	typing: { thread: false, channel: false },
	history: { thread: false, channelMessages: false, channelThreads: false },
	reactions: { add: false, remove: false, events: false },
	files: { read: false, upload: false },
	actions: false,
	threadInfo: false,
	channelInfo: false,
	createThread: false,
	directMessages: { ingress: false, open: false },
	ephemeral: { native: false, dmFallback: false },
	subject: false,
})

const renderInlineNode = Match.type<InlineContentNode>().pipe(
	Match.tagsExhaustive({
		TextContentNode: (node) => node.text,
		LinkContentNode: (node) => `[${node.label}](${node.url.toString()})`,
	}),
)

const renderContent = Match.type<Content>().pipe(
	Match.tagsExhaustive({
		PlainTextContent: (content) => ({
			text: content.text,
			degraded: content.files === undefined || content.files.length === 0 ? [] : ['files'],
		}),
		MarkdownContent: (content) => ({
			text: content.markdown,
			degraded: [
				...(content.actions === undefined || content.actions.length === 0 ? [] : ['actions']),
				...(content.files === undefined || content.files.length === 0 ? [] : ['files']),
			],
		}),
		StructuredContent: (content) => ({
			text: content.blocks
				.map(
					Match.type<(typeof content.blocks)[number]>().pipe(
						Match.tagsExhaustive({
							ParagraphContentBlock: (block) => block.children.map(renderInlineNode).join(''),
							CodeContentBlock: (block) => `\`\`\`${block.language ?? ''}\n${block.code}\n\`\`\``,
						}),
					),
				)
				.join('\n\n'),
			degraded: [
				'structured_content',
				...(content.actions === undefined || content.actions.length === 0 ? [] : ['actions']),
				...(content.files === undefined || content.files.length === 0 ? [] : ['files']),
			],
		}),
	}),
)

export class SlackProvider extends Context.Service<SlackProvider, ChannelProvider>()('channels/SlackProvider') {
	static readonly layer = Layer.effect(
		SlackProvider,
		Effect.gen(function* () {
			const client = yield* SlackClient
			const post = Effect.fn('slack.provider.post')(function* (input: Parameters<ChannelProvider['post']>[0]) {
				const ref = yield* decodeSlackThreadId(input.threadId).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(
							PostFailed.make({
								provider: 'slack',
								threadId: input.threadId,
								message: 'invalid Slack thread id',
							}),
						),
					),
				)
				const rendered = renderContent(input.content)
				const sent = yield* client
					.postMessage(
						SlackPostMessageInput.make({
							teamId: ref.teamId,
							channelId: ref.channelId,
							threadTs: ref.threadTs,
							text: rendered.text,
						}),
					)
					.pipe(
						Effect.tapError((error) =>
							Effect.logError('Slack provider post failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
							),
						),
						Effect.catchTags({
							SlackTransportError: () =>
								Effect.fail(
									PostFailed.make({
										provider: 'slack',
										threadId: input.threadId,
										message: 'Slack transport failed',
									}),
								),
							SlackApiError: () =>
								Effect.fail(
									PostFailed.make({
										provider: 'slack',
										threadId: input.threadId,
										message: 'Slack API rejected the post',
									}),
								),
						}),
					)
				const threadRef = slackThreadRef(ref, false)
				const now = yield* Clock.currentTimeMillis
				const message = Message.make({
					ref: MessageRef.make(sent.ts),
					threadRef,
					text: rendered.text,
					markdown: rendered.text,
					author: {
						userId: UserId.make('self'),
						userName: 'self',
						fullName: 'self',
						isBot: true,
						isMe: true,
					},
					metadata: { sentAt: DateTime.makeUnsafe({ epochMilliseconds: now }) },
					attachments: [],
					raw: { channel: sent.channelId, ts: sent.ts },
				})
				return SentMessage.make({
					ref: SentRef.make({
						threadId: input.threadId,
						messageRef: message.ref,
						provider: 'slack',
						degraded: rendered.degraded,
					}),
					message,
				})
			})

			return new ChannelProvider({
				name: 'slack',
				capabilities: SlackCapabilities,
				post,
				postToChannel: () => unimplemented('SlackProvider.postToChannel'),
				edit: () => unimplemented('SlackProvider.edit'),
				delete: () => unimplemented('SlackProvider.delete'),
				stream: () => unimplemented('SlackProvider.stream'),
				startThreadTyping: () => unimplemented('SlackProvider.startThreadTyping'),
				startChannelTyping: () => unimplemented('SlackProvider.startChannelTyping'),
				addReaction: () => unimplemented('SlackProvider.addReaction'),
				removeReaction: () => unimplemented('SlackProvider.removeReaction'),
				messages: () => unimplemented('SlackProvider.messages'),
				messageStream: () => Stream.fromEffect(unimplemented('SlackProvider.messageStream')),
				containerMessages: () => unimplemented('SlackProvider.containerMessages'),
				containerMessageStream: () => Stream.fromEffect(unimplemented('SlackProvider.containerMessageStream')),
				channelThreads: () => unimplemented('SlackProvider.channelThreads'),
				channelThreadStream: () => Stream.fromEffect(unimplemented('SlackProvider.channelThreadStream')),
				info: () => unimplemented('SlackProvider.info'),
				channelInfo: () => unimplemented('SlackProvider.channelInfo'),
				getUser: () => unimplemented('SlackProvider.getUser'),
				subject: () => unimplemented('SlackProvider.subject'),
				downloadAttachment: () => unimplemented('SlackProvider.downloadAttachment'),
				openDM: () => unimplemented('SlackProvider.openDM'),
				postEphemeral: () => unimplemented('SlackProvider.postEphemeral'),
			})
		}),
	)
}

export { SlackCapabilities }
