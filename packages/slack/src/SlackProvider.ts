import {
	Capabilities,
	ChannelGone,
	ChannelProvider,
	HistoryFailed,
	Message,
	MessageRef,
	MetadataFailed,
	PostFailed,
	SentMessage,
	SentRef,
	ThreadGone,
	ThreadId,
	ThreadInfo,
	UserId,
	UserLookupFailed,
	containerInputWithOptions,
	messagePageStream,
	threadSummaryPageStream,
	unimplemented,
} from '@humanlayer/channels'
import type {
	ChannelPostInput,
	ChannelThreadsInput,
	ContainerMessagesInput,
	Content,
	GetUserInput,
	InfoInput,
	InlineContentNode,
	MessagesInput,
	PostInput,
	StartThreadTypingInput,
} from '@humanlayer/channels'
import { Clock, Context, DateTime, Effect, HashSet, Layer, Match, Ref } from 'effect'

import {
	SlackChannelInfoInput,
	SlackGetUserInput,
	SlackHistoryInput,
	SlackListThreadsInput,
	SlackMessageTs,
	SlackPostMessageInput,
	SlackRepliesInput,
	SlackTeamId,
	SlackThreadRef,
	type SlackChannelId,
	type SlackThreadRef as SlackThreadRefType,
} from './Schema.ts'
import { SlackClient } from './SlackClient.ts'
import { decodeSlackChannelId, decodeSlackThreadId, slackThreadRef } from './SlackThreadId.ts'

const retryableUserLookupApiErrors = new Set(['ratelimited', 'internal_error', 'fatal_error', 'service_unavailable'])

const SlackCapabilities = Capabilities.make({
	threadPost: true,
	channelPost: true,
	edit: false,
	delete: false,
	streaming: 'unsupported',
	typing: { thread: true, channel: false },
	history: { thread: true, channelMessages: true, channelThreads: true },
	reactions: { add: false, remove: false, events: false },
	files: { read: false, upload: false },
	actions: false,
	threadInfo: true,
	channelInfo: true,
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

interface HistoryRequestFields {
	limit?: number
	cursor?: string
	direction?: 'forward' | 'backward'
}

interface SlackHistoryFields extends HistoryRequestFields {
	teamId: SlackTeamId
	channelId: SlackChannelId
	before?: SlackMessageTs
}

interface SlackThreadListFields {
	teamId: SlackTeamId
	channelId: SlackChannelId
	limit?: number
	cursor?: string
}

const historyRequestFields = (options: MessagesInput['options']): HistoryRequestFields => {
	const fields: HistoryRequestFields = {}
	if (options?.limit !== undefined) {
		fields.limit = options.limit
	}
	if (options?.cursor !== undefined) {
		fields.cursor = options.cursor
	}
	if (options?.direction !== undefined) {
		fields.direction = options.direction
	}
	return fields
}

export class SlackProvider extends Context.Service<SlackProvider, ChannelProvider>()('channels/SlackProvider') {
	static readonly layer = Layer.effect(
		SlackProvider,
		Effect.gen(function* () {
			const client = yield* SlackClient
			const typingThreads = yield* Ref.make(HashSet.empty<ThreadId>())

			const restoreActiveStatus = (ref: SlackThreadRefType, threadId: ThreadId) =>
				client
					.setSessionStatus({
						teamId: ref.teamId,
						channelId: ref.channelId,
						threadTs: ref.threadTs,
						status: 'active',
					})
					.pipe(
						Effect.tapError((error) =>
							Effect.logWarning('Slack session status restore failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', thread_id: threadId }),
							),
						),
						Effect.ignore,
						Effect.withSpan('channels.typing.end', {
							attributes: { provider: 'slack', thread_id: threadId, operation: 'typing_end' },
						}),
					)

			const endTypingIfStarted = (ref: SlackThreadRefType, threadId: ThreadId) =>
				Ref.modify(
					typingThreads,
					(threads) => [HashSet.has(threads, threadId), HashSet.remove(threads, threadId)] as const,
				).pipe(Effect.flatMap((started) => (started ? restoreActiveStatus(ref, threadId) : Effect.void)))

			const sentFromSlack = (input: {
				readonly threadRef: ReturnType<typeof slackThreadRef>
				readonly sentThreadId: ThreadId
				readonly text: string
				readonly degraded: ReadonlyArray<string>
				readonly channelId: string
				readonly ts: string
				readonly botUserId: string | undefined
			}) =>
				Effect.gen(function* () {
					const now = yield* Clock.currentTimeMillis
					const authorId = input.botUserId ?? 'self'
					const message = Message.make({
						ref: MessageRef.make(input.ts),
						threadRef: input.threadRef,
						text: input.text,
						markdown: input.text,
						author: {
							userId: UserId.make(authorId),
							userName: authorId,
							fullName: authorId,
							isBot: true,
							isMe: true,
						},
						metadata: { sentAt: DateTime.makeUnsafe({ epochMilliseconds: now }) },
						attachments: [],
						raw: { channel: input.channelId, ts: input.ts },
					})
					return SentMessage.make({
						ref: SentRef.make({
							threadId: input.sentThreadId,
							messageRef: message.ref,
							provider: 'slack',
							degraded: input.degraded,
						}),
						message,
					})
				})

			const post = Effect.fn('slack.provider.post')(function* (input: PostInput) {
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
				const send = Effect.gen(function* () {
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
					return yield* sentFromSlack({
						threadRef: slackThreadRef(ref, false),
						sentThreadId: input.threadId,
						text: rendered.text,
						degraded: rendered.degraded,
						channelId: sent.channelId,
						ts: sent.ts,
						botUserId: sent.botUserId,
					})
				})
				return yield* send.pipe(Effect.ensuring(endTypingIfStarted(ref, input.threadId)))
			})

			const postToChannel = Effect.fn('slack.provider.post_to_channel')(function* (input: ChannelPostInput) {
				const errorThreadId = ThreadId.make(input.channel.id)
				const address = yield* decodeSlackChannelId(input.channel.id).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(
							PostFailed.make({
								provider: 'slack',
								threadId: errorThreadId,
								message: 'invalid Slack channel id',
							}),
						),
					),
				)
				const rendered = renderContent(input.content)
				const sent = yield* client
					.postMessage(
						SlackPostMessageInput.make({
							teamId: address.teamId,
							channelId: address.channelId,
							text: rendered.text,
						}),
					)
					.pipe(
						Effect.tapError((error) =>
							Effect.logError('Slack provider channel post failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', tenant: address.teamId }),
							),
						),
						Effect.catchTags({
							SlackTransportError: () =>
								Effect.fail(
									PostFailed.make({
										provider: 'slack',
										threadId: errorThreadId,
										message: 'Slack transport failed',
									}),
								),
							SlackApiError: () =>
								Effect.fail(
									PostFailed.make({
										provider: 'slack',
										threadId: errorThreadId,
										message: 'Slack API rejected the post',
									}),
								),
						}),
					)
				const threadRef = slackThreadRef(
					SlackThreadRef.make({ teamId: address.teamId, channelId: sent.channelId, threadTs: sent.ts }),
					true,
				)
				return yield* sentFromSlack({
					threadRef,
					sentThreadId: threadRef.id,
					text: rendered.text,
					degraded: rendered.degraded,
					channelId: sent.channelId,
					ts: sent.ts,
					botUserId: sent.botUserId,
				})
			})

			const startThreadTyping = (input: StartThreadTypingInput) =>
				Effect.gen(function* () {
					const ref = yield* decodeSlackThreadId(input.threadId)
					yield* Ref.update(typingThreads, HashSet.add(input.threadId))
					yield* client.setSessionStatus({
						teamId: ref.teamId,
						channelId: ref.channelId,
						threadTs: ref.threadTs,
						status: 'processing',
					})
				}).pipe(
					Effect.tapError((error) =>
						Effect.logWarning('Slack thread typing failed', error).pipe(
							Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
						),
					),
					Effect.ignore,
				)

			const messages = Effect.fn('slack.provider.messages')(function* (input: MessagesInput) {
				const ref = yield* decodeSlackThreadId(input.threadId).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'invalid Slack thread id' })),
					),
				)
				return yield* client
					.replies(
						SlackRepliesInput.make({
							teamId: ref.teamId,
							channelId: ref.channelId,
							threadTs: ref.threadTs,
							...historyRequestFields(input.options),
						}),
					)
					.pipe(
						Effect.tapError((error) =>
							Effect.logError('Slack thread history failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
							),
						),
						Effect.catchTags({
							UnknownTenant: () =>
								Effect.fail(
									HistoryFailed.make({ provider: 'slack', message: 'unknown Slack workspace' }),
								),
							SlackTransportError: () =>
								Effect.fail(
									HistoryFailed.make({ provider: 'slack', message: 'Slack transport failed' }),
								),
							SlackApiError: () =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack API rejected the history request',
									}),
								),
						}),
					)
			})

			const containerMessages = Effect.fn('slack.provider.container_messages')(function* (
				input: ContainerMessagesInput,
			) {
				const address = yield* decodeSlackChannelId(input.channel.id).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'invalid Slack channel id' })),
					),
				)
				const fields: SlackHistoryFields = {
					teamId: address.teamId,
					channelId: address.channelId,
					...historyRequestFields(input.options),
				}
				if (input.before !== undefined) {
					fields.before = SlackMessageTs.make(input.before)
				}
				return yield* client.history(SlackHistoryInput.make(fields)).pipe(
					Effect.tapError((error) =>
						Effect.logError('Slack channel history failed', error).pipe(
							Effect.annotateLogs({ provider: 'slack', tenant: address.teamId }),
						),
					),
					Effect.catchTags({
						UnknownTenant: () =>
							Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'unknown Slack workspace' })),
						SlackTransportError: () =>
							Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'Slack transport failed' })),
						SlackApiError: () =>
							Effect.fail(
								HistoryFailed.make({
									provider: 'slack',
									message: 'Slack API rejected the history request',
								}),
							),
					}),
				)
			})

			const channelThreads = Effect.fn('slack.provider.channel_threads')(function* (input: ChannelThreadsInput) {
				const address = yield* decodeSlackChannelId(input.channel.id).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'invalid Slack channel id' })),
					),
				)
				const fields: SlackThreadListFields = { teamId: address.teamId, channelId: address.channelId }
				if (input.options?.limit !== undefined) {
					fields.limit = input.options.limit
				}
				if (input.options?.cursor !== undefined) {
					fields.cursor = input.options.cursor
				}
				return yield* client.listThreads(SlackListThreadsInput.make(fields)).pipe(
					Effect.tapError((error) =>
						Effect.logError('Slack channel thread listing failed', error).pipe(
							Effect.annotateLogs({ provider: 'slack', tenant: address.teamId }),
						),
					),
					Effect.catchTags({
						UnknownTenant: () =>
							Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'unknown Slack workspace' })),
						SlackTransportError: () =>
							Effect.fail(HistoryFailed.make({ provider: 'slack', message: 'Slack transport failed' })),
						SlackApiError: () =>
							Effect.fail(
								HistoryFailed.make({
									provider: 'slack',
									message: 'Slack API rejected the thread listing',
								}),
							),
					}),
				)
			})

			const metadataFailed = (message: string) => Effect.fail(MetadataFailed.make({ provider: 'slack', message }))

			const info = Effect.fn('slack.provider.info')(function* (input: InfoInput) {
				const ref = yield* decodeSlackThreadId(input.threadId).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(ThreadGone.make({ threadId: input.threadId })),
					),
				)
				const channelInfo = yield* client
					.channelInfo(SlackChannelInfoInput.make({ teamId: ref.teamId, channelId: ref.channelId }))
					.pipe(
						Effect.tapError((error) =>
							Effect.logError('Slack thread info failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
							),
						),
						Effect.catchTags({
							UnknownTenant: () => metadataFailed('unknown Slack workspace'),
							SlackTransportError: () => metadataFailed('Slack transport failed'),
							SlackApiError: (error) =>
								error.code === 'channel_not_found'
									? Effect.fail(ThreadGone.make({ threadId: input.threadId }))
									: metadataFailed('Slack API rejected the metadata request'),
						}),
					)
				const threadRef = slackThreadRef(ref, false)
				return channelInfo.name === undefined
					? ThreadInfo.make({ thread: threadRef })
					: ThreadInfo.make({ thread: threadRef, title: channelInfo.name })
			})

			const channelInfo = Effect.fn('slack.provider.channel_info')(function* (input: {
				readonly channel: ContainerMessagesInput['channel']
			}) {
				const address = yield* decodeSlackChannelId(input.channel.id).pipe(
					Effect.catchTag('InvalidSlackThreadId', () =>
						Effect.fail(ChannelGone.make({ channelId: input.channel.id })),
					),
				)
				return yield* client
					.channelInfo(SlackChannelInfoInput.make({ teamId: address.teamId, channelId: address.channelId }))
					.pipe(
						Effect.tapError((error) =>
							Effect.logError('Slack channel info failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', tenant: input.channel.tenant }),
							),
						),
						Effect.catchTags({
							UnknownTenant: () => metadataFailed('unknown Slack workspace'),
							SlackTransportError: () => metadataFailed('Slack transport failed'),
							SlackApiError: (error) =>
								error.code === 'channel_not_found'
									? Effect.fail(ChannelGone.make({ channelId: input.channel.id }))
									: metadataFailed('Slack API rejected the metadata request'),
						}),
					)
			})

			const getUser = Effect.fn('slack.provider.get_user')(function* (input: GetUserInput) {
				const teamId = SlackTeamId.make(input.tenant)
				const lookupFailed = (reason: UserLookupFailed['reason'], retryable: boolean) =>
					Effect.fail(
						UserLookupFailed.make({
							provider: 'slack',
							tenant: input.tenant,
							userId: input.userId,
							reason,
							retryable,
						}),
					)
				return yield* client.getUser(SlackGetUserInput.make({ teamId, userId: input.userId })).pipe(
					Effect.tapError((error) =>
						Effect.logError('Slack user lookup failed', error).pipe(
							Effect.annotateLogs({ provider: 'slack', tenant: input.tenant }),
						),
					),
					Effect.catchTags({
						SlackTransportError: () => lookupFailed('transport', true),
						SlackApiError: (error) =>
							error.code === 'user_not_found'
								? lookupFailed('not_found', false)
								: lookupFailed('api', retryableUserLookupApiErrors.has(error.code)),
					}),
				)
			})

			return new ChannelProvider({
				name: 'slack',
				capabilities: SlackCapabilities,
				post,
				postToChannel,
				edit: () => unimplemented('SlackProvider.edit'),
				delete: () => unimplemented('SlackProvider.delete'),
				stream: () => unimplemented('SlackProvider.stream'),
				startThreadTyping,
				startChannelTyping: () => unimplemented('SlackProvider.startChannelTyping'),
				addReaction: () => unimplemented('SlackProvider.addReaction'),
				removeReaction: () => unimplemented('SlackProvider.removeReaction'),
				messages,
				messageStream: (input) =>
					messagePageStream(input.options, (options) =>
						messages(
							options === undefined
								? { threadId: input.threadId }
								: { threadId: input.threadId, options },
						),
					),
				containerMessages,
				containerMessageStream: (input) =>
					messagePageStream(input.options, (options) =>
						containerMessages(containerInputWithOptions(input, options)),
					),
				channelThreads,
				channelThreadStream: (input) =>
					threadSummaryPageStream(input.options, (options) =>
						channelThreads(
							options === undefined ? { channel: input.channel } : { channel: input.channel, options },
						),
					),
				info,
				channelInfo,
				getUser,
				subject: () => unimplemented('SlackProvider.subject'),
				downloadAttachment: () => unimplemented('SlackProvider.downloadAttachment'),
				openDM: () => unimplemented('SlackProvider.openDM'),
				postEphemeral: () => unimplemented('SlackProvider.postEphemeral'),
			})
		}),
	)
}

export { SlackCapabilities }
