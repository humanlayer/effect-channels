import { Clock, Context, DateTime, Effect, Exit, HashSet, Layer, Match, Option, Ref, Schema, Stream } from 'effect'

import { Attachment } from './Attachment.ts'
import type { Content, InlineContentNode } from './Content.ts'
import {
	ChannelGone,
	DeleteFailed,
	DirectMessageOpenFailed,
	EditFailed,
	FileReadFailed,
	HistoryFailed,
	MetadataFailed,
	PostFailed,
	ReactionFailed,
	RetryabilityMetadata,
	StatusFailed,
	ThreadGone,
	UnknownTenant,
} from './DomainErrors.ts'
import { Emoji } from './Emoji.ts'
import { SlackApiError, slackErrorRetryability } from './Errors.ts'
import { containerInputWithOptions, messagePageStream, threadSummaryPageStream } from './History.ts'
import { Message } from './Message.ts'
import {
	AttachmentRef,
	Capabilities,
	MessageRef,
	TenantId,
	ThreadId,
	ThreadInfo,
	UserId,
	type FileUpload,
} from './Model.ts'
import { EphemeralFallbackToDm, EphemeralResult } from './Operations.ts'
import type {
	ChannelPostInput,
	ChannelThreadsInput,
	ContainerMessagesInput,
	DeleteInput,
	EditInput,
	InfoInput,
	MessagesInput,
	OpenDMInput,
	PostEphemeralInput,
	PostInput,
	ReactInput,
	StartThreadTypingInput,
	StreamInput,
} from './Operations.ts'
import {
	SlackChannelInfoInput,
	SlackDeleteMessageInput,
	SlackFileDownloadInput,
	SlackFileUploadInput,
	SlackHistoryInput,
	SlackListThreadsInput,
	SlackMessageTs,
	SlackOpenDMInput,
	SlackPostEphemeralInput,
	SlackPostMessageInput,
	SlackReactionInput,
	SlackRepliesInput,
	SlackAppendStreamInput,
	SlackStartStreamInput,
	SlackStopStreamInput,
	SlackTeamId,
	SlackThreadRef,
	SlackUpdateMessageInput,
	type SlackChannelId,
	type SlackSentMessage as SlackSentMessageRef,
	type SlackThreadRef as SlackThreadRefType,
} from './Schema.ts'
import { SentMessage, SentRef } from './SentMessage.ts'
import { SlackAuthors } from './SlackAuthors.ts'
import { SlackClient } from './SlackClient.ts'
import type { SlackService } from './SlackService.ts'
import {
	SlackDmConversationTs,
	decodeSlackChannelId,
	decodeSlackThreadId,
	encodeSlackThreadId,
	slackDmConversationRef,
	slackThreadRef,
} from './SlackThreadId.ts'
import { SlackUserDirectory } from './SlackUserDirectory.ts'
import type { StreamChunk } from './StreamChunk.ts'
import { renderStreamingMarkdown, streamEditIntervalMs } from './Streaming.ts'
import { Thread } from './Thread.ts'

const SlackCapabilities = Capabilities.make({
	threadPost: true,
	channelPost: true,
	edit: true,
	delete: true,
	streaming: 'native',
	typing: { thread: true, channel: false },
	history: { thread: true, channelMessages: true, channelThreads: true },
	reactions: { add: true, remove: true, events: true },
	files: { read: true, upload: true },
	actions: false,
	threadInfo: true,
	channelInfo: true,
	createThread: false,
	directMessages: { ingress: true, open: true },
	ephemeral: { native: true, dmFallback: true },
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
			files: content.files ?? [],
			degraded: [],
		}),
		MarkdownContent: (content) => ({
			text: content.markdown,
			files: content.files ?? [],
			degraded: [...(content.actions === undefined || content.actions.length === 0 ? [] : ['actions'])],
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
			files: content.files ?? [],
			degraded: [
				'structured_content',
				...(content.actions === undefined || content.actions.length === 0 ? [] : ['actions']),
			],
		}),
	}),
)

const renderEmoji = (emoji: Emoji) =>
	Match.value(emoji.name).pipe(
		Match.when('thumbs_up', () => 'thumbsup'),
		Match.when('check', () => 'white_check_mark'),
		Match.orElse((name) => name),
	)

const nativeStreamingUnsupported = new Set([
	'feature_not_enabled',
	'channel_type_not_supported',
	'method_not_supported_for_channel_type',
	'unknown_method',
	'missing_scope',
	'not_allowed_token_type',
])

const streamErrorRetryability = Match.type<
	import('./Errors.ts').SlackApiError | import('./Errors.ts').SlackTransportError | UnknownTenant
>().pipe(
	Match.tagsExhaustive({
		SlackApiError: slackErrorRetryability,
		SlackTransportError: slackErrorRetryability,
		UnknownTenant: () => 'non_retryable' as const,
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

interface UploadedAttachmentFields {
	provider: 'slack'
	tenant: ReturnType<typeof TenantId.make>
	id: string
	kind: string
	name: string
	size: number
	providerLocator: { readonly id: string }
	mimeType?: string
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

const slackUploadInput = (input: {
	readonly ref: SlackThreadRefType
	readonly text: string
	readonly files: ReadonlyArray<FileUpload>
}) =>
	input.ref.threadTs === SlackDmConversationTs
		? SlackFileUploadInput.make({
				teamId: input.ref.teamId,
				channelId: input.ref.channelId,
				initialComment: input.text,
				files: input.files,
			})
		: SlackFileUploadInput.make({
				teamId: input.ref.teamId,
				channelId: input.ref.channelId,
				threadTs: input.ref.threadTs,
				initialComment: input.text,
				files: input.files,
			})

const slackPostInput = (ref: SlackThreadRefType, text: string) =>
	ref.threadTs === SlackDmConversationTs
		? SlackPostMessageInput.make({ teamId: ref.teamId, channelId: ref.channelId, text })
		: SlackPostMessageInput.make({ teamId: ref.teamId, channelId: ref.channelId, threadTs: ref.threadTs, text })

const slackEphemeralInput = (ref: SlackThreadRefType, userId: UserId, text: string) =>
	ref.threadTs === SlackDmConversationTs
		? SlackPostEphemeralInput.make({ teamId: ref.teamId, channelId: ref.channelId, userId, text })
		: SlackPostEphemeralInput.make({
				teamId: ref.teamId,
				channelId: ref.channelId,
				threadTs: ref.threadTs,
				userId,
				text,
			})

const slackConversationHistoryInput = (ref: SlackThreadRefType, options: MessagesInput['options']) => {
	const fields = { teamId: ref.teamId, channelId: ref.channelId, ...historyRequestFields(options) }
	return ref.directMessageKind === undefined
		? SlackHistoryInput.make(fields)
		: SlackHistoryInput.make({ ...fields, directMessageKind: ref.directMessageKind })
}

const slackRepliesInput = (ref: SlackThreadRefType, options: MessagesInput['options']) => {
	const fields = {
		teamId: ref.teamId,
		channelId: ref.channelId,
		threadTs: ref.threadTs,
		...historyRequestFields(options),
	}
	return ref.directMessageKind === undefined
		? SlackRepliesInput.make(fields)
		: SlackRepliesInput.make({ ...fields, directMessageKind: ref.directMessageKind })
}

export class Slack extends Context.Service<Slack, SlackService>()('slack/Slack') {
	static readonly layerWith = (options: { readonly streaming?: 'native' | 'post_and_edit' } = {}) =>
		Layer.effect(
			Slack,
			Effect.gen(function* () {
				const client = yield* SlackClient
				const users = yield* SlackUserDirectory
				const authors = yield* SlackAuthors
				const typingThreads = yield* Ref.make(HashSet.empty<ThreadId>())

				const restoreActiveStatus = (ref: SlackThreadRefType, threadId: ThreadId) =>
					ref.threadTs === SlackDmConversationTs
						? Effect.void
						: client
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
					readonly attachments?: ReadonlyArray<Attachment>
					readonly raw?: Message['raw']
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
							attachments: input.attachments ?? [],
							raw: input.raw ?? { channel: input.channelId, ts: input.ts },
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

				const uploadedAttachments = (
					teamId: SlackTeamId,
					files: ReadonlyArray<FileUpload>,
					uploaded: ReadonlyArray<SlackSentMessageRef>,
				) =>
					files.map((file, index) => {
						const uploadedFile = uploaded[index]
						const id = uploadedFile?.fileId ?? `upload:${index}`
						const fields: UploadedAttachmentFields = {
							provider: 'slack',
							tenant: TenantId.make(teamId),
							id,
							kind: file.mimeType?.startsWith('image/') ? 'image' : 'file',
							name: file.filename,
							size: file.data.byteLength,
							providerLocator: { id },
						}
						if (file.mimeType !== undefined) fields.mimeType = file.mimeType
						return Attachment.make({
							ref: AttachmentRef.make(fields),
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
									retryability: 'non_retryable',
								}),
							),
						),
					)
					const rendered = renderContent(input.content)
					const send = Effect.gen(function* () {
						if (rendered.files.length > 0) {
							const uploaded = yield* client
								.uploadFiles(slackUploadInput({ ref, text: rendered.text, files: rendered.files }))
								.pipe(
									Effect.tapError((error) =>
										Effect.logError('Slack provider file upload failed', error).pipe(
											Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
										),
									),
									Effect.catchTags({
										SlackTransportError: (error) =>
											Effect.fail(
												PostFailed.make({
													provider: 'slack',
													threadId: input.threadId,
													message: 'Slack file upload transport failed',
													retryability: slackErrorRetryability(error),
												}),
											),
										SlackApiError: (error) =>
											Effect.fail(
												PostFailed.make({
													provider: 'slack',
													threadId: input.threadId,
													message: 'Slack API rejected the file upload',
													retryability: slackErrorRetryability(error),
												}),
											),
									}),
								)
							const first = uploaded.at(0)
							if (first === undefined) {
								return yield* PostFailed.make({
									provider: 'slack',
									threadId: input.threadId,
									message: 'Slack returned no uploaded files',
									retryability: 'non_retryable',
								})
							}
							const attachments = uploadedAttachments(ref.teamId, rendered.files, uploaded)
							return yield* sentFromSlack({
								threadRef: slackThreadRef(ref, false),
								sentThreadId: input.threadId,
								text: rendered.text,
								degraded: rendered.degraded,
								channelId: first.channelId,
								ts: first.ts,
								botUserId: first.botUserId,
								attachments,
								raw: {
									channel: first.channelId,
									ts: first.ts,
									files: uploaded.flatMap((file) => (file.fileId === undefined ? [] : [file.fileId])),
								},
							})
						}
						const sent = yield* client.postMessage(slackPostInput(ref, rendered.text)).pipe(
							Effect.tapError((error) =>
								Effect.logError('Slack provider post failed', error).pipe(
									Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
								),
							),
							Effect.catchTags({
								SlackTransportError: (error) =>
									Effect.fail(
										PostFailed.make({
											provider: 'slack',
											threadId: input.threadId,
											message: 'Slack transport failed',
											retryability: slackErrorRetryability(error),
										}),
									),
								SlackApiError: (error) =>
									Effect.fail(
										PostFailed.make({
											provider: 'slack',
											threadId: input.threadId,
											message: 'Slack API rejected the post',
											retryability: slackErrorRetryability(error),
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
									retryability: 'non_retryable',
								}),
							),
						),
					)
					const rendered = renderContent(input.content)
					if (rendered.files.length > 0) {
						const uploaded = yield* client
							.uploadFiles(
								SlackFileUploadInput.make({
									teamId: address.teamId,
									channelId: address.channelId,
									initialComment: rendered.text,
									files: rendered.files,
								}),
							)
							.pipe(
								Effect.tapError((error) =>
									Effect.logError('Slack provider channel file upload failed', error).pipe(
										Effect.annotateLogs({ provider: 'slack', tenant: address.teamId }),
									),
								),
								Effect.catchTags({
									SlackTransportError: (error) =>
										Effect.fail(
											PostFailed.make({
												provider: 'slack',
												threadId: errorThreadId,
												message: 'Slack file upload transport failed',
												retryability: slackErrorRetryability(error),
											}),
										),
									SlackApiError: (error) =>
										Effect.fail(
											PostFailed.make({
												provider: 'slack',
												threadId: errorThreadId,
												message: 'Slack API rejected the file upload',
												retryability: slackErrorRetryability(error),
											}),
										),
								}),
							)
						const first = uploaded.at(0)
						if (first === undefined) {
							return yield* PostFailed.make({
								provider: 'slack',
								threadId: errorThreadId,
								message: 'Slack returned no uploaded files',
								retryability: 'non_retryable',
							})
						}
						const threadRef = slackThreadRef(
							SlackThreadRef.make({
								teamId: address.teamId,
								channelId: first.channelId,
								threadTs: first.ts,
							}),
							true,
						)
						return yield* sentFromSlack({
							threadRef,
							sentThreadId: threadRef.id,
							text: rendered.text,
							degraded: rendered.degraded,
							channelId: first.channelId,
							ts: first.ts,
							botUserId: first.botUserId,
							attachments: uploadedAttachments(address.teamId, rendered.files, uploaded),
							raw: {
								channel: first.channelId,
								ts: first.ts,
								files: uploaded.flatMap((file) => (file.fileId === undefined ? [] : [file.fileId])),
							},
						})
					}
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
								SlackTransportError: (error) =>
									Effect.fail(
										PostFailed.make({
											provider: 'slack',
											threadId: errorThreadId,
											message: 'Slack transport failed',
											retryability: slackErrorRetryability(error),
										}),
									),
								SlackApiError: (error) =>
									Effect.fail(
										PostFailed.make({
											provider: 'slack',
											threadId: errorThreadId,
											message: 'Slack API rejected the post',
											retryability: slackErrorRetryability(error),
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
						if (ref.threadTs === SlackDmConversationTs) return
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
							Effect.fail(
								HistoryFailed.make({
									provider: 'slack',
									message: 'invalid Slack thread id',
									retryability: 'non_retryable',
								}),
							),
						),
					)
					const history =
						ref.threadTs === SlackDmConversationTs
							? client.history(slackConversationHistoryInput(ref, input.options))
							: client.replies(slackRepliesInput(ref, input.options))
					return yield* history.pipe(
						Effect.flatMap(authors.messagePage),
						Effect.tapError((error) =>
							Effect.logError('Slack thread history failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', thread_id: input.threadId }),
							),
						),
						Effect.catchTags({
							UnknownTenant: () =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'unknown Slack workspace',
										retryability: 'non_retryable',
									}),
								),
							SlackTransportError: (error) =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack transport failed',
										retryability: slackErrorRetryability(error),
									}),
								),
							SlackApiError: (error) =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack API rejected the history request',
										retryability: slackErrorRetryability(error),
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
							Effect.fail(
								HistoryFailed.make({
									provider: 'slack',
									message: 'invalid Slack channel id',
									retryability: 'non_retryable',
								}),
							),
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
						Effect.flatMap(authors.messagePage),
						Effect.tapError((error) =>
							Effect.logError('Slack channel history failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', tenant: address.teamId }),
							),
						),
						Effect.catchTags({
							UnknownTenant: () =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'unknown Slack workspace',
										retryability: 'non_retryable',
									}),
								),
							SlackTransportError: (error) =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack transport failed',
										retryability: slackErrorRetryability(error),
									}),
								),
							SlackApiError: (error) =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack API rejected the history request',
										retryability: slackErrorRetryability(error),
									}),
								),
						}),
					)
				})

				const channelThreads = Effect.fn('slack.provider.channel_threads')(function* (
					input: ChannelThreadsInput,
				) {
					const address = yield* decodeSlackChannelId(input.channel.id).pipe(
						Effect.catchTag('InvalidSlackThreadId', () =>
							Effect.fail(
								HistoryFailed.make({
									provider: 'slack',
									message: 'invalid Slack channel id',
									retryability: 'non_retryable',
								}),
							),
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
						Effect.flatMap(authors.threadPage),
						Effect.tapError((error) =>
							Effect.logError('Slack channel thread listing failed', error).pipe(
								Effect.annotateLogs({ provider: 'slack', tenant: address.teamId }),
							),
						),
						Effect.catchTags({
							UnknownTenant: () =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'unknown Slack workspace',
										retryability: 'non_retryable',
									}),
								),
							SlackTransportError: (error) =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack transport failed',
										retryability: slackErrorRetryability(error),
									}),
								),
							SlackApiError: (error) =>
								Effect.fail(
									HistoryFailed.make({
										provider: 'slack',
										message: 'Slack API rejected the thread listing',
										retryability: slackErrorRetryability(error),
									}),
								),
						}),
					)
				})

				const metadataFailed = (message: string, retryability: 'retryable' | 'non_retryable') =>
					Effect.fail(MetadataFailed.make({ provider: 'slack', message, retryability }))

				const info = Effect.fn('slack.provider.info')(function* (input: InfoInput) {
					const ref = yield* decodeSlackThreadId(input.threadId).pipe(
						Effect.catchTag('InvalidSlackThreadId', () =>
							Effect.fail(ThreadGone.make({ threadId: input.threadId, retryability: 'non_retryable' })),
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
								UnknownTenant: () => metadataFailed('unknown Slack workspace', 'non_retryable'),
								SlackTransportError: (error) =>
									metadataFailed('Slack transport failed', slackErrorRetryability(error)),
								SlackApiError: (error) =>
									error.code === 'channel_not_found'
										? Effect.fail(
												ThreadGone.make({
													threadId: input.threadId,
													retryability: 'non_retryable',
												}),
											)
										: metadataFailed(
												'Slack API rejected the metadata request',
												slackErrorRetryability(error),
											),
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
							Effect.fail(
								ChannelGone.make({ channelId: input.channel.id, retryability: 'non_retryable' }),
							),
						),
					)
					return yield* client
						.channelInfo(
							SlackChannelInfoInput.make({ teamId: address.teamId, channelId: address.channelId }),
						)
						.pipe(
							Effect.tapError((error) =>
								Effect.logError('Slack channel info failed', error).pipe(
									Effect.annotateLogs({ provider: 'slack', tenant: input.channel.tenant }),
								),
							),
							Effect.catchTags({
								UnknownTenant: () => metadataFailed('unknown Slack workspace', 'non_retryable'),
								SlackTransportError: (error) =>
									metadataFailed('Slack transport failed', slackErrorRetryability(error)),
								SlackApiError: (error) =>
									error.code === 'channel_not_found'
										? Effect.fail(
												ChannelGone.make({
													channelId: input.channel.id,
													retryability: 'non_retryable',
												}),
											)
										: metadataFailed(
												'Slack API rejected the metadata request',
												slackErrorRetryability(error),
											),
							}),
						)
				})

				const downloadAttachment = Effect.fn('slack.provider.download_attachment')(function* (input: {
					readonly attachment: AttachmentRef
				}) {
					const teamId = SlackTeamId.make(input.attachment.tenant)
					return yield* client
						.downloadFile(SlackFileDownloadInput.make({ teamId, attachment: input.attachment }))
						.pipe(
							Effect.tapError((error) =>
								Effect.logError('Slack attachment download failed', error).pipe(
									Effect.annotateLogs({ provider: 'slack', tenant: input.attachment.tenant }),
								),
							),
							Effect.catchTags({
								SlackTransportError: () =>
									Effect.fail(
										FileReadFailed.make({
											provider: 'slack',
											message: 'Slack file transport failed',
										}),
									),
								SlackApiError: () =>
									Effect.fail(
										FileReadFailed.make({
											provider: 'slack',
											message: 'Slack file download failed',
										}),
									),
							}),
						)
				})

				const edit = Effect.fn('slack.provider.edit')(function* (input: EditInput) {
					const ref = yield* decodeSlackThreadId(input.threadId).pipe(
						Effect.mapError(() =>
							EditFailed.make({
								provider: 'slack',
								threadId: input.threadId,
								message: 'invalid Slack thread id',
							}),
						),
					)
					const rendered = renderContent(input.content)
					if (rendered.files.length > 0) {
						return yield* EditFailed.make({
							provider: 'slack',
							threadId: input.threadId,
							message: 'Slack does not support replacing files while editing a message',
						})
					}
					const sent = yield* client
						.updateMessage(
							SlackUpdateMessageInput.make({
								teamId: ref.teamId,
								channelId: ref.channelId,
								ts: SlackMessageTs.make(input.messageRef),
								text: rendered.text,
							}),
						)
						.pipe(
							Effect.tapError((error) => Effect.logError('Slack provider edit failed', error)),
							Effect.mapError(() =>
								EditFailed.make({
									provider: 'slack',
									threadId: input.threadId,
									message: 'Slack edit failed',
								}),
							),
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

				const stream = <E, R>(input: StreamInput, chunks: Stream.Stream<StreamChunk, E, R>) =>
					Effect.gen(function* () {
						const ref = yield* decodeSlackThreadId(input.threadId).pipe(
							Effect.mapError(() =>
								PostFailed.make({
									provider: 'slack',
									threadId: input.threadId,
									message: 'invalid Slack thread id',
									retryability: 'non_retryable',
								}),
							),
						)
						let mode: 'native' | 'post_and_edit' = options.streaming ?? 'native'
						let nativeRef: import('./Schema.ts').SlackStreamRef | undefined
						let fallbackRef: SlackSentMessageRef | undefined
						let accumulated = ''
						let lastRendered = ''
						const degraded = new Set<string>()
						if (
							mode === 'native' &&
							((!ref.channelId.startsWith('D') && input.recipientUserId === undefined) ||
								ref.threadTs === SlackDmConversationTs)
						) {
							mode = 'post_and_edit'
							degraded.add('native_streaming')
						}

						const fail = (message: string, retryability: 'retryable' | 'non_retryable' = 'retryable') =>
							PostFailed.make({ provider: 'slack', threadId: input.threadId, message, retryability })

						const fallbackText = (text: string, final: boolean) =>
							Effect.gen(function* () {
								const rendered = final ? text : renderStreamingMarkdown(text)
								if (rendered.length === 0 || rendered === lastRendered) return
								if (fallbackRef === undefined) {
									fallbackRef = yield* client
										.postMessage(slackPostInput(ref, rendered))
										.pipe(
											Effect.mapError((error) =>
												fail(
													'Slack streaming fallback post failed',
													streamErrorRetryability(error),
												),
											),
										)
								} else {
									if (!final) yield* Effect.sleep(streamEditIntervalMs)
									fallbackRef = yield* client
										.updateMessage({
											teamId: ref.teamId,
											channelId: fallbackRef.channelId,
											ts: fallbackRef.ts,
											text: rendered,
										})
										.pipe(
											Effect.mapError((error) =>
												fail(
													'Slack streaming fallback edit failed',
													streamErrorRetryability(error),
												),
											),
										)
								}
								lastRendered = rendered
							})

						const processFallback = (chunk: StreamChunk) =>
							Match.value(chunk).pipe(
								Match.tagsExhaustive({
									MarkdownTextChunk: ({ text }) => {
										accumulated += text
										return fallbackText(accumulated, false)
									},
									TaskUpdateChunk: () => Effect.sync(() => degraded.add('task_update')),
									PlanUpdateChunk: () => Effect.sync(() => degraded.add('plan_update')),
								}),
							)

						const processNative = (chunk: StreamChunk): Effect.Effect<void, PostFailed> =>
							Effect.gen(function* () {
								const markdownText = Match.value(chunk).pipe(
									Match.tagsExhaustive({
										MarkdownTextChunk: ({ text }) => text,
										TaskUpdateChunk: () => undefined,
										PlanUpdateChunk: () => undefined,
									}),
								)
								if (markdownText !== undefined) accumulated += markdownText
								if (nativeRef === undefined) {
									const startFields = {
										teamId: ref.teamId,
										channelId: ref.channelId,
										threadTs: ref.threadTs,
										chunks: [chunk],
									} as const
									const startInput =
										input.recipientUserId === undefined
											? SlackStartStreamInput.make(startFields)
											: SlackStartStreamInput.make({
													...startFields,
													recipient: { userId: input.recipientUserId, teamId: ref.teamId },
												})
									const started = yield* client.startStream(startInput).pipe(
										Effect.map(Option.some),
										Effect.catchTags({
											UnknownTenant: () =>
												Effect.fail(fail('unknown Slack workspace', 'non_retryable')),
											SlackTransportError: (error) =>
												Effect.fail(
													fail(
														'Slack native stream transport failed',
														streamErrorRetryability(error),
													),
												),
											SlackApiError: (error) =>
												nativeStreamingUnsupported.has(error.code)
													? Effect.succeed(Option.none())
													: Effect.fail(
															fail(
																'Slack native stream start failed',
																streamErrorRetryability(error),
															),
														),
										}),
									)
									const didFallback = yield* Option.match(started, {
										onNone: () =>
											Effect.gen(function* () {
												mode = 'post_and_edit'
												degraded.add('native_streaming')
												yield* Match.value(chunk).pipe(
													Match.tagsExhaustive({
														MarkdownTextChunk: () => fallbackText(accumulated, false),
														TaskUpdateChunk: () => processFallback(chunk),
														PlanUpdateChunk: () => processFallback(chunk),
													}),
												)
												return true
											}),
										onSome: (streamRef) =>
											Effect.sync(() => {
												nativeRef = streamRef
												return false
											}),
									})
									if (didFallback) return
									return
								}
								const append = client.appendStream(
									SlackAppendStreamInput.make({
										teamId: ref.teamId,
										stream: nativeRef,
										chunks: [chunk],
									}),
								)
								if (markdownText !== undefined) {
									yield* append.pipe(
										Effect.mapError((error) =>
											fail('Slack native stream append failed', streamErrorRetryability(error)),
										),
									)
								} else {
									yield* append.pipe(
										Effect.tapError((error) =>
											Effect.logWarning('Slack structured stream chunk failed', error),
										),
										Effect.catch(() =>
											Effect.sync(() =>
												degraded.add(
													Match.value(chunk).pipe(
														Match.tagsExhaustive({
															MarkdownTextChunk: () => 'markdown_text',
															TaskUpdateChunk: () => 'task_update',
															PlanUpdateChunk: () => 'plan_update',
														}),
													),
												),
											),
										),
									)
								}
							})

						const source = chunks.pipe(
							Stream.tapError((error) =>
								Effect.logError('Slack stream source failed').pipe(
									Effect.annotateLogs({
										thread_id: input.threadId,
										retryability: Schema.is(RetryabilityMetadata)(error)
											? error.retryability
											: 'unknown',
									}),
								),
							),
							Stream.mapError((error) =>
								fail(
									'stream source failed',
									Schema.is(RetryabilityMetadata)(error) ? error.retryability : 'retryable',
								),
							),
						)
						const consume = Stream.runForEach(source, (chunk) =>
							mode === 'native' ? processNative(chunk) : processFallback(chunk),
						)
						yield* consume.pipe(
							Effect.onExit((exit) =>
								Exit.isSuccess(exit) || nativeRef === undefined
									? Effect.void
									: client
											.stopStream(
												SlackStopStreamInput.make({
													teamId: ref.teamId,
													stream: nativeRef,
													chunks: [],
												}),
											)
											.pipe(Effect.tapError(Effect.logWarning), Effect.ignore),
							),
						)

						if (mode === 'native' && nativeRef !== undefined) {
							const stopped = yield* client
								.stopStream(
									SlackStopStreamInput.make({ teamId: ref.teamId, stream: nativeRef, chunks: [] }),
								)
								.pipe(
									Effect.mapError((error) =>
										fail('Slack native stream stop failed', streamErrorRetryability(error)),
									),
								)
							return yield* sentFromSlack({
								threadRef: slackThreadRef(ref, false),
								sentThreadId: input.threadId,
								text: accumulated,
								degraded: [...degraded],
								channelId: stopped.channelId,
								ts: stopped.ts,
								botUserId: stopped.botUserId,
							})
						}
						if (fallbackRef === undefined) {
							degraded.add('empty_stream')
							yield* fallbackText('…', true)
						} else {
							yield* fallbackText(accumulated, true)
						}
						const sent = fallbackRef
						if (sent === undefined) return yield* fail('Slack stream produced no message', 'non_retryable')
						return yield* sentFromSlack({
							threadRef: slackThreadRef(ref, false),
							sentThreadId: input.threadId,
							text: accumulated.length === 0 ? '…' : accumulated,
							degraded: [...degraded],
							channelId: sent.channelId,
							ts: sent.ts,
							botUserId: sent.botUserId,
						})
					}).pipe(
						Effect.tapError((error) => Effect.logError('Slack provider stream failed', error)),
						Effect.ensuring(
							decodeSlackThreadId(input.threadId).pipe(
								Effect.flatMap((ref) => endTypingIfStarted(ref, input.threadId)),
								Effect.ignore,
							),
						),
					)

				const deleteMessage = Effect.fn('slack.provider.delete')(function* (input: DeleteInput) {
					const ref = yield* decodeSlackThreadId(input.threadId).pipe(
						Effect.mapError(() =>
							DeleteFailed.make({
								provider: 'slack',
								threadId: input.threadId,
								message: 'invalid Slack thread id',
							}),
						),
					)
					yield* client
						.deleteMessage(
							SlackDeleteMessageInput.make({
								teamId: ref.teamId,
								channelId: ref.channelId,
								ts: SlackMessageTs.make(input.messageRef),
							}),
						)
						.pipe(
							Effect.tapError((error) => Effect.logError('Slack provider delete failed', error)),
							Effect.mapError(() =>
								DeleteFailed.make({
									provider: 'slack',
									threadId: input.threadId,
									message: 'Slack delete failed',
								}),
							),
						)
				})

				const react = (method: 'add' | 'remove', input: ReactInput) =>
					Effect.gen(function* () {
						const ref = yield* decodeSlackThreadId(input.threadId).pipe(
							Effect.mapError(() =>
								ReactionFailed.make({
									provider: 'slack',
									threadId: input.threadId,
									message: 'invalid Slack thread id',
								}),
							),
						)
						const operation = method === 'add' ? client.addReaction : client.removeReaction
						yield* operation(
							SlackReactionInput.make({
								teamId: ref.teamId,
								channelId: ref.channelId,
								ts: SlackMessageTs.make(input.messageRef),
								emoji: renderEmoji(input.emoji),
							}),
						).pipe(
							Effect.tapError((error) =>
								Effect.logError(`Slack provider reaction ${method} failed`, error),
							),
							Effect.mapError(() =>
								ReactionFailed.make({
									provider: 'slack',
									threadId: input.threadId,
									message: `Slack reaction ${method} failed`,
								}),
							),
						)
					})

				const openDM = Effect.fn('slack.provider.open_dm')(function* (input: OpenDMInput) {
					const teamId = SlackTeamId.make(input.tenant)
					const channelId = yield* client
						.openDM(SlackOpenDMInput.make({ teamId, userId: input.user.userId }))
						.pipe(
							Effect.tapError((error) => Effect.logError('Slack provider open DM failed', error)),
							Effect.catchTags({
								SlackTransportError: (error) =>
									Effect.fail(
										DirectMessageOpenFailed.make({
											provider: 'slack',
											tenant: input.tenant,
											userId: input.user.userId,
											message: 'Slack transport failed while opening DM',
											retryability: slackErrorRetryability(error),
										}),
									),
								SlackApiError: (error) =>
									Effect.fail(
										DirectMessageOpenFailed.make({
											provider: 'slack',
											tenant: input.tenant,
											userId: input.user.userId,
											message: 'Slack API rejected opening the DM',
											retryability: slackErrorRetryability(error),
										}),
									),
							}),
						)
					return Thread.fromRef(slackDmConversationRef(teamId, channelId, 'im'))
				})

				const postEphemeral = Effect.fn('slack.provider.post_ephemeral')(function* (input: PostEphemeralInput) {
					const ref = yield* decodeSlackThreadId(input.threadId).pipe(
						Effect.mapError(() =>
							PostFailed.make({
								provider: 'slack',
								threadId: input.threadId,
								message: 'invalid Slack thread id',
								retryability: 'non_retryable',
							}),
						),
					)
					const rendered = renderContent(input.content)
					const send = client.postEphemeral(slackEphemeralInput(ref, input.user.userId, rendered.text)).pipe(
						Effect.map((sent) => ({ sent, fallback: false as const })),
						Effect.tapError((error) => Effect.logError('Slack provider ephemeral post failed', error)),
						Effect.catchTag(
							'SlackApiError',
							(
								error,
							): Effect.Effect<
								{ readonly sent: SentMessage; readonly fallback: true },
								PostFailed | SlackApiError
							> =>
								error.code === 'channel_type_not_supported' &&
								Schema.is(EphemeralFallbackToDm)(input.fallback)
									? Effect.gen(function* () {
											const dm = yield* openDM({
												provider: 'slack',
												tenant: TenantId.make(ref.teamId),
												user: input.user,
											})
											const sent = yield* post({ threadId: dm.ref.id, content: input.content })
											return { sent, fallback: true as const }
										}).pipe(
											Effect.tapError(Effect.logError),
											Effect.mapError((failure) =>
												PostFailed.make({
													provider: 'slack',
													threadId: input.threadId,
													message: 'Slack ephemeral DM fallback failed',
													retryability: Schema.is(RetryabilityMetadata)(failure)
														? failure.retryability
														: 'non_retryable',
												}),
											),
										)
									: Effect.fail(error),
						),
						Effect.catchTags({
							SlackTransportError: (error) =>
								Effect.fail(
									PostFailed.make({
										provider: 'slack',
										threadId: input.threadId,
										message: 'Slack ephemeral transport failed',
										retryability: slackErrorRetryability(error),
									}),
								),
							SlackApiError: (error) =>
								Effect.fail(
									PostFailed.make({
										provider: 'slack',
										threadId: input.threadId,
										message: 'Slack API rejected the ephemeral post',
										retryability: slackErrorRetryability(error),
									}),
								),
						}),
					)
					const result = yield* send.pipe(Effect.ensuring(endTypingIfStarted(ref, input.threadId)))
					if (result.fallback) return EphemeralResult.make({ sent: result.sent, usedFallback: true })
					const sent = result.sent
					const delivered = yield* sentFromSlack({
						threadRef: slackThreadRef(ref, false),
						sentThreadId: input.threadId,
						text: rendered.text,
						degraded: [...rendered.degraded, ...(rendered.files.length === 0 ? [] : ['files'])],
						channelId: sent.channelId,
						ts: sent.ts,
						botUserId: sent.botUserId,
					})
					return EphemeralResult.make({ sent: delivered, usedFallback: false })
				})

				return Slack.of({
					capabilities:
						options.streaming === 'post_and_edit'
							? Capabilities.make({ ...SlackCapabilities, streaming: 'post_and_edit' })
							: SlackCapabilities,
					post,
					postToChannel,
					edit,
					delete: deleteMessage,
					stream,
					startThreadTyping,
					startChannelTyping: () => Effect.void,
					setSessionStatus: (input) =>
						client.setSessionStatus(input).pipe(
							Effect.tapError((error) => Effect.logError('Slack session status failed', error)),
							Effect.catchTags({
								SlackTransportError: () =>
									Effect.fail(
										StatusFailed.make({
											provider: 'slack',
											threadId: encodeSlackThreadId(
												SlackThreadRef.make({
													teamId: input.teamId,
													channelId: input.channelId,
													threadTs: input.threadTs,
												}),
											),
											message: 'Slack transport failed',
										}),
									),
								SlackApiError: () =>
									Effect.fail(
										StatusFailed.make({
											provider: 'slack',
											threadId: encodeSlackThreadId(
												SlackThreadRef.make({
													teamId: input.teamId,
													channelId: input.channelId,
													threadTs: input.threadTs,
												}),
											),
											message: 'Slack API rejected the session status',
										}),
									),
							}),
						),
					addReaction: (input) => react('add', input),
					removeReaction: (input) => react('remove', input),
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
								options === undefined
									? { channel: input.channel }
									: { channel: input.channel, options },
							),
						),
					info,
					channelInfo,
					getUser: users.getUser,
					downloadAttachment,
					openDM,
					postEphemeral,
				})
			}),
		).pipe(Layer.provideMerge(SlackAuthors.layer))

	static readonly layer = Slack.layerWith()
}

export { SlackCapabilities }
