import { Config, Context, Effect, Layer, Match, Option, Predicate, Schema, Stream, Struct, type Types } from 'effect'
import * as HttpClient from 'effect/http/HttpClient'
import type * as UrlParams from 'effect/http/UrlParams'

import { completeSlackFileUpload, type SlackFileUploadDestination } from './api/CompleteSlackFileUpload'
import { downloadSlackFile, readBoundedSlackFileBytes } from './api/DownloadSlackFile'
import { getSlackFileUploadUrl } from './api/GetSlackFileUploadUrl'
import { SlackHttpClient, type SlackWebApiError } from './api/SlackHttpClient'
import { uploadSlackFileBytes } from './api/UploadSlackFileBytes'
import {
	SlackApiError,
	type SlackApiOperation,
	type SlackApiOperations,
	type SlackDownloadFileRequest,
	SlackFileAuthorizationError,
	SlackFileSizeLimitExceeded,
	type SlackMessageRequest,
	type SlackParticipantRequest,
} from './SlackApiContract'
import {
	chronological,
	contentText,
	nextCursor,
	planBlocks,
	planFallbackText,
	reportRejectedToken,
	SlackAuthResponse,
	SlackBotResponse,
	type SlackChannelInfoFields,
	SlackChannelResponse,
	type SlackHistoryBody,
	SlackMessageSnapshot,
	SlackMessagesResponse,
	type SlackParticipantFields,
	type SlackPostMessageRequest,
	SlackPostMessageResponse,
	type SlackRepliesBody,
	SlackResponse,
	type SlackStartStreamBody,
	slackHistoryPageSize,
	streamChunkJson,
	type SlackUser,
	SlackUserResponse,
} from './SlackApiWire'
import type { SlackMessageTs } from './SlackIdentity'
import {
	SlackChannelInfo,
	SlackChannelRef,
	type SlackContent,
	slackFileRecordFromMetadata,
	SlackMarkdownContent,
	SlackMessageRecord,
	SlackMessageRef,
	SlackParticipant,
	SlackSentMessageRecord,
	SlackThreadInfo,
	SlackThreadRef,
	type SlackUploadFileInput,
	SlackUserId,
} from './SlackModels'
import { SlackFileMetadata } from './SlackWebhookEventSchemas'

export * from './SlackApiContract'

/**
 * Application-wide Slack operations on plain records. Callback objects such as `SlackThread` call it and wrap
 * the records it returns.
 *
 * `layer` needs `SlackHttpClient` and `HttpClient`; `SlackApiLive` provides both.
 */
export class SlackApi extends Context.Service<SlackApi, SlackApiOperations>()('@humanlayer/channels-slack/SlackApi', {
	/** Reads the optional `SLACK_BOT_USER_ID`; without it the first call that needs the bot's user ID asks `auth.test`. */
	make: Effect.gen(function* () {
		const client = yield* SlackHttpClient
		const http = yield* HttpClient.HttpClient
		const configuredBotUserId = yield* Config.option(Config.String('SLACK_BOT_USER_ID'))

		const narrowMissingScope = <A>(effect: Effect.Effect<A, SlackWebApiError>): Effect.Effect<A, SlackApiError> =>
			effect.pipe(
				Effect.catchTag('SlackMissingScopeError', (error) =>
					Effect.fail(SlackApiError.make({ operation: error.operation, message: 'missing_scope' })),
				),
				reportRejectedToken,
			)

		const callSlack = <A>(
			operation: SlackApiOperation,
			method: string,
			body: Schema.Json,
			schema: Schema.Codec<A, unknown>,
		): Effect.Effect<A, SlackApiError> =>
			narrowMissingScope(client.postJson({ operation, method, params: body, response: schema }))

		const callSlackGet = <A>(
			operation: SlackApiOperation,
			method: string,
			query: UrlParams.CoercibleRecord,
			schema: Schema.Codec<A, unknown>,
		): Effect.Effect<A, SlackApiError> =>
			narrowMissingScope(client.get({ operation, method, params: query, response: schema }))

		const authUserId = yield* Effect.cached(
			Option.match(configuredBotUserId, {
				onNone: () =>
					callSlack('resolve_participant', 'auth.test', {}, SlackAuthResponse).pipe(
						Effect.flatMap((response) =>
							Predicate.isUndefined(response.user_id)
								? Effect.fail(
										SlackApiError.make({
											operation: 'resolve_participant',
											message: 'Slack did not return the bot user ID',
										}),
									)
								: Effect.succeed(response.user_id),
						),
					),
				onSome: Effect.succeed,
			}),
		)
		const setSessionStatus = (
			operation: 'post' | 'start_typing' | 'clear_thread_status',
			thread: SlackThreadRef,
			status: 'active' | 'processing',
		): Effect.Effect<void, SlackApiError> =>
			callSlack(
				operation,
				'agents.sessions.setStatus',
				{ channel_id: thread.channelId, thread_ts: thread.threadTs, status },
				SlackResponse,
			).pipe(Effect.asVoid)
		const participantFromUser = (user: typeof SlackUser.Type, ownUserId: string) => {
			const fields: SlackParticipantFields = {
				userId: SlackUserId.make(user.id),
				userName: user.profile?.display_name ?? user.name ?? user.id,
				fullName: user.profile?.real_name ?? user.real_name ?? user.name ?? user.id,
				isBot: user.is_bot ?? false,
				isMe: user.id === ownUserId,
			}
			if (Predicate.isNotUndefined(user.team_id)) fields.teamId = user.team_id
			return SlackParticipant.make(fields)
		}

		const resolveParticipant = Effect.fn('slack.api.resolve_participant')(function* (
			request: SlackParticipantRequest,
		) {
			const ownUserId = yield* authUserId
			if (Predicate.isNotUndefined(request.userId)) {
				const response = yield* callSlackGet(
					'resolve_participant',
					'users.info',
					{ user: request.userId },
					SlackUserResponse,
				)
				const user = response.user
				if (Predicate.isUndefined(user)) {
					return yield* SlackApiError.make({
						operation: 'resolve_participant',
						message: 'Slack did not return the requested user',
					})
				}
				return participantFromUser(user, ownUserId)
			}
			if (Predicate.isNotUndefined(request.botId)) {
				const response = yield* callSlackGet(
					'resolve_participant',
					'bots.info',
					{ bot: request.botId, team_id: request.teamId },
					SlackBotResponse,
				)
				const bot = response.bot
				if (Predicate.isUndefined(bot)) {
					return yield* SlackApiError.make({
						operation: 'resolve_participant',
						message: 'Slack did not return the requested bot',
					})
				}
				if (Predicate.isNotUndefined(bot.user_id)) {
					const userResponse = yield* callSlackGet(
						'resolve_participant',
						'users.info',
						{ user: bot.user_id },
						SlackUserResponse,
					)
					if (Predicate.isNotUndefined(userResponse.user)) {
						return participantFromUser(userResponse.user, ownUserId)
					}
				}
				return SlackParticipant.make({
					userId: SlackUserId.make(bot.id),
					userName: bot.name ?? bot.id,
					fullName: bot.name ?? bot.id,
					isBot: true,
					isMe: false,
				})
			}
			return yield* SlackApiError.make({
				operation: 'resolve_participant',
				message: 'A Slack user ID or bot ID is required',
			})
		})

		const normalizeMessage = Effect.fn('slack.api.normalize_message')(function* (
			thread: SlackThreadRef,
			snapshot: SlackMessageSnapshot,
		) {
			const author = yield* Predicate.isNotUndefined(snapshot.user)
				? resolveParticipant({ teamId: thread.teamId, userId: snapshot.user })
				: Predicate.isNotUndefined(snapshot.bot_id)
					? resolveParticipant({ teamId: thread.teamId, botId: snapshot.bot_id })
					: Effect.succeed(
							SlackParticipant.make({
								userId: SlackUserId.make('unknown'),
								userName: 'unknown',
								fullName: 'Unknown Slack participant',
								isBot: false,
								isMe: false,
							}),
						)
			return SlackMessageRecord.make({
				ref: SlackMessageRef.make({
					teamId: thread.teamId,
					channelId: thread.channelId,
					messageTs: snapshot.ts,
				}),
				thread,
				author,
				content: SlackMarkdownContent.make({ markdown: snapshot.text ?? '' }),
				files: (snapshot.files ?? []).map((file) => slackFileRecordFromMetadata(thread.teamId, file)),
				metadata: {},
				...Struct.renameKeys(Struct.pick(snapshot, ['user_team']), { user_team: 'authorTeamId' }),
			})
		})

		const requireFilesWrite = <A>(effect: Effect.Effect<A, SlackWebApiError, SlackHttpClient>) =>
			effect.pipe(
				Effect.catchTag('SlackMissingScopeError', (error) =>
					Effect.logWarning('Slack bot token cannot upload files').pipe(
						Effect.annotateLogs({ operation: error.operation, needed_scope: error.needed ?? 'unknown' }),
						Effect.andThen(
							Effect.fail(
								SlackFileAuthorizationError.make({
									operation: error.operation,
									requiredScope: 'files:write',
									retryable: false,
								}),
							),
						),
					),
				),
				Effect.provideService(SlackHttpClient, client),
			)

		const uploadFile = Effect.fn('slack.api.upload_file')(function* (
			destination: SlackFileUploadDestination,
			input: SlackUploadFileInput,
		) {
			const target = yield* requireFilesWrite(getSlackFileUploadUrl(input))
			yield* uploadSlackFileBytes({ target, bytes: input.bytes }).pipe(
				Effect.provideService(HttpClient.HttpClient, http),
			)
			const metadata = yield* requireFilesWrite(completeSlackFileUpload({ target, destination, input }))
			const file = slackFileRecordFromMetadata(
				destination.channel.teamId,
				SlackFileMetadata.make({ ...metadata, name: input.filename, size: input.bytes.byteLength }),
			)
			yield* Effect.logInfo('Slack file uploaded').pipe(
				Effect.annotateLogs({
					channel_id: destination.channel.channelId,
					thread_ts: destination.threadTs ?? 'none',
					file_id: file.ref.fileId,
					bytes: input.bytes.byteLength,
				}),
			)
			return file
		})

		const openDownload = (request: SlackDownloadFileRequest) =>
			downloadSlackFile(request).pipe(Effect.provideService(SlackHttpClient, client))

		const listThreadMessagesPage = Effect.fn('slack.api.list_thread_messages_page')(function* (
			thread: SlackThreadRef,
			cursor?: string,
		) {
			const body: SlackRepliesBody = {
				channel: thread.channelId,
				ts: thread.threadTs,
				limit: slackHistoryPageSize,
			}
			if (Predicate.isNotUndefined(cursor)) body.cursor = cursor
			const response = yield* callSlackGet(
				'list_thread_messages',
				'conversations.replies',
				body,
				SlackMessagesResponse,
			)
			const messages = yield* Effect.forEach(response.messages ?? [], (snapshot) =>
				normalizeMessage(thread, snapshot),
			)
			return [messages, nextCursor(response.response_metadata)] as const
		})

		const listThreadMessages = Effect.fn('slack.api.list_thread_messages')(function* (thread: SlackThreadRef) {
			return yield* Stream.paginate(Option.none<string>(), (cursor) =>
				listThreadMessagesPage(thread, Option.getOrUndefined(cursor)).pipe(
					Effect.map(([messages, next]) => [messages, Option.map(next, Option.some)] as const),
				),
			).pipe(
				Stream.runCollect,
				Effect.map((messages) => chronological(Array.from(messages))),
			)
		})

		const getMessage = Effect.fn('slack.api.get_message')(function* (request: SlackMessageRequest) {
			const response = yield* callSlackGet(
				'get_message',
				'conversations.replies',
				{
					channel: request.thread.channelId,
					ts: request.thread.threadTs,
					oldest: request.message.messageTs,
					latest: request.message.messageTs,
					inclusive: true,
					limit: 1,
				},
				SlackMessagesResponse,
			)
			const snapshot = (response.messages ?? []).find((message) => message.ts === request.message.messageTs)
			if (Predicate.isUndefined(snapshot)) {
				return yield* SlackApiError.make({ operation: 'get_message', message: 'Slack message was not found' })
			}
			return yield* normalizeMessage(request.thread, snapshot)
		})

		const postMessage = Effect.fn('slack.api.post_message')(function* (input: {
			readonly operation: 'post' | 'post_to_channel'
			readonly thread?: SlackThreadRef
			readonly channel: SlackChannelRef
			readonly content: SlackContent
		}) {
			const body: Types.Mutable<typeof SlackPostMessageRequest.Type> = {
				channel: input.channel.channelId,
				text: contentText(input.content),
				mrkdwn: Match.value(input.content).pipe(
					Match.discriminatorsExhaustive('_tag')({
						SlackMarkdownContent: () => true,
						SlackPlainTextContent: () => false,
					}),
				),
			}
			if (Predicate.isNotUndefined(input.thread)) body.thread_ts = input.thread.threadTs
			const response = yield* callSlack(input.operation, 'chat.postMessage', body, SlackPostMessageResponse)
			if (Predicate.isUndefined(response.ts)) {
				return yield* SlackApiError.make({
					operation: input.operation,
					message: 'Slack did not return a message ID',
				})
			}
			const thread =
				input.thread ??
				({
					teamId: input.channel.teamId,
					channelId: input.channel.channelId,
					threadTs: response.ts,
					isDm: input.channel.isDm,
				} as const)
			const snapshot =
				response.message ??
				SlackMessageSnapshot.make({
					ts: response.ts,
					text: contentText(input.content),
					user: yield* authUserId,
				})
			const message = yield* normalizeMessage(thread, snapshot)
			if (Predicate.isNotUndefined(input.thread)) {
				yield* setSessionStatus('post', input.thread, 'active').pipe(
					Effect.catchTag('SlackApiError', (error) =>
						Effect.logWarning('Could not mark Slack agent session active after posting').pipe(
							Effect.annotateLogs({ operation: error.operation }),
						),
					),
				)
			}
			return SlackSentMessageRecord.make({ ref: message.ref, message })
		})

		const service: SlackApiOperations = {
			listParticipants: ({ thread }) =>
				listThreadMessages(thread).pipe(
					Effect.map((messages) => {
						const seen = new Set<string>()
						return messages.flatMap(({ author }) => {
							if (author.isMe || author.isBot || seen.has(author.userId)) return []
							seen.add(author.userId)
							return [author]
						})
					}),
				),
			listThreadMessages: ({ thread }) => listThreadMessages(thread),
			listChannelMessagesBeforeThread: ({ thread, count }) => {
				return Stream.paginate(Option.none<string>(), (cursor) => {
					const body: SlackHistoryBody = {
						channel: thread.channelId,
						latest: thread.threadTs,
						inclusive: false,
						limit: Math.min(count, slackHistoryPageSize),
					}
					if (Option.isSome(cursor)) body.cursor = cursor.value
					return callSlackGet(
						'list_channel_messages_before_thread',
						'conversations.history',
						body,
						SlackMessagesResponse,
					).pipe(
						Effect.flatMap((response) =>
							Effect.forEach(response.messages ?? [], (snapshot) => {
								const messageThread = SlackThreadRef.make({
									teamId: thread.teamId,
									channelId: thread.channelId,
									threadTs: snapshot.thread_ts ?? snapshot.ts,
									isDm: thread.isDm,
								})
								return normalizeMessage(messageThread, snapshot)
							}).pipe(
								Effect.map(
									(messages) =>
										[
											messages,
											Option.map(nextCursor(response.response_metadata), Option.some),
										] as const,
								),
							),
						),
					)
				}).pipe(
					Stream.take(count),
					Stream.runCollect,
					Effect.map((messages) => chronological(Array.from(messages))),
				)
			},
			postToThread: ({ thread, content }) =>
				postMessage({
					operation: 'post',
					thread,
					channel: SlackChannelRef.make({
						teamId: thread.teamId,
						channelId: thread.channelId,
						isDm: thread.isDm,
					}),
					content,
				}),
			postToChannel: ({ channel, content }) => postMessage({ operation: 'post_to_channel', channel, content }),
			updateMessage: ({ message, content }) =>
				callSlack(
					'update_message',
					'chat.update',
					{ channel: message.channelId, ts: message.messageTs, text: contentText(content) },
					SlackResponse,
				).pipe(Effect.asVoid, Effect.withSpan('slack.api.update_message')),
			deleteMessage: ({ message }) =>
				callSlack(
					'delete_message',
					'chat.delete',
					{ channel: message.channelId, ts: message.messageTs },
					SlackResponse,
				).pipe(Effect.asVoid, Effect.withSpan('slack.api.delete_message')),
			startTyping: ({ thread }) => setSessionStatus('start_typing', thread, 'processing'),
			/**
			 * Status text goes through the Assistants API, whose bridge shows it in the agent-session
			 * loading line; `agents.sessions.setStatus` only knows lifecycle states.
			 */
			setThreadStatus: ({ thread, status }) =>
				callSlack(
					'set_thread_status',
					'assistant.threads.setStatus',
					{ channel_id: thread.channelId, thread_ts: thread.threadTs, status, loading_messages: [status] },
					SlackResponse,
				).pipe(Effect.asVoid, Effect.withSpan('slack.api.set_thread_status')),
			/** The agent-session lifecycle has no "clear"; `active` ends the loading state. */
			clearThreadStatus: ({ thread }) =>
				setSessionStatus('clear_thread_status', thread, 'active').pipe(
					Effect.withSpan('slack.api.clear_thread_status'),
				),
			stream: (thread, chunks) =>
				Effect.gen(function* () {
					const recipient = yield* thread.isDm
						? Effect.succeed(Option.none<SlackParticipant>())
						: listThreadMessages(thread).pipe(
								Effect.flatMap((messages) => {
									const participant = Array.from(messages)
										.reverse()
										.find((message) => !message.author.isMe && !message.author.isBot)?.author
									return Predicate.isUndefined(participant)
										? Effect.fail(
												SlackApiError.make({
													operation: 'stream',
													message: 'Could not determine the Slack stream recipient',
												}),
											)
										: Effect.succeedSome(participant)
								}),
							)
					const startStreamBody = (streamChunks: ReadonlyArray<Schema.Json>): SlackStartStreamBody => {
						const body: SlackStartStreamBody = {
							channel: thread.channelId,
							thread_ts: thread.threadTs,
							chunks: streamChunks,
						}
						if (Option.isSome(recipient)) {
							body.recipient_user_id = recipient.value.userId
							body.recipient_team_id = recipient.value.teamId ?? thread.teamId
						}
						return body
					}
					const started = yield* chunks.pipe(
						Stream.runFoldEffect(
							() => Option.none<SlackMessageTs>(),
							(current, chunk) =>
								Option.match(current, {
									onNone: () =>
										callSlack(
											'stream',
											'chat.startStream',
											startStreamBody([streamChunkJson(chunk)]),
											SlackPostMessageResponse,
										).pipe(
											Effect.flatMap((response) =>
												Predicate.isUndefined(response.ts)
													? Effect.fail(
															SlackApiError.make({
																operation: 'stream',
																message: 'Slack did not start the stream',
															}),
														)
													: Effect.succeedSome(response.ts),
											),
										),
									onSome: (messageTs) =>
										callSlack(
											'stream',
											'chat.appendStream',
											{
												channel: thread.channelId,
												ts: messageTs,
												chunks: [streamChunkJson(chunk)],
											},
											SlackResponse,
										).pipe(Effect.as(current)),
								}),
						),
					)
					const messageTs = yield* Option.match(started, {
						onNone: () =>
							callSlack('stream', 'chat.startStream', startStreamBody([]), SlackPostMessageResponse).pipe(
								Effect.flatMap((response) =>
									Predicate.isUndefined(response.ts)
										? Effect.fail(
												SlackApiError.make({
													operation: 'stream',
													message: 'Slack did not start the stream',
												}),
											)
										: Effect.succeed(response.ts),
								),
							),
						onSome: Effect.succeed,
					})
					const stopped = yield* callSlack(
						'stream',
						'chat.stopStream',
						{ channel: thread.channelId, ts: messageTs, chunks: [] },
						SlackPostMessageResponse,
					)
					if (Predicate.isUndefined(stopped.ts)) {
						return yield* SlackApiError.make({
							operation: 'stream',
							message: 'Slack did not stop the stream',
						})
					}
					const ownUserId = yield* authUserId
					const snapshot = stopped.message ?? SlackMessageSnapshot.make({ ts: stopped.ts, user: ownUserId })
					const message = yield* normalizeMessage(thread, snapshot)
					return SlackSentMessageRecord.make({ ref: message.ref, message })
				}),
			postPlanToThread: ({ thread, plan }) =>
				callSlack(
					'post_plan',
					'chat.postMessage',
					{
						channel: thread.channelId,
						thread_ts: thread.threadTs,
						text: planFallbackText(plan),
						blocks: planBlocks(plan),
					},
					SlackPostMessageResponse,
				).pipe(
					Effect.flatMap((response) =>
						Predicate.isUndefined(response.ts)
							? Effect.fail(
									SlackApiError.make({
										operation: 'post_plan',
										message: 'Slack did not return a message ID',
									}),
								)
							: Effect.succeed(
									SlackMessageRef.make({
										teamId: thread.teamId,
										channelId: thread.channelId,
										messageTs: response.ts,
									}),
								),
					),
					Effect.withSpan('slack.api.post_plan'),
				),
			updatePlan: ({ message, plan }) =>
				callSlack(
					'update_plan',
					'chat.update',
					{
						channel: message.channelId,
						ts: message.messageTs,
						text: planFallbackText(plan),
						blocks: planBlocks(plan),
					},
					SlackResponse,
				).pipe(Effect.asVoid, Effect.withSpan('slack.api.update_plan')),
			addReaction: ({ message, reaction }) =>
				callSlack(
					'add_reaction',
					'reactions.add',
					{ channel: message.channelId, timestamp: message.messageTs, name: reaction },
					SlackResponse,
				).pipe(Effect.asVoid),
			removeReaction: ({ message, reaction }) =>
				callSlack(
					'remove_reaction',
					'reactions.remove',
					{ channel: message.channelId, timestamp: message.messageTs, name: reaction },
					SlackResponse,
				).pipe(Effect.asVoid),
			resolveParticipant,
			getMessage,
			resolveReactionThread: ({ message }) =>
				callSlackGet(
					'resolve_reaction_thread',
					'conversations.replies',
					{ channel: message.channelId, ts: message.messageTs, limit: 1 },
					SlackMessagesResponse,
				).pipe(
					Effect.flatMap((response) => {
						const snapshot = response.messages?.find(({ ts }) => ts === message.messageTs)
						return Predicate.isUndefined(snapshot)
							? Effect.fail(
									SlackApiError.make({
										operation: 'resolve_reaction_thread',
										message: 'Slack did not return the reacted message',
									}),
								)
							: Effect.succeed(snapshot.thread_ts ?? snapshot.ts)
					}),
				),
			getThreadInfo: ({ thread }) =>
				listThreadMessagesPage(thread).pipe(
					Effect.map(([messages]) => {
						const root = messages.find((message) => message.ref.messageTs === thread.threadTs)
						const title = Predicate.isUndefined(root)
							? undefined
							: Match.value(root.content).pipe(
									Match.discriminatorsExhaustive('_tag')({
										SlackMarkdownContent: (content) => content.markdown,
										SlackPlainTextContent: (content) => content.text,
									}),
								)
						return Predicate.isUndefined(title)
							? SlackThreadInfo.make({ thread })
							: SlackThreadInfo.make({ thread, title })
					}),
				),
			getChannelInfo: ({ channel }) =>
				callSlackGet(
					'get_channel_info',
					'conversations.info',
					{ channel: channel.channelId, include_num_members: true },
					SlackChannelResponse,
				).pipe(
					Effect.flatMap((response) => {
						const snapshot = response.channel
						if (Predicate.isUndefined(snapshot)) {
							return Effect.fail(
								SlackApiError.make({
									operation: 'get_channel_info',
									message: 'Slack did not return the channel',
								}),
							)
						}
						const fields: SlackChannelInfoFields = {
							channel: SlackChannelRef.make({
								...channel,
								isDm: snapshot.is_im === true || snapshot.is_mpim === true || channel.isDm,
							}),
						}
						if (Predicate.isNotUndefined(snapshot.name)) fields.name = snapshot.name
						if (Predicate.isNotUndefined(snapshot.num_members)) fields.memberCount = snapshot.num_members
						return Effect.succeed(SlackChannelInfo.make(fields))
					}),
				),
			uploadFileToChannel: ({ channel, input }) => uploadFile({ channel, threadTs: null }, input),
			uploadFileToThread: ({ thread, input }) =>
				uploadFile(
					{
						channel: SlackChannelRef.make({
							teamId: thread.teamId,
							channelId: thread.channelId,
							isDm: thread.isDm,
						}),
						threadTs: thread.threadTs,
					},
					input,
				),
			downloadFile: (request) => openDownload(request).pipe(Effect.map((download) => download.stream)),
			downloadFileBytes: (request) => {
				if (request.size !== null && request.size > request.maxBytes)
					return Effect.fail(
						SlackFileSizeLimitExceeded.make({
							maxBytes: request.maxBytes,
							observedBytes: request.size,
							source: 'declared_size',
						}),
					)
				return openDownload(request).pipe(
					Effect.flatMap((download) => readBoundedSlackFileBytes(download, request.maxBytes)),
				)
			},
		}
		return service
	}),
}) {
	static readonly layer = Layer.effect(this, this.make)
}
