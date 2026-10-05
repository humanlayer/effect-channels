import { Config, Effect, Layer, Match, Option, Predicate, Schema, Stream, Struct, type Types } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HttpClient from 'effect/http/HttpClient'
import type * as UrlParams from 'effect/http/UrlParams'

import { completeSlackFileUpload, type SlackFileUploadDestination } from './api/CompleteSlackFileUpload'
import { downloadSlackFile, readBoundedSlackFileBytes } from './api/DownloadSlackFile'
import { getSlackFileUploadUrl } from './api/GetSlackFileUploadUrl'
import { SlackHttpClient, SlackHttpClientLive, type SlackWebApiError } from './api/SlackHttpClient'
import { uploadSlackFileBytes } from './api/UploadSlackFileBytes'
import {
	SlackApi,
	SlackApiError,
	isSlackTokenRejected,
	type SlackApiOperation,
	type SlackDownloadFileRequest,
	SlackFileAuthorizationError,
	SlackFileSizeLimitExceeded,
	type SlackMessageRequest,
	type SlackParticipantRequest,
} from './SlackApi'
import { SlackMessageTs, SlackTeamId } from './SlackIdentity'
import {
	SlackChannelInfo,
	SlackChannelRef,
	type SlackContent,
	slackFileFromMetadata,
	SlackMarkdownContent,
	SlackMessage,
	type SlackMessages,
	SlackMessageRef,
	type SlackPlan,
	SlackParticipant,
	SlackSentMessage,
	SlackThreadInfo,
	SlackThreadRef,
	type SlackUploadFileInput,
	SlackUserId,
} from './SlackModels'
import type { SlackStreamChunk } from './SlackStreamChunk'
import { SlackFileMetadata } from './SlackWebhookEventSchemas'

const SlackResponse = Schema.Struct({ ok: Schema.Boolean, error: Schema.optionalKey(Schema.String) })

const SlackResponseMetadata = Schema.Struct({ next_cursor: Schema.optionalKey(Schema.String) })

const SlackMessageSnapshot = Schema.Struct({
	type: Schema.optionalKey(Schema.String),
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	user_team: Schema.optionalKey(SlackTeamId),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
type SlackMessageSnapshot = typeof SlackMessageSnapshot.Type

const SlackMessagesResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	messages: Schema.optionalKey(Schema.Array(SlackMessageSnapshot)),
	response_metadata: Schema.optionalKey(SlackResponseMetadata),
})

const SlackUser = Schema.Struct({
	id: Schema.NonEmptyString,
	team_id: Schema.optionalKey(SlackTeamId),
	name: Schema.optionalKey(Schema.String),
	real_name: Schema.optionalKey(Schema.String),
	is_bot: Schema.optionalKey(Schema.Boolean),
	profile: Schema.optionalKey(
		Schema.Struct({
			display_name: Schema.optionalKey(Schema.NullOr(Schema.String)),
			real_name: Schema.optionalKey(Schema.NullOr(Schema.String)),
		}),
	),
})

const SlackUserResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(SlackUser),
})

const SlackBotResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	bot: Schema.optionalKey(
		Schema.Struct({
			id: Schema.NonEmptyString,
			user_id: Schema.optionalKey(Schema.NonEmptyString),
			name: Schema.optionalKey(Schema.String),
		}),
	),
})

const SlackAuthResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	user_id: Schema.optionalKey(Schema.NonEmptyString),
})

/** Response of Slack's `chat.postMessage` Web API method. */
export const SlackPostMessageResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	channel: Schema.optionalKey(Schema.NonEmptyString),
	ts: Schema.optionalKey(SlackMessageTs),
	message: Schema.optionalKey(SlackMessageSnapshot),
})

const SlackChannelResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	channel: Schema.optionalKey(
		Schema.Struct({
			id: Schema.NonEmptyString,
			name: Schema.optionalKey(Schema.String),
			is_im: Schema.optionalKey(Schema.Boolean),
			is_mpim: Schema.optionalKey(Schema.Boolean),
			num_members: Schema.optionalKey(Schema.Natural),
		}),
	),
})

const contentText = Match.type<SlackContent>().pipe(
	Match.discriminatorsExhaustive('_tag')({
		SlackMarkdownContent: (content) => content.markdown,
		SlackPlainTextContent: (content) => content.text,
	}),
)

type SlackTaskUpdateJson = {
	readonly type: 'task_update'
	readonly id: string
	readonly title: string
	readonly status: 'pending' | 'in_progress' | 'complete' | 'error'
	details?: string
	output?: string
}

type SlackRepliesBody = {
	channel: string
	ts: string
	limit: number
	cursor?: string
}

/** Body of Slack's `chat.postMessage` Web API method. */
export const SlackPostMessageRequest = Schema.Struct({
	channel: Schema.String,
	text: Schema.String,
	mrkdwn: Schema.Boolean,
	thread_ts: Schema.optionalKey(Schema.String),
})

type SlackHistoryBody = {
	channel: string
	latest: string
	inclusive: boolean
	limit: number
	cursor?: string
}

type SlackChannelInfoFields = {
	channel: SlackChannelRef
	name?: string
	memberCount?: number
}

type SlackParticipantFields = {
	userId: SlackUserId
	teamId?: SlackTeamId
	userName: string
	fullName: string
	isBot: boolean
	isMe: boolean
}

type SlackStartStreamBody = {
	channel: string
	thread_ts: string
	chunks: ReadonlyArray<Schema.Json>
	recipient_user_id?: string
	recipient_team_id?: string
}

const slackHistoryPageSize = 15

const streamChunkJson = Match.type<SlackStreamChunk>().pipe(
	Match.discriminatorsExhaustive('_tag')({
		MarkdownTextChunk: (chunk) => ({ type: 'markdown_text', text: chunk.text }),
		TaskUpdateChunk: (chunk) => {
			const json: SlackTaskUpdateJson = {
				type: 'task_update',
				id: chunk.id,
				title: chunk.title,
				status: chunk.status,
			}
			if (Predicate.isNotUndefined(chunk.details)) json.details = chunk.details
			if (Predicate.isNotUndefined(chunk.output)) json.output = chunk.output
			return json
		},
		PlanUpdateChunk: (chunk) => ({ type: 'plan_update', title: chunk.title }),
	}),
)

type SlackRichTextJson = {
	readonly type: 'rich_text'
	readonly elements: ReadonlyArray<Schema.Json>
}

/** One task of a plan block, as Block Kit takes it. */
type SlackTaskCardJson = {
	readonly type: 'task_card'
	readonly task_id: string
	readonly title: string
	readonly status: SlackPlan['tasks'][number]['status']
	details?: SlackRichTextJson
	output?: SlackRichTextJson
}

/** Slack's rich text for a plan task's note. */
const planNote = (text: string): SlackRichTextJson => ({
	type: 'rich_text',
	elements: [{ type: 'rich_text_section', elements: [{ type: 'text', text }] }],
})

/** A plan as one plan block, the way Slack's Block Kit takes it. */
const planBlocks = (plan: SlackPlan): ReadonlyArray<Schema.Json> => [
	{
		type: 'plan',
		title: plan.title,
		tasks: plan.tasks.map((task) => {
			const card: SlackTaskCardJson = {
				type: 'task_card',
				task_id: task.id,
				title: task.title,
				status: task.status,
			}
			if (Predicate.isNotUndefined(task.details)) card.details = planNote(task.details)
			if (Predicate.isNotUndefined(task.output)) card.output = planNote(task.output)
			return card
		}),
	},
]

/** The text Slack shows where it cannot show blocks, such as a notification. */
const planFallbackText = (plan: SlackPlan) =>
	[plan.title, ...plan.tasks.map((task) => `- (${task.status}) ${task.title}`)].join('\n')

/** Log a refused bot token as a configuration error that names the setting to check. */
const reportRejectedToken = <A>(effect: Effect.Effect<A, SlackApiError>) =>
	effect.pipe(
		Effect.tapError((error) =>
			isSlackTokenRejected(error)
				? Effect.logError('Slack rejected the bot token; check SLACK_BOT_TOKEN').pipe(
						Effect.annotateLogs({
							provider: 'slack',
							credential: 'bot_token',
							slack_error: error.message,
							operation: error.operation,
						}),
					)
				: Effect.void,
		),
	)

const nextCursor = (metadata: typeof SlackResponseMetadata.Type | undefined) => {
	const cursor = metadata?.next_cursor
	return Predicate.isUndefined(cursor) || cursor.length === 0 ? Option.none<string>() : Option.some(cursor)
}

const chronological = (messages: SlackMessages): SlackMessages =>
	Array.from(messages).sort((left, right) => left.ref.messageTs.localeCompare(right.ref.messageTs))

const SlackApiService = Layer.effect(
	SlackApi,
	Effect.gen(function* () {
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
			return SlackMessage.make({
				ref: SlackMessageRef.make({
					teamId: thread.teamId,
					channelId: thread.channelId,
					messageTs: snapshot.ts,
				}),
				thread,
				author,
				content: SlackMarkdownContent.make({ markdown: snapshot.text ?? '' }),
				files: (snapshot.files ?? []).map((file) => slackFileFromMetadata(thread.teamId, file)),
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
			const file = slackFileFromMetadata(
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
			return SlackSentMessage.make({ ref: message.ref, message })
		})

		return SlackApi.of({
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
					return SlackSentMessage.make({ ref: message.ref, message })
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
							? Effect.fail(SlackApiError.make({ operation: 'post_plan', message: 'Slack did not return a message ID' }))
							: Effect.succeed(
									SlackMessageRef.make({ teamId: thread.teamId, channelId: thread.channelId, messageTs: response.ts }),
								),
					),
					Effect.withSpan('slack.api.post_plan'),
				),
			updatePlan: ({ message, plan }) =>
				callSlack(
					'update_plan',
					'chat.update',
					{ channel: message.channelId, ts: message.messageTs, text: planFallbackText(plan), blocks: planBlocks(plan) },
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
		})
	}),
)

/** Live Slack API implementation. Slack transport, decoding, pagination, and credentials stay private. */
export const SlackApiLiveBase = SlackApiService.pipe(Layer.provide(SlackHttpClientLive))

/** Slack API implementation with the standard Fetch transport. */
export const SlackApiLive = SlackApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))
