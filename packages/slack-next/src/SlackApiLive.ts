import { Config, Duration, Effect, Layer, Match, Option, Predicate, Schedule, Schema, Stream } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import {
	SlackApi,
	SlackApiError,
	SlackApiOperation,
	type SlackMessageRequest,
	type SlackParticipantRequest,
} from './SlackApi'
import { SlackMessageTs, SlackTeamId } from './SlackIdentity'
import {
	SlackChannelInfo,
	SlackChannelRef,
	type SlackContent,
	SlackMarkdownContent,
	SlackMessage,
	type SlackMessages,
	SlackMessageRef,
	SlackParticipant,
	SlackSentMessage,
	SlackThreadInfo,
	SlackThreadRef,
	SlackUserId,
} from './SlackModels'
import type { SlackStreamChunk } from './SlackStreamChunk'

const SlackResponse = Schema.Struct({ ok: Schema.Boolean, error: Schema.optionalKey(Schema.String) })

class SlackRateLimitedError extends Schema.TaggedError<SlackRateLimitedError>()('SlackRateLimitedError', {
	operation: SlackApiOperation,
	retryAfterMs: Schema.optionalKey(Schema.Finite),
}) {}

const SlackResponseMetadata = Schema.Struct({ next_cursor: Schema.optionalKey(Schema.String) })

const SlackMessageSnapshot = Schema.Struct({
	type: Schema.optionalKey(Schema.String),
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
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

const SlackPostMessageResponse = Schema.Struct({
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
	readonly status: 'in_progress' | 'complete' | 'error'
	details?: string
	output?: string
}

type SlackRepliesBody = {
	channel: string
	ts: string
	limit: number
	cursor?: string
}

type SlackPostBody = {
	channel: string
	text: string
	mrkdwn: boolean
	thread_ts?: string
}

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

const nextCursor = (metadata: typeof SlackResponseMetadata.Type | undefined) => {
	const cursor = metadata?.next_cursor
	return Predicate.isUndefined(cursor) || cursor.length === 0 ? Option.none<string>() : Option.some(cursor)
}

const chronological = (messages: SlackMessages): SlackMessages =>
	Array.from(messages).sort((left, right) => left.ref.messageTs.localeCompare(right.ref.messageTs))

/** Live Slack API implementation. Slack transport, decoding, pagination, and credentials stay private. */
export const SlackApiLiveBase = Layer.effect(
	SlackApi,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const botToken = yield* Config.redacted('SLACK_BOT_TOKEN')
		const configuredBotUserId = yield* Config.option(Config.string('SLACK_BOT_USER_ID'))
		const apiOrigin = yield* Config.url('SLACK_API_ORIGIN').pipe(
			Config.withDefault(new URL('https://slack.com/api/')),
		)
		const rateLimitRetryPolicy = Schedule.exponential('200 millis').pipe(
			Schedule.setInputType<SlackRateLimitedError | SlackApiError>(),
			Schedule.jittered,
			Schedule.upTo({ times: 3 }),
			Schedule.passthrough,
			Schedule.while(({ input }) => Schema.is(SlackRateLimitedError)(input)),
			Schedule.modifyDelay(({ input, duration }) =>
				Effect.succeed(
					Schema.is(SlackRateLimitedError)(input) && Predicate.isNotUndefined(input.retryAfterMs)
						? Duration.max(duration, Duration.millis(input.retryAfterMs))
						: duration,
				),
			),
			Schedule.tap(({ input, attempt }) =>
				Schema.is(SlackRateLimitedError)(input)
					? Effect.logWarning('Slack rate limit reached; retrying request').pipe(
							Effect.annotateLogs({ operation: input.operation, retry_attempt: attempt }),
						)
					: Effect.void,
			),
		)

		const executeSlack = <A extends { readonly ok: boolean; readonly error?: string }>(
			operation: SlackApiOperation,
			method: string,
			request: Effect.Effect<HttpClientRequest.HttpClientRequest, SlackApiError>,
			schema: Schema.Codec<A, unknown, never, never>,
		): Effect.Effect<A, SlackApiError> =>
			Effect.gen(function* () {
				const slackRequest = yield* request
				const response = yield* client
					.execute(slackRequest)
					.pipe(Effect.mapError(() => SlackApiError.make({ operation, message: 'Could not reach Slack' })))
				if (response.status === 429) {
					const retryAfterSeconds = Number(response.headers['retry-after'])
					return yield* Number.isFinite(retryAfterSeconds) && retryAfterSeconds >= 0
						? SlackRateLimitedError.make({ operation, retryAfterMs: retryAfterSeconds * 1_000 })
						: SlackRateLimitedError.make({ operation })
				}
				if (response.status < 200 || response.status >= 300) {
					return yield* SlackApiError.make({ operation, message: `Slack returned HTTP ${response.status}` })
				}
				const decoded = yield* response.json.pipe(
					Effect.flatMap(Schema.decodeUnknownEffect(schema)),
					Effect.mapError(() =>
						SlackApiError.make({ operation, message: 'Slack returned an invalid response' }),
					),
				)
				if (!decoded.ok) {
					return yield* SlackApiError.make({
						operation,
						message: decoded.error ?? 'Slack rejected the request',
					})
				}
				return decoded
			}).pipe(
				Effect.retry(rateLimitRetryPolicy),
				Effect.catchTag('SlackRateLimitedError', () =>
					Effect.fail(SlackApiError.make({ operation, message: 'Slack rate limit persisted after retries' })),
				),
				Effect.withSpan('slack.api.request', {
					attributes: { 'slack.operation': operation, 'slack.method': method },
				}),
			)

		const callSlack = <A extends { readonly ok: boolean; readonly error?: string }>(
			operation: SlackApiOperation,
			method: string,
			body: Schema.Json,
			schema: Schema.Codec<A, unknown, never, never>,
		): Effect.Effect<A, SlackApiError> =>
			executeSlack(
				operation,
				method,
				HttpClientRequest.post(new URL(method, apiOrigin).toString()).pipe(
					HttpClientRequest.bearerToken(botToken),
					HttpClientRequest.schemaBodyJson(Schema.Json)(body),
					Effect.mapError(() => SlackApiError.make({ operation, message: 'Could not encode Slack request' })),
				),
				schema,
			)

		const callSlackGet = <A extends { readonly ok: boolean; readonly error?: string }>(
			operation: SlackApiOperation,
			method: string,
			query: Readonly<Record<string, string | number | boolean>>,
			schema: Schema.Codec<A, unknown, never, never>,
		): Effect.Effect<A, SlackApiError> =>
			executeSlack(
				operation,
				method,
				Effect.succeed(
					HttpClientRequest.get(new URL(method, apiOrigin).toString(), { urlParams: query }).pipe(
						HttpClientRequest.bearerToken(botToken),
					),
				),
				schema,
			)

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
			operation: 'post' | 'start_typing',
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
				metadata: {},
			})
		})

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
			const body: SlackPostBody = {
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
			startTyping: ({ thread }) => setSessionStatus('start_typing', thread, 'processing'),
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
		})
	}),
)

/** Slack API implementation with the standard Fetch transport. */
export const SlackApiLive = SlackApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))
