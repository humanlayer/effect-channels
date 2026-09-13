import { Config, Context, Effect, Layer, Match, Option, Predicate, Schema, Stream } from 'effect'
import type { DateTime, Redacted } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'
import type { UrlParams } from 'effect/unstable/http'

import { UnknownTenant } from './DomainErrors.js'
import { SlackApiError, SlackTransportError } from './Errors.js'
import type { Message } from './Message.js'
import {
	ChannelInfo,
	TenantId,
	UserId,
	UserProfile,
	type Author,
	type ChannelRef,
	type FileData,
	type ThreadRef,
} from './Model.js'
import { MessagePage, ThreadPage, ThreadSummary } from './Operations.js'
import {
	SlackConversationsInfoResponse,
	SlackConversationsPageResponse,
	SlackCompleteUploadResponse,
	SlackApiResponse,
	SlackFileInfoResponse,
	SlackGetUploadUrlResponse,
	SlackOkResponse,
	SlackOpenDMResponse,
	SlackPostEphemeralResponse,
	SlackPostMessageResponse,
	SlackSentMessage,
	SlackChannelId,
	SlackMessageTs,
	SlackThreadRef,
	SlackUsersInfoResponse,
	type SlackApiInput,
	type SlackAppendStreamInput,
	type SlackBotIdentity as SlackBotIdentityType,
	type SlackCompletedFile,
	type SlackChannelInfoInput,
	type SlackDeleteMessageInput,
	type SlackFileDownloadInput,
	type SlackFileUploadInput,
	type SlackGetUserInput,
	type SlackHistoryInput,
	type SlackHistoryMessage,
	type SlackListThreadsInput,
	type SlackOpenDMInput,
	type SlackPostEphemeralInput,
	type SlackPostMessageInput,
	type SlackReactionInput,
	type SlackRepliesInput,
	type SlackSentMessageList,
	type SlackSessionStatusInput,
	type SlackStartStreamInput,
	type SlackStopStreamInput,
	type SlackStreamRef,
	type SlackTeamId,
	type SlackTenantCreds,
	type SlackUpdateMessageInput,
} from './Schema.js'
import { mergeSlackBotIdentity, slackBotIdentity } from './SlackBotIdentity.js'
import { normalizeSlackHistoryMessage, slackTsToDateTime } from './SlackNormalize.js'
import { SlackTenantCredentials } from './SlackTenantCredentials.js'
import { slackChannelRef, slackDmConversationRef, slackThreadRef } from './SlackThreadId.js'
import type { StreamChunk } from './StreamChunk.js'

const SlackPostMessageBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.optionalKey(Schema.String),
	text: Schema.String,
})

const SlackPostEphemeralBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.optionalKey(Schema.String),
	user: Schema.String,
	text: Schema.String,
})

const SlackSessionStatusBody = Schema.Struct({
	channel_id: Schema.String,
	thread_ts: Schema.String,
	status: Schema.String,
})

const SlackStartStreamBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.String,
	recipient_user_id: Schema.optionalKey(Schema.String),
	recipient_team_id: Schema.optionalKey(Schema.String),
	chunks: Schema.Array(Schema.Json),
})

const SlackContinueStreamBody = Schema.Struct({
	channel: Schema.String,
	ts: Schema.String,
	chunks: Schema.Array(Schema.Json),
})

const toSlackStreamChunk = Match.type<StreamChunk>().pipe(
	Match.tagsExhaustive({
		MarkdownTextChunk: (chunk) => ({ type: 'markdown_text', text: chunk.text }),
		TaskUpdateChunk: (chunk) => {
			const base = { type: 'task_update', id: chunk.id, title: chunk.title, status: chunk.status }
			if (chunk.details === undefined) {
				return chunk.output === undefined ? base : { ...base, output: chunk.output }
			}
			return chunk.output === undefined
				? { ...base, details: chunk.details }
				: { ...base, details: chunk.details, output: chunk.output }
		},
		PlanUpdateChunk: (chunk) => ({ type: 'plan_update', title: chunk.title }),
	}),
)

const SlackCompleteUploadFile = Schema.Struct({
	id: Schema.NonEmptyString,
	title: Schema.optionalKey(Schema.String),
})

const SlackCompleteUploadBody = Schema.Struct({
	files: Schema.Array(SlackCompleteUploadFile),
	channel_id: SlackChannelId,
	thread_ts: Schema.optionalKey(Schema.String),
	initial_comment: Schema.optionalKey(Schema.String),
})

interface ChannelInfoFields {
	channel: ChannelRef
	name?: string
	memberCount?: number
}

interface ThreadSummaryFields {
	thread: ThreadRef
	rootMessage: Message
	replyCount: number
	lastActivityAt?: DateTime.Utc
}

interface UserProfileFields {
	author: Author
	email?: string
	avatarUrl?: URL
}

const toTransportError = (operation: string, status?: number, retryAfterMs?: number) => {
	if (status === undefined) {
		return SlackTransportError.make({ operation })
	}
	return retryAfterMs === undefined
		? SlackTransportError.make({ operation, status })
		: SlackTransportError.make({ operation, status, retryAfterMs })
}

const retryAfterMilliseconds = (header: string | undefined) => {
	if (header === undefined) return undefined
	const seconds = Number(header)
	return Number.isFinite(seconds) && seconds >= 0 ? Math.ceil(seconds * 1000) : undefined
}

const defaultSlackApiOrigin = new URL('https://slack.com/api')

export const slackFileLimits = {
	maxFilesPerMessage: 10,
	maxFileBytes: 20 * 1024 * 1024,
	maxTotalUploadBytes: 100 * 1024 * 1024,
	maxDownloadBytes: 20 * 1024 * 1024,
	maxRedirects: 3,
} as const

const slackFileOrigins = new Set([
	'https://files.slack.com',
	'https://files.slack-gov.com',
	'https://slack-files.com',
	'https://slack-files-gov.com',
	'https://slack.com',
	'https://slack-gov.com',
])

const slackApiUrl = (origin: URL, method: string) => `${origin.toString().replace(/\/$/, '')}/${method}`

const isTrustedSlackFileUrl = (url: URL, apiOrigin: URL) =>
	url.protocol === 'https:'
		? slackFileOrigins.has(url.origin) || url.origin === apiOrigin.origin
		: url.protocol === 'http:' && url.origin === apiOrigin.origin

interface TenantLookup {
	readonly teamId: SlackTeamId
	readonly operation: string
}

const loadTenantCredentials = (input: TenantLookup) =>
	Effect.gen(function* () {
		const credentialsService = yield* SlackTenantCredentials
		const credentials = yield* credentialsService.load({ teamId: input.teamId }).pipe(
			Effect.tapError((error) => Effect.logError('Slack credential lookup failed', error)),
			Effect.mapError(() => toTransportError(input.operation)),
		)
		if (Option.isNone(credentials)) {
			return yield* UnknownTenant.make({
				provider: 'slack',
				tenant: TenantId.make(input.teamId),
				retryability: 'non_retryable',
			})
		}
		return credentials.value
	})

const loadBotToken = (input: TenantLookup) =>
	Effect.map(loadTenantCredentials(input), (credentials) => credentials.botToken)

interface TenantSession {
	readonly token: Redacted.Redacted<string>
	readonly identity: SlackBotIdentityType
}

const loadTenantSession = (fallback: SlackBotIdentityType, input: TenantLookup) =>
	Effect.map(loadTenantCredentials(input), (credentials: SlackTenantCreds): TenantSession => ({
		token: credentials.botToken,
		identity: mergeSlackBotIdentity(fallback, Option.some(credentials)),
	}))

const fetchSlackJson = <S extends Schema.Constraint>(input: {
	readonly operation: string
	readonly schema: S
	readonly request: HttpClientRequest.HttpClientRequest
}): Effect.Effect<S['Type'], SlackTransportError | SlackApiError, HttpClient.HttpClient | S['DecodingServices']> =>
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const response = yield* client
			.execute(input.request)
			.pipe(Effect.mapError(() => toTransportError(input.operation)))
		if (response.status < 200 || response.status >= 300) {
			return yield* toTransportError(
				input.operation,
				response.status,
				retryAfterMilliseconds(response.headers['retry-after']),
			)
		}
		return yield* HttpClientResponse.schemaBodyJson(input.schema)(response).pipe(
			Effect.mapError(() => SlackApiError.make({ operation: input.operation, code: 'malformed_response' })),
		)
	})

const requireOk = <A extends { readonly ok: boolean; readonly error?: string }>(
	operation: string,
	decoded: A,
): Effect.Effect<A, SlackApiError> =>
	decoded.ok
		? Effect.succeed(decoded)
		: Effect.fail(SlackApiError.make({ operation, code: decoded.error ?? 'unknown_error' }))

const slackPost = (origin: URL, token: Redacted.Redacted<string>, method: string, params: UrlParams.CoercibleRecord) =>
	HttpClientRequest.post(slackApiUrl(origin, method)).pipe(
		HttpClientRequest.bodyUrlParams(params),
		HttpClientRequest.bearerToken(token),
	)

const cursorFromMetadata = (metadata: { readonly next_cursor?: string } | undefined) => {
	const cursor = metadata?.next_cursor
	return cursor === undefined || cursor === '' ? undefined : cursor
}

const messagePage = (messages: ReadonlyArray<Message>, nextCursor: string | undefined) =>
	nextCursor === undefined ? MessagePage.make({ messages }) : MessagePage.make({ messages, nextCursor })

const makePostMessage = (origin: URL) =>
	Effect.fn('slack.api.post_message')(function* (input: SlackPostMessageInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'chat.postMessage' })
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'chat.postMessage' })
		const body =
			input.threadTs === undefined
				? SlackPostMessageBody.make({ channel: input.channelId, text: input.text })
				: SlackPostMessageBody.make({ channel: input.channelId, thread_ts: input.threadTs, text: input.text })
		const request = yield* HttpClientRequest.post(slackApiUrl(origin, 'chat.postMessage')).pipe(
			HttpClientRequest.bearerToken(token),
			HttpClientRequest.schemaBodyJson(SlackPostMessageBody)(body),
			Effect.mapError(() => SlackApiError.make({ operation: 'chat.postMessage', code: 'request_encode_failed' })),
		)
		const decoded = yield* fetchSlackJson({
			operation: 'chat.postMessage',
			schema: SlackPostMessageResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('chat.postMessage', response)))
		if (!Predicate.isString(decoded.channel) || !Predicate.isString(decoded.ts)) {
			return yield* SlackApiError.make({ operation: 'chat.postMessage', code: 'missing_message_reference' })
		}
		const botUserId = decoded.message?.user
		return botUserId === undefined
			? SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts })
			: SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts, botUserId })
	})

const makeApi = (origin: URL) =>
	Effect.fn('slack.api.call')(function* (input: SlackApiInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: input.method })
		const token = yield* loadBotToken({ teamId: input.teamId, operation: input.method })
		const request = yield* HttpClientRequest.post(slackApiUrl(origin, input.method)).pipe(
			HttpClientRequest.bearerToken(token),
			HttpClientRequest.schemaBodyJson(Schema.Json)(input.payload),
			Effect.mapError(() => SlackApiError.make({ operation: input.method, code: 'request_encode_failed' })),
		)
		return yield* fetchSlackJson({ operation: input.method, schema: SlackApiResponse, request })
	})

const makeOpenDM = (origin: URL) =>
	Effect.fn('slack.api.open_dm')(function* (input: SlackOpenDMInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'conversations.open' })
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'conversations.open' })
		const decoded = yield* fetchSlackJson({
			operation: 'conversations.open',
			schema: SlackOpenDMResponse,
			request: slackPost(origin, token, 'conversations.open', { users: input.userId }),
		}).pipe(Effect.flatMap((response) => requireOk('conversations.open', response)))
		if (decoded.channel === undefined) {
			return yield* SlackApiError.make({ operation: 'conversations.open', code: 'missing_channel' })
		}
		return decoded.channel.id
	})

const makePostEphemeral = (origin: URL) =>
	Effect.fn('slack.api.post_ephemeral')(function* (input: SlackPostEphemeralInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'chat.postEphemeral' })
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'chat.postEphemeral' })
		const body =
			input.threadTs === undefined
				? SlackPostEphemeralBody.make({ channel: input.channelId, user: input.userId, text: input.text })
				: SlackPostEphemeralBody.make({
						channel: input.channelId,
						thread_ts: input.threadTs,
						user: input.userId,
						text: input.text,
					})
		const request = yield* HttpClientRequest.post(slackApiUrl(origin, 'chat.postEphemeral')).pipe(
			HttpClientRequest.bearerToken(token),
			HttpClientRequest.schemaBodyJson(SlackPostEphemeralBody)(body),
			Effect.mapError(() =>
				SlackApiError.make({ operation: 'chat.postEphemeral', code: 'request_encode_failed' }),
			),
		)
		const decoded = yield* fetchSlackJson({
			operation: 'chat.postEphemeral',
			schema: SlackPostEphemeralResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('chat.postEphemeral', response)))
		const messageTs = decoded.message_ts ?? decoded.ts
		if (messageTs === undefined) {
			return yield* SlackApiError.make({ operation: 'chat.postEphemeral', code: 'missing_message_reference' })
		}
		return SlackSentMessage.make({ channelId: input.channelId, ts: messageTs })
	})

const makeSetSessionStatus = (origin: URL) =>
	Effect.fn('slack.api.set_session_status')(function* (input: SlackSessionStatusInput) {
		yield* Effect.annotateCurrentSpan({
			provider: 'slack',
			tenant: input.teamId,
			operation: 'agents.sessions.setStatus',
		})
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'agents.sessions.setStatus' })
		const body = SlackSessionStatusBody.make({
			channel_id: input.channelId,
			thread_ts: input.threadTs,
			status: input.status,
		})
		const request = yield* HttpClientRequest.post(slackApiUrl(origin, 'agents.sessions.setStatus')).pipe(
			HttpClientRequest.bearerToken(token),
			HttpClientRequest.schemaBodyJson(SlackSessionStatusBody)(body),
			Effect.mapError(() =>
				SlackApiError.make({ operation: 'agents.sessions.setStatus', code: 'request_encode_failed' }),
			),
		)
		yield* fetchSlackJson({ operation: 'agents.sessions.setStatus', schema: SlackOkResponse, request }).pipe(
			Effect.flatMap((response) => requireOk('agents.sessions.setStatus', response)),
		)
	})

const streamRequest = <S extends typeof SlackStartStreamBody | typeof SlackContinueStreamBody>(
	origin: URL,
	token: Redacted.Redacted<string>,
	method: string,
	schema: S,
	body: S['Type'],
) =>
	HttpClientRequest.post(slackApiUrl(origin, method)).pipe(
		HttpClientRequest.bearerToken(token),
		HttpClientRequest.schemaBodyJson(schema)(body),
		Effect.mapError(() => SlackApiError.make({ operation: method, code: 'request_encode_failed' })),
	)

const makeStartStream = (origin: URL) =>
	Effect.fn('slack.api.start_stream')(function* (input: SlackStartStreamInput) {
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'chat.startStream' })
		const base = {
			channel: input.channelId,
			thread_ts: input.threadTs,
			chunks: input.chunks.map(toSlackStreamChunk),
		} as const
		const body =
			input.recipient === undefined
				? SlackStartStreamBody.make(base)
				: SlackStartStreamBody.make({
						...base,
						recipient_user_id: input.recipient.userId,
						recipient_team_id: input.recipient.teamId,
					})
		const request = yield* streamRequest(origin, token, 'chat.startStream', SlackStartStreamBody, body)
		const decoded = yield* fetchSlackJson({
			operation: 'chat.startStream',
			schema: SlackPostMessageResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('chat.startStream', response)))
		if (decoded.channel === undefined || decoded.ts === undefined) {
			return yield* SlackApiError.make({ operation: 'chat.startStream', code: 'missing_message_reference' })
		}
		return { channelId: decoded.channel, messageTs: decoded.ts, threadTs: input.threadTs }
	})

const makeAppendStream = (origin: URL) =>
	Effect.fn('slack.api.append_stream')(function* (input: SlackAppendStreamInput) {
		const method = 'chat.appendStream'
		const token = yield* loadBotToken({ teamId: input.teamId, operation: method })
		const body = SlackContinueStreamBody.make({
			channel: input.stream.channelId,
			ts: input.stream.messageTs,
			chunks: input.chunks.map(toSlackStreamChunk),
		})
		const request = yield* streamRequest(origin, token, method, SlackContinueStreamBody, body)
		yield* fetchSlackJson({ operation: method, schema: SlackOkResponse, request }).pipe(
			Effect.flatMap((response) => requireOk(method, response)),
		)
	})

const makeStopStream = (origin: URL) =>
	Effect.fn('slack.api.stop_stream')(function* (input: SlackStopStreamInput) {
		const method = 'chat.stopStream'
		const token = yield* loadBotToken({ teamId: input.teamId, operation: method })
		const body = SlackContinueStreamBody.make({
			channel: input.stream.channelId,
			ts: input.stream.messageTs,
			chunks: input.chunks.map(toSlackStreamChunk),
		})
		const request = yield* streamRequest(origin, token, method, SlackContinueStreamBody, body)
		const decoded = yield* fetchSlackJson({ operation: method, schema: SlackPostMessageResponse, request }).pipe(
			Effect.flatMap((response) => requireOk(method, response)),
		)
		if (decoded.channel === undefined || decoded.ts === undefined) {
			return yield* SlackApiError.make({ operation: method, code: 'missing_message_reference' })
		}
		return decoded.message?.user === undefined
			? SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts })
			: SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts, botUserId: decoded.message.user })
	})

const makeUpdateMessage = (origin: URL) =>
	Effect.fn('slack.api.update_message')(function* (input: SlackUpdateMessageInput) {
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'chat.update' })
		const decoded = yield* fetchSlackJson({
			operation: 'chat.update',
			schema: SlackPostMessageResponse,
			request: slackPost(origin, token, 'chat.update', {
				channel: input.channelId,
				ts: input.ts,
				text: input.text,
			}),
		}).pipe(Effect.flatMap((response) => requireOk('chat.update', response)))
		if (!Predicate.isString(decoded.channel) || !Predicate.isString(decoded.ts)) {
			return yield* SlackApiError.make({ operation: 'chat.update', code: 'missing_message_reference' })
		}
		return SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts })
	})

const makeDeleteMessage = (origin: URL) =>
	Effect.fn('slack.api.delete_message')(function* (input: SlackDeleteMessageInput) {
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'chat.delete' })
		yield* fetchSlackJson({
			operation: 'chat.delete',
			schema: SlackOkResponse,
			request: slackPost(origin, token, 'chat.delete', { channel: input.channelId, ts: input.ts }),
		}).pipe(Effect.flatMap((response) => requireOk('chat.delete', response)))
	})

const makeReaction = (origin: URL, method: 'reactions.add' | 'reactions.remove') =>
	Effect.fn(`slack.api.${method}`)(function* (input: SlackReactionInput) {
		const token = yield* loadBotToken({ teamId: input.teamId, operation: method })
		yield* fetchSlackJson({
			operation: method,
			schema: SlackOkResponse,
			request: slackPost(origin, token, method, {
				channel: input.channelId,
				timestamp: input.ts,
				name: input.emoji,
			}),
		}).pipe(Effect.flatMap((response) => requireOk(method, response)))
	})

const normalizePageMessages = (input: {
	readonly identity: SlackBotIdentityType
	readonly teamId: SlackTeamId
	readonly channelId: SlackChannelId
	readonly messages: ReadonlyArray<SlackHistoryMessage>
	readonly threadRef?: ThreadRef
	readonly directMessageKind?: 'im' | 'mpim'
}) =>
	input.messages.map((snapshot) => {
		const fields = {
			teamId: input.teamId,
			channelId: input.channelId,
			threadTs: snapshot.thread_ts ?? snapshot.ts,
		}
		const threadRef =
			input.threadRef ??
			slackThreadRef(
				input.directMessageKind === undefined
					? SlackThreadRef.make(fields)
					: SlackThreadRef.make({ ...fields, directMessageKind: input.directMessageKind }),
				false,
			)
		return normalizeSlackHistoryMessage({ snapshot, threadRef, teamId: input.teamId, identity: input.identity })
	})

const makeReplies = (fallback: SlackBotIdentityType, origin: URL) =>
	Effect.fn('slack.api.conversations_replies')(function* (input: SlackRepliesInput) {
		yield* Effect.annotateCurrentSpan({
			provider: 'slack',
			tenant: input.teamId,
			operation: 'conversations.replies',
		})
		const { token, identity } = yield* loadTenantSession(fallback, {
			teamId: input.teamId,
			operation: 'conversations.replies',
		})
		const direction = input.direction ?? 'backward'
		const limit = input.limit ?? 100
		const refFields = { teamId: input.teamId, channelId: input.channelId, threadTs: input.threadTs }
		const threadRef = slackThreadRef(
			input.directMessageKind === undefined
				? SlackThreadRef.make(refFields)
				: SlackThreadRef.make({ ...refFields, directMessageKind: input.directMessageKind }),
			false,
		)
		if (direction === 'forward') {
			const request = slackPost(origin, token, 'conversations.replies', {
				channel: input.channelId,
				ts: input.threadTs,
				limit,
				cursor: input.cursor,
			})
			const decoded = yield* fetchSlackJson({
				operation: 'conversations.replies',
				schema: SlackConversationsPageResponse,
				request,
			}).pipe(Effect.flatMap((response) => requireOk('conversations.replies', response)))
			const messages = normalizePageMessages({
				identity,
				teamId: input.teamId,
				channelId: input.channelId,
				messages: decoded.messages ?? [],
				threadRef,
			})
			return messagePage(messages, cursorFromMetadata(decoded.response_metadata))
		}
		const fetchLimit = Math.min(1000, Math.max(limit * 2, 200))
		let buffer: ReadonlyArray<SlackHistoryMessage> = []
		let pageCursor: string | undefined = undefined
		while (true) {
			const request = slackPost(origin, token, 'conversations.replies', {
				channel: input.channelId,
				ts: input.threadTs,
				limit: fetchLimit,
				latest: input.cursor,
				inclusive: input.cursor === undefined ? undefined : false,
				cursor: pageCursor,
			})
			const decoded = yield* fetchSlackJson({
				operation: 'conversations.replies',
				schema: SlackConversationsPageResponse,
				request,
			}).pipe(Effect.flatMap((response) => requireOk('conversations.replies', response)))
			const page = decoded.messages ?? []
			buffer = [...buffer, ...page].slice(-(limit + 1))
			const next = cursorFromMetadata(decoded.response_metadata)
			if (next === undefined || page.length === 0) {
				break
			}
			pageCursor = next
		}
		const overflow = buffer.length > limit
		const selected = overflow ? buffer.slice(1) : buffer
		const oldestSelected = selected.at(0)
		const nextCursor = overflow && oldestSelected !== undefined ? oldestSelected.ts : undefined
		const newestFirst = [...selected].reverse()
		const messages = normalizePageMessages({
			identity,
			teamId: input.teamId,
			channelId: input.channelId,
			messages: newestFirst,
			threadRef,
		})
		return messagePage(messages, nextCursor)
	})

const makeHistory = (fallback: SlackBotIdentityType, origin: URL) =>
	Effect.fn('slack.api.conversations_history')(function* (input: SlackHistoryInput) {
		yield* Effect.annotateCurrentSpan({
			provider: 'slack',
			tenant: input.teamId,
			operation: 'conversations.history',
		})
		const { token, identity } = yield* loadTenantSession(fallback, {
			teamId: input.teamId,
			operation: 'conversations.history',
		})
		const direction = input.direction ?? 'backward'
		const limit = input.limit ?? 100
		const conversationThreadRef =
			input.directMessageKind === undefined
				? undefined
				: slackDmConversationRef(input.teamId, input.channelId, input.directMessageKind)
		if (direction === 'forward') {
			const request = slackPost(origin, token, 'conversations.history', {
				channel: input.channelId,
				limit,
				oldest: input.cursor,
				latest: input.before,
				inclusive: input.cursor === undefined && input.before === undefined ? undefined : false,
			})
			const decoded = yield* fetchSlackJson({
				operation: 'conversations.history',
				schema: SlackConversationsPageResponse,
				request,
			}).pipe(Effect.flatMap((response) => requireOk('conversations.history', response)))
			const chronological = [...(decoded.messages ?? [])].reverse()
			const newest = chronological.at(-1)
			const nextCursor = decoded.has_more === true && newest !== undefined ? newest.ts : undefined
			const messageInput = {
				identity,
				teamId: input.teamId,
				channelId: input.channelId,
				messages: chronological,
			}
			const messages = normalizePageMessages(
				conversationThreadRef === undefined
					? messageInput
					: { ...messageInput, threadRef: conversationThreadRef },
			)
			return messagePage(messages, nextCursor)
		}
		const latest = input.cursor ?? input.before
		const request = slackPost(origin, token, 'conversations.history', {
			channel: input.channelId,
			limit,
			latest,
			inclusive: latest === undefined ? undefined : false,
		})
		const decoded = yield* fetchSlackJson({
			operation: 'conversations.history',
			schema: SlackConversationsPageResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('conversations.history', response)))
		const newestFirst = decoded.messages ?? []
		const oldest = newestFirst.at(-1)
		const nextCursor = decoded.has_more === true && oldest !== undefined ? oldest.ts : undefined
		const messageInput = { identity, teamId: input.teamId, channelId: input.channelId, messages: newestFirst }
		const messages = normalizePageMessages(
			conversationThreadRef === undefined ? messageInput : { ...messageInput, threadRef: conversationThreadRef },
		)
		return messagePage(messages, nextCursor)
	})

const makeChannelInfo = (origin: URL) =>
	Effect.fn('slack.api.conversations_info')(function* (input: SlackChannelInfoInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'conversations.info' })
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'conversations.info' })
		const request = slackPost(origin, token, 'conversations.info', { channel: input.channelId })
		const decoded = yield* fetchSlackJson({
			operation: 'conversations.info',
			schema: SlackConversationsInfoResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('conversations.info', response)))
		const snapshot = decoded.channel
		if (snapshot === undefined) {
			return yield* SlackApiError.make({ operation: 'conversations.info', code: 'channel_missing' })
		}
		const baseRef = slackChannelRef(input.teamId, input.channelId)
		const channel: ChannelRef = {
			...baseRef,
			isDm: snapshot.is_im === true || snapshot.is_mpim === true || baseRef.isDm,
		}
		const info: ChannelInfoFields = { channel }
		if (snapshot.name !== undefined) {
			info.name = snapshot.name
		}
		if (snapshot.num_members !== undefined) {
			info.memberCount = snapshot.num_members
		}
		return ChannelInfo.make(info)
	})

const makeListThreads = (fallback: SlackBotIdentityType, origin: URL) =>
	Effect.fn('slack.api.list_threads')(function* (input: SlackListThreadsInput) {
		yield* Effect.annotateCurrentSpan({
			provider: 'slack',
			tenant: input.teamId,
			operation: 'conversations.history',
		})
		const { token, identity } = yield* loadTenantSession(fallback, {
			teamId: input.teamId,
			operation: 'conversations.history',
		})
		const limit = input.limit ?? 50
		const request = slackPost(origin, token, 'conversations.history', {
			channel: input.channelId,
			limit: Math.min(limit * 3, 200),
			latest: input.cursor,
			inclusive: input.cursor === undefined ? undefined : false,
		})
		const decoded = yield* fetchSlackJson({
			operation: 'conversations.history',
			schema: SlackConversationsPageResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('conversations.history', response)))
		const fetched = decoded.messages ?? []
		const roots = fetched.filter((message) => (message.reply_count ?? 0) > 0)
		const returned = roots.slice(0, limit)
		const threads = returned.map((snapshot) => {
			const threadRef = slackThreadRef(
				SlackThreadRef.make({ teamId: input.teamId, channelId: input.channelId, threadTs: snapshot.ts }),
				false,
			)
			const rootMessage = normalizeSlackHistoryMessage({
				snapshot,
				threadRef,
				teamId: input.teamId,
				identity,
			})
			const summary: ThreadSummaryFields = {
				thread: threadRef,
				rootMessage,
				replyCount: snapshot.reply_count ?? 0,
			}
			if (snapshot.latest_reply !== undefined) {
				summary.lastActivityAt = slackTsToDateTime(snapshot.latest_reply, 0)
			}
			return ThreadSummary.make(summary)
		})
		const lastReturnedRoot = returned.at(-1)
		const oldestFetched = fetched.at(-1)
		const hasMore = decoded.has_more === true || cursorFromMetadata(decoded.response_metadata) !== undefined
		const nextCursor =
			roots.length > limit && lastReturnedRoot !== undefined
				? lastReturnedRoot.ts
				: hasMore && oldestFetched !== undefined
					? oldestFetched.ts
					: undefined
		return nextCursor === undefined ? ThreadPage.make({ threads }) : ThreadPage.make({ threads, nextCursor })
	})

const makeGetUser = (fallback: SlackBotIdentityType, origin: URL) =>
	Effect.fn('slack.api.users_info')(function* (input: SlackGetUserInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'users.info' })
		const { token, identity } = yield* loadTenantSession(fallback, {
			teamId: input.teamId,
			operation: 'users.info',
		})
		const request = slackPost(origin, token, 'users.info', { user: input.userId })
		const decoded = yield* fetchSlackJson({
			operation: 'users.info',
			schema: SlackUsersInfoResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('users.info', response)))
		const user = decoded.user
		if (user === undefined) {
			return yield* SlackApiError.make({ operation: 'users.info', code: 'user_missing' })
		}
		const displayName =
			user.profile?.display_name || user.profile?.real_name || user.real_name || user.name || user.id
		const fullName = user.real_name || user.profile?.real_name || displayName
		const author: Author = {
			userId: UserId.make(user.id),
			userName: displayName,
			fullName,
			isBot: user.is_bot ?? 'unknown',
			isMe: identity.botUserId !== undefined && user.id === identity.botUserId,
		}
		const profile: UserProfileFields = { author }
		if (user.profile?.email !== undefined) {
			profile.email = user.profile.email
		}
		if (user.profile?.image_192 !== undefined && URL.canParse(user.profile.image_192)) {
			profile.avatarUrl = new URL(user.profile.image_192)
		}
		return UserProfile.make(profile)
	})

const fileLimitError = (operation: string, code: string) => SlackApiError.make({ operation, code })

const validateUploadFiles = (input: SlackFileUploadInput) =>
	Effect.gen(function* () {
		if (input.files.length === 0) {
			return yield* fileLimitError('files.uploadV2', 'no_files')
		}
		if (input.files.length > slackFileLimits.maxFilesPerMessage) {
			return yield* fileLimitError('files.uploadV2', 'too_many_files')
		}
		let total = 0
		for (const file of input.files) {
			if (file.data.byteLength > slackFileLimits.maxFileBytes) {
				return yield* fileLimitError('files.uploadV2', 'file_too_large')
			}
			total += file.data.byteLength
		}
		if (total > slackFileLimits.maxTotalUploadBytes) {
			return yield* fileLimitError('files.uploadV2', 'upload_too_large')
		}
	})

const shareTimestamp = (file: SlackCompletedFile, channelId: SlackChannelId) =>
	file.shares?.public?.[channelId]?.[0]?.ts ?? file.shares?.private?.[channelId]?.[0]?.ts

const makeUploadFiles = (origin: URL) =>
	Effect.fn('slack.api.upload_files')(function* (input: SlackFileUploadInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'files.uploadV2' })
		yield* validateUploadFiles(input)
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'files.uploadV2' })
		const client = yield* HttpClient.HttpClient
		const tickets = yield* Effect.forEach(
			input.files,
			(file) =>
				Effect.gen(function* () {
					const request = slackPost(origin, token, 'files.getUploadURLExternal', {
						filename: file.filename,
						length: file.data.byteLength,
					})
					const decoded = yield* fetchSlackJson({
						operation: 'files.getUploadURLExternal',
						schema: SlackGetUploadUrlResponse,
						request,
					}).pipe(Effect.flatMap((response) => requireOk('files.getUploadURLExternal', response)))
					if (decoded.upload_url === undefined || decoded.file_id === undefined) {
						return yield* SlackApiError.make({
							operation: 'files.getUploadURLExternal',
							code: 'missing_upload_ticket',
						})
					}
					if (!isTrustedSlackFileUrl(decoded.upload_url, origin)) {
						return yield* SlackApiError.make({
							operation: 'files.getUploadURLExternal',
							code: 'untrusted_upload_url',
						})
					}
					return { file, fileId: decoded.file_id, uploadUrl: decoded.upload_url }
				}),
			{ concurrency: 4 },
		)
		yield* Effect.forEach(
			tickets,
			(ticket) => {
				const request = HttpClientRequest.post(ticket.uploadUrl).pipe(
					HttpClientRequest.bodyUint8Array(
						ticket.file.data,
						ticket.file.mimeType ?? 'application/octet-stream',
					),
				)
				return client.execute(request).pipe(
					Effect.mapError(() => toTransportError('files.uploadV2.binary')),
					Effect.flatMap((response) =>
						response.status >= 200 && response.status < 300
							? Effect.void
							: Effect.fail(toTransportError('files.uploadV2.binary', response.status)),
					),
				)
			},
			{ concurrency: 4, discard: true },
		)
		const files = tickets.map((ticket) =>
			SlackCompleteUploadFile.make({ id: ticket.fileId, title: ticket.file.filename }),
		)
		const completeFields =
			input.threadTs === undefined
				? input.initialComment === undefined || input.initialComment === ''
					? SlackCompleteUploadBody.make({ files, channel_id: input.channelId })
					: SlackCompleteUploadBody.make({
							files,
							channel_id: input.channelId,
							initial_comment: input.initialComment,
						})
				: input.initialComment === undefined || input.initialComment === ''
					? SlackCompleteUploadBody.make({ files, channel_id: input.channelId, thread_ts: input.threadTs })
					: SlackCompleteUploadBody.make({
							files,
							channel_id: input.channelId,
							thread_ts: input.threadTs,
							initial_comment: input.initialComment,
						})
		const request = yield* HttpClientRequest.post(slackApiUrl(origin, 'files.completeUploadExternal')).pipe(
			HttpClientRequest.bearerToken(token),
			HttpClientRequest.schemaBodyJson(SlackCompleteUploadBody)(completeFields),
			Effect.mapError(() =>
				SlackApiError.make({ operation: 'files.completeUploadExternal', code: 'request_encode_failed' }),
			),
		)
		const completed = yield* fetchSlackJson({
			operation: 'files.completeUploadExternal',
			schema: SlackCompleteUploadResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('files.completeUploadExternal', response)))
		if (completed.files === undefined || completed.files.length !== tickets.length) {
			return yield* SlackApiError.make({
				operation: 'files.completeUploadExternal',
				code: 'missing_completed_files',
			})
		}
		return completed.files.map((file) => {
			const timestamp = shareTimestamp(file, input.channelId) ?? `file:${file.id}`
			return SlackSentMessage.make({
				channelId: input.channelId,
				ts: SlackMessageTs.make(timestamp),
				fileId: file.id,
			})
		})
	})

interface DownloadAccumulator {
	readonly chunks: ReadonlyArray<Uint8Array>
	readonly size: number
}

const combineChunks = (chunks: ReadonlyArray<Uint8Array>, size: number) => {
	const output = new Uint8Array(size)
	let offset = 0
	for (const chunk of chunks) {
		output.set(chunk, offset)
		offset += chunk.byteLength
	}
	return output
}

const makeDownloadFile = (origin: URL) =>
	Effect.fn('slack.api.download_file')(function* (input: SlackFileDownloadInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'files.download' })
		if (input.attachment.provider !== 'slack' || String(input.attachment.tenant) !== String(input.teamId)) {
			return yield* SlackApiError.make({ operation: 'files.download', code: 'invalid_attachment_origin' })
		}
		if (input.attachment.size !== undefined && input.attachment.size > slackFileLimits.maxDownloadBytes) {
			return yield* SlackApiError.make({ operation: 'files.download', code: 'file_too_large' })
		}
		const token = yield* loadBotToken({ teamId: input.teamId, operation: 'files.download' })
		const infoRequest = slackPost(origin, token, 'files.info', { file: input.attachment.id })
		const info = yield* fetchSlackJson({
			operation: 'files.info',
			schema: SlackFileInfoResponse,
			request: infoRequest,
		}).pipe(Effect.flatMap((response) => requireOk('files.info', response)))
		if (info.file?.size !== undefined && info.file.size > slackFileLimits.maxDownloadBytes) {
			return yield* SlackApiError.make({ operation: 'files.download', code: 'file_too_large' })
		}
		const downloadUrl = info.file?.url_private_download ?? info.file?.url_private
		if (downloadUrl === undefined || !isTrustedSlackFileUrl(downloadUrl, origin)) {
			return yield* SlackApiError.make({ operation: 'files.download', code: 'untrusted_download_url' })
		}
		const client = yield* HttpClient.HttpClient
		const download = (url: URL, redirects: number): Effect.Effect<FileData, SlackTransportError | SlackApiError> =>
			Effect.gen(function* () {
				if (!isTrustedSlackFileUrl(url, origin)) {
					return yield* SlackApiError.make({ operation: 'files.download', code: 'untrusted_redirect' })
				}
				const response = yield* client
					.execute(HttpClientRequest.get(url).pipe(HttpClientRequest.bearerToken(token)))
					.pipe(Effect.mapError(() => toTransportError('files.download')))
				const location = response.headers.location
				if (response.status >= 300 && response.status < 400 && location !== undefined) {
					if (redirects >= slackFileLimits.maxRedirects) {
						return yield* SlackApiError.make({ operation: 'files.download', code: 'too_many_redirects' })
					}
					const next = yield* Effect.try({
						try: () => new URL(location, url),
						catch: () => SlackApiError.make({ operation: 'files.download', code: 'invalid_redirect' }),
					})
					if (!isTrustedSlackFileUrl(next, origin)) {
						return yield* SlackApiError.make({ operation: 'files.download', code: 'untrusted_redirect' })
					}
					return yield* download(next, redirects + 1)
				}
				if (response.status < 200 || response.status >= 300) {
					return yield* toTransportError('files.download', response.status)
				}
				const contentType = response.headers['content-type'] ?? ''
				if (contentType.toLowerCase().includes('text/html')) {
					return yield* SlackApiError.make({ operation: 'files.download', code: 'unexpected_html' })
				}
				const declaredSize = Number(response.headers['content-length'])
				if (Number.isFinite(declaredSize) && declaredSize > slackFileLimits.maxDownloadBytes) {
					return yield* SlackApiError.make({ operation: 'files.download', code: 'file_too_large' })
				}
				const accumulated = yield* response.stream.pipe(
					Stream.mapError(() => toTransportError('files.download')),
					Stream.runFoldEffect(
						(): DownloadAccumulator => ({ chunks: [], size: 0 }),
						(accumulator, chunk) => {
							const size = accumulator.size + chunk.byteLength
							return size > slackFileLimits.maxDownloadBytes
								? Effect.fail(fileLimitError('files.download', 'file_too_large'))
								: Effect.succeed({ chunks: [...accumulator.chunks, chunk], size })
						},
					),
				)
				return combineChunks(accumulated.chunks, accumulated.size)
			})
		return yield* download(downloadUrl, 0)
	})

export class SlackClient extends Context.Service<
	SlackClient,
	{
		readonly postMessage: (
			input: SlackPostMessageInput,
		) => Effect.Effect<SlackSentMessage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly setSessionStatus: (
			input: SlackSessionStatusInput,
		) => Effect.Effect<void, UnknownTenant | SlackTransportError | SlackApiError>
		readonly startStream: (
			input: SlackStartStreamInput,
		) => Effect.Effect<SlackStreamRef, UnknownTenant | SlackTransportError | SlackApiError>
		readonly appendStream: (
			input: SlackAppendStreamInput,
		) => Effect.Effect<void, UnknownTenant | SlackTransportError | SlackApiError>
		readonly stopStream: (
			input: SlackStopStreamInput,
		) => Effect.Effect<SlackSentMessage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly updateMessage: (
			input: SlackUpdateMessageInput,
		) => Effect.Effect<SlackSentMessage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly deleteMessage: (
			input: SlackDeleteMessageInput,
		) => Effect.Effect<void, UnknownTenant | SlackTransportError | SlackApiError>
		readonly addReaction: (
			input: SlackReactionInput,
		) => Effect.Effect<void, UnknownTenant | SlackTransportError | SlackApiError>
		readonly removeReaction: (
			input: SlackReactionInput,
		) => Effect.Effect<void, UnknownTenant | SlackTransportError | SlackApiError>
		readonly replies: (
			input: SlackRepliesInput,
		) => Effect.Effect<MessagePage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly history: (
			input: SlackHistoryInput,
		) => Effect.Effect<MessagePage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly channelInfo: (
			input: SlackChannelInfoInput,
		) => Effect.Effect<ChannelInfo, UnknownTenant | SlackTransportError | SlackApiError>
		readonly listThreads: (
			input: SlackListThreadsInput,
		) => Effect.Effect<ThreadPage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly getUser: (
			input: SlackGetUserInput,
		) => Effect.Effect<UserProfile, UnknownTenant | SlackTransportError | SlackApiError>
		readonly uploadFiles: (
			input: SlackFileUploadInput,
		) => Effect.Effect<SlackSentMessageList, UnknownTenant | SlackTransportError | SlackApiError>
		readonly downloadFile: (
			input: SlackFileDownloadInput,
		) => Effect.Effect<FileData, UnknownTenant | SlackTransportError | SlackApiError>
		readonly openDM: (
			input: SlackOpenDMInput,
		) => Effect.Effect<SlackChannelId, UnknownTenant | SlackTransportError | SlackApiError>
		readonly postEphemeral: (
			input: SlackPostEphemeralInput,
		) => Effect.Effect<SlackSentMessage, UnknownTenant | SlackTransportError | SlackApiError>
		readonly api: (
			input: SlackApiInput,
		) => Effect.Effect<SlackApiResponse, UnknownTenant | SlackTransportError | SlackApiError>
	}
>()('channels/SlackClient') {
	static readonly layerWith = (options: { readonly apiOrigin?: URL } = {}) =>
		Layer.effect(
			SlackClient,
			Effect.gen(function* () {
				const apiOrigin = options.apiOrigin ?? defaultSlackApiOrigin
				const httpClient = yield* HttpClient.HttpClient
				const credentials = yield* SlackTenantCredentials
				const botUserId = yield* Config.option(Config.string('SLACK_BOT_USER_ID'))
				const botId = yield* Config.option(Config.string('SLACK_BOT_ID'))
				const fallbackIdentity = slackBotIdentity({
					botUserId: Option.getOrUndefined(botUserId),
					botId: Option.getOrUndefined(botId),
				})
				const dependencies = Context.make(HttpClient.HttpClient, httpClient).pipe(
					Context.add(SlackTenantCredentials, credentials),
				)
				const run = <A, E>(
					operation: string,
					teamId: SlackTeamId,
					effect: Effect.Effect<A, E, HttpClient.HttpClient | SlackTenantCredentials>,
				): Effect.Effect<A, E> =>
					effect.pipe(
						Effect.provide(dependencies),
						Effect.tapError((error) =>
							Effect.logError('Slack API call failed', error).pipe(
								Effect.annotateLogs({ operation, team_id: teamId }),
							),
						),
					)
				const replies = makeReplies(fallbackIdentity, apiOrigin)
				const history = makeHistory(fallbackIdentity, apiOrigin)
				const listThreads = makeListThreads(fallbackIdentity, apiOrigin)
				const getUser = makeGetUser(fallbackIdentity, apiOrigin)
				const uploadFiles = makeUploadFiles(apiOrigin)
				const downloadFile = makeDownloadFile(apiOrigin)
				const updateMessage = makeUpdateMessage(apiOrigin)
				const deleteMessage = makeDeleteMessage(apiOrigin)
				const addReaction = makeReaction(apiOrigin, 'reactions.add')
				const removeReaction = makeReaction(apiOrigin, 'reactions.remove')
				const startStream = makeStartStream(apiOrigin)
				const appendStream = makeAppendStream(apiOrigin)
				const stopStream = makeStopStream(apiOrigin)
				const openDM = makeOpenDM(apiOrigin)
				const postEphemeral = makePostEphemeral(apiOrigin)
				const api = makeApi(apiOrigin)
				return SlackClient.of({
					postMessage: (input) => run('chat.postMessage', input.teamId, makePostMessage(apiOrigin)(input)),
					setSessionStatus: (input) =>
						run('agents.sessions.setStatus', input.teamId, makeSetSessionStatus(apiOrigin)(input)),
					startStream: (input) => run('chat.startStream', input.teamId, startStream(input)),
					appendStream: (input) =>
						run('chat.appendStream', input.teamId, appendStream(input).pipe(Effect.asVoid)),
					stopStream: (input) => run('chat.stopStream', input.teamId, stopStream(input)),
					updateMessage: (input) => run('chat.update', input.teamId, updateMessage(input)),
					deleteMessage: (input) => run('chat.delete', input.teamId, deleteMessage(input)),
					addReaction: (input) => run('reactions.add', input.teamId, addReaction(input)),
					removeReaction: (input) => run('reactions.remove', input.teamId, removeReaction(input)),
					replies: (input) => run('conversations.replies', input.teamId, replies(input)),
					history: (input) => run('conversations.history', input.teamId, history(input)),
					channelInfo: (input) => run('conversations.info', input.teamId, makeChannelInfo(apiOrigin)(input)),
					listThreads: (input) => run('conversations.history', input.teamId, listThreads(input)),
					getUser: (input) => run('users.info', input.teamId, getUser(input)),
					uploadFiles: (input) => run('files.uploadV2', input.teamId, uploadFiles(input)),
					downloadFile: (input) => run('files.download', input.teamId, downloadFile(input)),
					openDM: (input) => run('conversations.open', input.teamId, openDM(input)),
					postEphemeral: (input) => run('chat.postEphemeral', input.teamId, postEphemeral(input)),
					api: (input) => run(input.method, input.teamId, api(input)),
				})
			}),
		)

	static readonly layer = SlackClient.layerWith()
}
