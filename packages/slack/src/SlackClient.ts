import {
	ChannelInfo,
	MessagePage,
	TenantId,
	ThreadPage,
	ThreadSummary,
	UnknownTenant,
	UserId,
	UserProfile,
	unimplemented,
	type Author,
	type ChannelRef,
	type Message,
	type ThreadRef,
} from '@humanlayer/channels'
import type { FileData } from '@humanlayer/channels'
import { Config, Context, Effect, Layer, Option, Predicate, Schema } from 'effect'
import type { DateTime, Redacted } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'
import type { UrlParams } from 'effect/unstable/http'

import { SlackApiError, SlackTransportError } from './Errors.ts'
import {
	SlackConversationsInfoResponse,
	SlackConversationsPageResponse,
	SlackOkResponse,
	SlackPostMessageResponse,
	SlackSentMessage,
	SlackThreadRef,
	SlackUsersInfoResponse,
	type SlackApiInput,
	type SlackApiResponse,
	type SlackAppendStreamInput,
	type SlackBotIdentity as SlackBotIdentityType,
	type SlackChannelId,
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
} from './Schema.ts'
import { mergeSlackBotIdentity, slackBotIdentity } from './SlackBotIdentity.ts'
import { normalizeSlackHistoryMessage, slackTsToDateTime } from './SlackNormalize.ts'
import { SlackTenantCredentials } from './SlackTenantCredentials.ts'
import { slackChannelRef, slackThreadRef } from './SlackThreadId.ts'

const SlackPostMessageBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.optionalKey(Schema.String),
	text: Schema.String,
})

const SlackSessionStatusBody = Schema.Struct({
	channel_id: Schema.String,
	thread_ts: Schema.String,
	status: Schema.String,
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

const toTransportError = (operation: string, status?: number) => {
	if (status === undefined) {
		return SlackTransportError.make({ operation })
	}
	return SlackTransportError.make({ operation, status })
}

const slackApiUrl = (method: string) => `https://slack.com/api/${method}`

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
			return yield* UnknownTenant.make({ provider: 'slack', tenant: TenantId.make(input.teamId) })
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
			return yield* toTransportError(input.operation, response.status)
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

const slackGet = (token: Redacted.Redacted<string>, method: string, params: UrlParams.CoercibleRecord) =>
	HttpClientRequest.get(slackApiUrl(method)).pipe(
		HttpClientRequest.setUrlParams(params),
		HttpClientRequest.bearerToken(token),
	)

const cursorFromMetadata = (metadata: { readonly next_cursor?: string } | undefined) => {
	const cursor = metadata?.next_cursor
	return cursor === undefined || cursor === '' ? undefined : cursor
}

const messagePage = (messages: ReadonlyArray<Message>, nextCursor: string | undefined) =>
	nextCursor === undefined ? MessagePage.make({ messages }) : MessagePage.make({ messages, nextCursor })

const makePostMessage = Effect.fn('slack.api.post_message')(function* (input: SlackPostMessageInput) {
	yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'chat.postMessage' })
	const token = yield* loadBotToken({ teamId: input.teamId, operation: 'chat.postMessage' })
	const body =
		input.threadTs === undefined
			? SlackPostMessageBody.make({ channel: input.channelId, text: input.text })
			: SlackPostMessageBody.make({ channel: input.channelId, thread_ts: input.threadTs, text: input.text })
	const request = yield* HttpClientRequest.post(slackApiUrl('chat.postMessage')).pipe(
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
	return SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts })
})

const makeSetSessionStatus = Effect.fn('slack.api.set_session_status')(function* (input: SlackSessionStatusInput) {
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
	const request = yield* HttpClientRequest.post(slackApiUrl('agents.sessions.setStatus')).pipe(
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

const normalizePageMessages = (input: {
	readonly identity: SlackBotIdentityType
	readonly teamId: SlackTeamId
	readonly channelId: SlackChannelId
	readonly messages: ReadonlyArray<SlackHistoryMessage>
	readonly threadRef?: ThreadRef
}) =>
	input.messages.map((snapshot) => {
		const threadRef =
			input.threadRef ??
			slackThreadRef(
				SlackThreadRef.make({
					teamId: input.teamId,
					channelId: input.channelId,
					threadTs: snapshot.thread_ts ?? snapshot.ts,
				}),
				false,
			)
		return normalizeSlackHistoryMessage({ snapshot, threadRef, teamId: input.teamId, identity: input.identity })
	})

const makeReplies = (fallback: SlackBotIdentityType) =>
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
		const threadRef = slackThreadRef(
			SlackThreadRef.make({ teamId: input.teamId, channelId: input.channelId, threadTs: input.threadTs }),
			false,
		)
		if (direction === 'forward') {
			const request = slackGet(token, 'conversations.replies', {
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
		const request = slackGet(token, 'conversations.replies', {
			channel: input.channelId,
			ts: input.threadTs,
			limit: fetchLimit,
			latest: input.cursor,
			inclusive: input.cursor === undefined ? undefined : false,
		})
		const decoded = yield* fetchSlackJson({
			operation: 'conversations.replies',
			schema: SlackConversationsPageResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('conversations.replies', response)))
		const chronological = decoded.messages ?? []
		const startIndex = Math.max(0, chronological.length - limit)
		const selected = chronological.slice(startIndex)
		const oldestSelected = selected.at(0)
		const nextCursor =
			(startIndex > 0 || decoded.has_more === true) && oldestSelected !== undefined
				? oldestSelected.ts
				: undefined
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

const makeHistory = (fallback: SlackBotIdentityType) =>
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
		if (direction === 'forward') {
			const request = slackGet(token, 'conversations.history', {
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
			const messages = normalizePageMessages({
				identity,
				teamId: input.teamId,
				channelId: input.channelId,
				messages: chronological,
			})
			return messagePage(messages, nextCursor)
		}
		const latest = input.cursor ?? input.before
		const request = slackGet(token, 'conversations.history', {
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
		const messages = normalizePageMessages({
			identity,
			teamId: input.teamId,
			channelId: input.channelId,
			messages: newestFirst,
		})
		return messagePage(messages, nextCursor)
	})

const makeChannelInfo = Effect.fn('slack.api.conversations_info')(function* (input: SlackChannelInfoInput) {
	yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'conversations.info' })
	const token = yield* loadBotToken({ teamId: input.teamId, operation: 'conversations.info' })
	const request = slackGet(token, 'conversations.info', { channel: input.channelId })
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

const makeListThreads = (fallback: SlackBotIdentityType) =>
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
		const request = slackGet(token, 'conversations.history', {
			channel: input.channelId,
			limit: Math.min(limit * 3, 200),
			cursor: input.cursor,
		})
		const decoded = yield* fetchSlackJson({
			operation: 'conversations.history',
			schema: SlackConversationsPageResponse,
			request,
		}).pipe(Effect.flatMap((response) => requireOk('conversations.history', response)))
		const roots = (decoded.messages ?? []).filter((message) => (message.reply_count ?? 0) > 0).slice(0, limit)
		const threads = roots.map((snapshot) => {
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
		const nextCursor = cursorFromMetadata(decoded.response_metadata)
		return nextCursor === undefined ? ThreadPage.make({ threads }) : ThreadPage.make({ threads, nextCursor })
	})

const makeGetUser = (fallback: SlackBotIdentityType) =>
	Effect.fn('slack.api.users_info')(function* (input: SlackGetUserInput) {
		yield* Effect.annotateCurrentSpan({ provider: 'slack', tenant: input.teamId, operation: 'users.info' })
		const { token, identity } = yield* loadTenantSession(fallback, {
			teamId: input.teamId,
			operation: 'users.info',
		})
		const request = slackGet(token, 'users.info', { user: input.userId })
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
		if (user.profile?.image_192 !== undefined) {
			profile.avatarUrl = user.profile.image_192
		}
		return UserProfile.make(profile)
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
	static readonly layer = Layer.effect(
		SlackClient,
		Effect.gen(function* () {
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
			const replies = makeReplies(fallbackIdentity)
			const history = makeHistory(fallbackIdentity)
			const listThreads = makeListThreads(fallbackIdentity)
			const getUser = makeGetUser(fallbackIdentity)
			return SlackClient.of({
				postMessage: (input) => run('chat.postMessage', input.teamId, makePostMessage(input)),
				setSessionStatus: (input) =>
					run('agents.sessions.setStatus', input.teamId, makeSetSessionStatus(input)),
				startStream: () => unimplemented('SlackClient.startStream'),
				appendStream: () => unimplemented('SlackClient.appendStream'),
				stopStream: () => unimplemented('SlackClient.stopStream'),
				updateMessage: () => unimplemented('SlackClient.updateMessage'),
				deleteMessage: () => unimplemented('SlackClient.deleteMessage'),
				addReaction: () => unimplemented('SlackClient.addReaction'),
				removeReaction: () => unimplemented('SlackClient.removeReaction'),
				replies: (input) => run('conversations.replies', input.teamId, replies(input)),
				history: (input) => run('conversations.history', input.teamId, history(input)),
				channelInfo: (input) => run('conversations.info', input.teamId, makeChannelInfo(input)),
				listThreads: (input) => run('conversations.history', input.teamId, listThreads(input)),
				getUser: (input) => run('users.info', input.teamId, getUser(input)),
				uploadFiles: () => unimplemented('SlackClient.uploadFiles'),
				downloadFile: () => unimplemented('SlackClient.downloadFile'),
				openDM: () => unimplemented('SlackClient.openDM'),
				postEphemeral: () => unimplemented('SlackClient.postEphemeral'),
				api: () => unimplemented('SlackClient.api'),
			})
		}),
	)
}
