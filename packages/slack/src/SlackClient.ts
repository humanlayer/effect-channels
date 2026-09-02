import {
	ChannelInfo,
	MessagePage,
	TenantId,
	ThreadPage,
	UnknownTenant,
	UserProfile,
	unimplemented,
} from '@humanlayer/channels'
import type { FileData } from '@humanlayer/channels'
import { Context, Effect, Layer, Option, Predicate, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { SlackApiError, SlackTransportError } from './Errors.ts'
import {
	SlackAppendStreamInput,
	SlackApiInput,
	SlackApiResponse,
	SlackChannelInfoInput,
	SlackChannelId,
	SlackDeleteMessageInput,
	SlackFileDownloadInput,
	SlackFileUploadInput,
	SlackGetUserInput,
	SlackHistoryInput,
	SlackListThreadsInput,
	SlackOpenDMInput,
	SlackPostEphemeralInput,
	SlackPostMessageInput,
	SlackPostMessageResponse,
	SlackReactionInput,
	SlackRepliesInput,
	SlackSentMessage,
	SlackSentMessageList,
	SlackSessionStatusInput,
	SlackStartStreamInput,
	SlackStopStreamInput,
	SlackStreamRef,
	SlackUpdateMessageInput,
	type SlackPostMessageInput as SlackPostMessageInputType,
} from './Schema.ts'
import { SlackTenantCredentials } from './SlackTenantCredentials.ts'

const SlackPostMessageBody = Schema.Struct({
	channel: Schema.String,
	thread_ts: Schema.optionalKey(Schema.String),
	text: Schema.String,
})

const toTransportError = (operation: string, status?: number) => {
	if (status === undefined) {
		return SlackTransportError.make({ operation })
	}
	return SlackTransportError.make({ operation, status })
}

const makePostMessage = Effect.fn('slack.api.post_message')(function* (input: SlackPostMessageInputType) {
	const client = yield* HttpClient.HttpClient
	const credentialsService = yield* SlackTenantCredentials
	const credentials = yield* credentialsService.load({ teamId: input.teamId }).pipe(
		Effect.tapError((error) => Effect.logError('Slack credential lookup failed', error)),
		Effect.mapError(() => toTransportError('chat.postMessage')),
	)
	if (Option.isNone(credentials)) {
		return yield* UnknownTenant.make({ provider: 'slack', tenant: TenantId.make(input.teamId) })
	}
	const body = SlackPostMessageBody.make({
		channel: input.channelId,
		thread_ts: input.threadTs,
		text: input.text,
	})
	const request = yield* HttpClientRequest.post('https://slack.com/api/chat.postMessage').pipe(
		HttpClientRequest.bearerToken(credentials.value.botToken),
		HttpClientRequest.schemaBodyJson(SlackPostMessageBody)(body),
		Effect.mapError(() => SlackApiError.make({ operation: 'chat.postMessage', code: 'request_encode_failed' })),
	)
	const response = yield* client.execute(request).pipe(Effect.mapError(() => toTransportError('chat.postMessage')))
	if (response.status < 200 || response.status >= 300) {
		return yield* toTransportError('chat.postMessage', response.status)
	}
	const decoded = yield* HttpClientResponse.schemaBodyJson(SlackPostMessageResponse)(response).pipe(
		Effect.mapError(() => SlackApiError.make({ operation: 'chat.postMessage', code: 'malformed_response' })),
	)
	if (!decoded.ok) {
		return yield* SlackApiError.make({
			operation: 'chat.postMessage',
			code: decoded.error ?? 'unknown_error',
		})
	}
	if (!Predicate.isString(decoded.channel) || !Predicate.isString(decoded.ts)) {
		return yield* SlackApiError.make({ operation: 'chat.postMessage', code: 'missing_message_reference' })
	}
	return SlackSentMessage.make({ channelId: decoded.channel, ts: decoded.ts })
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
			const client = yield* HttpClient.HttpClient
			const credentials = yield* SlackTenantCredentials
			const postMessage = (input: SlackPostMessageInputType) =>
				makePostMessage(input).pipe(
					Effect.provideService(HttpClient.HttpClient, client),
					Effect.provideService(SlackTenantCredentials, credentials),
					Effect.tapError((error) =>
						Effect.logError('Slack API call failed', error).pipe(
							Effect.annotateLogs({ operation: 'chat.postMessage', team_id: input.teamId }),
						),
					),
				)
			return SlackClient.of({
				postMessage,
				setSessionStatus: () => unimplemented('SlackClient.setSessionStatus'),
				startStream: () => unimplemented('SlackClient.startStream'),
				appendStream: () => unimplemented('SlackClient.appendStream'),
				stopStream: () => unimplemented('SlackClient.stopStream'),
				updateMessage: () => unimplemented('SlackClient.updateMessage'),
				deleteMessage: () => unimplemented('SlackClient.deleteMessage'),
				addReaction: () => unimplemented('SlackClient.addReaction'),
				removeReaction: () => unimplemented('SlackClient.removeReaction'),
				replies: () => unimplemented('SlackClient.replies'),
				history: () => unimplemented('SlackClient.history'),
				channelInfo: () => unimplemented('SlackClient.channelInfo'),
				listThreads: () => unimplemented('SlackClient.listThreads'),
				getUser: () => unimplemented('SlackClient.getUser'),
				uploadFiles: () => unimplemented('SlackClient.uploadFiles'),
				downloadFile: () => unimplemented('SlackClient.downloadFile'),
				openDM: () => unimplemented('SlackClient.openDM'),
				postEphemeral: () => unimplemented('SlackClient.postEphemeral'),
				api: () => unimplemented('SlackClient.api'),
			})
		}),
	)
}
