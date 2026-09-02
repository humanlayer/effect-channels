import {
	Attachment,
	AttachmentRef,
	IdempotencyKey,
	Message,
	MessageRef,
	NormalizedMessage,
	TenantId,
	Thread,
	UserId,
	type Author,
	type ThreadRef,
} from '@humanlayer/channels'
import { Crypto, DateTime, Effect, Option } from 'effect'

import { SlackWebhookError } from './Errors.ts'
import type {
	SlackBotIdentity,
	SlackEventCallback,
	SlackFileMetadata,
	SlackHistoryMessage,
	SlackTeamId,
} from './Schema.ts'
import { SlackThreadRef } from './Schema.ts'
import { slackThreadRef } from './SlackThreadId.ts'

const ineligibleMessageSubtypes = new Set([
	'message_changed',
	'message_deleted',
	'message_replied',
	'channel_join',
	'channel_leave',
	'channel_topic',
	'channel_purpose',
	'channel_name',
	'channel_archive',
	'channel_unarchive',
	'group_join',
	'group_leave',
	'group_topic',
	'group_purpose',
	'group_name',
	'group_archive',
	'group_unarchive',
	'ekm_access_denied',
	'tombstone',
])

const digestIdempotencyKey = (address: { readonly channelId: string; readonly messageTs: string }) =>
	Effect.gen(function* () {
		const crypto = yield* Crypto.Crypto
		return yield* crypto
			.digest(
				'SHA-256',
				new TextEncoder().encode(
					`humanlayer-channels-event-v1\nslack\nmessage\n${address.channelId}:${address.messageTs}\n0`,
				),
			)
			.pipe(
				Effect.map((digest) => {
					const hexadecimal = Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('')
					return IdempotencyKey.make(`evt_${hexadecimal.slice(0, 32)}`)
				}),
				Effect.tapError((error) => Effect.logError('Slack idempotency digest failed', error)),
				Effect.mapError(() => SlackWebhookError.make({ reason: 'crypto' })),
			)
	})

export const isOwnSlackMessage = (
	identity: SlackBotIdentity,
	message: { readonly user?: string; readonly bot_id?: string },
) => {
	if (identity.botUserId !== undefined && message.user === identity.botUserId) {
		return true
	}
	return identity.botId !== undefined && message.bot_id === identity.botId
}

export const slackTsToDateTime = (ts: string, fallbackEpochMillis: number) => {
	const millis = Number(ts) * 1000
	return DateTime.makeUnsafe({
		epochMilliseconds: Number.isFinite(millis) ? Math.round(millis) : fallbackEpochMillis,
	})
}

export const slackFileAttachments = (
	teamId: SlackTeamId,
	files: ReadonlyArray<SlackFileMetadata> | undefined,
): Array<Attachment> =>
	files?.map((file) =>
		Attachment.make({
			ref: AttachmentRef.make({
				provider: 'slack',
				tenant: TenantId.make(teamId),
				id: file.id,
				kind: 'file',
				name: file.name,
				mimeType: file.mimetype,
				size: file.size,
				providerLocator: { id: file.id },
			}),
		}),
	) ?? []

const slackAuthor = (
	identity: SlackBotIdentity,
	message: { readonly user?: string; readonly bot_id?: string },
): Author => {
	const authorId = message.user ?? message.bot_id ?? 'unknown'
	const isMe = isOwnSlackMessage(identity, message)
	return {
		userId: UserId.make(authorId),
		userName: authorId,
		fullName: authorId,
		isBot: message.bot_id !== undefined || isMe ? true : 'unknown',
		isMe,
	}
}

export const normalizeSlackHistoryMessage = (input: {
	readonly snapshot: SlackHistoryMessage
	readonly threadRef: ThreadRef
	readonly teamId: SlackTeamId
	readonly identity: SlackBotIdentity
}): Message => {
	const text = input.snapshot.text ?? ''
	return Message.make({
		ref: MessageRef.make(input.snapshot.ts),
		threadRef: input.threadRef,
		text,
		markdown: text,
		author: slackAuthor(input.identity, input.snapshot),
		metadata: { sentAt: slackTsToDateTime(input.snapshot.ts, 0) },
		attachments: slackFileAttachments(input.teamId, input.snapshot.files),
		raw: input.snapshot,
	})
}

export const normalizeSlackMessage = Effect.fn('slack.normalize.message')(function* (input: {
	readonly callback: SlackEventCallback
	readonly identity: SlackBotIdentity
}) {
	const event = input.callback.event
	if (event.type !== 'app_mention' && event.type !== 'message') {
		return yield* SlackWebhookError.make({ reason: 'decode' })
	}
	if (event.type === 'message' && event.subtype !== undefined && ineligibleMessageSubtypes.has(event.subtype)) {
		return Option.none<NormalizedMessage>()
	}
	const rootTs = event.thread_ts ?? event.ts
	const threadRef = slackThreadRef(
		SlackThreadRef.make({
			teamId: input.callback.team_id,
			channelId: event.channel,
			threadTs: rootTs,
		}),
		event.thread_ts === undefined,
	)
	const rawText = event.text ?? ''
	const text =
		input.identity.botUserId === undefined
			? rawText.trim()
			: rawText.replaceAll(`<@${input.identity.botUserId}>`, '').trim()
	const message = Message.make({
		ref: MessageRef.make(event.ts),
		threadRef,
		text,
		markdown: text,
		author: slackAuthor(input.identity, event),
		metadata: { sentAt: slackTsToDateTime(event.ts, input.callback.event_time * 1000) },
		attachments: slackFileAttachments(input.callback.team_id, event.files),
		raw: event,
	})
	const thread = Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] })
	const idempotencyKey = yield* digestIdempotencyKey({ channelId: event.channel, messageTs: event.ts })
	return Option.some(
		NormalizedMessage.make({
			provider: 'slack',
			tenant: TenantId.make(input.callback.team_id),
			idempotencyKey,
			thread,
			message,
			mentioned: event.type === 'app_mention',
			raw: input.callback,
		}),
	)
})
