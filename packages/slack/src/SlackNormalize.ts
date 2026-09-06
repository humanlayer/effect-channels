import { Crypto, DateTime, Effect, Match, Option, Schema } from 'effect'

import { Attachment } from './Attachment.ts'
import { Emoji } from './Emoji.ts'
import { SlackWebhookError } from './Errors.ts'
import { Message } from './Message.ts'
import { AttachmentRef, IdempotencyKey, MessageRef, TenantId, UserId, type Author, type ThreadRef } from './Model.ts'
import type { SlackBotIdentity, SlackFileMetadata, SlackMessageTs, SlackTeamId } from './Schema.ts'
import { SlackEventCallback, SlackHistoryMessage } from './Schema.ts'
import {
	NormalizedConversationStopped,
	NormalizedMessage,
	NormalizedMessageDeleted,
	NormalizedMessageUpdated,
	NormalizedReaction,
	ReactionAdded,
	ReactionRemoved,
} from './SlackEvents.ts'
import { slackEventThreadRefs } from './SlackThreadId.ts'
import { Thread } from './Thread.ts'

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

interface SlackAttachmentRefFields {
	provider: 'slack'
	tenant: ReturnType<typeof TenantId.make>
	id: string
	kind: string
	providerLocator: { readonly id: string }
	name?: string
	mimeType?: string
	size?: number
	width?: number
	height?: number
}

const digestIdempotencyKey = (address: {
	readonly channelId: string
	readonly messageTs: string
	readonly kind?: string
}) =>
	Effect.gen(function* () {
		const crypto = yield* Crypto.Crypto
		return yield* crypto
			.digest(
				'SHA-256',
				new TextEncoder().encode(
					`humanlayer-channels-event-v1\nslack\n${address.kind ?? 'message'}\n${address.channelId}:${address.messageTs}\n0`,
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
	files?.map((file) => {
		const fields: SlackAttachmentRefFields = {
			provider: 'slack',
			tenant: TenantId.make(teamId),
			id: file.id,
			kind: file.mimetype?.startsWith('image/')
				? 'image'
				: file.mimetype?.startsWith('video/')
					? 'video'
					: file.mimetype?.startsWith('audio/')
						? 'audio'
						: 'file',
			providerLocator: { id: file.id },
		}
		if (file.name !== undefined) fields.name = file.name
		if (file.mimetype !== undefined) fields.mimeType = file.mimetype
		if (file.size !== undefined) fields.size = file.size
		if (file.original_w !== undefined) fields.width = file.original_w
		if (file.original_h !== undefined) fields.height = file.original_h
		const ref = AttachmentRef.make(fields)
		return Attachment.make({ ref })
	}) ?? []

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
		raw: Schema.encodeSync(SlackHistoryMessage)(input.snapshot),
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
	const directMessageKind =
		event.type === 'message' && (event.channel_type === 'im' || event.channel_type === 'mpim')
			? event.channel_type
			: undefined
	const threadRefs = slackEventThreadRefs({
		teamId: input.callback.team_id,
		channelId: event.channel,
		rootTs,
		isNew: event.thread_ts === undefined,
		directMessageKind,
	})
	const threadRef = threadRefs.threadRef
	const rawText = event.text ?? ''
	const text =
		input.identity.botUserId === undefined
			? rawText.trim()
			: rawText.replaceAll(`<@${input.identity.botUserId}>`, '').trim()
	const rawCallback = yield* Schema.encodeEffect(SlackEventCallback)(input.callback).pipe(
		Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })),
	)
	const message = Message.make({
		ref: MessageRef.make(event.ts),
		threadRef,
		text,
		markdown: text,
		author: slackAuthor(input.identity, event),
		metadata: { sentAt: slackTsToDateTime(event.ts, input.callback.event_time * 1000) },
		attachments: slackFileAttachments(input.callback.team_id, event.files),
		raw: rawCallback.event,
	})
	const thread = Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] })
	const idempotencyKey = yield* digestIdempotencyKey({ channelId: event.channel, messageTs: event.ts })
	const normalizedFields = {
		provider: 'slack',
		tenant: TenantId.make(input.callback.team_id),
		idempotencyKey,
		thread,
		message,
		mentioned: event.type === 'app_mention',
		raw: rawCallback,
	} as const
	return Option.some(NormalizedMessage.make({ ...normalizedFields, ...threadRefs }))
})

const slackEmoji = (name: string) =>
	Match.value(name).pipe(
		Match.when('thumbsup', () => Emoji.ThumbsUp),
		Match.when('white_check_mark', () => Emoji.Check),
		Match.when('heart', () => Emoji.Heart),
		Match.orElse((name) => Emoji.custom(name)),
	)

export const normalizeSlackMessageUpdated = Effect.fn('slack.normalize.message_updated')(function* (input: {
	readonly callback: SlackEventCallback
	readonly identity: SlackBotIdentity
	readonly directMessageKind?: 'im' | 'mpim'
}) {
	const event = input.callback.event
	if (event.type !== 'message' || event.subtype !== 'message_changed' || event.message === undefined) {
		return yield* SlackWebhookError.make({ reason: 'decode' })
	}
	const snapshot = event.message
	const rootTs = snapshot.thread_ts ?? snapshot.ts
	const eventDirectMessageKind =
		event.channel_type === 'im' || event.channel_type === 'mpim' ? event.channel_type : input.directMessageKind
	const threadRefs = slackEventThreadRefs({
		teamId: input.callback.team_id,
		channelId: event.channel,
		rootTs,
		isNew: snapshot.thread_ts === undefined,
		directMessageKind: eventDirectMessageKind,
	})
	const threadRef = threadRefs.threadRef
	const normalizedMessage = normalizeSlackHistoryMessage({
		snapshot,
		threadRef,
		teamId: input.callback.team_id,
		identity: input.identity,
	})
	const messageFields = {
		ref: normalizedMessage.ref,
		threadRef: normalizedMessage.threadRef,
		text: normalizedMessage.text,
		markdown: normalizedMessage.markdown,
		author: normalizedMessage.author,
		metadata: {
			sentAt: normalizedMessage.metadata.sentAt,
			editedAt: DateTime.makeUnsafe({ epochMilliseconds: input.callback.event_time * 1000 }),
		},
		attachments: normalizedMessage.attachments,
		raw: normalizedMessage.raw,
	} as const
	const message =
		normalizedMessage.replyTo === undefined
			? Message.make(messageFields)
			: Message.make({ ...messageFields, replyTo: normalizedMessage.replyTo })
	const previousMessage =
		event.previous_message === undefined
			? undefined
			: normalizeSlackHistoryMessage({
					snapshot: event.previous_message,
					threadRef,
					teamId: input.callback.team_id,
					identity: input.identity,
				})
	const raw = yield* Schema.encodeEffect(SlackEventCallback)(input.callback).pipe(
		Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })),
	)
	const idempotencyKey = yield* digestIdempotencyKey({
		channelId: event.channel,
		messageTs: event.ts,
		kind: 'message_updated',
	})
	const fields = {
		provider: 'slack',
		tenant: TenantId.make(input.callback.team_id),
		idempotencyKey,
		thread: Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] }),
		message,
		raw,
		...threadRefs,
	} as const
	return previousMessage === undefined
		? NormalizedMessageUpdated.make(fields)
		: NormalizedMessageUpdated.make({ ...fields, previousMessage })
})

export const normalizeSlackMessageDeleted = Effect.fn('slack.normalize.message_deleted')(function* (input: {
	readonly callback: SlackEventCallback
	readonly identity: SlackBotIdentity
	readonly directMessageKind?: 'im' | 'mpim'
}) {
	const event = input.callback.event
	if (event.type !== 'message' || event.subtype !== 'message_deleted') {
		return yield* SlackWebhookError.make({ reason: 'decode' })
	}
	const messageTs = event.deleted_ts ?? event.previous_message?.ts ?? event.ts
	const rootTs = event.previous_message?.thread_ts ?? messageTs
	const eventDirectMessageKind =
		event.channel_type === 'im' || event.channel_type === 'mpim' ? event.channel_type : input.directMessageKind
	const threadRefs = slackEventThreadRefs({
		teamId: input.callback.team_id,
		channelId: event.channel,
		rootTs,
		isNew: event.previous_message?.thread_ts === undefined,
		directMessageKind: eventDirectMessageKind,
	})
	const threadRef = threadRefs.threadRef
	const previousMessage =
		event.previous_message === undefined
			? undefined
			: normalizeSlackHistoryMessage({
					snapshot: event.previous_message,
					threadRef,
					teamId: input.callback.team_id,
					identity: input.identity,
				})
	const raw = yield* Schema.encodeEffect(SlackEventCallback)(input.callback).pipe(
		Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })),
	)
	const idempotencyKey = yield* digestIdempotencyKey({
		channelId: event.channel,
		messageTs,
		kind: 'message_deleted',
	})
	const fields = {
		provider: 'slack',
		tenant: TenantId.make(input.callback.team_id),
		idempotencyKey,
		messageRef: MessageRef.make(messageTs),
		deletedAt: DateTime.makeUnsafe({ epochMilliseconds: input.callback.event_time * 1000 }),
		raw,
		...threadRefs,
	} as const
	return previousMessage === undefined
		? NormalizedMessageDeleted.make(fields)
		: NormalizedMessageDeleted.make({ ...fields, previousMessage })
})

export const normalizeSlackReaction = Effect.fn('slack.normalize.reaction')(function* (input: {
	readonly callback: SlackEventCallback
	readonly identity: SlackBotIdentity
	readonly parentThreadTs?: SlackMessageTs
	readonly directMessageKind?: 'im' | 'mpim'
}) {
	const event = input.callback.event
	if (event.type !== 'reaction_added' && event.type !== 'reaction_removed') {
		return yield* SlackWebhookError.make({ reason: 'decode' })
	}
	const threadRefs = slackEventThreadRefs({
		teamId: input.callback.team_id,
		channelId: event.item.channel,
		rootTs: input.parentThreadTs ?? event.item.ts,
		isNew: false,
		directMessageKind: input.directMessageKind,
	})
	const threadRef = threadRefs.threadRef
	const raw = yield* Schema.encodeEffect(SlackEventCallback)(input.callback).pipe(
		Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })),
	)
	const idempotencyKey = yield* digestIdempotencyKey({
		channelId: event.item.channel,
		messageTs: event.event_ts,
		kind: event.type,
	})
	return NormalizedReaction.make({
		provider: 'slack',
		tenant: TenantId.make(input.callback.team_id),
		idempotencyKey,
		thread: Thread.fromRef(threadRef),
		messageRef: MessageRef.make(event.item.ts),
		change: event.type === 'reaction_added' ? ReactionAdded.make({}) : ReactionRemoved.make({}),
		emoji: slackEmoji(event.reaction),
		rawEmoji: event.reaction,
		actor: slackAuthor(input.identity, { user: event.user }),
		raw,
		...threadRefs,
	})
})

export const normalizeSlackConversationStopped = Effect.fn('slack.normalize.conversation_stopped')(function* (input: {
	readonly callback: SlackEventCallback
	readonly directMessageKind?: 'im' | 'mpim'
}) {
	const callback = input.callback
	const event = callback.event
	if (event.type !== 'agent_session_stopped') return yield* SlackWebhookError.make({ reason: 'decode' })
	const threadRefs = slackEventThreadRefs({
		teamId: callback.team_id,
		channelId: event.channel,
		rootTs: event.thread_ts,
		isNew: false,
		directMessageKind: input.directMessageKind,
	})
	const raw = yield* Schema.encodeEffect(SlackEventCallback)(callback).pipe(
		Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })),
	)
	const idempotencyKey = yield* digestIdempotencyKey({
		channelId: event.channel,
		messageTs: event.thread_ts,
		kind: `agent_session_stopped:${callback.event_id}`,
	})
	const fields = {
		provider: 'slack',
		tenant: TenantId.make(callback.team_id),
		idempotencyKey,
		raw,
		...threadRefs,
	} as const
	return NormalizedConversationStopped.make({ ...fields, userId: UserId.make(event.user) })
})
