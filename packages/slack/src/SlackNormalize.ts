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
} from '@humanlayer/channels'
import { Crypto, DateTime, Effect } from 'effect'

import { SlackWebhookError } from './Errors.ts'
import type { SlackEventCallback } from './Schema.ts'
import { SlackThreadRef } from './Schema.ts'
import { slackThreadRef } from './SlackThreadId.ts'

const digestIdempotencyKey = (eventId: string) =>
	Effect.gen(function* () {
		const crypto = yield* Crypto.Crypto
		return yield* crypto
			.digest('SHA-256', new TextEncoder().encode(`humanlayer-channels-event-v1\nslack\nmessage\n${eventId}\n0`))
			.pipe(
				Effect.map((digest) => {
					const hexadecimal = Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('')
					return IdempotencyKey.make(`evt_${hexadecimal.slice(0, 32)}`)
				}),
				Effect.tapError((error) => Effect.logError('Slack idempotency digest failed', error)),
				Effect.mapError(() => SlackWebhookError.make({ reason: 'crypto' })),
			)
	})

export const normalizeSlackMessage = Effect.fn('slack.normalize.message')(function* (input: {
	readonly callback: SlackEventCallback
	readonly botUserId: string
}) {
	const event = input.callback.event
	if (event.type !== 'app_mention' && event.type !== 'message') {
		return yield* SlackWebhookError.make({ reason: 'decode' })
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
	const authorId = event.user ?? event.bot_id ?? 'unknown'
	const isMe = event.user === input.botUserId || event.bot_id === input.botUserId
	const text = (event.text ?? '').replaceAll(`<@${input.botUserId}>`, '').trim()
	const message = Message.make({
		ref: MessageRef.make(event.ts),
		threadRef,
		text,
		markdown: text,
		author: {
			userId: UserId.make(authorId),
			userName: authorId,
			fullName: authorId,
			isBot: event.bot_id === undefined ? 'unknown' : true,
			isMe,
		},
		metadata: { sentAt: DateTime.makeUnsafe({ epochMilliseconds: input.callback.event_time * 1000 }) },
		attachments:
			event.files?.map((file) =>
				Attachment.make({
					ref: AttachmentRef.make({
						provider: 'slack',
						tenant: TenantId.make(input.callback.team_id),
						id: file.id,
						kind: 'file',
						name: file.name,
						mimeType: file.mimetype,
						size: file.size,
						providerLocator: { id: file.id },
					}),
				}),
			) ?? [],
		raw: event,
	})
	const thread = Thread.make({ ref: threadRef, currentMessage: message, recentMessages: [message] })
	const idempotencyKey = yield* digestIdempotencyKey(input.callback.event_id)
	return NormalizedMessage.make({
		provider: 'slack',
		tenant: TenantId.make(input.callback.team_id),
		idempotencyKey,
		thread,
		message,
		mentioned: event.type === 'app_mention',
		raw: input.callback,
	})
})
