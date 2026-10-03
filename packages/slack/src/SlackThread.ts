import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery'
import { Effect, Schema, Stream } from 'effect'

import type { SlackApiError, SlackFileUploadError } from './SlackApi'
import { SlackApi } from './SlackApi'
import { SlackChannel } from './SlackChannel'
import {
	type SlackChannelInfo,
	SlackChannelRef,
	type SlackContent,
	type SlackFile,
	type SlackMessageCount,
	type SlackMessages,
	type SlackParticipants,
	type SlackSentMessage,
	SlackThreadInfo,
	SlackThreadRef,
	type SlackUploadFileInput,
} from './SlackModels'
import type { SlackStreamChunk } from './SlackStreamChunk'

export class SlackChannelHistoryUnavailable extends Schema.TaggedError<SlackChannelHistoryUnavailable>()(
	'SlackChannelHistoryUnavailable',
	{ thread: SlackThreadRef },
) {}

const spanAttributes = (thread: SlackThreadRef) => ({
	'slack.team_id': thread.teamId,
	'slack.channel_id': thread.channelId,
	'slack.thread_ts': thread.threadTs,
	'slack.is_dm': thread.isDm,
})

export class SlackThread extends Schema.TaggedClass<SlackThread>()('SlackThread', {
	ref: SlackThreadRef,
	mailboxKey: Schema.NonEmptyString,
}) {
	get channel(): SlackChannel {
		return SlackChannel.make({
			ref: SlackChannelRef.make({
				teamId: this.ref.teamId,
				channelId: this.ref.channelId,
				isDm: this.ref.isDm,
			}),
		})
	}

	listParticipants(): Effect.Effect<SlackParticipants, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.listParticipants({ thread: this.ref })).pipe(
			Effect.withSpan('slack.thread.list_participants', { attributes: spanAttributes(this.ref) }),
		)
	}

	listMessages(): Effect.Effect<SlackMessages, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.listThreadMessages({ thread: this.ref })).pipe(
			Effect.withSpan('slack.thread.list_messages', { attributes: spanAttributes(this.ref) }),
		)
	}

	listChannelMessagesBeforeThread(
		count: SlackMessageCount,
	): Effect.Effect<SlackMessages, SlackApiError | SlackChannelHistoryUnavailable, SlackApi> {
		if (this.ref.isDm) {
			return Effect.fail(new SlackChannelHistoryUnavailable({ thread: this.ref })).pipe(
				Effect.withSpan('slack.thread.list_channel_messages_before_thread', {
					attributes: spanAttributes(this.ref),
				}),
			)
		}
		return Effect.flatMap(SlackApi, (api) => api.listChannelMessagesBeforeThread({ thread: this.ref, count })).pipe(
			Effect.withSpan('slack.thread.list_channel_messages_before_thread', {
				attributes: spanAttributes(this.ref),
			}),
		)
	}

	post(content: SlackContent): Effect.Effect<SlackSentMessage, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.postToThread({ thread: this.ref, content })).pipe(
			Effect.withSpan('slack.thread.post', { attributes: spanAttributes(this.ref) }),
		)
	}

	/** Uploads bytes and shares the file as a reply in this thread. */
	uploadFile(input: SlackUploadFileInput): Effect.Effect<SlackFile, SlackFileUploadError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.uploadFileToThread({ thread: this.ref, input })).pipe(
			Effect.withSpan('slack.thread.upload_file', { attributes: spanAttributes(this.ref) }),
		)
	}

	startTyping(): Effect.Effect<void, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.startTyping({ thread: this.ref })).pipe(
			Effect.withSpan('slack.thread.start_typing', { attributes: spanAttributes(this.ref) }),
		)
	}

	stream<E, R>(
		chunks: Stream.Stream<SlackStreamChunk, E, R>,
	): Effect.Effect<SlackSentMessage, SlackApiError | E, SlackApi | R> {
		return Effect.flatMap(SlackApi, (api) => api.stream(this.ref, chunks)).pipe(
			Effect.withSpan('slack.thread.stream', { attributes: spanAttributes(this.ref) }),
		)
	}

	subscribe(): Effect.Effect<MailboxSubscriptionResult, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.subscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('slack.thread.subscribe', { attributes: spanAttributes(this.ref) }))
	}

	isSubscribed(): Effect.Effect<boolean, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('slack.thread.is_subscribed', { attributes: spanAttributes(this.ref) }))
	}

	unsubscribe(): Effect.Effect<void, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.unsubscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('slack.thread.unsubscribe', { attributes: spanAttributes(this.ref) }))
	}

	fetchThreadInfo(): Effect.Effect<SlackThreadInfo, SlackApiError, SlackApi> {
		return Effect.flatMap(SlackApi, (api) => api.getThreadInfo({ thread: this.ref })).pipe(
			Effect.withSpan('slack.thread.fetch_thread_info', { attributes: spanAttributes(this.ref) }),
		)
	}

	fetchChannelInfo(): Effect.Effect<SlackChannelInfo, SlackApiError, SlackApi> {
		return this.channel
			.fetchInfo()
			.pipe(Effect.withSpan('slack.thread.fetch_channel_info', { attributes: spanAttributes(this.ref) }))
	}
}
