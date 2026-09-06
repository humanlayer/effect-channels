import { Effect, Stream } from 'effect'

import type {
	ChannelGone,
	DeleteFailed,
	DirectMessageOpenFailed,
	EditFailed,
	FileReadFailed,
	HistoryFailed,
	MetadataFailed,
	PostFailed,
	ReactionFailed,
	StatusFailed,
	ThreadGone,
	UnknownTenant,
	UnsupportedContextScope,
	UserLookupFailed,
} from './DomainErrors.ts'
import type { Message } from './Message.ts'
import type { Capabilities, ChannelInfo, FileData, ThreadInfo, UserProfile } from './Model.ts'
import type {
	ChannelInfoInput,
	ChannelPostInput,
	ChannelThreadsInput,
	ContainerMessagesInput,
	DeleteInput,
	DownloadAttachmentInput,
	EditInput,
	EphemeralResult,
	GetUserInput,
	InfoInput,
	MessagePage,
	MessagesInput,
	OpenDMInput,
	PostEphemeralInput,
	PostInput,
	ReactInput,
	StartChannelTypingInput,
	StartThreadTypingInput,
	StreamInput,
	ThreadPage,
	ThreadSummary,
} from './Operations.ts'
import type { SlackSessionStatusInput } from './Schema.ts'
import type { SentMessage } from './SentMessage.ts'
import type { StreamChunk } from './StreamChunk.ts'
import type { Thread } from './Thread.ts'

export type SlackService = {
	readonly capabilities: Capabilities
	readonly post: (input: PostInput) => Effect.Effect<SentMessage, UnknownTenant | PostFailed>
	readonly postToChannel: (input: ChannelPostInput) => Effect.Effect<SentMessage, UnknownTenant | PostFailed>
	readonly edit: (input: EditInput) => Effect.Effect<SentMessage, UnknownTenant | EditFailed>
	readonly delete: (input: DeleteInput) => Effect.Effect<void, UnknownTenant | DeleteFailed>
	readonly stream: <E, R>(
		input: StreamInput,
		chunks: Stream.Stream<StreamChunk, E, R>,
	) => Effect.Effect<SentMessage, UnknownTenant | PostFailed, R>
	readonly startThreadTyping: (input: StartThreadTypingInput) => Effect.Effect<void>
	readonly startChannelTyping: (input: StartChannelTypingInput) => Effect.Effect<void>
	readonly setSessionStatus: (input: SlackSessionStatusInput) => Effect.Effect<void, UnknownTenant | StatusFailed>
	readonly addReaction: (input: ReactInput) => Effect.Effect<void, UnknownTenant | ReactionFailed>
	readonly removeReaction: (input: ReactInput) => Effect.Effect<void, UnknownTenant | ReactionFailed>
	readonly messages: (input: MessagesInput) => Effect.Effect<MessagePage, HistoryFailed>
	readonly messageStream: (input: MessagesInput) => Stream.Stream<Message, HistoryFailed>
	readonly containerMessages: (
		input: ContainerMessagesInput,
	) => Effect.Effect<MessagePage, HistoryFailed | UnsupportedContextScope>
	readonly containerMessageStream: (
		input: ContainerMessagesInput,
	) => Stream.Stream<Message, HistoryFailed | UnsupportedContextScope>
	readonly channelThreads: (
		input: ChannelThreadsInput,
	) => Effect.Effect<ThreadPage, HistoryFailed | UnsupportedContextScope>
	readonly channelThreadStream: (
		input: ChannelThreadsInput,
	) => Stream.Stream<ThreadSummary, HistoryFailed | UnsupportedContextScope>
	readonly info: (input: InfoInput) => Effect.Effect<ThreadInfo, ThreadGone | MetadataFailed>
	readonly channelInfo: (input: ChannelInfoInput) => Effect.Effect<ChannelInfo, ChannelGone | MetadataFailed>
	readonly getUser: (input: GetUserInput) => Effect.Effect<UserProfile, UnknownTenant | UserLookupFailed>
	readonly downloadAttachment: (
		input: DownloadAttachmentInput,
	) => Effect.Effect<FileData, UnknownTenant | FileReadFailed>
	readonly openDM: (input: OpenDMInput) => Effect.Effect<Thread, UnknownTenant | DirectMessageOpenFailed>
	readonly postEphemeral: (input: PostEphemeralInput) => Effect.Effect<EphemeralResult, UnknownTenant | PostFailed>
}
