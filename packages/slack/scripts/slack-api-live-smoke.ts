import { NodeCrypto, NodeRuntime } from '@effect/platform-node'
import { Array as Arr, Clock, Config, Crypto, Data, Effect, Layer, Option, Schema, Stream } from 'effect'
import { Hex } from 'effect/encoding'

import { SlackApi } from '../src/SlackApi'
import { SlackApiLive } from '../src/SlackApiLive'
import { SlackChannel } from '../src/SlackChannel'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from '../src/SlackIdentity'
import {
	SlackChannelRef,
	type SlackFile,
	SlackMarkdownContent,
	SlackPlainTextContent,
	SlackReaction,
	SlackThreadRef,
} from '../src/SlackModels'
import { MarkdownTextChunk, PlanUpdateChunk, TaskUpdateChunk } from '../src/SlackStreamChunk'
import { SlackThread } from '../src/SlackThread'

class SlackLiveSmokeError extends Data.TaggedError('SlackLiveSmokeError')<{ readonly message: string }> {}

const SmokeConfig = Config.all({
	confirmation: Config.schema(Schema.Literal('confirmed'), 'SLACK_SMOKE_CONFIRM'),
	teamId: Config.schema(SlackTeamId, 'SLACK_SMOKE_TEAM_ID'),
	channelId: Config.schema(SlackChannelId, 'SLACK_SMOKE_CHANNEL_ID'),
	threadTs: Config.schema(SlackMessageTs, 'SLACK_SMOKE_THREAD_TS'),
	files: Config.Boolean('SLACK_SMOKE_FILES').pipe(Config.withDefault(false)),
	expectedFileSha256: Config.option(Config.String('SLACK_SMOKE_EXPECTED_FILE_SHA256')),
})

const sha256 = Effect.fn('slack.api.live_smoke.sha256')(function* (bytes: Uint8Array) {
	const crypto = yield* Crypto.Crypto
	return Hex.encode(yield* crypto.digest('SHA-256', bytes))
})

const concatenate = (chunks: ReadonlyArray<Uint8Array>) => {
	const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.byteLength, 0))
	let offset = 0
	for (const chunk of chunks) {
		bytes.set(chunk, offset)
		offset += chunk.byteLength
	}
	return bytes
}

const downloadedSha256 = Effect.fn('slack.api.live_smoke.downloaded_sha256')(function* (file: SlackFile) {
	const streamed = concatenate(yield* Stream.runCollect(yield* file.download()))
	const buffered = yield* file.downloadBytes({ maxBytes: Math.max(streamed.byteLength, 1) })
	const streamedHash = yield* sha256(streamed)
	if ((yield* sha256(buffered)) !== streamedHash) {
		return yield* new SlackLiveSmokeError({ message: `Stream and buffered bytes differ for ${file.ref.fileId}` })
	}
	return { bytes: streamed.byteLength, sha256: streamedHash }
})

const verifyUploadedFile = Effect.fn('slack.api.live_smoke.verify_uploaded_file')(function* (
	file: SlackFile,
	original: Uint8Array,
) {
	const expected = yield* sha256(original)
	const downloaded = yield* downloadedSha256(file)
	if (downloaded.sha256 !== expected) {
		return yield* new SlackLiveSmokeError({ message: `Downloaded bytes differ for ${file.ref.fileId}` })
	}
	const limited = yield* file.downloadBytes({ maxBytes: original.byteLength - 1 }).pipe(Effect.flip)
	yield* Effect.logInfo('Slack file verified').pipe(
		Effect.annotateLogs({
			file_id: file.ref.fileId,
			filename: file.name ?? 'unnamed',
			bytes: original.byteLength,
			sha256: expected,
			limit_error: limited._tag,
		}),
	)
})

const fileSmoke = Effect.fn('slack.api.live_smoke.files')(function* (input: {
	readonly marker: string
	readonly channel: SlackChannelRef
	readonly thread: SlackThreadRef
}) {
	yield* Effect.logInfo('Uploading one channel-root file and one thread file')
	const channelBytes = new TextEncoder().encode(`${input.marker} channel file\n`)
	const threadBytes = new TextEncoder().encode(`${input.marker} thread file\n`)
	const channelFile = yield* SlackChannel.make({ ref: input.channel }).uploadFile({
		filename: `${input.marker}-channel.txt`,
		bytes: channelBytes,
	})
	const threadFile = yield* SlackThread.make({ ref: input.thread, mailboxKey: 'slack-api-live-smoke' }).uploadFile({
		filename: `${input.marker}-thread.txt`,
		bytes: threadBytes,
		title: `${input.marker} thread file`,
		initialComment: `[${input.marker}] thread upload`,
	})
	yield* verifyUploadedFile(channelFile, channelBytes)
	yield* verifyUploadedFile(threadFile, threadBytes)
})

const verifyKnownThreadFile = Effect.fn('slack.api.live_smoke.verify_known_thread_file')(function* (
	thread: SlackThreadRef,
	expectedSha256: string,
) {
	const messages = yield* Effect.flatMap(SlackApi, (api) => api.listThreadMessages({ thread }))
	const known = messages.flatMap((message) => message.files)[0]
	if (known === undefined) {
		return yield* new SlackLiveSmokeError({ message: 'The smoke-test thread does not contain a file' })
	}
	const downloaded = yield* downloadedSha256(known)
	if (downloaded.sha256 !== expectedSha256.toLowerCase()) {
		return yield* new SlackLiveSmokeError({ message: `Known file ${known.ref.fileId} has a different SHA-256` })
	}
	yield* Effect.logInfo('Known Slack thread file verified').pipe(
		Effect.annotateLogs({ file_id: known.ref.fileId, bytes: downloaded.bytes, sha256: downloaded.sha256 }),
	)
})

const program = Effect.gen(function* () {
	const config = yield* SmokeConfig
	const api = yield* SlackApi
	const channel = SlackChannelRef.make({
		teamId: config.teamId,
		channelId: config.channelId,
		isDm: false,
	})
	const thread = SlackThreadRef.make({
		teamId: config.teamId,
		channelId: config.channelId,
		threadTs: config.threadTs,
		isDm: false,
	})

	yield* Effect.logInfo('Checking the Slack channel and human-created thread')
	const channelInfo = yield* api.getChannelInfo({ channel })
	const threadInfo = yield* api.getThreadInfo({ thread })
	const messagesBefore = yield* api.listThreadMessages({ thread })
	const participants = yield* api.listParticipants({ thread })
	if (Arr.isReadonlyArrayEmpty(participants)) {
		return yield* new SlackLiveSmokeError({
			message: 'The smoke-test thread must contain at least one message written by a human Slack user.',
		})
	}
	yield* Effect.logInfo('Slack read checks passed').pipe(
		Effect.annotateLogs({
			channel_id: channelInfo.channel.channelId,
			channel_name: channelInfo.name ?? '(unnamed)',
			thread_title: threadInfo.title ?? '(untitled)',
			messages_before: messagesBefore.length,
			human_participants: participants.length,
		}),
	)

	const now = yield* Clock.currentTimeMillis
	const marker = `slack-api-live-smoke-${now}`

	yield* Effect.logInfo('Posting one top-level smoke-test message')
	const channelPost = yield* api.postToChannel({
		channel,
		content: SlackPlainTextContent.make({ text: `[${marker}] plain-text channel post` }),
	})

	yield* Effect.logInfo('Posting and reading one threaded smoke-test reply')
	const threadPost = yield* api.postToThread({
		thread,
		content: SlackMarkdownContent.make({ markdown: `*[${marker}]* threaded reply` }),
	})
	yield* api.getMessage({ thread, message: threadPost.ref })

	yield* Effect.logInfo('Adding and removing a reaction')
	const reaction = SlackReaction.make('eyes')
	yield* api.addReaction({ message: threadPost.ref, reaction })
	yield* api.removeReaction({ message: threadPost.ref, reaction })

	yield* Effect.logInfo('Checking agent status and streaming')
	yield* api.startTyping({ thread })
	const streamed = yield* api.stream(
		thread,
		Stream.make(
			PlanUpdateChunk.make({ title: `[${marker}] live stream check` }),
			TaskUpdateChunk.make({ id: marker, title: 'Send a streamed Slack reply', status: 'in_progress' }),
			MarkdownTextChunk.make({ text: `Live streaming works. Marker: \`${marker}\`` }),
			TaskUpdateChunk.make({
				id: marker,
				title: 'Send a streamed Slack reply',
				status: 'complete',
				output: 'Slack accepted the streamed message.',
			}),
		),
	)

	if (Option.isSome(config.expectedFileSha256)) {
		yield* verifyKnownThreadFile(thread, config.expectedFileSha256.value)
	}
	if (config.files) yield* fileSmoke({ marker, channel, thread })

	const messagesAfter = yield* api.listThreadMessages({ thread })
	yield* Effect.logInfo('Slack live smoke test passed').pipe(
		Effect.annotateLogs({
			marker,
			channel_message_ts: channelPost.ref.messageTs,
			thread_message_ts: threadPost.ref.messageTs,
			stream_message_ts: streamed.ref.messageTs,
			messages_after: messagesAfter.length,
		}),
	)
}).pipe(
	Effect.withSpan('slack.api.live_smoke'),
	Effect.provide(Layer.merge(SlackApiLive, NodeCrypto.layer)),
	Effect.timeout('90 seconds'),
)

NodeRuntime.runMain(program)
