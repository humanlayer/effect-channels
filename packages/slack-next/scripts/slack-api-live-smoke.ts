import { NodeRuntime } from '@effect/platform-node'
import { Clock, Config, Data, Effect, Schema, Stream } from 'effect'

import { SlackApi } from '../src/SlackApi'
import { SlackApiLive } from '../src/SlackApiLive'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from '../src/SlackIdentity'
import {
	SlackChannelRef,
	SlackMarkdownContent,
	SlackPlainTextContent,
	SlackReaction,
	SlackThreadRef,
} from '../src/SlackModels'
import { MarkdownTextChunk, PlanUpdateChunk, TaskUpdateChunk } from '../src/SlackStreamChunk'

class SlackLiveSmokeError extends Data.TaggedError('SlackLiveSmokeError')<{ readonly message: string }> {}

const SmokeConfig = Config.all({
	confirmation: Config.schema(Schema.Literal('confirmed'), 'SLACK_SMOKE_CONFIRM'),
	teamId: Config.schema(SlackTeamId, 'SLACK_SMOKE_TEAM_ID'),
	channelId: Config.schema(SlackChannelId, 'SLACK_SMOKE_CHANNEL_ID'),
	threadTs: Config.schema(SlackMessageTs, 'SLACK_SMOKE_THREAD_TS'),
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
	if (participants.length === 0) {
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
}).pipe(Effect.withSpan('slack.api.live_smoke'), Effect.provide(SlackApiLive), Effect.timeout('45 seconds'))

NodeRuntime.runMain(program)
