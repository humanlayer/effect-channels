/**
 * This file holds what `SlackApi` sends to and reads from Slack's Web API: response schemas, request bodies, and the
 * plain helpers that convert between them and the package's models. None of it needs a service.
 */
import { Effect, Match, Option, Predicate, Schema } from 'effect'

import { isSlackTokenRejected, type SlackApiError } from './SlackApiContract'
import { SlackMessageTs, SlackTeamId } from './SlackIdentity'
import type { SlackChannelRef, SlackContent, SlackMessageRecords, SlackPlan, SlackUserId } from './SlackModels'
import type { SlackStreamChunk } from './SlackStreamChunk'
import { SlackFileMetadata } from './SlackWebhookEventSchemas'

export const SlackResponse = Schema.Struct({ ok: Schema.Boolean, error: Schema.optionalKey(Schema.String) })

export const SlackResponseMetadata = Schema.Struct({ next_cursor: Schema.optionalKey(Schema.String) })

export const SlackMessageSnapshot = Schema.Struct({
	type: Schema.optionalKey(Schema.String),
	subtype: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(Schema.NonEmptyString),
	bot_id: Schema.optionalKey(Schema.NonEmptyString),
	text: Schema.optionalKey(Schema.String),
	ts: SlackMessageTs,
	thread_ts: Schema.optionalKey(SlackMessageTs),
	user_team: Schema.optionalKey(SlackTeamId),
	files: Schema.optionalKey(Schema.Array(SlackFileMetadata)),
})
export type SlackMessageSnapshot = typeof SlackMessageSnapshot.Type

export const SlackMessagesResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	messages: Schema.optionalKey(Schema.Array(SlackMessageSnapshot)),
	response_metadata: Schema.optionalKey(SlackResponseMetadata),
})

export const SlackUser = Schema.Struct({
	id: Schema.NonEmptyString,
	team_id: Schema.optionalKey(SlackTeamId),
	name: Schema.optionalKey(Schema.String),
	real_name: Schema.optionalKey(Schema.String),
	is_bot: Schema.optionalKey(Schema.Boolean),
	profile: Schema.optionalKey(
		Schema.Struct({
			display_name: Schema.optionalKey(Schema.NullOr(Schema.String)),
			real_name: Schema.optionalKey(Schema.NullOr(Schema.String)),
		}),
	),
})

export const SlackUserResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	user: Schema.optionalKey(SlackUser),
})

export const SlackBotResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	bot: Schema.optionalKey(
		Schema.Struct({
			id: Schema.NonEmptyString,
			user_id: Schema.optionalKey(Schema.NonEmptyString),
			name: Schema.optionalKey(Schema.String),
		}),
	),
})

export const SlackAuthResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	user_id: Schema.optionalKey(Schema.NonEmptyString),
})

/** Response of Slack's `chat.postMessage` Web API method. */
export const SlackPostMessageResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	channel: Schema.optionalKey(Schema.NonEmptyString),
	ts: Schema.optionalKey(SlackMessageTs),
	message: Schema.optionalKey(SlackMessageSnapshot),
})

export const SlackChannelResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	channel: Schema.optionalKey(
		Schema.Struct({
			id: Schema.NonEmptyString,
			name: Schema.optionalKey(Schema.String),
			is_im: Schema.optionalKey(Schema.Boolean),
			is_mpim: Schema.optionalKey(Schema.Boolean),
			num_members: Schema.optionalKey(Schema.Natural),
		}),
	),
})

export const contentText = Match.type<SlackContent>().pipe(
	Match.discriminatorsExhaustive('_tag')({
		SlackMarkdownContent: (content) => content.markdown,
		SlackPlainTextContent: (content) => content.text,
	}),
)

type SlackTaskUpdateJson = {
	readonly type: 'task_update'
	readonly id: string
	readonly title: string
	readonly status: 'pending' | 'in_progress' | 'complete' | 'error'
	details?: string
	output?: string
}

export type SlackRepliesBody = {
	channel: string
	ts: string
	limit: number
	cursor?: string
}

/** Body of Slack's `chat.postMessage` Web API method. */
export const SlackPostMessageRequest = Schema.Struct({
	channel: Schema.String,
	text: Schema.String,
	mrkdwn: Schema.Boolean,
	thread_ts: Schema.optionalKey(Schema.String),
})

export type SlackHistoryBody = {
	channel: string
	latest: string
	inclusive: boolean
	limit: number
	cursor?: string
}

export type SlackChannelInfoFields = {
	channel: SlackChannelRef
	name?: string
	memberCount?: number
}

export type SlackParticipantFields = {
	userId: SlackUserId
	teamId?: SlackTeamId
	userName: string
	fullName: string
	isBot: boolean
	isMe: boolean
}

export type SlackStartStreamBody = {
	channel: string
	thread_ts: string
	chunks: ReadonlyArray<Schema.Json>
	recipient_user_id?: string
	recipient_team_id?: string
}

export const slackHistoryPageSize = 15

export const streamChunkJson = Match.type<SlackStreamChunk>().pipe(
	Match.discriminatorsExhaustive('_tag')({
		MarkdownTextChunk: (chunk) => ({ type: 'markdown_text', text: chunk.text }),
		TaskUpdateChunk: (chunk) => {
			const json: SlackTaskUpdateJson = {
				type: 'task_update',
				id: chunk.id,
				title: chunk.title,
				status: chunk.status,
			}
			if (Predicate.isNotUndefined(chunk.details)) json.details = chunk.details
			if (Predicate.isNotUndefined(chunk.output)) json.output = chunk.output
			return json
		},
		PlanUpdateChunk: (chunk) => ({ type: 'plan_update', title: chunk.title }),
	}),
)

type SlackRichTextJson = {
	readonly type: 'rich_text'
	readonly elements: ReadonlyArray<Schema.Json>
}

/** One task of a plan block, as Block Kit takes it. */
type SlackTaskCardJson = {
	readonly type: 'task_card'
	readonly task_id: string
	readonly title: string
	readonly status: SlackPlan['tasks'][number]['status']
	details?: SlackRichTextJson
	output?: SlackRichTextJson
}

/** Slack's rich text for a plan task's note. */
const planNote = (text: string): SlackRichTextJson => ({
	type: 'rich_text',
	elements: [{ type: 'rich_text_section', elements: [{ type: 'text', text }] }],
})

/** A plan as one plan block, the way Slack's Block Kit takes it. */
export const planBlocks = (plan: SlackPlan): ReadonlyArray<Schema.Json> => [
	{
		type: 'plan',
		title: plan.title,
		tasks: plan.tasks.map((task) => {
			const card: SlackTaskCardJson = {
				type: 'task_card',
				task_id: task.id,
				title: task.title,
				status: task.status,
			}
			if (Predicate.isNotUndefined(task.details)) card.details = planNote(task.details)
			if (Predicate.isNotUndefined(task.output)) card.output = planNote(task.output)
			return card
		}),
	},
]

/** The text Slack shows where it cannot show blocks, such as a notification. */
export const planFallbackText = (plan: SlackPlan) =>
	[plan.title, ...plan.tasks.map((task) => `- (${task.status}) ${task.title}`)].join('\n')

/** Log a refused bot token as a configuration error that names the setting to check. */
export const reportRejectedToken = <A>(effect: Effect.Effect<A, SlackApiError>) =>
	effect.pipe(
		Effect.tapError((error) =>
			isSlackTokenRejected(error)
				? Effect.logError('Slack rejected the bot token; check SLACK_BOT_TOKEN').pipe(
						Effect.annotateLogs({
							provider: 'slack',
							credential: 'bot_token',
							slack_error: error.message,
							operation: error.operation,
						}),
					)
				: Effect.void,
		),
	)

export const nextCursor = (metadata: typeof SlackResponseMetadata.Type | undefined) => {
	const cursor = metadata?.next_cursor
	return Predicate.isUndefined(cursor) || cursor.length === 0 ? Option.none<string>() : Option.some(cursor)
}

export const chronological = (messages: SlackMessageRecords): SlackMessageRecords =>
	Array.from(messages).sort((left, right) => left.ref.messageTs.localeCompare(right.ref.messageTs))
