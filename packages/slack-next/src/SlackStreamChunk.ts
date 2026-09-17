import { Schema } from 'effect'

export const MarkdownTextChunk = Schema.TaggedStruct('MarkdownTextChunk', {
	text: Schema.String,
})
export type MarkdownTextChunk = typeof MarkdownTextChunk.Type

export const TaskUpdateChunk = Schema.TaggedStruct('TaskUpdateChunk', {
	id: Schema.NonEmptyString,
	title: Schema.String,
	status: Schema.Literals(['in_progress', 'complete', 'error']),
	details: Schema.optionalKey(Schema.String),
	output: Schema.optionalKey(Schema.String),
})
export type TaskUpdateChunk = typeof TaskUpdateChunk.Type

export const PlanUpdateChunk = Schema.TaggedStruct('PlanUpdateChunk', {
	title: Schema.String,
})
export type PlanUpdateChunk = typeof PlanUpdateChunk.Type

export const SlackStreamChunk = Schema.Union([MarkdownTextChunk, TaskUpdateChunk, PlanUpdateChunk])
export type SlackStreamChunk = typeof SlackStreamChunk.Type
