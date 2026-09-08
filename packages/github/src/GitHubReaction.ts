import { Schema } from 'effect'

import { GitHubUser } from './GitHubEvents.js'
import { GitHubCommentRef, GitHubDiscussionRef, GitHubId } from './GitHubResource.js'

export const GitHubReactionContent = Schema.Literals([
	'+1',
	'-1',
	'laugh',
	'confused',
	'heart',
	'hooray',
	'rocket',
	'eyes',
])
export type GitHubReactionContent = typeof GitHubReactionContent.Type
export const GitHubReactionTarget = Schema.Union([GitHubDiscussionRef, GitHubCommentRef])
export type GitHubReactionTarget = typeof GitHubReactionTarget.Type
export const GitHubReactionRef = Schema.Struct({
	kind: Schema.Literal('github.reaction'),
	target: GitHubReactionTarget,
	id: GitHubId,
})
export interface GitHubReactionRef extends Schema.Schema.Type<typeof GitHubReactionRef> {}
export const GitHubReactionData = Schema.Struct({
	id: GitHubId,
	node_id: Schema.String,
	user: Schema.NullOr(GitHubUser),
	content: GitHubReactionContent,
	created_at: Schema.String,
})
export interface GitHubReactionData extends Schema.Schema.Type<typeof GitHubReactionData> {}
export const GitHubReaction = Schema.Struct({ ref: GitHubReactionRef, data: GitHubReactionData })
export interface GitHubReaction extends Schema.Schema.Type<typeof GitHubReaction> {}
export const AddReactionInput = Schema.Struct({ target: GitHubReactionTarget, content: GitHubReactionContent })
export interface AddReactionInput extends Schema.Schema.Type<typeof AddReactionInput> {}
export const ListReactionsInput = Schema.Struct({
	target: GitHubReactionTarget,
	page: GitHubId,
	perPage: GitHubId.check(Schema.isLessThanOrEqualTo(100)),
	content: Schema.optionalKey(GitHubReactionContent),
})
export interface ListReactionsInput extends Schema.Schema.Type<typeof ListReactionsInput> {}
export const RemoveReactionInput = Schema.Struct({ reaction: GitHubReactionRef })
export interface RemoveReactionInput extends Schema.Schema.Type<typeof RemoveReactionInput> {}
