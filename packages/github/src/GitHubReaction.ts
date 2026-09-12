import { Schema } from 'effect'

import { GitHubUser } from './GitHubEvents.js'
import { GitHubCommentRef, GitHubDiscussionRef, GitHubId } from './GitHubResource.js'

const emojiValues = {
	ThumbsUp: '+1',
	ThumbsDown: '-1',
	Laugh: 'laugh',
	Confused: 'confused',
	Heart: 'heart',
	Hooray: 'hooray',
	Rocket: 'rocket',
	Eyes: 'eyes',
} as const

export const GitHubEmoji = Object.assign(Schema.Literals(Object.values(emojiValues)), emojiValues)
export type GitHubEmoji = typeof GitHubEmoji.Type
export const GitHubReactionContent = GitHubEmoji
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
