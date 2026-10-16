/**
 * The comments, reactions, and attachments `LinearApi` returns, as plain data. `LinearComment`,
 * `LinearReaction`, and `LinearIssueAttachment` hold the same fields and add the methods that act on them.
 */
import { Schema } from 'effect'

import { LinearFileRecord } from './LinearFileModels'
import { LinearReactionId } from './LinearIdentity'
import {
	LinearAttachmentRef,
	LinearCommentSnapshot,
	LinearIssueAttachmentSnapshot,
	LinearIssueRef,
	LinearReactionSnapshot,
} from './LinearModels'

export const LinearCommentRecord = Schema.Struct({
	...LinearCommentSnapshot.fields,
	files: Schema.Array(LinearFileRecord),
})
export type LinearCommentRecord = typeof LinearCommentRecord.Type

export const LinearReactionRef = Schema.Struct({ issue: LinearIssueRef, reactionId: LinearReactionId })
export type LinearReactionRef = typeof LinearReactionRef.Type

export const LinearReactionRecord = Schema.Struct({
	...LinearReactionSnapshot.fields,
	ref: LinearReactionRef,
})
export type LinearReactionRecord = typeof LinearReactionRecord.Type

export const LinearIssueAttachmentRecord = Schema.Struct({
	...LinearIssueAttachmentSnapshot.fields,
	ref: LinearAttachmentRef,
})
export type LinearIssueAttachmentRecord = typeof LinearIssueAttachmentRecord.Type
