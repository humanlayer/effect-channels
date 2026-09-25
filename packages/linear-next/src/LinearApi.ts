import { Context, Effect, Schema, type Stream } from 'effect'

import type {
	LinearDownloadFileBytesRequest,
	LinearDownloadFileRequest,
	LinearFile,
	LinearFileError,
	LinearUploadAttachmentRequest,
	LinearUploadFileRequest,
} from './LinearFiles'
import {
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueLabelId,
	LinearReactionId,
	LinearUserId,
	LinearWorkflowStateId,
} from './LinearIdentity'
import {
	LinearAgentActivityReceipt,
	LinearAppUserPage,
	LinearCommentRef,
	LinearContent,
	LinearCreateAgentActivityRequest,
	LinearIssueInfo,
	LinearIssueRef,
	LinearUser,
	LinearUserPage,
} from './LinearModels'
import type { LinearComment, LinearIssueAttachment, LinearReaction } from './LinearResources'

export const LinearIssueRequest = Schema.Struct({ issue: LinearIssueRef })
export type LinearIssueRequest = typeof LinearIssueRequest.Type

export const LinearIssueUpdate = Schema.Struct({
	title: Schema.optionalKey(Schema.String),
	description: Schema.optionalKey(Schema.NullOr(Schema.String)),
	priority: Schema.optionalKey(Schema.Int),
	stateId: Schema.optionalKey(LinearWorkflowStateId),
	addLabelIds: Schema.optionalKey(Schema.Array(LinearIssueLabelId)),
	removeLabelIds: Schema.optionalKey(Schema.Array(LinearIssueLabelId)),
	assigneeId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
	delegateId: Schema.optionalKey(Schema.NullOr(LinearUserId)),
})
export type LinearIssueUpdate = typeof LinearIssueUpdate.Type

export const LinearUpdateIssueRequest = Schema.Struct({ issue: LinearIssueRef, update: LinearIssueUpdate })
export type LinearUpdateIssueRequest = typeof LinearUpdateIssueRequest.Type

export const LinearUserPageOptions = Schema.Struct({
	first: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100))),
	after: Schema.optionalKey(Schema.String),
	query: Schema.optionalKey(Schema.String),
})
export type LinearUserPageOptions = typeof LinearUserPageOptions.Type

export const LinearListUsersRequest = Schema.Struct({ issue: LinearIssueRef, ...LinearUserPageOptions.fields })
export type LinearListUsersRequest = typeof LinearListUsersRequest.Type
export const LinearListAssignableUsersRequest = Schema.Struct({
	issue: LinearIssueRef,
	...LinearUserPageOptions.fields,
})
export type LinearListAssignableUsersRequest = typeof LinearListAssignableUsersRequest.Type
export const LinearListAppUsersRequest = Schema.Struct({ issue: LinearIssueRef, ...LinearUserPageOptions.fields })
export type LinearListAppUsersRequest = typeof LinearListAppUsersRequest.Type
export const LinearGetUserRequest = Schema.Struct({ issue: LinearIssueRef, userId: LinearUserId })
export type LinearGetUserRequest = typeof LinearGetUserRequest.Type
export const LinearCreateCommentRequest = Schema.Struct({
	issue: LinearIssueRef,
	content: LinearContent,
	parentId: Schema.optionalKey(LinearCommentId),
})
export type LinearCreateCommentRequest = typeof LinearCreateCommentRequest.Type
export const LinearUpdateCommentRequest = Schema.Struct({ comment: LinearCommentRef, content: LinearContent })
export type LinearUpdateCommentRequest = typeof LinearUpdateCommentRequest.Type
export const LinearDeleteCommentRequest = Schema.Struct({ comment: LinearCommentRef })
export type LinearDeleteCommentRequest = typeof LinearDeleteCommentRequest.Type
export const LinearReactionTarget = Schema.TaggedUnion({
	Issue: { issue: LinearIssueRef },
	Comment: { comment: LinearCommentRef },
})
export type LinearReactionTarget = typeof LinearReactionTarget.Type
export const LinearCreateReactionRequest = Schema.Struct({ target: LinearReactionTarget, emoji: Schema.NonEmptyString })
export type LinearCreateReactionRequest = typeof LinearCreateReactionRequest.Type
export const LinearDeleteReactionRequest = Schema.Struct({ issue: LinearIssueRef, reactionId: LinearReactionId })
export type LinearDeleteReactionRequest = typeof LinearDeleteReactionRequest.Type
export const LinearCreateAttachmentInput = Schema.Struct({
	url: Schema.String,
	title: Schema.String,
	subtitle: Schema.optionalKey(Schema.String),
	metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})
export type LinearCreateAttachmentInput = typeof LinearCreateAttachmentInput.Type
export const LinearCreateAttachmentRequest = Schema.Struct({
	issue: LinearIssueRef,
	input: LinearCreateAttachmentInput,
})
export type LinearCreateAttachmentRequest = typeof LinearCreateAttachmentRequest.Type
export const LinearAttachmentRef = Schema.Struct({ issue: LinearIssueRef, attachmentId: LinearAttachmentId })
export type LinearAttachmentRef = typeof LinearAttachmentRef.Type
export const LinearUpdateAttachmentInput = Schema.Struct({
	title: Schema.String,
	subtitle: Schema.optionalKey(Schema.NullOr(Schema.String)),
	metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})
export type LinearUpdateAttachmentInput = typeof LinearUpdateAttachmentInput.Type
export const LinearUpdateAttachmentRequest = Schema.Struct({
	attachment: LinearAttachmentRef,
	input: LinearUpdateAttachmentInput,
})
export type LinearUpdateAttachmentRequest = typeof LinearUpdateAttachmentRequest.Type
export const LinearDeleteAttachmentRequest = Schema.Struct({ attachment: LinearAttachmentRef })
export type LinearDeleteAttachmentRequest = typeof LinearDeleteAttachmentRequest.Type

export const LinearApiOperation = Schema.Literals([
	'acquire_client_credentials_token',
	'viewer_identity',
	'create_agent_activity',
	'get_issue',
	'update_issue',
	'list_assignable_users',
	'list_app_users',
	'get_user',
	'list_issue_comments',
	'list_issue_attachments',
	'create_comment',
	'update_comment',
	'delete_comment',
	'create_reaction',
	'delete_reaction',
	'create_attachment',
	'update_attachment',
	'delete_attachment',
	'request_file_upload',
	'upload_file_bytes',
	'download_file',
])
export type LinearApiOperation = typeof LinearApiOperation.Type

export class LinearApiError extends Schema.TaggedError<LinearApiError>()('LinearApiError', {
	operation: LinearApiOperation,
	reason: Schema.Literals([
		'unavailable',
		'unauthorized',
		'forbidden',
		'invalid_response',
		'identity_mismatch',
		'rejected',
		'not_found',
		'validation',
		'rate_limited',
	]),
	retryable: Schema.Boolean,
	status: Schema.optionalKey(Schema.Int),
	message: Schema.optionalKey(Schema.String),
	retryAfterMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
}) {}

/** Provider-native Linear operations used by behavior-bearing Linear resources. */
export class LinearApi extends Context.Service<
	LinearApi,
	{
		readonly createAgentActivity: (
			request: LinearCreateAgentActivityRequest,
		) => Effect.Effect<LinearAgentActivityReceipt, LinearApiError>
		readonly getIssue: (request: LinearIssueRequest) => Effect.Effect<LinearIssueInfo, LinearApiError>
		readonly updateIssue: (request: LinearUpdateIssueRequest) => Effect.Effect<LinearIssueInfo, LinearApiError>
		readonly listAssignableUsers: (
			request: LinearListAssignableUsersRequest,
		) => Effect.Effect<LinearUserPage, LinearApiError>
		readonly listAppUsers: (request: LinearListAppUsersRequest) => Effect.Effect<LinearAppUserPage, LinearApiError>
		readonly getUser: (request: LinearGetUserRequest) => Effect.Effect<LinearUser, LinearApiError>
		readonly listIssueComments: (
			request: LinearIssueRequest,
		) => Effect.Effect<ReadonlyArray<LinearComment>, LinearApiError>
		readonly listIssueAttachments: (
			request: LinearIssueRequest,
		) => Effect.Effect<ReadonlyArray<LinearIssueAttachment>, LinearApiError>
		readonly createComment: (request: LinearCreateCommentRequest) => Effect.Effect<LinearComment, LinearApiError>
		readonly updateComment: (request: LinearUpdateCommentRequest) => Effect.Effect<LinearComment, LinearApiError>
		readonly deleteComment: (request: LinearDeleteCommentRequest) => Effect.Effect<void, LinearApiError>
		readonly createReaction: (request: LinearCreateReactionRequest) => Effect.Effect<LinearReaction, LinearApiError>
		readonly deleteReaction: (request: LinearDeleteReactionRequest) => Effect.Effect<void, LinearApiError>
		readonly createAttachment: (
			request: LinearCreateAttachmentRequest,
		) => Effect.Effect<LinearIssueAttachment, LinearApiError>
		readonly updateAttachment: (
			request: LinearUpdateAttachmentRequest,
		) => Effect.Effect<LinearIssueAttachment, LinearApiError>
		readonly deleteAttachment: (request: LinearDeleteAttachmentRequest) => Effect.Effect<void, LinearApiError>
		readonly uploadFile: (request: LinearUploadFileRequest) => Effect.Effect<LinearFile, LinearApiError>
		readonly uploadAttachment: (
			request: LinearUploadAttachmentRequest,
		) => Effect.Effect<LinearIssueAttachment, LinearApiError>
		readonly downloadFile: (
			request: LinearDownloadFileRequest,
		) => Effect.Effect<Stream.Stream<Uint8Array, LinearApiError>, LinearApiError>
		readonly downloadFileBytes: (
			request: LinearDownloadFileBytesRequest,
		) => Effect.Effect<Uint8Array, LinearFileError>
	}
>()('@humanlayer/channels-linear-next/LinearApi') {}
