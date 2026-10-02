import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Predicate, Schema } from 'effect'

import {
	LinearApi,
	type LinearApiError,
	LinearAttachmentRef,
	LinearCreateAttachmentRequest,
	type LinearCreateAttachmentInput,
	LinearCreateCommentRequest,
	LinearCreateReactionRequest,
	LinearDeleteAttachmentRequest,
	LinearDeleteCommentRequest,
	LinearDeleteReactionRequest,
	LinearGetUserRequest,
	LinearIssueRequest,
	LinearIssueUpdate,
	LinearListUsersRequest,
	LinearReactionTarget,
	type LinearUpdateAttachmentInput,
	LinearUpdateAttachmentRequest,
	LinearUpdateCommentRequest,
	LinearUpdateIssueRequest,
	type LinearUserPageOptions,
} from './LinearApi'
import {
	LinearFile,
	LinearUploadAttachmentRequest,
	LinearUploadFileRequest,
	type LinearUploadAttachmentInput,
	type LinearUploadFileInput,
} from './LinearFiles'
import type { LinearAgentActivityId, LinearIssueLabelId, LinearUserId, LinearWorkflowStateId } from './LinearIdentity'
import { LinearReactionId } from './LinearIdentity'
import { LinearWebhookDeliveryId } from './LinearIdentity'
import {
	LinearActivityContent,
	LinearAgentActivityReceipt,
	LinearAgentSessionSnapshot,
	LinearCommentSnapshot,
	LinearContent,
	LinearCreateAgentActivityRequest,
	LinearIssueSnapshot,
	LinearIssueRef,
	LinearIssueAttachmentSnapshot,
	LinearReactionSnapshot,
	type LinearAppUserPage,
	type LinearIssueInfo,
	type LinearUser,
	type LinearUserPage,
} from './LinearModels'

const issueAttributes = (issueId: string) => ({ 'linear.issue_id': issueId })

const linearListUsersRequest = (issue: LinearIssueRef, options: LinearUserPageOptions) =>
	LinearListUsersRequest.make({ issue, ...options })

export class LinearIssue extends Schema.TaggedClass<LinearIssue>()('LinearIssue', {
	...LinearIssueSnapshot.fields,
	mailboxKey: Schema.NonEmptyString,
	files: Schema.Array(LinearFile),
}) {
	subscribe(): Effect.Effect<MailboxSubscriptionResult, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.subscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('linear.issue.subscribe', { attributes: { 'linear.issue_id': this.ref.issueId } }))
	}

	isSubscribed(): Effect.Effect<boolean, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('linear.issue.is_subscribed', { attributes: { 'linear.issue_id': this.ref.issueId } }))
	}

	unsubscribe(): Effect.Effect<void, MailboxSubscriptionError, MailboxSubscriptions> {
		return Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.unsubscribe({ mailboxKey: this.mailboxKey }),
		).pipe(Effect.withSpan('linear.issue.unsubscribe', { attributes: { 'linear.issue_id': this.ref.issueId } }))
	}

	fetchInfo(): Effect.Effect<LinearIssueInfo, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) => api.getIssue(LinearIssueRequest.make({ issue: this.ref }))).pipe(
			Effect.withSpan('linear.issue.fetch_info', { attributes: issueAttributes(this.ref.issueId) }),
		)
	}
	listAssignableUsers(options: LinearUserPageOptions = {}): Effect.Effect<LinearUserPage, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) => api.listAssignableUsers(linearListUsersRequest(this.ref, options)))
	}
	listAppUsers(options: LinearUserPageOptions = {}): Effect.Effect<LinearAppUserPage, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) => api.listAppUsers(linearListUsersRequest(this.ref, options)))
	}
	getUser(userId: LinearUserId): Effect.Effect<LinearUser, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) => api.getUser(LinearGetUserRequest.make({ issue: this.ref, userId })))
	}
	listComments(): Effect.Effect<ReadonlyArray<LinearComment>, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) => api.listIssueComments(LinearIssueRequest.make({ issue: this.ref })))
	}
	listAttachments(): Effect.Effect<ReadonlyArray<LinearIssueAttachment>, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.listIssueAttachments(LinearIssueRequest.make({ issue: this.ref })),
		)
	}
	postComment(content: LinearContent): Effect.Effect<LinearComment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createComment(LinearCreateCommentRequest.make({ issue: this.ref, content })),
		)
	}
	update(update: LinearIssueUpdate): Effect.Effect<LinearIssueInfo, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.updateIssue(LinearUpdateIssueRequest.make({ issue: this.ref, update })),
		)
	}
	setStatus(stateId: LinearWorkflowStateId) {
		return this.update({ stateId })
	}
	setPriority(priority: number) {
		return this.update({ priority })
	}
	addLabels(addLabelIds: ReadonlyArray<LinearIssueLabelId>) {
		return this.update({ addLabelIds })
	}
	removeLabels(removeLabelIds: ReadonlyArray<LinearIssueLabelId>) {
		return this.update({ removeLabelIds })
	}
	assignTo(assigneeId: LinearUserId | null) {
		return this.update({ assigneeId })
	}
	delegateTo(delegateId: LinearUserId | null) {
		return this.update({ delegateId })
	}
	addReaction(emoji: string): Effect.Effect<LinearReaction, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createReaction(
				LinearCreateReactionRequest.make({
					target: LinearReactionTarget.cases.Issue.make({ issue: this.ref }),
					emoji,
				}),
			),
		)
	}
	createAttachment(
		input: LinearCreateAttachmentInput,
	): Effect.Effect<LinearIssueAttachment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createAttachment(LinearCreateAttachmentRequest.make({ issue: this.ref, input })),
		)
	}
	/** Uploads bytes as a Linear asset. Place the returned `url` in issue or comment Markdown to show it. */
	uploadFile(input: LinearUploadFileInput): Effect.Effect<LinearFile, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.uploadFile(LinearUploadFileRequest.make({ issue: this.ref, input })),
		).pipe(Effect.withSpan('linear.issue.upload_file', { attributes: issueAttributes(this.ref.issueId) }))
	}
	/** Uploads bytes and attaches the asset to this issue as a first-class Linear attachment card. */
	uploadAttachment(
		input: LinearUploadAttachmentInput,
	): Effect.Effect<LinearIssueAttachment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.uploadAttachment(LinearUploadAttachmentRequest.make({ issue: this.ref, input })),
		).pipe(Effect.withSpan('linear.issue.upload_attachment', { attributes: issueAttributes(this.ref.issueId) }))
	}
}

export class LinearComment extends Schema.TaggedClass<LinearComment>()('LinearComment', {
	...LinearCommentSnapshot.fields,
	files: Schema.Array(LinearFile),
}) {
	reply(content: LinearContent): Effect.Effect<LinearComment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createComment(
				LinearCreateCommentRequest.make({ issue: this.issue, parentId: this.ref.commentId, content }),
			),
		)
	}
	update(content: LinearContent): Effect.Effect<LinearComment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.updateComment(LinearUpdateCommentRequest.make({ comment: this.ref, content })),
		)
	}
	remove(): Effect.Effect<void, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.deleteComment(LinearDeleteCommentRequest.make({ comment: this.ref })),
		)
	}
	addReaction(emoji: string): Effect.Effect<LinearReaction, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createReaction(
				LinearCreateReactionRequest.make({
					target: LinearReactionTarget.cases.Comment.make({ comment: this.ref }),
					emoji,
				}),
			),
		)
	}
}

export class LinearReaction extends Schema.TaggedClass<LinearReaction>()('LinearReaction', {
	...LinearReactionSnapshot.fields,
	ref: Schema.Struct({ issue: LinearIssueRef, reactionId: LinearReactionId }),
}) {
	remove(): Effect.Effect<void, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.deleteReaction(
				LinearDeleteReactionRequest.make({ issue: this.ref.issue, reactionId: this.ref.reactionId }),
			),
		)
	}
}

export class LinearIssueAttachment extends Schema.TaggedClass<LinearIssueAttachment>()('LinearIssueAttachment', {
	...LinearIssueAttachmentSnapshot.fields,
	ref: LinearAttachmentRef,
}) {
	update(input: LinearUpdateAttachmentInput): Effect.Effect<LinearIssueAttachment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.updateAttachment(LinearUpdateAttachmentRequest.make({ attachment: this.ref, input })),
		)
	}
	remove(): Effect.Effect<void, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.deleteAttachment(LinearDeleteAttachmentRequest.make({ attachment: this.ref })),
		)
	}
}

/** Options for a session's own thought. */
export type LinearAgentThoughtOptions = {
	/**
	 * A UUID v4 to send as the activity's ID. Linear refuses a second activity with the same ID, so a
	 * retry with the same ID cannot post the thought twice; that refusal fails with reason `already_exists`.
	 */
	readonly activityId?: LinearAgentActivityId
}

export class LinearAgentSession extends Schema.TaggedClass<LinearAgentSession>()('LinearAgentSession', {
	...LinearAgentSessionSnapshot.fields,
	mailboxKey: Schema.NonEmptyString,
	deliveryId: LinearWebhookDeliveryId,
}) {
	private createActivity(
		purpose: 'thought' | 'response' | 'error',
		content: LinearActivityContent,
		ephemeral: boolean,
		activityId: LinearAgentActivityId | undefined,
	): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		const request = {
			organizationId: this.ref.organizationId,
			sessionId: this.ref.sessionId,
			content,
			ephemeral,
			deliveryId: this.deliveryId,
		}
		return Effect.flatMap(LinearApi, (api) =>
			api.createAgentActivity(
				LinearCreateAgentActivityRequest.make(
					Predicate.isUndefined(activityId) ? request : { ...request, activityId },
				),
			),
		).pipe(
			Effect.withSpan(`linear.agent_session.${purpose}`, {
				attributes: {
					'linear.agent_session_id': this.ref.sessionId,
					'linear.delivery_id': this.deliveryId,
				},
			}),
		)
	}

	/** An ephemeral thought: Linear replaces it with the agent's next activity. */
	thought(
		body: string,
		options: LinearAgentThoughtOptions = {},
	): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		return this.createActivity('thought', LinearActivityContent.cases.Thought.make({ body }), true, options.activityId)
	}

	/** The final answer: the session becomes `complete`. */
	respond(body: string): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		return this.createActivity('response', LinearActivityContent.cases.Response.make({ body }), false, undefined)
	}

	/** Report that the work failed: the session becomes `error`. */
	error(body: string): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		return this.createActivity('error', LinearActivityContent.cases.Error.make({ body }), false, undefined)
	}
}
