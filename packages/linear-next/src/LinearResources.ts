import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

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
	type LinearUpdateAttachmentInput,
	LinearUpdateAttachmentRequest,
	LinearUpdateCommentRequest,
	LinearUpdateIssueRequest,
	type LinearUserPageOptions,
} from './LinearApi'
import type { LinearIssueLabelId, LinearUserId, LinearWorkflowStateId } from './LinearIdentity'
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

export class LinearIssue extends Schema.TaggedClass<LinearIssue>()('LinearIssue', {
	...LinearIssueSnapshot.fields,
	mailboxKey: Schema.NonEmptyString,
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
		return Effect.flatMap(LinearApi, (api) =>
			api.listAssignableUsers(
				LinearListUsersRequest.make({
					issue: this.ref,
					...(options.first === undefined ? {} : { first: options.first }),
					...(options.after === undefined ? {} : { after: options.after }),
					...(options.query === undefined ? {} : { query: options.query }),
				}),
			),
		)
	}
	listAppUsers(options: LinearUserPageOptions = {}): Effect.Effect<LinearAppUserPage, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.listAppUsers(
				LinearListUsersRequest.make({
					issue: this.ref,
					...(options.first === undefined ? {} : { first: options.first }),
					...(options.after === undefined ? {} : { after: options.after }),
					...(options.query === undefined ? {} : { query: options.query }),
				}),
			),
		)
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
	postComment(content: typeof LinearContent.Type): Effect.Effect<LinearComment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createComment(LinearCreateCommentRequest.make({ issue: this.ref, content })),
		)
	}
	update(update: typeof LinearIssueUpdate.Type): Effect.Effect<LinearIssueInfo, LinearApiError, LinearApi> {
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
			api.createReaction(LinearCreateReactionRequest.make({ target: { _tag: 'Issue', issue: this.ref }, emoji })),
		)
	}
	createAttachment(
		input: LinearCreateAttachmentInput,
	): Effect.Effect<LinearIssueAttachment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createAttachment(LinearCreateAttachmentRequest.make({ issue: this.ref, input })),
		)
	}
}

export class LinearComment extends Schema.TaggedClass<LinearComment>()('LinearComment', {
	...LinearCommentSnapshot.fields,
}) {
	reply(content: typeof LinearContent.Type): Effect.Effect<LinearComment, LinearApiError, LinearApi> {
		return Effect.flatMap(LinearApi, (api) =>
			api.createComment(
				LinearCreateCommentRequest.make({ issue: this.issue, parentId: this.ref.commentId, content }),
			),
		)
	}
	update(content: typeof LinearContent.Type): Effect.Effect<LinearComment, LinearApiError, LinearApi> {
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
				LinearCreateReactionRequest.make({ target: { _tag: 'Comment', comment: this.ref }, emoji }),
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

export class LinearAgentSession extends Schema.TaggedClass<LinearAgentSession>()('LinearAgentSession', {
	...LinearAgentSessionSnapshot.fields,
	mailboxKey: Schema.NonEmptyString,
	deliveryId: LinearWebhookDeliveryId,
}) {
	private createActivity(
		purpose: 'thought' | 'response',
		body: string,
		ephemeral: boolean,
	): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		const content =
			purpose === 'thought'
				? LinearActivityContent.cases.Thought.make({ body })
				: LinearActivityContent.cases.Response.make({ body })
		return Effect.flatMap(LinearApi, (api) =>
			api.createAgentActivity(
				LinearCreateAgentActivityRequest.make({
					organizationId: this.ref.organizationId,
					sessionId: this.ref.sessionId,
					content,
					ephemeral,
					deliveryId: this.deliveryId,
				}),
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

	thought(body: string): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		return this.createActivity('thought', body, true)
	}

	respond(body: string): Effect.Effect<LinearAgentActivityReceipt, LinearApiError, LinearApi> {
		return this.createActivity('response', body, false)
	}
}
