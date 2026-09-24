import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

import { LinearApi, type LinearApiError } from './LinearApi'
import { LinearWebhookDeliveryId } from './LinearIdentity'
import {
	LinearActivityContent,
	LinearAgentActivityReceipt,
	LinearAgentSessionSnapshot,
	LinearCommentSnapshot,
	LinearCreateAgentActivityRequest,
	LinearIssueSnapshot,
} from './LinearModels'

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
}

export class LinearComment extends Schema.TaggedClass<LinearComment>()('LinearComment', {
	...LinearCommentSnapshot.fields,
}) {}

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
