import {
	type MailboxSubscriptionError,
	type MailboxSubscriptionResult,
	MailboxSubscriptions,
} from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

import { LinearIssueSnapshot } from './LinearModels'

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
