import { Effect, Match, Predicate, Schema } from 'effect'

import { LinearCommentId, LinearIssueId, LinearUserId } from './LinearIdentity'
import {
	LinearAgentPromptActivityWebhook,
	LinearAgentSessionEventWebhook,
	type LinearAgentSessionEventWebhook as LinearAgentSessionEventWebhookType,
	LinearAppUserNotificationWebhook,
	type LinearAppUserNotificationWebhook as LinearAppUserNotificationWebhookType,
	LinearNotificationIssue,
	LinearWebhookActor,
} from './LinearWebhookEventSchemas'

export class LinearWebhookRelationshipError extends Schema.TaggedError<LinearWebhookRelationshipError>()(
	'LinearWebhookRelationshipError',
	{
		reason: Schema.Literals(['notification_identity_mismatch', 'session_identity_mismatch']),
	},
) {}

const sameWhenPresent = <A>(left: A | null | undefined, right: A | null | undefined): boolean =>
	Predicate.isNullish(left) || Predicate.isNullish(right) || left === right

const nullable = <A>(value: A | null | undefined): A | null => (Predicate.isNullish(value) ? null : value)

const collection = <A>(value: ReadonlyArray<A> | null | undefined): ReadonlyArray<A> =>
	Predicate.isNullish(value) ? [] : value

const notificationRelationshipsMatch = (webhook: LinearAppUserNotificationWebhookType): boolean => {
	const { notification } = webhook
	if (notification.issue.id !== notification.issueId) return false
	if (notification.issue.teamId !== notification.issue.team.id) return false
	if (notification.userId !== webhook.appUserId) return false
	if (!sameWhenPresent(notification.actorId, notification.actor?.id)) return false

	return Match.value(webhook).pipe(
		Match.discriminatorsExhaustive('action')({
			issueMention: () => true,
			issueAssignedToYou: () => true,
			issueUnassignedFromYou: () => true,
			issueStatusChanged: () => true,
			issueEmojiReaction: () => true,
			issueCommentMention: ({ notification: value }) =>
				value.commentId === value.comment.id && sameWhenPresent(value.comment.issueId, value.issueId),
			issueNewComment: ({ notification: value }) =>
				value.commentId === value.comment.id && sameWhenPresent(value.comment.issueId, value.issueId),
			issueCommentReaction: ({ notification: value }) =>
				value.commentId === value.comment.id && sameWhenPresent(value.comment.issueId, value.issueId),
		}),
	)
}

const agentSessionRelationshipsMatch = (webhook: LinearAgentSessionEventWebhookType): boolean => {
	const session = webhook.agentSession
	if (session.organizationId !== webhook.organizationId) return false
	if (session.appUserId !== webhook.appUserId) return false
	if (Predicate.isNullish(session.issue) || Predicate.isNullish(session.issueId)) return false
	if (session.issueId !== session.issue.id) return false
	if (session.issue.teamId !== session.issue.team.id) return false
	if (!sameWhenPresent(session.creatorId, session.creator?.id)) return false
	if (!sameWhenPresent(session.commentId, session.comment?.id)) return false
	if (!sameWhenPresent(session.comment?.issueId, session.issueId)) return false
	if (
		Predicate.isNotNullish(webhook.agentActivity) &&
		(webhook.agentActivity.agentSessionId !== session.id ||
			webhook.agentActivity.userId !== webhook.agentActivity.user.id)
	)
		return false
	return collection(webhook.previousComments).every((comment) => sameWhenPresent(comment.issueId, session.issueId))
}

export const normalizeLinearAppUserNotificationWebhook = (webhook: LinearAppUserNotificationWebhookType) =>
	Effect.gen(function* () {
		if (!notificationRelationshipsMatch(webhook))
			return yield* LinearWebhookRelationshipError.make({ reason: 'notification_identity_mismatch' })
		return webhook
	})

const LinearNormalizedAgentSessionComment = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	issueId: Schema.NullOr(LinearIssueId),
	userId: Schema.NullOr(LinearUserId),
})

const normalizedAgentSessionFields = {
	webhook: LinearAgentSessionEventWebhook,
	issue: LinearNotificationIssue,
	creator: Schema.NullOr(LinearWebhookActor),
	endedAt: Schema.NullOr(Schema.String),
	commentId: Schema.NullOr(LinearCommentId),
	sourceCommentId: Schema.NullOr(LinearCommentId),
	promptContext: Schema.NullOr(Schema.String),
	previousComments: Schema.Array(LinearNormalizedAgentSessionComment),
	guidance: Schema.Array(Schema.Struct({ body: Schema.String, origin: Schema.String })),
}

const LinearNormalizedAgentSessionCreatedWebhook = Schema.Struct({
	...normalizedAgentSessionFields,
	action: Schema.Literal('created'),
	agentActivity: Schema.NullOr(LinearAgentPromptActivityWebhook),
})
const LinearNormalizedAgentSessionPromptedWebhook = Schema.Struct({
	...normalizedAgentSessionFields,
	action: Schema.Literal('prompted'),
	agentActivity: LinearAgentPromptActivityWebhook,
})

export const LinearNormalizedAgentSessionWebhook = Schema.Union([
	LinearNormalizedAgentSessionCreatedWebhook,
	LinearNormalizedAgentSessionPromptedWebhook,
])
export type LinearNormalizedAgentSessionWebhook = typeof LinearNormalizedAgentSessionWebhook.Type

const normalizeAgentSession = (
	webhook: LinearAgentSessionEventWebhookType,
	issue: typeof LinearNotificationIssue.Type,
): LinearNormalizedAgentSessionWebhook =>
	Match.value(webhook).pipe(
		Match.discriminatorsExhaustive('action')({
			created: (created) => ({
				action: 'created' as const,
				webhook: created,
				issue,
				creator: nullable(created.agentSession.creator),
				endedAt: nullable(created.agentSession.endedAt),
				commentId: nullable(created.agentSession.commentId),
				sourceCommentId: nullable(created.agentSession.sourceCommentId),
				promptContext: nullable(created.promptContext),
				previousComments: collection(created.previousComments).map((comment) => ({
					id: comment.id,
					body: comment.body,
					issueId: nullable(comment.issueId),
					userId: nullable(comment.userId),
				})),
				guidance: collection(created.guidance).map((rule) => ({ body: rule.body, origin: rule.origin.type })),
				agentActivity: nullable(created.agentActivity),
			}),
			prompted: (prompted) => ({
				action: 'prompted' as const,
				webhook: prompted,
				issue,
				creator: nullable(prompted.agentSession.creator),
				endedAt: nullable(prompted.agentSession.endedAt),
				commentId: nullable(prompted.agentSession.commentId),
				sourceCommentId: nullable(prompted.agentSession.sourceCommentId),
				promptContext: nullable(prompted.promptContext),
				previousComments: collection(prompted.previousComments).map((comment) => ({
					id: comment.id,
					body: comment.body,
					issueId: nullable(comment.issueId),
					userId: nullable(comment.userId),
				})),
				guidance: collection(prompted.guidance).map((rule) => ({ body: rule.body, origin: rule.origin.type })),
				agentActivity: prompted.agentActivity,
			}),
		}),
	)

export const normalizeLinearAgentSessionWebhook = (webhook: LinearAgentSessionEventWebhookType) =>
	Effect.gen(function* () {
		if (!agentSessionRelationshipsMatch(webhook))
			return yield* LinearWebhookRelationshipError.make({ reason: 'session_identity_mismatch' })
		const issue = webhook.agentSession.issue
		if (Predicate.isNullish(issue))
			return yield* LinearWebhookRelationshipError.make({ reason: 'session_identity_mismatch' })
		return normalizeAgentSession(webhook, issue)
	})

/** Decodes and normalizes one Agent Session provider payload at a trust boundary. */
export const parseLinearAgentSessionWebhook = (input: unknown) =>
	Schema.decodeUnknownEffect(LinearAgentSessionEventWebhook)(input, { onExcessProperty: 'preserve' }).pipe(
		Effect.flatMap(normalizeLinearAgentSessionWebhook),
	)

/** Decodes and verifies one Inbox Notification provider payload at a trust boundary. */
export const parseLinearAppUserNotificationWebhook = (input: unknown) =>
	Schema.decodeUnknownEffect(LinearAppUserNotificationWebhook)(input, { onExcessProperty: 'preserve' }).pipe(
		Effect.flatMap(normalizeLinearAppUserNotificationWebhook),
	)
