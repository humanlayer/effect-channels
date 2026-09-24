import {
	deliveryMailboxKey,
	type DeliveryAdmission,
	type DeliveryAdmissionBatch,
	MailboxSubscriptions,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery-next'
import { Effect, Match, Predicate, Schema } from 'effect'

import { LinearApi } from './LinearApi'
import {
	LinearAgentSessionCreated,
	LinearAgentSessionPrompted,
	LinearAssignmentNotification,
	LinearCommentMention,
	LinearCommentMentioned,
	LinearCommentReaction,
	LinearIssueActivity,
	LinearIssueAssigned,
	LinearIssueCreated,
	LinearIssueMention,
	LinearIssueMentioned,
	LinearIssueNewComment,
	LinearIssueOpened,
	LinearIssueReaction,
	LinearIssueStatusChanged,
	LinearIssueUnassigned,
} from './LinearCallbackEvents'
import { LinearCallbacks } from './LinearCallbacks'
import {
	LinearAgentSessionId,
	LinearIssueId,
	LinearWebhookDeliveryId,
	linearAgentSessionResourceId,
	linearIssueResourceId,
} from './LinearIdentity'
import {
	LinearAgentGuidance,
	LinearAgentPrompt,
	LinearAgentSessionComment,
	LinearAgentSessionRef,
	LinearCommentRef,
	LinearContent,
	LinearIssueRef,
	LinearParticipant,
	LinearTeamSnapshot,
} from './LinearModels'
import { LinearAgentSession, LinearComment, LinearIssue } from './LinearResources'
import type { LinearAppUserNotificationWebhook } from './LinearWebhookEventSchemas'
import {
	normalizeLinearAgentSessionWebhook,
	normalizeLinearAppUserNotificationWebhook,
	type LinearNormalizedAgentSessionWebhook,
} from './LinearWebhookParsers'
import { LinearStoredAgentSessionWebhook, LinearStoredWebhook, LinearSupportedWebhook } from './LinearWebhookSchemas'

export type LinearEventProcessorOptions = {
	readonly namespace: string
	readonly bot?: {
		readonly organizationId: string
		readonly appUserId: string
	}
	readonly oauthClientId?: string
}

const invalidPayload = () => ProviderEventInvalid.make({ provider: 'linear', reason: 'invalid_payload' })
const identityMismatch = () => ProviderEventInvalid.make({ provider: 'linear', reason: 'identity_mismatch' })
const providerFailure = (safeCode: string) =>
	ProviderEventExecutionFailed.make({ provider: 'linear', retryable: true, safeCode })
const nullable = <A>(value: A | null | undefined): A | null => (Predicate.isNullish(value) ? null : value)

const issueMailboxKey = (admission: DeliveryAdmission, issueId: LinearIssueId) =>
	deliveryMailboxKey({
		namespace: admission.namespace,
		provider: admission.provider,
		installationId: admission.installationId,
		resourceId: linearIssueResourceId(issueId),
	})

const decodeWebhook = (admission: DeliveryAdmission) =>
	Effect.gen(function* () {
		const webhook = yield* Schema.decodeUnknownEffect(LinearStoredWebhook)(admission.payload, {
			onExcessProperty: 'preserve',
		})
		if (Schema.is(LinearStoredAgentSessionWebhook)(webhook)) return webhook
		if (webhook.type === 'AppUserNotification') return yield* normalizeLinearAppUserNotificationWebhook(webhook)
		return webhook
	}).pipe(
		Effect.tapError((error) => Effect.logError('Stored Linear webhook could not be decoded', error)),
		Effect.mapError(invalidPayload),
	)

const parseIssueAddress = (resourceId: string) =>
	Effect.gen(function* () {
		const parts = resourceId.split(':')
		if (parts.length !== 4 || parts[0] !== 'linear' || parts[1] !== 'v1' || parts[2] !== 'issue')
			return yield* identityMismatch()
		const encoded = parts[3]
		if (Predicate.isUndefined(encoded)) return yield* identityMismatch()
		const decoded = yield* Effect.try({ try: () => decodeURIComponent(encoded), catch: identityMismatch })
		return yield* Schema.decodeUnknownEffect(LinearIssueId)(decoded).pipe(Effect.mapError(identityMismatch))
	})

const parseAgentSessionAddress = (resourceId: string) =>
	Effect.gen(function* () {
		const prefix = 'linear:v1:agent-session:'
		if (!resourceId.startsWith(prefix)) return yield* identityMismatch()
		const encoded = resourceId.slice(prefix.length)
		if (encoded.length === 0) return yield* identityMismatch()
		const decoded = yield* Effect.try({ try: () => decodeURIComponent(encoded), catch: identityMismatch })
		return yield* Schema.decodeUnknownEffect(LinearAgentSessionId)(decoded).pipe(Effect.mapError(identityMismatch))
	})

const participant = (value: {
	readonly id: (typeof LinearParticipant.Type)['id']
	readonly name: string
	readonly email?: string | null
	readonly url?: string | null
	readonly avatarUrl?: string | null
}) =>
	LinearParticipant.make({
		id: value.id,
		name: value.name,
		email: nullable(value.email),
		url: nullable(value.url),
		avatarUrl: nullable(value.avatarUrl),
	})

const participantOrNull = (value: Parameters<typeof participant>[0] | null | undefined) =>
	Predicate.isNullish(value) ? null : participant(value)

const issueFromCreate = (
	webhook: Extract<typeof LinearSupportedWebhook.Type, { readonly type: 'Issue' }>,
	mailboxKey: string,
) =>
	LinearIssue.make({
		ref: LinearIssueRef.make({
			organizationId: webhook.organizationId,
			teamId: webhook.data.team.id,
			issueId: webhook.data.id,
		}),
		mailboxKey,
		identifier: webhook.data.identifier,
		number: webhook.data.number,
		title: webhook.data.title,
		description: nullable(webhook.data.description),
		priority: nullable(webhook.data.priority),
		url: webhook.data.url,
		team: LinearTeamSnapshot.make(webhook.data.team),
		creator: participantOrNull(webhook.data.creator),
	})

const issueFromNotification = (webhook: LinearAppUserNotificationWebhook, mailboxKey: string) => {
	const issue = webhook.notification.issue
	return LinearIssue.make({
		ref: LinearIssueRef.make({
			organizationId: webhook.organizationId,
			teamId: issue.team.id,
			issueId: issue.id,
		}),
		mailboxKey,
		identifier: issue.identifier,
		number: null,
		title: issue.title,
		description: nullable(issue.description),
		priority: null,
		url: issue.url,
		team: LinearTeamSnapshot.make(issue.team),
		creator: null,
	})
}

const issueFromAgentSession = (normalized: LinearNormalizedAgentSessionWebhook, mailboxKey: string) => {
	const { issue, webhook } = normalized
	return LinearIssue.make({
		ref: LinearIssueRef.make({
			organizationId: webhook.organizationId,
			teamId: issue.team.id,
			issueId: issue.id,
		}),
		mailboxKey,
		identifier: issue.identifier,
		number: null,
		title: issue.title,
		description: nullable(issue.description),
		priority: null,
		url: issue.url,
		team: LinearTeamSnapshot.make(issue.team),
		creator: null,
	})
}

const sessionFromWebhook = (
	normalized: LinearNormalizedAgentSessionWebhook,
	admission: DeliveryAdmission,
	mailboxKey: string,
	deliveryId: LinearWebhookDeliveryId,
) => {
	const { issue: sessionIssue, webhook } = normalized
	const issue = issueFromAgentSession(normalized, issueMailboxKey(admission, sessionIssue.id))
	return LinearAgentSession.make({
		ref: LinearAgentSessionRef.make({
			organizationId: webhook.organizationId,
			appUserId: webhook.appUserId,
			sessionId: webhook.agentSession.id,
			issueId: issue.ref.issueId,
		}),
		triggerEventId: admission.eventId,
		createdAt: webhook.agentSession.createdAt,
		endedAt: normalized.endedAt,
		commentId: normalized.commentId,
		sourceCommentId: normalized.sourceCommentId,
		creator: participantOrNull(normalized.creator),
		mailboxKey,
		deliveryId,
	})
}

const commentFromNotification = (
	webhook: Extract<
		LinearAppUserNotificationWebhook,
		{ readonly action: 'issueCommentMention' | 'issueNewComment' | 'issueCommentReaction' }
	>,
	issue: LinearIssue,
) => {
	const value = webhook.notification.comment
	const actor = webhook.notification.actor
	return LinearComment.make({
		ref: LinearCommentRef.make({
			organizationId: issue.ref.organizationId,
			teamId: issue.ref.teamId,
			issueId: issue.ref.issueId,
			commentId: value.id,
		}),
		issue: issue.ref,
		parentCommentId: nullable(webhook.notification.parentCommentId),
		content: LinearContent.make({ markdown: value.body }),
		author: Predicate.isNotNullish(actor) && actor.id === value.userId ? participant(actor) : null,
	})
}

const normalizeWebhook = (
	webhook: Exclude<typeof LinearSupportedWebhook.Type, { readonly type: 'AgentSessionEvent' }>,
	admission: DeliveryAdmission,
	mailboxKey: string,
): LinearIssueActivity => {
	const eventId = LinearWebhookDeliveryId.make(admission.eventId)
	if (webhook.type === 'Issue') {
		const issue = issueFromCreate(webhook, mailboxKey)
		return LinearIssueOpened.make({
			eventId,
			issue,
			actor: participantOrNull(webhook.actor),
		})
	}

	const issue = issueFromNotification(webhook, mailboxKey)
	const actor = webhook.notification.actor
	switch (webhook.action) {
		case 'issueMention':
			return LinearIssueMention.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				notification: webhook.notification,
			})
		case 'issueCommentMention':
			return LinearCommentMention.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				comment: commentFromNotification(webhook, issue),
				notification: webhook.notification,
			})
		case 'issueAssignedToYou':
			return LinearAssignmentNotification.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				notification: webhook.notification,
			})
		case 'issueUnassignedFromYou':
			return LinearIssueUnassigned.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				notification: webhook.notification,
			})
		case 'issueNewComment':
			return LinearIssueNewComment.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				comment: commentFromNotification(webhook, issue),
				notification: webhook.notification,
			})
		case 'issueStatusChanged':
			return LinearIssueStatusChanged.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				notification: webhook.notification,
			})
		case 'issueEmojiReaction':
			return LinearIssueReaction.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				notification: webhook.notification,
			})
		case 'issueCommentReaction':
			return LinearCommentReaction.make({
				eventId,
				issue,
				actor: participantOrNull(actor),
				comment: commentFromNotification(webhook, issue),
				notification: webhook.notification,
			})
	}
}

const runCallback = (effect: Effect.Effect<void, { readonly retryable: boolean }>) =>
	effect.pipe(
		Effect.mapError((error) =>
			ProviderEventExecutionFailed.make({
				provider: 'linear',
				retryable: error.retryable,
				safeCode: 'callback_failed',
			}),
		),
		Effect.as(ProviderEventHandled.make({})),
	)

const processAgentSessionBatch = (
	options: LinearEventProcessorOptions,
	admissions: DeliveryAdmissionBatch,
	storedWebhooks: ReadonlyArray<typeof LinearStoredWebhook.Type>,
) =>
	Effect.gen(function* () {
		const callbacks = yield* LinearCallbacks
		const api = yield* LinearApi
		const firstAdmission = admissions[0]
		const firstStoredWebhook = storedWebhooks[0]
		if (
			Predicate.isUndefined(firstStoredWebhook) ||
			!Schema.is(LinearStoredAgentSessionWebhook)(firstStoredWebhook)
		)
			return yield* identityMismatch()
		const normalizedWebhook = yield* normalizeLinearAgentSessionWebhook(firstStoredWebhook.webhook).pipe(
			Effect.tapError((error) => Effect.logError('Stored Linear Agent Session could not be normalized', error)),
			Effect.mapError(identityMismatch),
		)
		const firstWebhook = normalizedWebhook.webhook
		const sessionId = yield* parseAgentSessionAddress(firstAdmission.resourceId)
		if (admissions.length !== 1) return yield* invalidPayload()
		if (
			firstAdmission.namespace !== options.namespace ||
			firstAdmission.provider !== 'linear' ||
			firstAdmission.installationId !== firstWebhook.organizationId ||
			firstWebhook.agentSession.id !== sessionId ||
			linearAgentSessionResourceId(firstWebhook.agentSession.id) !== firstAdmission.resourceId ||
			(Predicate.isNotUndefined(options.bot) &&
				(firstWebhook.organizationId !== options.bot.organizationId ||
					firstWebhook.appUserId !== options.bot.appUserId)) ||
			(Predicate.isNotUndefined(options.oauthClientId) && firstWebhook.oauthClientId !== options.oauthClientId)
		)
			return yield* identityMismatch()
		const issue = issueFromAgentSession(
			normalizedWebhook,
			issueMailboxKey(firstAdmission, normalizedWebhook.issue.id),
		)
		const session = sessionFromWebhook(
			normalizedWebhook,
			firstAdmission,
			deliveryMailboxKey(firstAdmission),
			firstStoredWebhook.deliveryId,
		)
		const deliveryId = firstStoredWebhook.deliveryId

		return yield* Match.value(normalizedWebhook).pipe(
			Match.discriminatorsExhaustive('action')({
				created: (created) => {
					if (Predicate.isUndefined(callbacks.onAgentSessionCreated))
						return Effect.succeed(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
					return session.thought('Working on this…').pipe(
						Effect.provideService(LinearApi, api),
						Effect.mapError((error) =>
							ProviderEventExecutionFailed.make({
								provider: 'linear',
								retryable: error.retryable,
								safeCode: 'initial_thought_failed',
							}),
						),
						Effect.andThen(
							runCallback(
								callbacks.onAgentSessionCreated(
									LinearAgentSessionCreated.make({
										session,
										issue,
										promptContext: created.promptContext,
										previousComments: created.previousComments.map((comment) =>
											LinearAgentSessionComment.make(comment),
										),
										guidance: created.guidance.map((guidance) =>
											LinearAgentGuidance.make(guidance),
										),
										deliveryId,
									}),
								),
							),
						),
					)
				},
				prompted: ({ agentActivity: activity }) => {
					if (Predicate.isUndefined(callbacks.onAgentSessionPrompted))
						return Effect.succeed(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
					return runCallback(
						callbacks.onAgentSessionPrompted(
							LinearAgentSessionPrompted.make({
								session,
								issue,
								prompt: LinearAgentPrompt.make({
									id: activity.id,
									body: activity.content.body,
									createdAt: activity.createdAt,
									user: participant(activity.user),
								}),
								deliveryId,
							}),
						),
					)
				},
			}),
		)
	})

const processLinearBatch = (options: LinearEventProcessorOptions) =>
	Effect.fn('linear.process_event_batch')(function* (admissions: DeliveryAdmissionBatch) {
		const callbacks = yield* LinearCallbacks
		yield* LinearApi
		const first = admissions[0]
		const webhooks = yield* Effect.forEach(admissions, decodeWebhook)
		if (Predicate.isNotUndefined(webhooks[0]) && Schema.is(LinearStoredAgentSessionWebhook)(webhooks[0]))
			return yield* processAgentSessionBatch(options, admissions, webhooks)
		const issueWebhooks = yield* Effect.forEach(webhooks, (webhook) =>
			Schema.is(LinearStoredAgentSessionWebhook)(webhook)
				? Effect.fail(identityMismatch())
				: Effect.succeed(webhook),
		)
		const issueId = yield* parseIssueAddress(first.resourceId)

		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const webhook = issueWebhooks[index]
			if (Predicate.isUndefined(admission) || Predicate.isUndefined(webhook)) return yield* identityMismatch()
			const webhookIssueId = webhook.type === 'Issue' ? webhook.data.id : webhook.notification.issueId
			if (
				admission.namespace !== options.namespace ||
				admission.provider !== 'linear' ||
				admission.installationId !== first.installationId ||
				admission.resourceId !== first.resourceId ||
				admission.installationId !== webhook.organizationId ||
				webhookIssueId !== issueId ||
				linearIssueResourceId(webhookIssueId) !== admission.resourceId ||
				(webhook.type === 'AppUserNotification' &&
					((Predicate.isNotUndefined(options.bot) &&
						(webhook.organizationId !== options.bot.organizationId ||
							webhook.appUserId !== options.bot.appUserId)) ||
						(Predicate.isNotUndefined(options.oauthClientId) &&
							webhook.oauthClientId !== options.oauthClientId)))
			)
				return yield* identityMismatch()
		}

		const mailboxKey = deliveryMailboxKey(first)
		const normalized = yield* Effect.forEach(issueWebhooks, (webhook, index) => {
			const admission = admissions[index]
			return Predicate.isUndefined(admission)
				? Effect.fail(identityMismatch())
				: Effect.succeed(normalizeWebhook(webhook, admission, mailboxKey))
		})
		const hasAgentSessionCallbacks =
			!Predicate.isUndefined(callbacks.onAgentSessionCreated) ||
			!Predicate.isUndefined(callbacks.onAgentSessionPrompted)
		if (
			hasAgentSessionCallbacks &&
			normalized.some(
				(event) =>
					Schema.is(LinearIssueMention)(event) ||
					Schema.is(LinearCommentMention)(event) ||
					Schema.is(LinearAssignmentNotification)(event),
			)
		)
			return ProviderEventIgnored.make({ reason: 'supplemental_signal' })
		const directedIndex = normalized.findIndex(
			(event) =>
				((Schema.is(LinearIssueMention)(event) || Schema.is(LinearCommentMention)(event)) &&
					!Predicate.isUndefined(callbacks.onMentioned)) ||
				(Schema.is(LinearAssignmentNotification)(event) && !Predicate.isUndefined(callbacks.onAssigned)),
		)
		if (directedIndex >= 0) {
			const trigger = normalized[directedIndex]
			if (Predicate.isUndefined(trigger)) return ProviderEventIgnored.make({ reason: 'no_activation_event' })
			const events = normalized.filter((_, index) => index !== directedIndex)
			if (Schema.is(LinearIssueMention)(trigger) && !Predicate.isUndefined(callbacks.onMentioned))
				return yield* runCallback(
					callbacks.onMentioned(LinearIssueMentioned.make({ issue: trigger.issue, trigger, events })),
				)
			if (Schema.is(LinearCommentMention)(trigger) && !Predicate.isUndefined(callbacks.onMentioned))
				return yield* runCallback(
					callbacks.onMentioned(LinearCommentMentioned.make({ issue: trigger.issue, trigger, events })),
				)
			if (Schema.is(LinearAssignmentNotification)(trigger) && !Predicate.isUndefined(callbacks.onAssigned))
				return yield* runCallback(
					callbacks.onAssigned(LinearIssueAssigned.make({ issue: trigger.issue, trigger, events })),
				)
			return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		}

		const openedIndex = normalized.findIndex(Schema.is(LinearIssueOpened))
		if (openedIndex >= 0 && !Predicate.isUndefined(callbacks.onIssueCreated)) {
			const trigger = normalized[openedIndex]
			if (Predicate.isUndefined(trigger) || !Schema.is(LinearIssueOpened)(trigger))
				return ProviderEventIgnored.make({ reason: 'no_activation_event' })
			return yield* runCallback(
				callbacks.onIssueCreated(
					LinearIssueCreated.make({
						issue: trigger.issue,
						trigger,
						events: normalized.filter((_, index) => index !== openedIndex),
					}),
				),
			)
		}

		const subscribed = yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey }),
		).pipe(
			Effect.tapError((error) => Effect.logError('Linear subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)
		if (subscribed) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })
		const hasUnconfiguredEntry = normalized.some(
			(event) =>
				Schema.is(LinearIssueMention)(event) ||
				Schema.is(LinearCommentMention)(event) ||
				Schema.is(LinearAssignmentNotification)(event) ||
				Schema.is(LinearIssueOpened)(event),
		)
		return ProviderEventIgnored.make({
			reason: hasUnconfiguredEntry ? 'callback_not_configured' : 'not_subscribed',
		})
	})

export const makeLinearEventProcessor = (
	options: LinearEventProcessorOptions,
): ProviderEventProcessor<LinearCallbacks | LinearApi | MailboxSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'linear',
	process: processLinearBatch(options),
})
