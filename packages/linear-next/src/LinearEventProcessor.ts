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
	LinearCommentCreated,
	LinearCommentRemoved,
	LinearCommentUpdated,
	LinearCommentReaction,
	LinearIssueActivity,
	LinearIssueAssigned,
	LinearIssueCreated,
	LinearIssueAttachmentCreated,
	LinearIssueAttachmentRemoved,
	LinearIssueAttachmentUpdated,
	LinearIssueRemoved,
	LinearIssueUpdated,
	LinearIssueMention,
	LinearIssueMentioned,
	LinearIssueNewComment,
	LinearIssueOpened,
	LinearIssueReaction,
	LinearIssueStatusChanged,
	LinearIssueUnassigned,
	LinearReactionAdded,
	LinearReactionRemoved,
	LinearSubscribedEvents,
	type LinearSubscribedIssueEvent,
} from './LinearCallbackEvents'
import { LinearCallbacks } from './LinearCallbacks'
import {
	LinearAgentSessionId,
	LinearIssueId,
	LinearTeamId,
	LinearWebhookDeliveryId,
	linearAgentSessionResourceId,
	linearInstallationResourceId,
	linearIssueResourceId,
} from './LinearIdentity'
import { normalizeLinearIssueChanges } from './LinearIssueChanges'
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
import {
	LinearAgentSession,
	LinearComment,
	LinearIssue,
	LinearIssueAttachment,
	LinearReaction,
} from './LinearResources'
import type { LinearAppUserNotificationWebhook, LinearResourceWebhookEvent } from './LinearWebhookEventSchemas'
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

const participantOrNull = (
	value:
		| {
				readonly id: (typeof LinearParticipant.Type)['id']
				readonly name?: string
				readonly email?: string | null
				readonly url?: string | null
				readonly avatarUrl?: string | null
		  }
		| null
		| undefined,
) =>
	Predicate.isNullish(value) || !Predicate.isString(value.name) ? null : participant({ ...value, name: value.name })

type IssueSnapshotInput = {
	readonly id: LinearIssueId
	readonly identifier: string
	readonly title: string
	readonly url: string
	readonly teamId: LinearTeamId
	readonly team: { readonly id: LinearTeamId; readonly key: string; readonly name: string } | null
	readonly number?: number
	readonly description?: string | null
	readonly priority?: number
	readonly creator?: Parameters<typeof participantOrNull>[0]
}

const issueFromSnapshot = (
	organizationId: (typeof LinearIssueRef.Type)['organizationId'],
	issue: IssueSnapshotInput,
	mailboxKey: string,
) => {
	const team = issue.team ?? { id: issue.teamId, key: issue.teamId, name: '' }
	return LinearIssue.make({
		ref: LinearIssueRef.make({ organizationId, teamId: issue.teamId, issueId: issue.id }),
		mailboxKey,
		identifier: issue.identifier,
		number: nullable(issue.number),
		title: issue.title,
		description: nullable(issue.description),
		priority: nullable(issue.priority),
		url: issue.url,
		team: LinearTeamSnapshot.make(team),
		creator: participantOrNull(issue.creator),
	})
}

const issueFromCreate = (
	webhook: Extract<typeof LinearSupportedWebhook.Type, { readonly type: 'Issue' }>,
	mailboxKey: string,
) => issueFromSnapshot(webhook.organizationId, webhook.data, mailboxKey)

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
			teamId: webhook.notification.issue.team.id,
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
	webhook:
		| Extract<typeof LinearSupportedWebhook.Type, { readonly type: 'Issue'; readonly action: 'create' }>
		| LinearAppUserNotificationWebhook,
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

const resourceIssueId = (webhook: LinearResourceWebhookEvent): LinearIssueId | undefined => {
	if (webhook.type === 'Issue') return webhook.data.id
	if (webhook.type === 'Attachment') return webhook.data.issueId
	if (webhook.type === 'Comment') return webhook.data.issueId ?? webhook.data.issue?.id
	return webhook.data.issueId ?? webhook.data.issue?.id ?? webhook.data.comment?.issueId ?? undefined
}

const isSelfAuthoredResource = (webhook: LinearResourceWebhookEvent, options: LinearEventProcessorOptions) =>
	(Predicate.isNotUndefined(options.bot) && webhook.actor?.id === options.bot.appUserId) ||
	(Predicate.isNotUndefined(options.oauthClientId) && webhook.actor?.id === options.oauthClientId)

const hasIssueSnapshot = (webhook: LinearResourceWebhookEvent) =>
	webhook.type === 'Issue' ||
	((webhook.type === 'Comment' || webhook.type === 'Reaction') && Predicate.isNotNullish(webhook.data.issue))

const issueFromResource = (webhook: LinearResourceWebhookEvent, issueId: LinearIssueId, mailboxKey: string) => {
	const snapshot =
		webhook.type === 'Issue'
			? webhook.data
			: webhook.type === 'Reaction'
				? webhook.data.issue
				: webhook.type === 'Comment'
					? webhook.data.issue
					: undefined
	if (Predicate.isNotNullish(snapshot)) return issueFromSnapshot(webhook.organizationId, snapshot, mailboxKey)
	return LinearIssue.make({
		ref: LinearIssueRef.make({
			organizationId: webhook.organizationId,
			teamId: null,
			issueId,
		}),
		mailboxKey,
		identifier: null,
		number: null,
		title: null,
		description: null,
		priority: null,
		url: null,
		team: null,
		creator: null,
	})
}

const commentFromResource = (
	webhook: Extract<LinearResourceWebhookEvent, { readonly type: 'Comment' }>,
	issueId: LinearIssueId,
) => {
	const teamId = webhook.data.issue?.teamId ?? null
	const issue = LinearIssueRef.make({ organizationId: webhook.organizationId, teamId, issueId })
	return LinearComment.make({
		ref: LinearCommentRef.make({
			organizationId: webhook.organizationId,
			teamId,
			issueId,
			commentId: webhook.data.id,
		}),
		issue,
		parentCommentId: nullable(webhook.data.parentId),
		content: LinearContent.make({ markdown: webhook.data.body }),
		author: participantOrNull(webhook.data.user),
	})
}

const reactionFromResource = (
	webhook: Extract<LinearResourceWebhookEvent, { readonly type: 'Reaction' }>,
	issueId: LinearIssueId,
) =>
	LinearReaction.make({
		id: webhook.data.id,
		issueId,
		commentId: nullable(webhook.data.commentId ?? webhook.data.comment?.id),
		emoji: webhook.data.emoji,
		author: participantOrNull(webhook.data.user),
		ref: {
			issue: LinearIssueRef.make({
				organizationId: webhook.organizationId,
				teamId: webhook.data.issue?.teamId ?? null,
				issueId,
			}),
			reactionId: webhook.data.id,
		},
	})

const isJsonObject = (value: Schema.Json): value is Schema.JsonObject =>
	typeof value === 'object' && value !== null && !Array.isArray(value)

const attachmentFromResource = (webhook: Extract<LinearResourceWebhookEvent, { readonly type: 'Attachment' }>) =>
	LinearIssueAttachment.make({
		id: webhook.data.id,
		issueId: webhook.data.issueId,
		title: webhook.data.title,
		subtitle: nullable(webhook.data.subtitle),
		url: webhook.data.url,
		...(isJsonObject(webhook.data.metadata) ? { metadata: webhook.data.metadata } : {}),
		ref: {
			issue: LinearIssueRef.make({
				organizationId: webhook.organizationId,
				teamId: null,
				issueId: webhook.data.issueId,
			}),
			attachmentId: webhook.data.id,
		},
	})

const normalizeResourceWebhook = (
	webhook: LinearResourceWebhookEvent,
	admission: DeliveryAdmission,
	issueId: LinearIssueId,
): LinearSubscribedIssueEvent | undefined => {
	if (webhook.action === 'create' && webhook.type === 'Issue') return undefined
	const eventId = LinearWebhookDeliveryId.make(admission.eventId)
	const actor = participantOrNull(webhook.actor)
	if (webhook.type === 'Issue') {
		if (webhook.action === 'update') {
			const { changes, otherChanges } = normalizeLinearIssueChanges(
				webhook.updatedFrom as Readonly<Record<string, unknown>>,
			)
			if (changes.length === 0 && Object.keys(otherChanges).length === 0) return undefined
			return LinearIssueUpdated.make({
				eventId,
				actor,
				issue: webhook.data,
				changes,
				otherChanges,
			})
		}
		return LinearIssueRemoved.make({ eventId, actor, issue: webhook.data })
	}
	if (webhook.type === 'Comment') {
		const comment = commentFromResource(webhook, issueId)
		if (webhook.action === 'create') return LinearCommentCreated.make({ eventId, actor, comment })
		if (webhook.action === 'remove') return LinearCommentRemoved.make({ eventId, actor, comment })
		const { body, ...otherChanges } = webhook.updatedFrom
		return LinearCommentUpdated.make({
			eventId,
			actor,
			comment,
			...(Predicate.isString(body) || body === null ? { previousBody: body } : {}),
			otherChanges,
		})
	}
	if (webhook.type === 'Reaction') {
		const reaction = reactionFromResource(webhook, issueId)
		return webhook.action === 'create'
			? LinearReactionAdded.make({ eventId, actor, reaction })
			: LinearReactionRemoved.make({ eventId, actor, reaction })
	}
	const attachment = attachmentFromResource(webhook)
	if (webhook.action === 'create') return LinearIssueAttachmentCreated.make({ eventId, actor, attachment })
	if (webhook.action === 'remove') return LinearIssueAttachmentRemoved.make({ eventId, actor, attachment })
	return LinearIssueAttachmentUpdated.make({
		eventId,
		actor,
		attachment,
		otherChanges: webhook.updatedFrom as Record<string, Schema.Json>,
	})
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
		if (
			webhooks.every(
				(webhook) =>
					!Schema.is(LinearStoredAgentSessionWebhook)(webhook) &&
					(webhook.type === 'OAuthApp' || webhook.type === 'PermissionChange'),
			)
		) {
			for (let index = 0; index < admissions.length; index += 1) {
				const admission = admissions[index]
				const webhook = webhooks[index]
				if (
					Predicate.isUndefined(admission) ||
					Predicate.isUndefined(webhook) ||
					Schema.is(LinearStoredAgentSessionWebhook)(webhook) ||
					(webhook.type !== 'OAuthApp' && webhook.type !== 'PermissionChange') ||
					admission.namespace !== options.namespace ||
					admission.provider !== 'linear' ||
					admission.installationId !== webhook.organizationId ||
					admission.resourceId !== linearInstallationResourceId() ||
					(Predicate.isNotUndefined(options.bot) && webhook.organizationId !== options.bot.organizationId) ||
					(webhook.type === 'PermissionChange' &&
						Predicate.isNotUndefined(options.bot) &&
						webhook.appUserId !== options.bot.appUserId) ||
					(Predicate.isNotUndefined(options.oauthClientId) && webhook.oauthClientId !== options.oauthClientId)
				)
					return yield* identityMismatch()
			}
			yield* Effect.logInfo('Linear installation lifecycle event observed').pipe(
				Effect.annotateLogs({
					disposition: webhooks.some(
						(webhook) =>
							!Schema.is(LinearStoredAgentSessionWebhook)(webhook) && webhook.type === 'OAuthApp',
					)
						? 'observed_revocation'
						: 'observed_team_access_change',
				}),
			)
			return ProviderEventIgnored.make({ reason: 'lifecycle_event' })
		}
		const issueWebhooks = yield* Effect.forEach(webhooks, (webhook) =>
			Schema.is(LinearStoredAgentSessionWebhook)(webhook)
				? Effect.fail(identityMismatch())
				: webhook.type === 'OAuthApp' || webhook.type === 'PermissionChange'
					? Effect.fail(identityMismatch())
					: Effect.succeed(webhook),
		)
		const issueId = yield* parseIssueAddress(first.resourceId)

		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const webhook = issueWebhooks[index]
			if (Predicate.isUndefined(admission) || Predicate.isUndefined(webhook)) return yield* identityMismatch()
			const webhookIssueId =
				webhook.type === 'AppUserNotification' ? webhook.notification.issueId : resourceIssueId(webhook)
			if (Predicate.isUndefined(webhookIssueId)) return yield* identityMismatch()
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
				: webhook.type === 'AppUserNotification' ||
					  (webhook.type === 'Issue' &&
							webhook.action === 'create' &&
							!isSelfAuthoredResource(webhook, options))
					? Effect.succeed(normalizeWebhook(webhook, admission, mailboxKey))
					: Effect.succeed(undefined)
		})
		const activationEvents = normalized.filter(Predicate.isNotUndefined)
		const subscribedEventsByIndex = issueWebhooks.map((webhook, index) => {
			if (webhook.type === 'AppUserNotification' || isSelfAuthoredResource(webhook, options)) return undefined
			const admission = admissions[index]
			return Predicate.isUndefined(admission) ? undefined : normalizeResourceWebhook(webhook, admission, issueId)
		})
		const subscribedEvents = subscribedEventsByIndex.filter(Predicate.isNotUndefined)
		const callbackEvents = normalized
			.map((event, index) => event ?? subscribedEventsByIndex[index])
			.filter(Predicate.isNotUndefined)
		const hasAgentSessionCallbacks =
			!Predicate.isUndefined(callbacks.onAgentSessionCreated) ||
			!Predicate.isUndefined(callbacks.onAgentSessionPrompted)
		const supplementalSignal =
			hasAgentSessionCallbacks &&
			activationEvents.some(
				(event) =>
					Schema.is(LinearIssueMention)(event) ||
					Schema.is(LinearCommentMention)(event) ||
					Schema.is(LinearAssignmentNotification)(event),
			)
		let entryHandled = false
		const directedIndex = supplementalSignal
			? -1
			: activationEvents.findIndex(
					(event) =>
						((Schema.is(LinearIssueMention)(event) || Schema.is(LinearCommentMention)(event)) &&
							!Predicate.isUndefined(callbacks.onMentioned)) ||
						(Schema.is(LinearAssignmentNotification)(event) &&
							!Predicate.isUndefined(callbacks.onAssigned)),
				)
		if (directedIndex >= 0) {
			const trigger = activationEvents[directedIndex]
			if (Predicate.isUndefined(trigger)) return ProviderEventIgnored.make({ reason: 'no_activation_event' })
			const events = callbackEvents.filter((event) => event !== trigger)
			if (Schema.is(LinearIssueMention)(trigger) && !Predicate.isUndefined(callbacks.onMentioned)) {
				yield* runCallback(
					callbacks.onMentioned(LinearIssueMentioned.make({ issue: trigger.issue, trigger, events })),
				)
				entryHandled = true
			}
			if (Schema.is(LinearCommentMention)(trigger) && !Predicate.isUndefined(callbacks.onMentioned)) {
				yield* runCallback(
					callbacks.onMentioned(LinearCommentMentioned.make({ issue: trigger.issue, trigger, events })),
				)
				entryHandled = true
			}
			if (Schema.is(LinearAssignmentNotification)(trigger) && !Predicate.isUndefined(callbacks.onAssigned)) {
				yield* runCallback(
					callbacks.onAssigned(LinearIssueAssigned.make({ issue: trigger.issue, trigger, events })),
				)
				entryHandled = true
			}
		}

		const openedIndex = activationEvents.findIndex(Schema.is(LinearIssueOpened))
		if (!entryHandled && openedIndex >= 0 && !Predicate.isUndefined(callbacks.onIssueCreated)) {
			const trigger = activationEvents[openedIndex]
			if (Predicate.isUndefined(trigger) || !Schema.is(LinearIssueOpened)(trigger))
				return ProviderEventIgnored.make({ reason: 'no_activation_event' })
			yield* runCallback(
				callbacks.onIssueCreated(
					LinearIssueCreated.make({
						issue: trigger.issue,
						trigger,
						events: callbackEvents.filter((event) => event !== trigger),
					}),
				),
			)
			entryHandled = true
		}

		const subscribed = yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey }),
		).pipe(
			Effect.tapError((error) => Effect.logError('Linear subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)
		if (subscribed) {
			const removed = issueWebhooks.some((webhook) => webhook.type === 'Issue' && webhook.action === 'remove')
			if (entryHandled) {
				if (removed)
					yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
						subscriptions.unsubscribe({ mailboxKey }),
					).pipe(Effect.mapError(() => providerFailure('subscription_cleanup_failed')))
				return ProviderEventHandled.make({})
			}
			const resourceWebhooks = issueWebhooks.filter(
				(webhook): webhook is LinearResourceWebhookEvent => webhook.type !== 'AppUserNotification',
			)
			const firstResource = resourceWebhooks.find(hasIssueSnapshot) ?? resourceWebhooks[0]
			let subscribedHandled = false
			if (
				subscribedEvents.length > 0 &&
				Predicate.isNotUndefined(firstResource) &&
				Predicate.isNotUndefined(callbacks.onSubscribedEvent)
			) {
				const issue = issueFromResource(firstResource, issueId, mailboxKey)
				yield* runCallback(
					callbacks.onSubscribedEvent(
						LinearSubscribedEvents.make({
							issue,
							events: subscribedEvents as [
								LinearSubscribedIssueEvent,
								...Array<LinearSubscribedIssueEvent>,
							],
						}),
					),
				)
				subscribedHandled = true
			}
			if (removed) {
				yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
					subscriptions.unsubscribe({ mailboxKey }),
				).pipe(Effect.mapError(() => providerFailure('subscription_cleanup_failed')))
				return ProviderEventHandled.make({})
			}
			if (subscribedHandled) return ProviderEventHandled.make({})
			return ProviderEventIgnored.make({
				reason: Predicate.isUndefined(callbacks.onSubscribedEvent)
					? 'callback_not_configured'
					: 'no_relevant_event',
			})
		}
		if (entryHandled) return ProviderEventHandled.make({})
		if (supplementalSignal) return ProviderEventIgnored.make({ reason: 'supplemental_signal' })
		const hasUnconfiguredEntry = activationEvents.some(
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
