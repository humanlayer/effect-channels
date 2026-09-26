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
import { Array as Arr, Effect, Match, Predicate, Schema, Struct } from 'effect'

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
	type LinearIssueCallbackEvent,
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
import { discoverLinearFiles, LinearFileRef } from './LinearFiles'
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
import type {
	LinearAppUserNotificationWebhook,
	LinearLifecycleWebhookEvent,
	LinearResourceWebhookEvent,
} from './LinearWebhookEventSchemas'
import {
	normalizeLinearAgentSessionWebhook,
	normalizeLinearAppUserNotificationWebhook,
	resourceIssueId,
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
const filesIn = (organizationId: LinearIssueRef['organizationId'], issueId: LinearIssueId, markdown: string | null) =>
	discoverLinearFiles(LinearFileRef.make({ organizationId, issueId }), markdown)

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
		return yield* Schema.decodeEffect(LinearIssueId)(decoded).pipe(Effect.mapError(identityMismatch))
	})

const parseAgentSessionAddress = (resourceId: string) =>
	Effect.gen(function* () {
		const prefix = 'linear:v1:agent-session:'
		if (!resourceId.startsWith(prefix)) return yield* identityMismatch()
		const encoded = resourceId.slice(prefix.length)
		if (encoded.length === 0) return yield* identityMismatch()
		const decoded = yield* Effect.try({ try: () => decodeURIComponent(encoded), catch: identityMismatch })
		return yield* Schema.decodeEffect(LinearAgentSessionId)(decoded).pipe(Effect.mapError(identityMismatch))
	})

const participant = (value: {
	readonly id: LinearParticipant['id']
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
				readonly id: LinearParticipant['id']
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
	organizationId: LinearIssueRef['organizationId'],
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
		files: filesIn(organizationId, issue.id, nullable(issue.description)),
	})
}

const issueFromCreate = (webhook: Extract<LinearSupportedWebhook, { readonly type: 'Issue' }>, mailboxKey: string) =>
	issueFromSnapshot(webhook.organizationId, webhook.data, mailboxKey)

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
		files: filesIn(webhook.organizationId, issue.id, nullable(issue.description)),
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
		files: filesIn(webhook.organizationId, issue.id, nullable(issue.description)),
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
		files: filesIn(issue.ref.organizationId, issue.ref.issueId, value.body),
	})
}

const normalizeWebhook = (
	webhook:
		| Extract<LinearSupportedWebhook, { readonly type: 'Issue'; readonly action: 'create' }>
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
	return Match.value(webhook).pipe(
		Match.discriminatorsExhaustive('action')({
			issueMention: (notification) =>
				LinearIssueMention.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					notification: notification.notification,
				}),
			issueCommentMention: (notification) =>
				LinearCommentMention.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					comment: commentFromNotification(notification, issue),
					notification: notification.notification,
				}),
			issueAssignedToYou: (notification) =>
				LinearAssignmentNotification.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					notification: notification.notification,
				}),
			issueUnassignedFromYou: (notification) =>
				LinearIssueUnassigned.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					notification: notification.notification,
				}),
			issueNewComment: (notification) =>
				LinearIssueNewComment.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					comment: commentFromNotification(notification, issue),
					notification: notification.notification,
				}),
			issueStatusChanged: (notification) =>
				LinearIssueStatusChanged.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					notification: notification.notification,
				}),
			issueEmojiReaction: (notification) =>
				LinearIssueReaction.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					notification: notification.notification,
				}),
			issueCommentReaction: (notification) =>
				LinearCommentReaction.make({
					eventId,
					issue,
					actor: participantOrNull(actor),
					comment: commentFromNotification(notification, issue),
					notification: notification.notification,
				}),
		}),
	)
}

const isSelfAuthoredResource = (webhook: LinearResourceWebhookEvent, options: LinearEventProcessorOptions) =>
	(Predicate.isNotUndefined(options.bot) && webhook.actor?.id === options.bot.appUserId) ||
	(Predicate.isNotUndefined(options.oauthClientId) && webhook.actor?.id === options.oauthClientId)

const hasIssueSnapshot = (webhook: LinearResourceWebhookEvent) =>
	webhook.type === 'Issue' ||
	((webhook.type === 'Comment' || webhook.type === 'Reaction') && Predicate.isNotNullish(webhook.data.issue))

const issueFromResource = (webhook: LinearResourceWebhookEvent, issueId: LinearIssueId, mailboxKey: string) => {
	const snapshot = Match.value(webhook).pipe(
		Match.discriminators('type')({
			Issue: ({ data }) => data,
			Reaction: ({ data }) => data.issue,
			Comment: ({ data }) => data.issue,
		}),
		Match.orElse(() => undefined),
	)
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
		files: [],
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
		files: filesIn(webhook.organizationId, issueId, webhook.data.body),
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

const isJsonObject = (value: Schema.Json): value is Schema.JsonObject => Predicate.isObject(value)

const attachmentFromResource = (webhook: Extract<LinearResourceWebhookEvent, { readonly type: 'Attachment' }>) => {
	const fields = {
		id: webhook.data.id,
		issueId: webhook.data.issueId,
		title: webhook.data.title,
		subtitle: nullable(webhook.data.subtitle),
		url: webhook.data.url,
		ref: {
			issue: LinearIssueRef.make({
				organizationId: webhook.organizationId,
				teamId: null,
				issueId: webhook.data.issueId,
			}),
			attachmentId: webhook.data.id,
		},
	}
	const metadata = webhook.data.metadata
	if (isJsonObject(metadata)) return LinearIssueAttachment.make({ ...fields, metadata })
	return LinearIssueAttachment.make(fields)
}

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
			return LinearIssueUpdated.make({
				eventId,
				actor,
				issue: webhook.data,
				changes: normalizeLinearIssueChanges(webhook.updatedFrom),
			})
		}
		return LinearIssueRemoved.make({ eventId, actor, issue: webhook.data })
	}
	if (webhook.type === 'Comment') {
		const comment = commentFromResource(webhook, issueId)
		if (webhook.action === 'create') return LinearCommentCreated.make({ eventId, actor, comment })
		if (webhook.action === 'remove') return LinearCommentRemoved.make({ eventId, actor, comment })
		return LinearCommentUpdated.make({
			eventId,
			actor,
			comment,
			...Struct.renameKeys(Struct.pick(webhook.updatedFrom, ['body']), { body: 'previousBody' }),
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
	return LinearIssueAttachmentUpdated.make({ eventId, actor, attachment })
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
	storedWebhooks: ReadonlyArray<LinearStoredWebhook>,
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

type LinearIssueBatchWebhook = LinearResourceWebhookEvent | LinearAppUserNotificationWebhook
type LinearEventCallbacks = LinearCallbacks['Service']

const lifecycleWebhook = (webhook: LinearStoredWebhook): LinearLifecycleWebhookEvent | undefined => {
	if (Schema.is(LinearStoredAgentSessionWebhook)(webhook)) return undefined
	return webhook.type === 'OAuthApp' || webhook.type === 'PermissionChange' ? webhook : undefined
}

const isLifecycleIdentityMismatch = (
	options: LinearEventProcessorOptions,
	admission: DeliveryAdmission,
	webhook: LinearLifecycleWebhookEvent,
) =>
	admission.namespace !== options.namespace ||
	admission.provider !== 'linear' ||
	admission.installationId !== webhook.organizationId ||
	admission.resourceId !== linearInstallationResourceId() ||
	(Predicate.isNotUndefined(options.bot) && webhook.organizationId !== options.bot.organizationId) ||
	(webhook.type === 'PermissionChange' &&
		Predicate.isNotUndefined(options.bot) &&
		webhook.appUserId !== options.bot.appUserId) ||
	(Predicate.isNotUndefined(options.oauthClientId) && webhook.oauthClientId !== options.oauthClientId)

const processLifecycleBatch = (
	options: LinearEventProcessorOptions,
	admissions: DeliveryAdmissionBatch,
	webhooks: ReadonlyArray<LinearLifecycleWebhookEvent>,
) =>
	Effect.gen(function* () {
		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const webhook = webhooks[index]
			if (
				Predicate.isUndefined(admission) ||
				Predicate.isUndefined(webhook) ||
				isLifecycleIdentityMismatch(options, admission, webhook)
			)
				return yield* identityMismatch()
		}
		yield* Effect.logInfo('Linear installation lifecycle event observed').pipe(
			Effect.annotateLogs({
				disposition: webhooks.some((webhook) => webhook.type === 'OAuthApp')
					? 'observed_revocation'
					: 'observed_team_access_change',
			}),
		)
		return ProviderEventIgnored.make({ reason: 'lifecycle_event' })
	})

const issueBatchWebhook = (webhook: LinearStoredWebhook) => {
	if (Schema.is(LinearStoredAgentSessionWebhook)(webhook)) return Effect.fail(identityMismatch())
	if (webhook.type === 'OAuthApp' || webhook.type === 'PermissionChange') return Effect.fail(identityMismatch())
	return Effect.succeed(webhook)
}

const isForeignAppUserNotification = (options: LinearEventProcessorOptions, webhook: LinearIssueBatchWebhook) =>
	webhook.type === 'AppUserNotification' &&
	((Predicate.isNotUndefined(options.bot) &&
		(webhook.organizationId !== options.bot.organizationId || webhook.appUserId !== options.bot.appUserId)) ||
		(Predicate.isNotUndefined(options.oauthClientId) && webhook.oauthClientId !== options.oauthClientId))

const isIssueIdentityMismatch = (
	options: LinearEventProcessorOptions,
	first: DeliveryAdmission,
	admission: DeliveryAdmission,
	webhook: LinearIssueBatchWebhook,
	webhookIssueId: LinearIssueId,
	issueId: LinearIssueId,
) =>
	admission.namespace !== options.namespace ||
	admission.provider !== 'linear' ||
	admission.installationId !== first.installationId ||
	admission.resourceId !== first.resourceId ||
	admission.installationId !== webhook.organizationId ||
	webhookIssueId !== issueId ||
	linearIssueResourceId(webhookIssueId) !== admission.resourceId ||
	isForeignAppUserNotification(options, webhook)

const validateIssueBatch = (
	options: LinearEventProcessorOptions,
	admissions: DeliveryAdmissionBatch,
	webhooks: ReadonlyArray<LinearIssueBatchWebhook>,
	issueId: LinearIssueId,
) =>
	Effect.gen(function* () {
		const first = admissions[0]
		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const webhook = webhooks[index]
			if (Predicate.isUndefined(admission) || Predicate.isUndefined(webhook)) return yield* identityMismatch()
			const webhookIssueId =
				webhook.type === 'AppUserNotification' ? webhook.notification.issueId : resourceIssueId(webhook)
			if (Predicate.isUndefined(webhookIssueId)) return yield* identityMismatch()
			if (isIssueIdentityMismatch(options, first, admission, webhook, webhookIssueId, issueId))
				return yield* identityMismatch()
		}
	})

const activationEvent = (
	options: LinearEventProcessorOptions,
	webhook: LinearIssueBatchWebhook,
	admission: DeliveryAdmission,
	mailboxKey: string,
) => {
	if (webhook.type === 'AppUserNotification') return normalizeWebhook(webhook, admission, mailboxKey)
	if (webhook.type === 'Issue' && webhook.action === 'create' && !isSelfAuthoredResource(webhook, options))
		return normalizeWebhook(webhook, admission, mailboxKey)
	return undefined
}

const isEntrySignal = (event: LinearIssueActivity) =>
	Schema.is(LinearIssueMention)(event) ||
	Schema.is(LinearCommentMention)(event) ||
	Schema.is(LinearAssignmentNotification)(event)

const isDirectedTrigger = (callbacks: LinearEventCallbacks) => (event: LinearIssueActivity) =>
	((Schema.is(LinearIssueMention)(event) || Schema.is(LinearCommentMention)(event)) &&
		!Predicate.isUndefined(callbacks.onMentioned)) ||
	(Schema.is(LinearAssignmentNotification)(event) && !Predicate.isUndefined(callbacks.onAssigned))

const handled = Effect.as(true)

const runDirectedCallback = (
	callbacks: LinearEventCallbacks,
	trigger: LinearIssueActivity,
	events: ReadonlyArray<LinearIssueCallbackEvent>,
) => {
	if (Schema.is(LinearIssueMention)(trigger) && !Predicate.isUndefined(callbacks.onMentioned))
		return handled(
			runCallback(callbacks.onMentioned(LinearIssueMentioned.make({ issue: trigger.issue, trigger, events }))),
		)
	if (Schema.is(LinearCommentMention)(trigger) && !Predicate.isUndefined(callbacks.onMentioned))
		return handled(
			runCallback(callbacks.onMentioned(LinearCommentMentioned.make({ issue: trigger.issue, trigger, events }))),
		)
	if (Schema.is(LinearAssignmentNotification)(trigger) && !Predicate.isUndefined(callbacks.onAssigned))
		return handled(
			runCallback(callbacks.onAssigned(LinearIssueAssigned.make({ issue: trigger.issue, trigger, events }))),
		)
	return Effect.succeed(false)
}

const runIssueCreatedCallback = (
	callbacks: LinearEventCallbacks,
	activationEvents: ReadonlyArray<LinearIssueActivity>,
	callbackEvents: ReadonlyArray<LinearIssueCallbackEvent>,
) => {
	const trigger = activationEvents.find(Schema.is(LinearIssueOpened))
	if (Predicate.isUndefined(trigger) || Predicate.isUndefined(callbacks.onIssueCreated)) return Effect.succeed(false)
	return handled(
		runCallback(
			callbacks.onIssueCreated(
				LinearIssueCreated.make({
					issue: trigger.issue,
					trigger,
					events: callbackEvents.filter((event) => event !== trigger),
				}),
			),
		),
	)
}

/** Runs at most one entry callback: a directed mention or assignment first, then issue creation. */
const runEntryCallbacks = (
	callbacks: LinearEventCallbacks,
	activationEvents: ReadonlyArray<LinearIssueActivity>,
	callbackEvents: ReadonlyArray<LinearIssueCallbackEvent>,
	supplementalSignal: boolean,
) =>
	Effect.gen(function* () {
		const directed = supplementalSignal ? undefined : activationEvents.find(isDirectedTrigger(callbacks))
		if (Predicate.isNotUndefined(directed)) {
			const events = callbackEvents.filter((event) => event !== directed)
			if (yield* runDirectedCallback(callbacks, directed, events)) return true
		}
		return yield* runIssueCreatedCallback(callbacks, activationEvents, callbackEvents)
	})

const unsubscribeIssue = (mailboxKey: string) =>
	Effect.flatMap(MailboxSubscriptions, (subscriptions) => subscriptions.unsubscribe({ mailboxKey })).pipe(
		Effect.mapError(() => providerFailure('subscription_cleanup_failed')),
	)

const runSubscribedCallback = (
	callbacks: LinearEventCallbacks,
	webhooks: ReadonlyArray<LinearIssueBatchWebhook>,
	subscribedEvents: ReadonlyArray<LinearSubscribedIssueEvent>,
	issueId: LinearIssueId,
	mailboxKey: string,
) => {
	const resourceWebhooks = webhooks.filter(
		(webhook): webhook is LinearResourceWebhookEvent => webhook.type !== 'AppUserNotification',
	)
	const firstResource = resourceWebhooks.find(hasIssueSnapshot) ?? resourceWebhooks[0]
	if (
		!Arr.isReadonlyArrayNonEmpty(subscribedEvents) ||
		Predicate.isUndefined(firstResource) ||
		Predicate.isUndefined(callbacks.onSubscribedEvent)
	)
		return Effect.succeed(false)
	const issue = issueFromResource(firstResource, issueId, mailboxKey)
	return handled(
		runCallback(callbacks.onSubscribedEvent(LinearSubscribedEvents.make({ issue, events: subscribedEvents }))),
	)
}

const processSubscribedIssueBatch = (
	callbacks: LinearEventCallbacks,
	webhooks: ReadonlyArray<LinearIssueBatchWebhook>,
	subscribedEvents: ReadonlyArray<LinearSubscribedIssueEvent>,
	issueId: LinearIssueId,
	mailboxKey: string,
	entryHandled: boolean,
) =>
	Effect.gen(function* () {
		const removed = webhooks.some((webhook) => webhook.type === 'Issue' && webhook.action === 'remove')
		if (entryHandled) {
			if (removed) yield* unsubscribeIssue(mailboxKey)
			return ProviderEventHandled.make({})
		}
		const subscribedHandled = yield* runSubscribedCallback(
			callbacks,
			webhooks,
			subscribedEvents,
			issueId,
			mailboxKey,
		)
		if (removed) {
			yield* unsubscribeIssue(mailboxKey)
			return ProviderEventHandled.make({})
		}
		if (subscribedHandled) return ProviderEventHandled.make({})
		return ProviderEventIgnored.make({
			reason: Predicate.isUndefined(callbacks.onSubscribedEvent)
				? 'callback_not_configured'
				: 'no_relevant_event',
		})
	})

const unsubscribedIssueDisposition = (
	activationEvents: ReadonlyArray<LinearIssueActivity>,
	entryHandled: boolean,
	supplementalSignal: boolean,
) => {
	if (entryHandled) return ProviderEventHandled.make({})
	if (supplementalSignal) return ProviderEventIgnored.make({ reason: 'supplemental_signal' })
	const hasUnconfiguredEntry = activationEvents.some(
		(event) => isEntrySignal(event) || Schema.is(LinearIssueOpened)(event),
	)
	return ProviderEventIgnored.make({
		reason: hasUnconfiguredEntry ? 'callback_not_configured' : 'not_subscribed',
	})
}

const processIssueBatch = (
	options: LinearEventProcessorOptions,
	callbacks: LinearEventCallbacks,
	admissions: DeliveryAdmissionBatch,
	webhooks: ReadonlyArray<LinearStoredWebhook>,
) =>
	Effect.gen(function* () {
		const first = admissions[0]
		const issueWebhooks = yield* Effect.forEach(webhooks, issueBatchWebhook)
		const issueId = yield* parseIssueAddress(first.resourceId)
		yield* validateIssueBatch(options, admissions, issueWebhooks, issueId)

		const mailboxKey = deliveryMailboxKey(first)
		const normalized = yield* Effect.forEach(issueWebhooks, (webhook, index) => {
			const admission = admissions[index]
			return Predicate.isUndefined(admission)
				? Effect.fail(identityMismatch())
				: Effect.succeed(activationEvent(options, webhook, admission, mailboxKey))
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
		const supplementalSignal = hasAgentSessionCallbacks && activationEvents.some(isEntrySignal)
		const entryHandled = yield* runEntryCallbacks(callbacks, activationEvents, callbackEvents, supplementalSignal)

		const subscribed = yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey }),
		).pipe(
			Effect.tapError((error) => Effect.logError('Linear subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)
		if (subscribed)
			return yield* processSubscribedIssueBatch(
				callbacks,
				issueWebhooks,
				subscribedEvents,
				issueId,
				mailboxKey,
				entryHandled,
			)
		return unsubscribedIssueDisposition(activationEvents, entryHandled, supplementalSignal)
	})

const processLinearBatch = (options: LinearEventProcessorOptions) =>
	Effect.fn('linear.process_event_batch')(function* (admissions: DeliveryAdmissionBatch) {
		const callbacks = yield* LinearCallbacks
		yield* LinearApi
		const webhooks = yield* Effect.forEach(admissions, decodeWebhook)
		if (Predicate.isNotUndefined(webhooks[0]) && Schema.is(LinearStoredAgentSessionWebhook)(webhooks[0]))
			return yield* processAgentSessionBatch(options, admissions, webhooks)
		const lifecycleWebhooks = webhooks.map(lifecycleWebhook)
		if (lifecycleWebhooks.every(Predicate.isNotUndefined))
			return yield* processLifecycleBatch(options, admissions, lifecycleWebhooks)
		return yield* processIssueBatch(options, callbacks, admissions, webhooks)
	})

export const makeLinearEventProcessor = (
	options: LinearEventProcessorOptions,
): ProviderEventProcessor<LinearCallbacks | LinearApi | MailboxSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'linear',
	process: processLinearBatch(options),
})
