import {
	deliveryMailboxKey,
	type DeliveryAdmission,
	type DeliveryAdmissionBatch,
	type DeliveryCallbackResult,
	type DeliveryContext,
	MailboxSubscriptions,
	type PreparedDeliveryInvocation,
	type ProviderDeliveryExecution,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Option, Predicate, Schema, Struct } from 'effect'

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
import { type LinearCallbackError, LinearCallbackName, LinearCallbacks } from './LinearCallbacks'
import {
	type LinearActivationTarget,
	LinearAgentSessionDestination,
	LinearCommentActivationTarget,
	type LinearDeliveryDestination,
	LinearDeliveryPreparation,
	LinearIssueActivationTarget,
	LinearIssueDestination,
	encodeLinearDeliveryPreparation,
} from './LinearDeliveryDestination'
import { discoverLinearFiles, LinearFileRef } from './LinearFiles'
import {
	LinearAgentActivityId,
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

type LinearIssueBatchWebhook = LinearResourceWebhookEvent | LinearAppUserNotificationWebhook
type LinearEventCallbacks = LinearCallbacks['Service']

const executionFailure = (safeCode: string, retryable: boolean) =>
	ProviderEventExecutionFailed.make({ provider: 'linear', retryable, safeCode })

/** The saved callback is unknown or no longer configured. Another callback must not run in its place. */
const preparedCallbackMissing = () => executionFailure('prepared_callback_missing', false)
/** The saved callback is configured, but this batch cannot produce the event it takes. */
const preparedCallbackUnbuildable = () => executionFailure('prepared_callback_unbuildable', false)

const requireHandler = <Handler>(handler: Handler | undefined) =>
	Predicate.isUndefined(handler) ? Effect.fail(preparedCallbackMissing()) : Effect.succeed(handler)

const requireCallbackInput = <A>(value: A | undefined) =>
	Predicate.isUndefined(value) ? Effect.fail(preparedCallbackUnbuildable()) : Effect.succeed(value)

const callbackFailure = <A>(effect: Effect.Effect<A, LinearCallbackError>) =>
	effect.pipe(
		Effect.mapError((error) => executionFailure('callback_failed', error.retryable)),
		Effect.asVoid,
	)

/** A selected callback: what to save before it runs, and how to run it with its delivery context. */
type LinearCallbackInvocation = {
	readonly preparation: LinearDeliveryPreparation
	readonly invoke: (execution: ProviderDeliveryExecution) => Effect.Effect<void, ProviderEventExecutionFailed, LinearApi>
}

const issueActivationTarget = (issue: LinearIssue) =>
	LinearIssueActivationTarget.make({ organizationId: issue.ref.organizationId, issueId: issue.ref.issueId })

const commentActivationTarget = (comment: LinearComment) =>
	LinearCommentActivationTarget.make({
		organizationId: comment.ref.organizationId,
		issueId: comment.ref.issueId,
		commentId: comment.ref.commentId,
	})

const linearPreparation = (
	callback: LinearCallbackName,
	destination: LinearDeliveryDestination,
	activationTarget: LinearActivationTarget | undefined,
) =>
	Predicate.isUndefined(activationTarget)
		? LinearDeliveryPreparation.make({ callback, destination })
		: LinearDeliveryPreparation.make({ callback, destination, activationTarget })

const prepareDelivery = Effect.fn('linear.prepare_delivery')(function* (
	execution: ProviderDeliveryExecution,
	preparation: LinearDeliveryPreparation,
) {
	const invocation = yield* encodeLinearDeliveryPreparation(preparation).pipe(
		Effect.tapError((error) => Effect.logError('Linear delivery preparation could not be encoded', error)),
		Effect.mapError(() => executionFailure('delivery_destination_invalid', false)),
	)
	return yield* execution.prepare(invocation).pipe(
		Effect.tapError((error) => Effect.logError('Linear delivery preparation failed', error)),
		Effect.catchTags({
			DeliveryPreparationUnavailable: () => Effect.fail(executionFailure('delivery_prepare_unavailable', true)),
			DeliveryPreparationConflict: () => Effect.fail(executionFailure('delivery_prepare_conflict', false)),
		}),
		Effect.annotateLogs({ callback: preparation.callback }),
	)
})

/** First attempt: save the selection before any provider side effect or application code runs. */
const runSelectedInvocation = (execution: ProviderDeliveryExecution, invocation: LinearCallbackInvocation) =>
	prepareDelivery(execution, invocation.preparation).pipe(
		Effect.andThen(invocation.invoke(execution)),
		Effect.as(ProviderEventHandled.make({})),
	)

const decodePreparedCallback = (prepared: PreparedDeliveryInvocation) =>
	Schema.decodeUnknownEffect(LinearCallbackName)(prepared.callback).pipe(
		Effect.tapError((error) => Effect.logError('Prepared Linear callback is not a Linear callback', error)),
		Effect.mapError(preparedCallbackMissing),
	)

/** Later attempt: run the saved callback without selecting again. */
const runPreparedInvocation = <R>(
	execution: ProviderDeliveryExecution,
	prepared: PreparedDeliveryInvocation,
	build: (callback: LinearCallbackName) => Effect.Effect<LinearCallbackInvocation, ProviderEventExecutionFailed, R>,
) =>
	decodePreparedCallback(prepared).pipe(
		Effect.flatMap(build),
		Effect.tapError((error) => Effect.logError('Prepared Linear callback could not run', error)),
		Effect.flatMap((invocation) => invocation.invoke(execution)),
		Effect.as(ProviderEventHandled.make({})),
		Effect.annotateLogs({ callback: prepared.callback }),
	)

type LinearAgentSessionBatch = {
	readonly callbacks: LinearEventCallbacks
	readonly normalized: LinearNormalizedAgentSessionWebhook
	readonly session: LinearAgentSession
	readonly issue: LinearIssue
	readonly deliveryId: LinearWebhookDeliveryId
}

const agentSessionPreparation = (callback: LinearCallbackName, session: LinearAgentSession) =>
	linearPreparation(
		callback,
		LinearAgentSessionDestination.make({
			organizationId: session.ref.organizationId,
			appUserId: session.ref.appUserId,
			sessionId: session.ref.sessionId,
			issueId: session.ref.issueId,
		}),
		Predicate.isNull(session.sourceCommentId)
			? LinearIssueActivationTarget.make({
					organizationId: session.ref.organizationId,
					issueId: session.ref.issueId,
				})
			: LinearCommentActivationTarget.make({
					organizationId: session.ref.organizationId,
					issueId: session.ref.issueId,
					commentId: session.sourceCommentId,
				}),
	)

const createdWebhook = (normalized: LinearNormalizedAgentSessionWebhook) =>
	Match.value(normalized).pipe(
		Match.discriminatorsExhaustive('action')({ created: (created) => created, prompted: () => undefined }),
	)

const promptedWebhook = (normalized: LinearNormalizedAgentSessionWebhook) =>
	Match.value(normalized).pipe(
		Match.discriminatorsExhaustive('action')({ created: () => undefined, prompted: (prompted) => prompted }),
	)

/**
 * Linear's automatic thought, posted before `onAgentSessionCreated` so the session gets an activity
 * within Linear's 10 seconds. Its ID is the delivery's idempotency key, made from the delivery ID, so a
 * callback retry finds it already posted instead of posting it again.
 */
const postInitialThought = (session: LinearAgentSession, execution: ProviderDeliveryExecution) =>
	session.thought('Working on this…', { activityId: LinearAgentActivityId.make(execution.idempotencyKey) }).pipe(
		Effect.asVoid,
		Effect.catchIf(
			(error) => error.reason === 'already_exists',
			() => Effect.logInfo('Linear automatic thought already posted by an earlier attempt'),
		),
		Effect.mapError((error) => executionFailure('initial_thought_failed', error.retryable)),
	)

/** Builds a session callback. `onAgentSessionCreated` posts Linear's automatic thought before the callback. */
const agentSessionInvocation = (batch: LinearAgentSessionBatch, callback: LinearCallbackName) => {
	const { callbacks, normalized, session, issue, deliveryId } = batch
	return Match.value(callback).pipe(
		Match.withReturnType<Effect.Effect<LinearCallbackInvocation, ProviderEventExecutionFailed>>(),
		Match.when('onAgentSessionCreated', (name) =>
			Effect.gen(function* () {
				const handler = yield* requireHandler(callbacks.onAgentSessionCreated)
				const created = yield* requireCallbackInput(createdWebhook(normalized))
				const event = LinearAgentSessionCreated.make({
					session,
					issue,
					promptContext: created.promptContext,
					previousComments: created.previousComments.map((comment) =>
						LinearAgentSessionComment.make(comment),
					),
					guidance: created.guidance.map((guidance) => LinearAgentGuidance.make(guidance)),
					deliveryId,
				})
				return {
					preparation: agentSessionPreparation(name, session),
					invoke: (execution: ProviderDeliveryExecution) =>
						postInitialThought(session, execution).pipe(
							Effect.andThen(callbackFailure(handler(event, execution.context))),
						),
				}
			}),
		),
		Match.when('onAgentSessionPrompted', (name) =>
			Effect.gen(function* () {
				const handler = yield* requireHandler(callbacks.onAgentSessionPrompted)
				const { agentActivity: activity } = yield* requireCallbackInput(promptedWebhook(normalized))
				const event = LinearAgentSessionPrompted.make({
					session,
					issue,
					prompt: LinearAgentPrompt.make({
						id: activity.id,
						body: activity.content.body,
						createdAt: activity.createdAt,
						user: participant(activity.user),
						signal: nullable(activity.signal),
					}),
					deliveryId,
				})
				return {
					preparation: agentSessionPreparation(name, session),
					invoke: (execution: ProviderDeliveryExecution) => callbackFailure(handler(event, execution.context)),
				}
			}),
		),
		Match.orElse(() => Effect.fail(preparedCallbackUnbuildable())),
	)
}

const selectAgentSessionCallback = (normalized: LinearNormalizedAgentSessionWebhook): LinearCallbackName =>
	Match.value(normalized).pipe(
		Match.discriminatorsExhaustive('action')({
			created: () => 'onAgentSessionCreated' as const,
			prompted: () => 'onAgentSessionPrompted' as const,
		}),
	)

const processAgentSessionBatch = (
	options: LinearEventProcessorOptions,
	admissions: DeliveryAdmissionBatch,
	storedWebhooks: ReadonlyArray<LinearStoredWebhook>,
	execution: ProviderDeliveryExecution,
) =>
	Effect.gen(function* () {
		const callbacks = yield* LinearCallbacks
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
		const batch: LinearAgentSessionBatch = {
			callbacks,
			normalized: normalizedWebhook,
			issue: issueFromAgentSession(
				normalizedWebhook,
				issueMailboxKey(firstAdmission, normalizedWebhook.issue.id),
			),
			session: sessionFromWebhook(
				normalizedWebhook,
				firstAdmission,
				deliveryMailboxKey(firstAdmission),
				firstStoredWebhook.deliveryId,
			),
			deliveryId: firstStoredWebhook.deliveryId,
		}

		if (Option.isSome(execution.prepared))
			return yield* runPreparedInvocation(execution, execution.prepared.value, (callback) =>
				agentSessionInvocation(batch, callback),
			)
		const callback = selectAgentSessionCallback(normalizedWebhook)
		if (Predicate.isUndefined(callbacks[callback]))
			return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
		const invocation = yield* agentSessionInvocation(batch, callback)
		return yield* runSelectedInvocation(execution, invocation)
	})

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

const isMention = Schema.is(Schema.Union([LinearIssueMention, LinearCommentMention]))

type LinearIssueBatch = {
	readonly callbacks: LinearEventCallbacks
	readonly webhooks: ReadonlyArray<LinearIssueBatchWebhook>
	readonly activationEvents: ReadonlyArray<LinearIssueActivity>
	readonly callbackEvents: ReadonlyArray<LinearIssueCallbackEvent>
	readonly subscribedEvents: ReadonlyArray<LinearSubscribedIssueEvent>
	readonly issueId: LinearIssueId
	readonly mailboxKey: string
}

const issueCallbackInvocation = (
	callback: LinearCallbackName,
	issue: LinearIssue,
	activationTarget: LinearActivationTarget | undefined,
	run: (delivery: DeliveryContext) => Effect.Effect<DeliveryCallbackResult, LinearCallbackError>,
): LinearCallbackInvocation => ({
	preparation: linearPreparation(
		callback,
		LinearIssueDestination.make({ organizationId: issue.ref.organizationId, issueId: issue.ref.issueId }),
		activationTarget,
	),
	invoke: (execution) => callbackFailure(run(execution.context)),
})

/** The issue snapshot and events for `onSubscribedEvent`, when the batch has any. */
const subscribedCallbackInput = (batch: LinearIssueBatch) => {
	const resourceWebhooks = batch.webhooks.filter(
		(webhook): webhook is LinearResourceWebhookEvent => webhook.type !== 'AppUserNotification',
	)
	const firstResource = resourceWebhooks.find(hasIssueSnapshot) ?? resourceWebhooks[0]
	if (!Arr.isReadonlyArrayNonEmpty(batch.subscribedEvents) || Predicate.isUndefined(firstResource)) return undefined
	return {
		issue: issueFromResource(firstResource, batch.issueId, batch.mailboxKey),
		events: batch.subscribedEvents,
	}
}

const issueInvocation = (batch: LinearIssueBatch, callback: LinearCallbackName) => {
	const { callbacks, activationEvents, callbackEvents } = batch
	return Match.value(callback).pipe(
		Match.withReturnType<Effect.Effect<LinearCallbackInvocation, ProviderEventExecutionFailed>>(),
		Match.when('onMentioned', (name) =>
			Effect.gen(function* () {
				const handler = yield* requireHandler(callbacks.onMentioned)
				const trigger = yield* requireCallbackInput(activationEvents.find(isMention))
				const events = callbackEvents.filter((event) => event !== trigger)
				return Match.value(trigger).pipe(
					Match.tagsExhaustive({
						LinearIssueMention: (mention) =>
							issueCallbackInvocation(
								name,
								mention.issue,
								issueActivationTarget(mention.issue),
								(delivery) =>
									handler(
										LinearIssueMentioned.make({ issue: mention.issue, trigger: mention, events }),
										delivery,
									),
							),
						LinearCommentMention: (mention) =>
							issueCallbackInvocation(
								name,
								mention.issue,
								commentActivationTarget(mention.comment),
								(delivery) =>
									handler(
										LinearCommentMentioned.make({ issue: mention.issue, trigger: mention, events }),
										delivery,
									),
							),
					}),
				)
			}),
		),
		Match.when('onAssigned', (name) =>
			Effect.gen(function* () {
				const handler = yield* requireHandler(callbacks.onAssigned)
				const trigger = yield* requireCallbackInput(
					activationEvents.find(Schema.is(LinearAssignmentNotification)),
				)
				const events = callbackEvents.filter((event) => event !== trigger)
				return issueCallbackInvocation(name, trigger.issue, issueActivationTarget(trigger.issue), (delivery) =>
					handler(LinearIssueAssigned.make({ issue: trigger.issue, trigger, events }), delivery),
				)
			}),
		),
		Match.when('onIssueCreated', (name) =>
			Effect.gen(function* () {
				const handler = yield* requireHandler(callbacks.onIssueCreated)
				const trigger = yield* requireCallbackInput(activationEvents.find(Schema.is(LinearIssueOpened)))
				const events = callbackEvents.filter((event) => event !== trigger)
				return issueCallbackInvocation(name, trigger.issue, issueActivationTarget(trigger.issue), (delivery) =>
					handler(LinearIssueCreated.make({ issue: trigger.issue, trigger, events }), delivery),
				)
			}),
		),
		Match.when('onSubscribedEvent', (name) =>
			Effect.gen(function* () {
				const handler = yield* requireHandler(callbacks.onSubscribedEvent)
				const { issue, events } = yield* requireCallbackInput(subscribedCallbackInput(batch))
				return issueCallbackInvocation(name, issue, undefined, (delivery) =>
					handler(LinearSubscribedEvents.make({ issue, events }), delivery),
				)
			}),
		),
		Match.orElse(() => Effect.fail(preparedCallbackUnbuildable())),
	)
}

/** At most one entry callback: a directed mention or assignment first, then issue creation. */
const selectEntryCallback = (batch: LinearIssueBatch, supplementalSignal: boolean): LinearCallbackName | undefined => {
	const directed = supplementalSignal ? undefined : batch.activationEvents.find(isDirectedTrigger(batch.callbacks))
	if (Predicate.isNotUndefined(directed))
		return Schema.is(LinearAssignmentNotification)(directed) ? 'onAssigned' : 'onMentioned'
	const opened = batch.activationEvents.some(Schema.is(LinearIssueOpened))
	return opened && Predicate.isNotUndefined(batch.callbacks.onIssueCreated) ? 'onIssueCreated' : undefined
}

const isIssueSubscribed = (mailboxKey: string) =>
	Effect.flatMap(MailboxSubscriptions, (subscriptions) => subscriptions.isSubscribed({ mailboxKey })).pipe(
		Effect.tapError((error) => Effect.logError('Linear subscription lookup failed', error)),
		Effect.mapError(() => providerFailure('subscription_lookup_failed')),
	)

const unsubscribeIssue = (mailboxKey: string) =>
	Effect.flatMap(MailboxSubscriptions, (subscriptions) => subscriptions.unsubscribe({ mailboxKey })).pipe(
		Effect.mapError(() => providerFailure('subscription_cleanup_failed')),
	)

const unsubscribeIfSubscribed = (mailboxKey: string) =>
	isIssueSubscribed(mailboxKey).pipe(
		Effect.flatMap((subscribed) => (subscribed ? unsubscribeIssue(mailboxKey) : Effect.void)),
	)

const unsubscribedIssueDisposition = (
	activationEvents: ReadonlyArray<LinearIssueActivity>,
	supplementalSignal: boolean,
) => {
	if (supplementalSignal) return ProviderEventIgnored.make({ reason: 'supplemental_signal' })
	const hasUnconfiguredEntry = activationEvents.some(
		(event) => isEntrySignal(event) || Schema.is(LinearIssueOpened)(event),
	)
	return ProviderEventIgnored.make({
		reason: hasUnconfiguredEntry ? 'callback_not_configured' : 'not_subscribed',
	})
}

/** First attempt at an issue batch: select from the events and subscription state, save the choice, then run it. */
const processSelectedIssueBatch = (execution: ProviderDeliveryExecution, batch: LinearIssueBatch, removed: boolean) =>
	Effect.gen(function* () {
		const { callbacks, activationEvents, mailboxKey } = batch
		const hasAgentSessionCallbacks =
			Predicate.isNotUndefined(callbacks.onAgentSessionCreated) ||
			Predicate.isNotUndefined(callbacks.onAgentSessionPrompted)
		const supplementalSignal = hasAgentSessionCallbacks && activationEvents.some(isEntrySignal)
		const entryCallback = selectEntryCallback(batch, supplementalSignal)
		if (Predicate.isNotUndefined(entryCallback)) {
			const invocation = yield* issueInvocation(batch, entryCallback)
			yield* runSelectedInvocation(execution, invocation)
			if (removed) yield* unsubscribeIfSubscribed(mailboxKey)
			return ProviderEventHandled.make({})
		}

		const subscribed = yield* isIssueSubscribed(mailboxKey)
		if (!subscribed) return unsubscribedIssueDisposition(activationEvents, supplementalSignal)
		const subscribedSelected =
			Predicate.isNotUndefined(callbacks.onSubscribedEvent) &&
			Predicate.isNotUndefined(subscribedCallbackInput(batch))
		if (subscribedSelected) {
			const invocation = yield* issueInvocation(batch, 'onSubscribedEvent')
			yield* runSelectedInvocation(execution, invocation)
		}
		if (removed) {
			yield* unsubscribeIssue(mailboxKey)
			return ProviderEventHandled.make({})
		}
		if (subscribedSelected) return ProviderEventHandled.make({})
		return ProviderEventIgnored.make({
			reason: Predicate.isUndefined(callbacks.onSubscribedEvent)
				? 'callback_not_configured'
				: 'no_relevant_event',
		})
	})

const processIssueBatch = (
	options: LinearEventProcessorOptions,
	callbacks: LinearEventCallbacks,
	admissions: DeliveryAdmissionBatch,
	webhooks: ReadonlyArray<LinearStoredWebhook>,
	execution: ProviderDeliveryExecution,
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
		const subscribedEventsByIndex = issueWebhooks.map((webhook, index) => {
			if (webhook.type === 'AppUserNotification' || isSelfAuthoredResource(webhook, options)) return undefined
			const admission = admissions[index]
			return Predicate.isUndefined(admission) ? undefined : normalizeResourceWebhook(webhook, admission, issueId)
		})
		const batch: LinearIssueBatch = {
			callbacks,
			webhooks: issueWebhooks,
			activationEvents: normalized.filter(Predicate.isNotUndefined),
			callbackEvents: normalized
				.map((event, index) => event ?? subscribedEventsByIndex[index])
				.filter(Predicate.isNotUndefined),
			subscribedEvents: subscribedEventsByIndex.filter(Predicate.isNotUndefined),
			issueId,
			mailboxKey,
		}
		const removed = issueWebhooks.some((webhook) => webhook.type === 'Issue' && webhook.action === 'remove')

		if (Option.isNone(execution.prepared)) return yield* processSelectedIssueBatch(execution, batch, removed)
		const result = yield* runPreparedInvocation(execution, execution.prepared.value, (callback) =>
			issueInvocation(batch, callback),
		)
		if (removed) yield* unsubscribeIfSubscribed(mailboxKey)
		return result
	})

const processLinearBatch = (options: LinearEventProcessorOptions) =>
	Effect.fn('linear.process_event_batch')(function* (
		admissions: DeliveryAdmissionBatch,
		execution: ProviderDeliveryExecution,
	) {
		const callbacks = yield* LinearCallbacks
		yield* LinearApi
		const webhooks = yield* Effect.forEach(admissions, decodeWebhook)
		if (Predicate.isNotUndefined(webhooks[0]) && Schema.is(LinearStoredAgentSessionWebhook)(webhooks[0]))
			return yield* processAgentSessionBatch(options, admissions, webhooks, execution)
		const lifecycleWebhooks = webhooks.map(lifecycleWebhook)
		if (lifecycleWebhooks.every(Predicate.isNotUndefined))
			return yield* processLifecycleBatch(options, admissions, lifecycleWebhooks)
		return yield* processIssueBatch(options, callbacks, admissions, webhooks, execution)
	})

export const makeLinearEventProcessor = (
	options: LinearEventProcessorOptions,
): ProviderEventProcessor<LinearCallbacks | LinearApi | MailboxSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'linear',
	process: processLinearBatch(options),
})
