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
import { Effect, Schema } from 'effect'

import { LinearApi } from './LinearApi'
import { LinearIssueCreated, LinearIssueOpened } from './LinearCallbackEvents'
import { LinearCallbacks } from './LinearCallbacks'
import { LinearIssueId, LinearWebhookDeliveryId, linearIssueResourceId } from './LinearIdentity'
import { LinearIssueRef, LinearParticipant, LinearTeamSnapshot } from './LinearModels'
import { LinearIssue } from './LinearResources'
import { LinearIssueCreateWebhook } from './LinearWebhookEventSchemas'

export type LinearEventProcessorOptions = {
	readonly namespace: string
}

const invalidPayload = () => ProviderEventInvalid.make({ provider: 'linear', reason: 'invalid_payload' })
const identityMismatch = () => ProviderEventInvalid.make({ provider: 'linear', reason: 'identity_mismatch' })

const decodeWebhook = (admission: DeliveryAdmission) =>
	Schema.decodeUnknownEffect(LinearIssueCreateWebhook)(admission.payload, { onExcessProperty: 'preserve' }).pipe(
		Effect.tapError((error) => Effect.logError('Stored Linear webhook could not be decoded', error)),
		Effect.mapError(invalidPayload),
	)

const parseIssueAddress = (resourceId: string) =>
	Effect.gen(function* () {
		const parts = resourceId.split(':')
		if (parts.length !== 4 || parts[0] !== 'linear' || parts[1] !== 'v1' || parts[2] !== 'issue')
			return yield* identityMismatch()
		const encoded = parts[3]
		if (encoded === undefined) return yield* identityMismatch()
		const decoded = yield* Effect.try({
			try: () => decodeURIComponent(encoded),
			catch: identityMismatch,
		})
		return yield* Schema.decodeUnknownEffect(LinearIssueId)(decoded).pipe(Effect.mapError(identityMismatch))
	})

const participant = (value: {
	readonly id: typeof LinearParticipant.Type['id']
	readonly name: string
	readonly email?: string | null
}) => LinearParticipant.make({ id: value.id, name: value.name, ...(value.email === undefined ? {} : { email: value.email }) })

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

const processLinearBatch = (options: LinearEventProcessorOptions) =>
	Effect.fn('linear.process_event_batch')(function* (admissions: DeliveryAdmissionBatch) {
		const callbacks = yield* LinearCallbacks
		yield* LinearApi
		const first = admissions[0]
		const issueId = yield* parseIssueAddress(first.resourceId)
		const webhooks = yield* Effect.forEach(admissions, decodeWebhook)
		for (let index = 0; index < admissions.length; index += 1) {
			const admission = admissions[index]
			const webhook = webhooks[index]
			if (admission === undefined || webhook === undefined) return yield* identityMismatch()
			if (
				admission.namespace !== options.namespace ||
				admission.provider !== 'linear' ||
				admission.installationId !== first.installationId ||
				admission.resourceId !== first.resourceId ||
				admission.installationId !== webhook.organizationId ||
				webhook.data.id !== issueId ||
				linearIssueResourceId(webhook.data.id) !== admission.resourceId
			)
				return yield* identityMismatch()
		}
		if (callbacks.onIssueCreated === undefined)
			return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
		const webhook = webhooks[0]
		if (webhook === undefined) return yield* invalidPayload()
		const ref = LinearIssueRef.make({
			organizationId: webhook.organizationId,
			teamId: webhook.data.team.id,
			issueId: webhook.data.id,
		})
		const issue = LinearIssue.make({
			ref,
			mailboxKey: deliveryMailboxKey(first),
			identifier: webhook.data.identifier,
			number: webhook.data.number,
			title: webhook.data.title,
			description: webhook.data.description ?? null,
			priority: webhook.data.priority ?? 0,
			url: webhook.data.url,
			team: LinearTeamSnapshot.make(webhook.data.team),
			creator: webhook.data.creator === undefined || webhook.data.creator === null ? null : participant(webhook.data.creator),
		})
		const trigger = LinearIssueOpened.make({
			eventId: LinearWebhookDeliveryId.make(first.eventId),
			issue,
			actor: webhook.actor === undefined || webhook.actor === null ? null : participant(webhook.actor),
		})
		return yield* runCallback(callbacks.onIssueCreated(LinearIssueCreated.make({ issue, trigger, events: [] })))
	})

export const makeLinearEventProcessor = (
	options: LinearEventProcessorOptions,
): ProviderEventProcessor<LinearCallbacks | LinearApi | MailboxSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'linear',
	process: processLinearBatch(options),
})
