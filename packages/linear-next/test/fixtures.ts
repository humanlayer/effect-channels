import { createHmac } from 'node:crypto'

import { NodeCrypto } from '@effect/platform-node'
import {
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	processProviderEvent,
	type ProviderEventProcessor,
	type RawWebhookInput,
} from '@humanlayer/channels-delivery-next'
import { Effect, Match, Queue, Redacted, Schema } from 'effect'
import { Headers } from 'effect/unstable/http'

import { LinearOrganizationId, LinearUserId } from '../src/LinearIdentity'
import { makeLinearWebhookProvider } from '../src/LinearWebhookProvider'
import issueCreateJson from './fixtures/issue-create.json' with { type: 'json' }

export const linearWebhookSecret = 'linear-next-test-secret'
export const linearOrganizationId = LinearOrganizationId.make(issueCreateJson.organizationId)
export const linearAppUserId = LinearUserId.make('a75b41db-2746-4c05-a792-31535319f512')
export const issueCreatePayload: unknown = issueCreateJson

export const linearIssueCreateAdmission = (namespace = 'linear-processing-test') =>
	DeliveryAdmission.make({
		namespace,
		provider: 'linear',
		installationId: issueCreateJson.organizationId,
		resourceId: `linear:v1:issue:${issueCreateJson.data.id}`,
		eventId: '6d601ea0-cafe-4e33-8db1-d7c21fc6a773',
		payload: issueCreateJson,
	})

export const signedLinearBody = (
	body: Uint8Array,
	deliveryId = '6d601ea0-cafe-4e33-8db1-d7c21fc6a773',
	timestamp = 0,
): RawWebhookInput => ({
	headers: Headers.fromInput({
		'linear-delivery': deliveryId,
		'linear-event': 'Issue',
		'linear-signature': createHmac('sha256', linearWebhookSecret).update(body).digest('hex'),
		'linear-timestamp': String(timestamp),
	}),
	body,
})

export const signedLinearInput = (payload: unknown = issueCreatePayload, deliveryId?: string, timestamp?: number) =>
	signedLinearBody(new TextEncoder().encode(JSON.stringify(payload)), deliveryId, timestamp)

export const makeLinearTestProvider = (namespace = 'linear-test') => {
	const provider = makeLinearWebhookProvider({
		namespace,
		webhookSecret: Redacted.make(linearWebhookSecret),
		organizationId: linearOrganizationId,
	})
	return {
		...provider,
		handle: (input: RawWebhookInput) => provider.handle(input).pipe(Effect.provide(NodeCrypto.layer)),
	}
}

export const admittedIssueCreate = (namespace = 'linear-processing-test') =>
	Effect.gen(function* () {
		const outcome = yield* makeLinearTestProvider(namespace).handle(signedLinearInput())
		return yield* Match.value(outcome).pipe(
			Match.tagsExhaustive({
				Event: ({ event }) => Effect.succeed(event),
				Events: () => Effect.die('Expected one Linear admission'),
				Ignored: () => Effect.die('Expected admitted Linear webhook'),
				Response: () => Effect.die('Expected admitted Linear webhook'),
			}),
		)
	}).pipe(Effect.scoped)

export const roundTripAdmission = (admission: DeliveryAdmission) =>
	Schema.encodeEffect(Schema.fromJsonString(DeliveryAdmission))(admission).pipe(
		Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(DeliveryAdmission))),
	)

export const makeControlledMailbox = <R>(processors: ReadonlyArray<ProviderEventProcessor<R>>) =>
	Effect.gen(function* () {
		const queue = yield* Queue.unbounded<DeliveryAdmission>()
		return {
			service: MailboxDelivery.of({
				deliver: (admission) =>
					Queue.offer(queue, admission).pipe(
						Effect.as(DeliveryReceipt.make({ mailboxKey: admission.resourceId, accepted: true })),
					),
			}),
			processNext: Queue.take(queue).pipe(Effect.flatMap((admission) => processProviderEvent(processors)([admission]))),
		}
	})
