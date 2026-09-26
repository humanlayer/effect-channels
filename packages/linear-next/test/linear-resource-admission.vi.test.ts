import { describe, it } from '@effect/vitest'
import { ProviderWebhookEvent } from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

import { issueCreatePayload, makeLinearTestProvider, signedLinearInput } from './fixtures'

const issue = issueCreatePayload.data
const issueId = issue.id
const { organizationId } = issueCreatePayload
const { number: _number, ...issueWithoutNumber } = issue
const timestamp = '2026-01-01T00:00:00.000Z'
const entityEnvelope = (index: number) => ({
	organizationId,
	createdAt: timestamp,
	webhookId: `webhook-${index}`,
	webhookTimestamp: 1_767_225_600_000 + index,
})
const commentFields = { body: 'Hello', createdAt: timestamp, updatedAt: timestamp, reactionData: {} }
const attachmentFields = {
	title: 'Attachment',
	url: 'https://example.com',
	metadata: {},
	groupBySource: false,
	createdAt: timestamp,
	updatedAt: timestamp,
}

const issueUpdate = {
	...entityEnvelope(0),
	type: 'Issue',
	action: 'update',
	data: issue,
	actor: { id: 'integration-1', service: 'github', type: 'integration' },
	updatedFrom: {
		title: null,
		labelIds: null,
		subIssueSortOrder: null,
		trashed: null,
		futureProviderField: null,
	},
}
const issueRemove = { ...entityEnvelope(1), type: 'Issue', action: 'remove', data: issue, updatedFrom: null }
const commentCreate = {
	...entityEnvelope(2),
	type: 'Comment',
	action: 'create',
	data: { id: 'comment-1', ...commentFields, issueId, issue: issueWithoutNumber },
	updatedFrom: null,
}
const commentUpdate = {
	...entityEnvelope(3),
	type: 'Comment',
	action: 'update',
	data: {
		id: 'comment-1',
		...commentFields,
		body: 'Edited',
		issueId,
		issue: null,
		user: null,
		userId: null,
	},
	updatedFrom: { body: 'Hello' },
}
const commentRemove = {
	...entityEnvelope(4),
	type: 'Comment',
	action: 'remove',
	data: { id: 'comment-1', ...commentFields, body: 'Edited', issueId, issue: null },
}
const reactionCreate = {
	...entityEnvelope(5),
	type: 'Reaction',
	action: 'create',
	data: {
		id: 'reaction-1',
		emoji: '+1',
		comment: { id: 'comment-1', body: 'Hello', issueId },
		issue: null,
		user: null,
		userId: null,
		createdAt: timestamp,
		updatedAt: timestamp,
	},
}
const reactionRemove = {
	...entityEnvelope(6),
	type: 'Reaction',
	action: 'remove',
	data: { id: 'reaction-1', emoji: '+1', issueId, comment: null, createdAt: timestamp, updatedAt: timestamp },
}
const attachmentCreate = {
	...entityEnvelope(7),
	type: 'Attachment',
	action: 'create',
	data: { id: 'attachment-1', issueId, ...attachmentFields },
}
const attachmentUpdate = {
	...entityEnvelope(8),
	type: 'Attachment',
	action: 'update',
	data: { id: 'attachment-1', issueId, ...attachmentFields, url: 'https://example.com/2' },
	updatedFrom: { url: 'https://example.com' },
}
const attachmentRemove = {
	...entityEnvelope(9),
	type: 'Attachment',
	action: 'remove',
	data: { id: 'attachment-1', issueId, ...attachmentFields, url: 'https://example.com/2' },
}
const payloads = [
	issueUpdate,
	issueRemove,
	commentCreate,
	commentUpdate,
	commentRemove,
	reactionCreate,
	reactionRemove,
	attachmentCreate,
	attachmentUpdate,
	attachmentRemove,
]

describe('Linear resource admission', () => {
	it.effect('routes every supported resource action to the issue mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const [index, payload] of payloads.entries()) {
				const outcome = yield* makeLinearTestProvider().handle(
					signedLinearInput(payload, payload.type, `resource-${index}`),
				)
				expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
				if (Schema.is(ProviderWebhookEvent)(outcome)) {
					expect(outcome.event.resourceId).toBe(`linear:v1:issue:${issueId}`)
					expect(outcome.event.eventId).toBe(`resource-${index}`)
				}
			}
		}),
	)

	it.effect('accepts the official child issue shape without a number', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = yield* makeLinearTestProvider().handle(
				signedLinearInput(commentCreate, commentCreate.type, 'child-issue'),
			)
			expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
		}),
	)

	it.effect('accepts nullable comment and reaction relations plus integration actors', ({ expect }) =>
		Effect.gen(function* () {
			for (const [index, payload] of [issueUpdate, commentUpdate, reactionCreate, reactionRemove].entries()) {
				const outcome = yield* makeLinearTestProvider().handle(
					signedLinearInput(payload, payload.type, `nullable-${index}`),
				)
				expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
			}
		}),
	)
})
