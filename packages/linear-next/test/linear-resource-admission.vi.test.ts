import { describe, it } from '@effect/vitest'
import { ProviderWebhookEvent } from '@humanlayer/channels-delivery-next'
import { Effect, Schema } from 'effect'

import { issueCreatePayload, makeLinearTestProvider, signedLinearInput } from './fixtures'

const issue = (issueCreatePayload as { readonly data: Record<string, unknown> }).data
const issueId = issue.id as string
const organizationId = (issueCreatePayload as { readonly organizationId: string }).organizationId
const issueWithoutNumber = Object.fromEntries(Object.entries(issue).filter(([key]) => key !== 'number'))
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

const payloads = [
	{
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
	},
	{ type: 'Issue', action: 'remove', data: issue, updatedFrom: null },
	{
		type: 'Comment',
		action: 'create',
		data: { id: 'comment-1', ...commentFields, issueId, issue: issueWithoutNumber },
		updatedFrom: null,
	},
	{
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
	},
	{
		type: 'Comment',
		action: 'remove',
		data: { id: 'comment-1', ...commentFields, body: 'Edited', issueId, issue: null },
	},
	{
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
	},
	{
		type: 'Reaction',
		action: 'remove',
		data: { id: 'reaction-1', emoji: '+1', issueId, comment: null, createdAt: timestamp, updatedAt: timestamp },
	},
	{
		type: 'Attachment',
		action: 'create',
		data: { id: 'attachment-1', issueId, ...attachmentFields },
	},
	{
		type: 'Attachment',
		action: 'update',
		data: { id: 'attachment-1', issueId, ...attachmentFields, url: 'https://example.com/2' },
		updatedFrom: { url: 'https://example.com' },
	},
	{
		type: 'Attachment',
		action: 'remove',
		data: { id: 'attachment-1', issueId, ...attachmentFields, url: 'https://example.com/2' },
	},
].map((payload, index) => ({ ...entityEnvelope(index), ...payload }))

describe('Linear resource admission', () => {
	it.effect('routes every supported resource action to the issue mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const [index, payload] of payloads.entries()) {
				const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payload, `resource-${index}`))
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
			const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payloads[2], 'child-issue'))
			expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
		}),
	)

	it.effect('accepts nullable comment and reaction relations plus integration actors', ({ expect }) =>
		Effect.gen(function* () {
			for (const index of [0, 3, 5, 6]) {
				const outcome = yield* makeLinearTestProvider().handle(
					signedLinearInput(payloads[index], `nullable-${index}`),
				)
				expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
			}
		}),
	)
})
