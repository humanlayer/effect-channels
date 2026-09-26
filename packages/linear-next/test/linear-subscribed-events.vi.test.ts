import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	MailboxSubscriptions,
	ProviderEventHandled,
	ProviderEventIgnored,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer } from 'effect'
import { vi } from 'vite-plus/test'

import { LinearApi } from '../src/LinearApi'
import type { LinearMentioned, LinearSubscribedEvents } from '../src/LinearCallbackEvents'
import { LinearCallbacks } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import {
	appUserNotificationPayloads,
	issueCreatePayload,
	linearAppUserId,
	linearOauthClientId,
	linearOrganizationId,
} from './fixtures'

const namespace = 'linear-subscribed-test'
const created = issueCreatePayload
const issueId = created.data.id
const { number: _number, ...issueWithoutNumber } = created.data
const timestamp = '2026-01-01T00:00:00.000Z'

describe('Linear subscribed events', () => {
	it.effect('preserves ordered issue-scoped resource events in one callback', ({ expect }) =>
		Effect.gen(function* () {
			const callback = vi.fn((_event: LinearSubscribedEvents) => Effect.void)
			const payloads = [
				{
					createdAt: timestamp,
					webhookId: 'comment-webhook',
					webhookTimestamp: 1,
					type: 'Comment',
					action: 'create',
					organizationId: created.organizationId,
					data: {
						id: 'comment-1',
						body: 'one',
						issueId,
						issue: issueWithoutNumber,
						createdAt: timestamp,
						updatedAt: timestamp,
						reactionData: {},
					},
				},
				{
					createdAt: timestamp,
					webhookId: 'reaction-webhook',
					webhookTimestamp: 2,
					type: 'Reaction',
					action: 'create',
					organizationId: created.organizationId,
					data: {
						id: 'reaction-1',
						emoji: '+1',
						comment: { id: 'comment-1', body: 'one', issueId },
						createdAt: timestamp,
						updatedAt: timestamp,
					},
				},
				{
					createdAt: timestamp,
					webhookId: 'integration-webhook',
					webhookTimestamp: 3,
					type: 'Issue',
					action: 'update',
					organizationId: created.organizationId,
					actor: { id: 'integration-1', service: 'github', type: 'integration' },
					data: created.data,
					updatedFrom: { title: null, futureProviderField: null },
				},
				{
					createdAt: timestamp,
					webhookId: 'app-self-webhook',
					webhookTimestamp: 4,
					type: 'Issue',
					action: 'update',
					organizationId: created.organizationId,
					actor: { id: linearAppUserId, name: 'App', type: 'user' },
					data: created.data,
					updatedFrom: { title: 'before app edit' },
				},
				{
					createdAt: timestamp,
					webhookId: 'oauth-self-webhook',
					webhookTimestamp: 5,
					type: 'Issue',
					action: 'update',
					organizationId: created.organizationId,
					actor: { id: linearOauthClientId, name: 'OAuth app', type: 'oauthClient' },
					data: created.data,
					updatedFrom: { title: 'before oauth edit' },
				},
			] as const
			const admissions = payloads.map((payload, index) =>
				DeliveryAdmission.make({
					namespace,
					provider: 'linear',
					installationId: created.organizationId,
					resourceId: `linear:v1:issue:${issueId}`,
					eventId: `event-${index}`,
					payload,
				}),
			)
			const services = Layer.mergeAll(
				LinearCallbacks.layer({ onSubscribedEvent: callback }),
				Layer.mock(LinearApi, {}),
				Layer.mock(MailboxSubscriptions, {
					subscribe: () => Effect.die('not used'),
					isSubscribed: () => Effect.succeed(true),
					unsubscribe: () => Effect.void,
				}),
			)
			const result = yield* makeLinearEventProcessor({
				namespace,
				bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
				oauthClientId: linearOauthClientId,
			})
				.process([admissions[0]!, ...admissions.slice(1)])
				.pipe(Effect.provide(services))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(callback.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual([
				'LinearCommentCreated',
				'LinearReactionAdded',
				'LinearIssueUpdated',
			])
			expect(callback.mock.calls[0]?.[0].events[0]).toMatchObject({
				comment: {
					_tag: 'LinearComment',
					ref: { issueId, commentId: 'comment-1' },
					content: { markdown: 'one' },
				},
			})
			expect(callback.mock.calls[0]?.[0].events[1]).toMatchObject({
				reaction: { _tag: 'LinearReaction', issueId, commentId: 'comment-1' },
			})
			expect(callback.mock.calls[0]?.[0].events[2]?.actor).toBeNull()
		}),
	)

	it.effect('delivers updates with unmodeled changes and keeps honest reduced issue context', ({ expect }) =>
		Effect.gen(function* () {
			const callback = vi.fn((_event: LinearSubscribedEvents) => Effect.void)
			const payloads = [
				{
					createdAt: timestamp,
					webhookId: 'unknown-issue-change',
					webhookTimestamp: 1,
					type: 'Issue',
					action: 'update',
					organizationId: created.organizationId,
					data: created.data,
					updatedFrom: { futureProviderField: null },
				},
				{
					createdAt: timestamp,
					webhookId: 'comment-change',
					webhookTimestamp: 2,
					type: 'Comment',
					action: 'update',
					organizationId: created.organizationId,
					data: {
						id: 'comment-1',
						body: 'updated',
						issueId,
						createdAt: timestamp,
						updatedAt: timestamp,
						reactionData: {},
					},
					updatedFrom: { body: 'before', resolvedAt: null },
				},
			] as const
			const admissions = payloads.map((payload, index) =>
				DeliveryAdmission.make({
					namespace,
					provider: 'linear',
					installationId: created.organizationId,
					resourceId: `linear:v1:issue:${issueId}`,
					eventId: `change-${index}`,
					payload,
				}),
			)
			const services = Layer.mergeAll(
				LinearCallbacks.layer({ onSubscribedEvent: callback }),
				Layer.mock(LinearApi, {}),
				Layer.mock(MailboxSubscriptions, { isSubscribed: () => Effect.succeed(true) }),
			)
			yield* makeLinearEventProcessor({ namespace })
				.process([admissions[0]!, admissions[1]!])
				.pipe(Effect.provide(services))
			const event = callback.mock.calls[0]?.[0]
			expect(event?.events).toHaveLength(2)
			expect(event?.events[0]).toMatchObject({ _tag: 'LinearIssueUpdated', changes: [] })
			expect(event?.events[1]).toMatchObject({ _tag: 'LinearCommentUpdated', previousBody: 'before' })
			const attachmentAdmission = DeliveryAdmission.make({
				namespace,
				provider: 'linear',
				installationId: created.organizationId,
				resourceId: `linear:v1:issue:${issueId}`,
				eventId: 'attachment-only',
				payload: {
					createdAt: timestamp,
					webhookId: 'attachment-only',
					webhookTimestamp: 3,
					type: 'Attachment',
					action: 'create',
					organizationId: created.organizationId,
					data: {
						id: 'attachment-1',
						issueId,
						title: 'Link',
						subtitle: null,
						url: 'https://example.com',
						metadata: {},
						groupBySource: false,
						createdAt: timestamp,
						updatedAt: timestamp,
						archivedAt: null,
						creatorId: null,
						externalUserCreatorId: null,
						originalIssueId: null,
						source: null,
						sourceType: null,
					},
				},
			})
			yield* makeLinearEventProcessor({ namespace }).process([attachmentAdmission]).pipe(Effect.provide(services))
			const reducedIssue = callback.mock.calls[1]?.[0].issue
			expect(reducedIssue?.ref.teamId).toBeNull()
			expect(reducedIssue?.identifier).toBeNull()
			expect(callback.mock.calls[1]?.[0].events[0]).toMatchObject({
				attachment: { _tag: 'LinearIssueAttachment', issueId, id: 'attachment-1' },
			})
			yield* makeLinearEventProcessor({ namespace })
				.process([attachmentAdmission, admissions[0]!])
				.pipe(Effect.provide(services))
			const recoveredIssue = callback.mock.calls[2]?.[0].issue
			expect(recoveredIssue?.ref.teamId).toBe(created.data.teamId)
			expect(recoveredIssue?.identifier).toBe(created.data.identifier)
		}),
	)

	it.effect('delivers resource events batched with supplemental notifications', ({ expect }) =>
		Effect.gen(function* () {
			const callback = vi.fn((_event: LinearSubscribedEvents) => Effect.void)
			const notification = appUserNotificationPayloads[0]
			const mixedIssueId = notification.notification.issueId
			const comment = {
				createdAt: timestamp,
				webhookId: 'mixed-comment',
				webhookTimestamp: 2,
				type: 'Comment',
				action: 'create',
				organizationId: created.organizationId,
				data: {
					id: 'comment-1',
					body: 'one',
					issueId: mixedIssueId,
					createdAt: timestamp,
					updatedAt: timestamp,
					reactionData: {},
				},
			} as const
			const admissions = [notification, comment].map((payload, index) =>
				DeliveryAdmission.make({
					namespace,
					provider: 'linear',
					installationId: created.organizationId,
					resourceId: `linear:v1:issue:${mixedIssueId}`,
					eventId: `mixed-${index}`,
					payload,
				}),
			)
			const services = Layer.mergeAll(
				LinearCallbacks.layer({ onAgentSessionCreated: () => Effect.void, onSubscribedEvent: callback }),
				Layer.mock(LinearApi, {}),
				Layer.mock(MailboxSubscriptions, { isSubscribed: () => Effect.succeed(true) }),
			)
			const result = yield* makeLinearEventProcessor({ namespace })
				.process([admissions[0]!, admissions[1]!])
				.pipe(Effect.provide(services))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(callback.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual(['LinearCommentCreated'])
		}),
	)

	it.effect('includes resource events in the winning directed callback', ({ expect }) =>
		Effect.gen(function* () {
			const onMentioned = vi.fn((_event: LinearMentioned) => Effect.void)
			const onSubscribedEvent = vi.fn((_event: LinearSubscribedEvents) => Effect.void)
			const notification = appUserNotificationPayloads[0]
			const mixedIssueId = notification.notification.issueId
			const comment = {
				createdAt: timestamp,
				webhookId: 'directed-comment',
				webhookTimestamp: 2,
				type: 'Comment',
				action: 'create',
				organizationId: created.organizationId,
				data: {
					id: 'comment-2',
					body: 'one',
					issueId: mixedIssueId,
					createdAt: timestamp,
					updatedAt: timestamp,
					reactionData: {},
				},
			} as const
			const admissions = [notification, comment].map((payload, index) =>
				DeliveryAdmission.make({
					namespace,
					provider: 'linear',
					installationId: created.organizationId,
					resourceId: `linear:v1:issue:${mixedIssueId}`,
					eventId: `directed-${index}`,
					payload,
				}),
			)
			const services = Layer.mergeAll(
				LinearCallbacks.layer({ onMentioned, onSubscribedEvent }),
				Layer.mock(LinearApi, {}),
				Layer.mock(MailboxSubscriptions, { isSubscribed: () => Effect.succeed(true) }),
			)
			const result = yield* makeLinearEventProcessor({ namespace })
				.process([admissions[0]!, admissions[1]!])
				.pipe(Effect.provide(services))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(onMentioned.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual(['LinearCommentCreated'])
			expect(onSubscribedEvent).not.toHaveBeenCalled()
		}),
	)

	it.effect('suppresses self-created issues and retires removed subscriptions without a callback', ({ expect }) =>
		Effect.gen(function* () {
			const createdCallback = vi.fn(() => Effect.void)
			const unsubscribe = vi.fn(() => Effect.void)
			const selfCreate = {
				...issueCreatePayload,
				actor: { id: linearAppUserId, name: 'App', type: 'user' },
			}
			const selfCreateAdmission = DeliveryAdmission.make({
				namespace,
				provider: 'linear',
				installationId: created.organizationId,
				resourceId: `linear:v1:issue:${issueId}`,
				eventId: 'self-create',
				payload: selfCreate,
			})
			const removeAdmission = DeliveryAdmission.make({
				namespace,
				provider: 'linear',
				installationId: created.organizationId,
				resourceId: `linear:v1:issue:${issueId}`,
				eventId: 'self-remove',
				payload: {
					...issueCreatePayload,
					action: 'remove',
					actor: { id: linearAppUserId, name: 'App', type: 'user' },
				},
			})
			const services = Layer.mergeAll(
				LinearCallbacks.layer({ onIssueCreated: createdCallback }),
				Layer.mock(LinearApi, {}),
				Layer.mock(MailboxSubscriptions, {
					isSubscribed: () => Effect.succeed(true),
					unsubscribe,
				}),
			)
			const processor = makeLinearEventProcessor({
				namespace,
				bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
			})
			const createResult = yield* processor.process([selfCreateAdmission]).pipe(Effect.provide(services))
			expect(createResult).toEqual(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
			expect(createdCallback).not.toHaveBeenCalled()
			const removeResult = yield* processor.process([removeAdmission]).pipe(Effect.provide(services))
			expect(removeResult).toEqual(ProviderEventHandled.make({}))
			expect(unsubscribe).toHaveBeenCalledOnce()
		}),
	)
})
