import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	MailboxSubscriptions,
	ProviderEventInvalid,
	ProviderWebhookEvent,
	ProviderWebhookResponse,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Schema } from 'effect'

import { LinearApi } from '../src/LinearApi'
import { LinearCallbacks } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import {
	linearAppUserId,
	linearOauthClientId,
	linearOrganizationId,
	makeLinearTestProvider,
	signedLinearInput,
} from './fixtures'

const lifecycleMetadata = {
	organizationId: linearOrganizationId,
	createdAt: '2026-01-01T00:00:00.000Z',
	webhookId: 'lifecycle-webhook',
	webhookTimestamp: 1_767_225_600_000,
}

describe('Linear installation events', () => {
	it.effect('admits permission and revocation events to the installation mailbox', ({ expect }) =>
		Effect.gen(function* () {
			for (const payload of [
				{
					...lifecycleMetadata,
					type: 'PermissionChange',
					action: 'teamAccessChanged',
					appUserId: 'a75b41db-2746-4c05-a792-31535319f512',
					oauthClientId: 'linear-test-client',
					canAccessAllPublicTeams: false,
					addedTeamIds: ['team-1'],
					removedTeamIds: [],
				},
				{
					...lifecycleMetadata,
					type: 'OAuthApp',
					action: 'revoked',
					oauthClientId: 'linear-test-client',
				},
			]) {
				const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payload, payload.type))
				expect(Schema.is(ProviderWebhookEvent)(outcome)).toBe(true)
				if (Schema.is(ProviderWebhookEvent)(outcome))
					expect(outcome.event.resourceId).toBe('linear:v1:installation')
			}
		}),
	)

	it.effect('rejects configured lifecycle identity mismatches at ingress', ({ expect }) =>
		Effect.gen(function* () {
			for (const payload of [
				{
					...lifecycleMetadata,
					type: 'PermissionChange',
					action: 'teamAccessChanged',
					appUserId: 'different-app-user',
					oauthClientId: 'linear-test-client',
					canAccessAllPublicTeams: false,
					addedTeamIds: [],
					removedTeamIds: [],
				},
				{
					...lifecycleMetadata,
					type: 'OAuthApp',
					action: 'revoked',
					oauthClientId: 'different-client',
				},
			]) {
				const outcome = yield* makeLinearTestProvider().handle(signedLinearInput(payload, payload.type))
				expect(outcome).toEqual(ProviderWebhookResponse.make({ status: 403, body: null, headers: {} }))
			}
		}),
	)

	it.effect('revalidates lifecycle identity during durable processing', ({ expect }) =>
		Effect.gen(function* () {
			const admission = DeliveryAdmission.make({
				namespace: 'linear-lifecycle-test',
				provider: 'linear',
				installationId: linearOrganizationId,
				resourceId: 'linear:v1:installation',
				eventId: 'lifecycle-mismatch',
				payload: {
					...lifecycleMetadata,
					type: 'PermissionChange',
					action: 'teamAccessChanged',
					appUserId: 'different-app-user',
					oauthClientId: linearOauthClientId,
					canAccessAllPublicTeams: false,
					addedTeamIds: [],
					removedTeamIds: [],
				},
			})
			const error = yield* makeLinearEventProcessor({
				namespace: 'linear-lifecycle-test',
				bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
				oauthClientId: linearOauthClientId,
			})
				.process([admission])
				.pipe(
					Effect.provide(
						Layer.mergeAll(
							LinearCallbacks.layer<never, never>({}),
							Layer.mock(LinearApi, {}),
							Layer.mock(MailboxSubscriptions, {
								subscribe: () => Effect.die('not used'),
								isSubscribed: () => Effect.succeed(false),
								unsubscribe: () => Effect.void,
							}),
						),
					),
					Effect.flip,
				)
			expect(error).toEqual(ProviderEventInvalid.make({ provider: 'linear', reason: 'identity_mismatch' }))
		}),
	)
})
