/**
 * The verified credential and the service that supplies it, kept apart from the resolvers so the HTTP
 * transport can read them without importing the API operations the resolvers use.
 */
import { Context, Schema } from 'effect'
import type { Effect } from 'effect'

import type { LinearApiError } from './LinearApiContract'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'

export const LinearVerifiedCredential = Schema.Struct({
	accessToken: Schema.Redacted(Schema.NonEmptyString, { disallowJsonEncode: true }),
	expiresAt: Schema.Finite,
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
})
export type LinearVerifiedCredential = typeof LinearVerifiedCredential.Type

export class LinearCredentialResolver extends Context.Service<
	LinearCredentialResolver,
	{
		readonly organizationId: LinearOrganizationId
		readonly canRefresh: boolean
		readonly resolve: Effect.Effect<LinearVerifiedCredential, LinearApiError>
		readonly invalidate: Effect.Effect<void>
	}
>()('@humanlayer/channels-linear/LinearCredentialResolver') {}
