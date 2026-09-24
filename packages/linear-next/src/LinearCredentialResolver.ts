import { Clock, Context, Effect, Option, Redacted, Ref, Schema, Semaphore } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'

import { getViewerIdentity } from './api/GetViewerIdentity'
import { acquireClientCredentialsToken } from './auth/AcquireClientCredentialsToken'
import { LinearApiError } from './LinearApi'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'

export const LinearVerifiedCredential = Schema.Struct({
	accessToken: Schema.Redacted(Schema.NonEmptyString, { disallowJsonEncode: true }),
	expiresAt: Schema.Finite,
	organizationId: LinearOrganizationId,
	appUserId: LinearUserId,
})
export interface LinearVerifiedCredential extends Schema.Schema.Type<typeof LinearVerifiedCredential> {}

export type LinearCredentialResolverOptions = {
	readonly clientId: string
	readonly clientSecret: Redacted.Redacted<string>
	readonly organizationId: LinearOrganizationId
	readonly appUserId: LinearUserId
}

export type LinearDeveloperTokenResolverOptions = {
	readonly token: Redacted.Redacted<string>
	readonly organizationId: LinearOrganizationId
	readonly appUserId: LinearUserId
}

export class LinearCredentialResolver extends Context.Service<
	LinearCredentialResolver,
	{
		readonly canRefresh: boolean
		readonly resolve: (
			organizationId: LinearOrganizationId,
		) => Effect.Effect<LinearVerifiedCredential, LinearApiError>
		readonly invalidate: (organizationId: LinearOrganizationId) => Effect.Effect<void>
	}
>()('@humanlayer/channels-linear-next/LinearCredentialResolver') {}

type AcquiredLinearCredential = {
	readonly accessToken: Redacted.Redacted<string>
	readonly expiresAt: number
}

const makeResolver = Effect.fn('linear.credentials.make')(function* (options: {
	readonly acquire: Effect.Effect<AcquiredLinearCredential, LinearApiError, HttpClient.HttpClient>
	readonly canRefresh: boolean
	readonly organizationId: LinearOrganizationId
	readonly appUserId: LinearUserId
}) {
	const client = yield* HttpClient.HttpClient
	const credential = yield* Ref.make(Option.none<LinearVerifiedCredential>())
	const semaphore = yield* Semaphore.make(1)

	const acquire = Effect.gen(function* () {
		const token = yield* options.acquire.pipe(Effect.provideService(HttpClient.HttpClient, client))
		const identity = yield* getViewerIdentity(token.accessToken).pipe(
			Effect.provideService(HttpClient.HttpClient, client),
		)
		if (identity.viewer.id !== options.appUserId || identity.viewer.organization.id !== options.organizationId)
			return yield* LinearApiError.make({
				operation: 'viewer_identity',
				reason: 'identity_mismatch',
				retryable: false,
			})
		const verified = LinearVerifiedCredential.make({
			accessToken: token.accessToken,
			expiresAt: token.expiresAt,
			organizationId: identity.viewer.organization.id,
			appUserId: identity.viewer.id,
		})
		yield* Ref.set(credential, Option.some(verified))
		return verified
	})

	const resolve = Effect.fn('linear.credentials.resolve')(function* (organizationId: LinearOrganizationId) {
		if (organizationId !== options.organizationId)
			return yield* LinearApiError.make({
				operation: 'viewer_identity',
				reason: 'identity_mismatch',
				retryable: false,
			})
		const now = yield* Clock.currentTimeMillis
		const cached = yield* Ref.get(credential)
		if (Option.isSome(cached) && cached.value.expiresAt > now) return cached.value
		return yield* semaphore.withPermit(
			Effect.gen(function* () {
				const currentTime = yield* Clock.currentTimeMillis
				const current = yield* Ref.get(credential)
				return Option.isSome(current) && current.value.expiresAt > currentTime ? current.value : yield* acquire
			}),
		)
	})

	return LinearCredentialResolver.of({
		canRefresh: options.canRefresh,
		resolve,
		invalidate: (organizationId) =>
			organizationId === options.organizationId ? Ref.set(credential, Option.none()) : Effect.void,
	})
})

/** Builds the one-workspace, expiry-aware client-credentials resolver. No network call occurs until resolve. */
export const makeLinearCredentialResolver = (options: LinearCredentialResolverOptions) =>
	makeResolver({
		acquire: acquireClientCredentialsToken({
			clientId: options.clientId,
			clientSecret: options.clientSecret,
			scopes: ['read', 'write', 'app:assignable', 'app:mentionable'],
		}),
		canRefresh: true,
		organizationId: options.organizationId,
		appUserId: options.appUserId,
	})

/** Builds a resolver for a caller-managed app developer token. The token is verified lazily once per runtime. */
export const makeLinearDeveloperTokenResolver = (options: LinearDeveloperTokenResolverOptions) =>
	makeResolver({
		acquire: Effect.succeed({
			accessToken: options.token,
			expiresAt: Number.MAX_SAFE_INTEGER,
		}),
		canRefresh: false,
		organizationId: options.organizationId,
		appUserId: options.appUserId,
	})
