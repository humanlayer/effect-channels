import { Config, Duration, Effect, Layer, Match, Predicate, Schedule, Schema } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import { createAgentActivity } from './api/CreateAgentActivity'
import { type LinearProviderError, narrowLinearProviderError } from './api/LinearApiErrors'
import {
	createAttachment,
	createComment,
	createReaction,
	deleteAttachment,
	deleteComment,
	deleteReaction,
	getIssue,
	getUser,
	listAppUsers,
	listAssignableUsers,
	listIssueAttachments,
	listIssueComments,
	updateAttachment,
	updateComment,
	updateIssue,
} from './api/Operations'
import { LinearApi, LinearApiError } from './LinearApi'
import { LinearAuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import {
	LinearCredentialResolver,
	makeLinearCredentialResolver,
	makeLinearDeveloperTokenResolver,
} from './LinearCredentialResolver'
import { LinearOrganizationId, LinearUserId } from './LinearIdentity'

const LinearOrganizationIdConfig = Schema.declare<Config.Config<LinearOrganizationId>>(
	(value): value is Config.Config<LinearOrganizationId> => Config.isConfig(value),
)
const LinearUserIdConfig = Schema.declare<Config.Config<LinearUserId>>((value): value is Config.Config<LinearUserId> =>
	Config.isConfig(value),
)

export const LinearApiLiveOptions = Schema.TaggedStruct('LinearApiLiveOptions', {
	auth: LinearAuthenticationInput,
	organizationId: LinearOrganizationIdConfig,
	appUserId: LinearUserIdConfig,
})
export type LinearApiLiveOptions = typeof LinearApiLiveOptions.Type

const apiService = (client: HttpClient.HttpClient, credentials: LinearCredentialResolver['Service']) => {
	const readRetryPolicy = Schedule.exponential('50 millis').pipe(
		Schedule.setInputType<LinearApiError>(),
		Schedule.jittered,
		Schedule.upTo({ times: 2 }),
		Schedule.passthrough,
		Schedule.while(({ input }) => input.retryable),
		Schedule.modifyDelay(({ input, duration }) =>
			Effect.succeed(
				Predicate.isUndefined(input.retryAfterMs)
					? duration
					: Duration.max(duration, Duration.millis(input.retryAfterMs)),
			),
		),
	)

	const executeAuthenticated = <A>(
		organizationId: LinearOrganizationId,
		procedure: Effect.Effect<A, LinearProviderError, HttpClient.HttpClient>,
	) =>
		organizationId === credentials.organizationId
			? Effect.suspend(() =>
					credentials.resolve.pipe(
						Effect.flatMap((credential) =>
							procedure.pipe(
								Effect.provideService(
									HttpClient.HttpClient,
									client.pipe(
										HttpClient.mapRequest(HttpClientRequest.bearerToken(credential.accessToken)),
									),
								),
								Effect.mapError(narrowLinearProviderError),
							),
						),
					),
				)
			: Effect.fail(
					LinearApiError.make({
						operation: 'viewer_identity',
						reason: 'identity_mismatch',
						retryable: false,
					}),
				)

	const refreshAuthentication = <A>(
		organizationId: LinearOrganizationId,
		procedure: Effect.Effect<A, LinearProviderError, HttpClient.HttpClient>,
	) =>
		executeAuthenticated(organizationId, procedure).pipe(
			Effect.catchTag('LinearApiError', (error) =>
				error.reason === 'unauthorized' && credentials.canRefresh
					? credentials.invalidate.pipe(Effect.andThen(executeAuthenticated(organizationId, procedure)))
					: Effect.fail(error),
			),
		)

	const executeRead = <A>(
		organizationId: LinearOrganizationId,
		procedure: Effect.Effect<A, LinearProviderError, HttpClient.HttpClient>,
	) => refreshAuthentication(organizationId, procedure).pipe(Effect.retry(readRetryPolicy))

	const executeMutation = refreshAuthentication

	return LinearApi.of({
		createAgentActivity: (request) => executeMutation(request.organizationId, createAgentActivity(request)),
		getIssue: (request) => executeRead(request.issue.organizationId, getIssue(request)),
		updateIssue: (request) => executeMutation(request.issue.organizationId, updateIssue(request)),
		listAssignableUsers: (request) => executeRead(request.issue.organizationId, listAssignableUsers(request)),
		listAppUsers: (request) =>
			executeRead(
				request.issue.organizationId,
				getIssue({ issue: request.issue }).pipe(
					Effect.flatMap((resolved) => listAppUsers({ ...request, issue: resolved.ref })),
				),
			),
		getUser: (request) =>
			executeRead(
				request.issue.organizationId,
				getIssue({ issue: request.issue }).pipe(
					Effect.flatMap((resolved) => getUser({ ...request, issue: resolved.ref })),
				),
			),
		listIssueComments: (request) => executeRead(request.issue.organizationId, listIssueComments(request)),
		listIssueAttachments: (request) => executeRead(request.issue.organizationId, listIssueAttachments(request)),
		createComment: (request) => executeMutation(request.issue.organizationId, createComment(request)),
		updateComment: (request) => executeMutation(request.comment.organizationId, updateComment(request)),
		deleteComment: (request) => executeMutation(request.comment.organizationId, deleteComment(request)),
		createReaction: (request) =>
			executeMutation(
				request.target._tag === 'Issue'
					? request.target.issue.organizationId
					: request.target.comment.organizationId,
				createReaction(request),
			),
		deleteReaction: (request) => executeMutation(request.issue.organizationId, deleteReaction(request)),
		createAttachment: (request) => executeMutation(request.issue.organizationId, createAttachment(request)),
		updateAttachment: (request) =>
			executeMutation(request.attachment.issue.organizationId, updateAttachment(request)),
		deleteAttachment: (request) =>
			executeMutation(request.attachment.issue.organizationId, deleteAttachment(request)),
	})
}

/** Builds LinearApi from an injected HTTP transport and credential resolver. */
export const LinearApiLiveFromResolver = Layer.effect(
	LinearApi,
	Effect.gen(function* () {
		return apiService(yield* HttpClient.HttpClient, yield* LinearCredentialResolver)
	}),
)

/** Injectable transport seam. Construction reads configuration and initializes local state but performs no I/O. */
export const makeLinearApiLiveBase = (options: LinearApiLiveOptions) =>
	Layer.effect(
		LinearApi,
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient
			const organizationId = yield* options.organizationId
			const appUserId = yield* options.appUserId
			const auth = yield* LinearAuth.resolve(options.auth)
			const credentials = yield* Match.value(auth).pipe(
				Match.tagsExhaustive({
					LinearClientCredentials: (credentials) =>
						Effect.gen(function* () {
							const clientId = yield* credentials.clientId
							const clientSecret = yield* credentials.clientSecret
							return yield* makeLinearCredentialResolver({
								clientId,
								clientSecret,
								organizationId,
								appUserId,
							})
						}),
					LinearDeveloperToken: (developer) =>
						Effect.gen(function* () {
							const token = yield* developer.token
							return yield* makeLinearDeveloperTokenResolver({ token, organizationId, appUserId })
						}),
				}),
				Effect.provideService(HttpClient.HttpClient, client),
			)
			return apiService(client, credentials)
		}),
	)

const defaultOptions = LinearApiLiveOptions.make({
	auth: LinearAuth.fromEnvironment,
	organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
	appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
})

export const LinearApiLiveBase = makeLinearApiLiveBase(defaultOptions)
export const LinearApiLive = LinearApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))

export const makeLinearApiLive = (options: LinearApiLiveOptions) =>
	makeLinearApiLiveBase(options).pipe(Layer.provide(FetchHttpClient.layer))
