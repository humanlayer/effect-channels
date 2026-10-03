import { Config, Duration, Effect, Layer, Match, Schedule, Schema } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HttpClient from 'effect/http/HttpClient'

import { createAgentActivity } from './api/CreateAgentActivity'
import {
	type LinearProviderError,
	narrowLinearProviderErrors,
	narrowLinearProviderStreamErrors,
} from './api/LinearApiErrors'
import { projectLinearUploadedFile } from './api/LinearApiProjections'
import { LinearHttpClient, makeAuthenticatedLinearHttpClient } from './api/LinearHttpClient'
import {
	createAttachment,
	createComment,
	createReaction,
	deleteAttachment,
	deleteComment,
	deleteReaction,
	downloadLinearFile,
	getIssue,
	getUser,
	listAppUsers,
	listAssignableUsers,
	listIssueAttachments,
	listIssueComments,
	readBoundedLinearFileBytes,
	requestLinearFileUpload,
	updateAgentSession,
	updateAttachment,
	updateComment,
	updateIssue,
	uploadLinearFileBytes,
} from './api/Operations'
import { LinearApi, LinearApiError } from './LinearApi'
import { LinearAuthenticationInput } from './LinearAuth'
import * as LinearAuth from './LinearAuth'
import {
	LinearCredentialResolver,
	LinearCredentialResolverClientCredentials,
	LinearCredentialResolverDeveloperToken,
} from './LinearCredentialResolver'
import {
	defaultLinearFileTransferPolicy,
	type LinearDownloadFileRequest,
	LinearFileSizeLimitExceeded,
	LinearFileTransferPolicy,
	type LinearUploadFileRequest,
} from './LinearFiles'
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
	filePolicy: Schema.optionalKey(LinearFileTransferPolicy),
})
export type LinearApiLiveOptions = typeof LinearApiLiveOptions.Type

const readRetryPolicy = Schedule.exponential('50 millis').pipe(
	Schedule.setInputType<LinearApiError>(),
	Schedule.jittered,
	Schedule.upTo({ times: 2 }),
	Schedule.passthrough,
	Schedule.while(({ input }) => input.retryable),
	Schedule.modifyDelay(({ input, duration }) => {
		if (input.retryAfterMs === undefined) return Effect.succeed(duration)
		return Effect.succeed(Duration.max(duration, Duration.millis(input.retryAfterMs)))
	}),
)

const uploadedAttachmentSubtitle = (size: number) => `${(size / 1024).toFixed(1)} KB`

const apiService = (
	client: LinearHttpClient['Service'],
	http: HttpClient.HttpClient,
	filePolicy: LinearFileTransferPolicy,
) => {
	const executeAuthenticated = <A>(
		organizationId: LinearOrganizationId,
		procedure: Effect.Effect<A, LinearProviderError | LinearApiError, LinearHttpClient>,
	): Effect.Effect<A, LinearApiError> => {
		if (organizationId !== client.organizationId) {
			return Effect.fail(
				LinearApiError.make({ operation: 'viewer_identity', reason: 'identity_mismatch', retryable: false }),
			)
		}
		return procedure.pipe(Effect.provideService(LinearHttpClient, client), narrowLinearProviderErrors)
	}

	/**
	 * Log a credential Linear refused as a configuration error that names it. A developer token cannot be
	 * renewed; client credentials were already renewed once.
	 */
	const reportRejectedCredential = (error: LinearApiError) =>
		error.reason === 'unauthorized'
			? Effect.logError(
					client.canRefresh
						? 'Linear rejected the app client credentials; check LINEAR_CLIENT_ID and LINEAR_CLIENT_SECRET'
						: 'Linear rejected the developer token; check LINEAR_DEVELOPER_TOKEN, or remove it to use the app client credentials',
				).pipe(
					Effect.annotateLogs({
						provider: 'linear',
						credential: client.canRefresh ? 'client_credentials' : 'developer_token',
						operation: error.operation,
					}),
				)
			: Effect.void

	const refreshAuthentication = <A>(
		organizationId: LinearOrganizationId,
		procedure: Effect.Effect<A, LinearProviderError | LinearApiError, LinearHttpClient>,
	) =>
		executeAuthenticated(organizationId, procedure).pipe(
			Effect.catchTag('LinearApiError', (error) => {
				if (error.reason !== 'unauthorized' || !client.canRefresh) return Effect.fail(error)
				return client.invalidateCredential.pipe(Effect.andThen(executeAuthenticated(organizationId, procedure)))
			}),
			Effect.tapError(reportRejectedCredential),
		)

	const executeRead = <A>(
		organizationId: LinearOrganizationId,
		procedure: Effect.Effect<A, LinearProviderError | LinearApiError, LinearHttpClient>,
	) => refreshAuthentication(organizationId, procedure).pipe(Effect.retry(readRetryPolicy))

	const executeMutation = refreshAuthentication

	const uploadFile = Effect.fn('linear.api_live.upload_file')(function* (request: LinearUploadFileRequest) {
		const target = yield* executeMutation(request.issue.organizationId, requestLinearFileUpload(request))
		yield* uploadLinearFileBytes({
			target,
			contentType: request.input.contentType,
			bytes: request.input.bytes,
		}).pipe(Effect.provideService(HttpClient.HttpClient, http), narrowLinearProviderErrors)
		const file = projectLinearUploadedFile(request.issue, request.input.filename, target)
		yield* Effect.logInfo('Linear file uploaded').pipe(
			Effect.annotateLogs({ issue_id: request.issue.issueId, bytes: request.input.bytes.byteLength }),
		)
		return file
	})

	const openDownload = (request: LinearDownloadFileRequest) =>
		executeRead(request.file.organizationId, downloadLinearFile({ request, policy: filePolicy })).pipe(
			Effect.map((download) => ({
				contentLength: download.contentLength,
				stream: narrowLinearProviderStreamErrors(download.stream),
			})),
		)

	return LinearApi.of({
		createAgentActivity: (request) => executeMutation(request.organizationId, createAgentActivity(request)),
		updateAgentSession: (request) => executeMutation(request.organizationId, updateAgentSession(request)),
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
		createReaction: (request) => {
			const organizationId = Match.value(request.target).pipe(
				Match.tagsExhaustive({
					Issue: ({ issue }) => issue.organizationId,
					Comment: ({ comment }) => comment.organizationId,
				}),
			)
			return executeMutation(organizationId, createReaction(request))
		},
		deleteReaction: (request) => executeMutation(request.issue.organizationId, deleteReaction(request)),
		createAttachment: (request) => executeMutation(request.issue.organizationId, createAttachment(request)),
		updateAttachment: (request) =>
			executeMutation(request.attachment.issue.organizationId, updateAttachment(request)),
		deleteAttachment: (request) =>
			executeMutation(request.attachment.issue.organizationId, deleteAttachment(request)),
		uploadFile,
		uploadAttachment: (request) =>
			uploadFile({ issue: request.issue, input: request.input }).pipe(
				Effect.flatMap((file) => {
					const size = request.input.bytes.byteLength
					return executeMutation(
						request.issue.organizationId,
						createAttachment({
							issue: request.issue,
							input: {
								url: file.url,
								title: request.input.title ?? request.input.filename,
								subtitle: request.input.subtitle ?? uploadedAttachmentSubtitle(size),
								metadata: { contentType: request.input.contentType, size, ...request.input.metadata },
							},
						}),
					)
				}),
			),
		downloadFile: (request) => openDownload(request).pipe(Effect.map((download) => download.stream)),
		downloadFileBytes: (request) => {
			if (request.size !== null && request.size > request.maxBytes)
				return Effect.fail(
					LinearFileSizeLimitExceeded.make({
						maxBytes: request.maxBytes,
						observedBytes: request.size,
						source: 'declared_size',
					}),
				)
			return openDownload(request).pipe(
				Effect.flatMap((download) => readBoundedLinearFileBytes(download, request.maxBytes)),
			)
		},
	})
}

const LinearHttpClientLive = Layer.effect(
	LinearHttpClient,
	Effect.gen(function* () {
		return makeAuthenticatedLinearHttpClient(yield* HttpClient.HttpClient, yield* LinearCredentialResolver)
	}),
)

const makeLinearApiServiceLive = (filePolicy: LinearFileTransferPolicy) =>
	Layer.effect(
		LinearApi,
		Effect.gen(function* () {
			return apiService(yield* LinearHttpClient, yield* HttpClient.HttpClient, filePolicy)
		}),
	)

/** Builds LinearApi from an injected HTTP transport and credential resolver. */
export const LinearApiLiveFromResolver = makeLinearApiServiceLive(defaultLinearFileTransferPolicy).pipe(
	Layer.provide(LinearHttpClientLive),
)

/** Injectable transport seam. Construction reads configuration and initializes local state but performs no I/O. */
export const makeLinearApiLiveBase = (options: LinearApiLiveOptions) => {
	const resolverLayer = Layer.unwrap(
		Effect.gen(function* () {
			const organizationId = yield* options.organizationId
			const appUserId = yield* options.appUserId
			const auth = yield* LinearAuth.resolve(options.auth)
			return yield* Match.value(auth).pipe(
				Match.tagsExhaustive({
					LinearClientCredentials: (credentials) =>
						Effect.gen(function* () {
							const clientId = yield* credentials.clientId
							const clientSecret = yield* credentials.clientSecret
							return LinearCredentialResolverClientCredentials({
								clientId,
								clientSecret,
								organizationId,
								appUserId,
							})
						}),
					LinearDeveloperToken: (developer) =>
						Effect.gen(function* () {
							const token = yield* developer.token
							return LinearCredentialResolverDeveloperToken({ token, organizationId, appUserId })
						}),
				}),
			)
		}),
	)
	const linearHttpLayer = LinearHttpClientLive.pipe(Layer.provide(resolverLayer))
	return makeLinearApiServiceLive(options.filePolicy ?? defaultLinearFileTransferPolicy).pipe(
		Layer.provide(linearHttpLayer),
	)
}

const defaultOptions = LinearApiLiveOptions.make({
	auth: LinearAuth.fromEnvironment,
	organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
	appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
})

export const LinearApiLiveBase = makeLinearApiLiveBase(defaultOptions)
export const LinearApiLive = LinearApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))

export const makeLinearApiLive = (options: LinearApiLiveOptions) =>
	makeLinearApiLiveBase(options).pipe(Layer.provide(FetchHttpClient.layer))
