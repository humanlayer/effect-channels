import { Config, Effect, Layer, Match, Redacted, Schema, Stream } from 'effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import {
	GitHubActivityEvent,
	GitHubPullRequestData,
	GitHubReviewData,
	GitHubReviewCommentData,
	GitHubReviewThreadData,
	reviewRequestFields,
} from './GitHubActivity'
import { GitHubCredentials } from './GitHubCredentials'
import { GitHubCrypto } from './GitHubCrypto'
import { GitHubWebhookError } from './GitHubErrors'
import { GitHubCommentData, GitHubIssueData, GitHubUser } from './GitHubEvents'
import { GitHubIngress } from './GitHubIngress'
import { GitHubDiscussionRef, GitHubId } from './GitHubResource'

const Payload = Schema.Struct({
	action: Schema.String,
	installation: Schema.Struct({ id: GitHubId }),
	repository: Schema.Struct({
		id: GitHubId,
		name: Schema.NonEmptyString,
		owner: Schema.Struct({ login: Schema.NonEmptyString }),
	}),
	issue: Schema.optionalKey(GitHubIssueData),
	pull_request: Schema.optionalKey(GitHubPullRequestData),
	sender: Schema.optionalKey(GitHubUser),
	comment: Schema.optionalKey(Schema.Union([GitHubReviewCommentData, GitHubCommentData])),
	review: Schema.optionalKey(GitHubReviewData),
	thread: Schema.optionalKey(GitHubReviewThreadData),
	number: Schema.optionalKey(GitHubId),
	before: Schema.optionalKey(Schema.String),
	after: Schema.optionalKey(Schema.String),
	assignee: Schema.optionalKey(Schema.NullOr(GitHubUser)),
	label: Schema.optionalKey(
		Schema.Struct({
			id: GitHubId,
			name: Schema.String,
			color: Schema.String,
			description: Schema.optionalKey(Schema.NullOr(Schema.String)),
		}),
	),
	...reviewRequestFields,
	changes: Schema.optionalKey(
		Schema.Struct({ body: Schema.optionalKey(Schema.Struct({ from: Schema.NullOr(Schema.String) })) }),
	),
})
const optionsSchema = Schema.Struct({
	signingSecret: Schema.Redacted(Schema.NonEmptyString),
	maxBodyBytes: Schema.Int.check(Schema.isGreaterThan(0)),
	botLogin: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9-]*\[bot\]$/))),
})
export interface GitHubRoutesOptions {
	readonly signingSecret: Redacted.Redacted<string>
	readonly maxBodyBytes: number
	readonly botLogin?: string
}

export const GitHubWebhookPath = '/integrations/github/webhook' as const

const layer = (options: GitHubRoutesOptions, mountPath?: string) =>
	Layer.unwrap(
		Effect.gen(function* () {
			const config = yield* optionsSchema
				.makeEffect(options)
				.pipe(Effect.mapError(() => GitHubWebhookError.make({ reason: 'decode' })))
			const crypto = yield* GitHubCrypto
			const credentials = yield* GitHubCredentials
			const ingress = yield* GitHubIngress
			const mention =
				config.botLogin === undefined
					? undefined
					: new RegExp(
							`(?:^|[^\\w@/\\\\.+-])@${config.botLogin.slice(0, -5)}(?:\\[bot\\])?(?![\\w\\[\\]/-])`,
							'i',
						)
			const isOwn = (user: Pick<GitHubUser, 'id' | 'login'> | null | undefined) =>
				user != null &&
				(user.id === credentials.botUserId || user.login.toLowerCase() === config.botLogin?.toLowerCase())
			const acceptActivity = Effect.fn('github.webhook.accept_activity')(function* (input: {
				readonly event: string
				readonly deliveryId: string
				readonly payload: typeof Payload.Type
				readonly issue: GitHubIssueData | GitHubPullRequestData
			}) {
				const { event, deliveryId, payload, issue } = input
				const supported = Match.value(event).pipe(
					Match.when('issues', () => [
						'opened',
						'edited',
						'closed',
						'reopened',
						'assigned',
						'unassigned',
						'labeled',
						'unlabeled',
					]),
					Match.when('pull_request', () => [
						'opened',
						'edited',
						'closed',
						'reopened',
						'synchronize',
						'review_requested',
						'review_request_removed',
						'assigned',
						'unassigned',
						'labeled',
						'unlabeled',
						'converted_to_draft',
						'ready_for_review',
					]),
					Match.when('pull_request_review', () => ['submitted', 'edited', 'dismissed']),
					Match.when('pull_request_review_thread', () => ['resolved', 'unresolved']),
					Match.orElse(() => ['created', 'edited', 'deleted']),
				)
				if (!supported.includes(payload.action)) return HttpServerResponse.empty({ status: 200 })
				if (payload.number !== undefined && payload.number !== issue.number)
					return yield* GitHubWebhookError.make({ reason: 'decode' })
				const resource = yield* GitHubDiscussionRef.makeEffect({
					kind:
						event.startsWith('pull_request') || issue.pull_request !== undefined
							? 'github.pull-request'
							: 'github.issue',
					repository: {
						kind: 'github.repository',
						installationId: payload.installation.id,
						id: payload.repository.id,
						owner: payload.repository.owner.login,
						name: payload.repository.name,
					},
					number: issue.number,
				}).pipe(Effect.mapError(() => GitHubWebhookError.make({ reason: 'decode' })))
				const normalized = yield* Schema.decodeUnknownEffect(GitHubActivityEvent)({
					...payload,
					event,
					deliveryId,
					resource,
				}).pipe(Effect.mapError(() => GitHubWebhookError.make({ reason: 'decode' })))
				const content = Match.value(event).pipe(
					Match.when('pull_request_review', () => payload.review),
					Match.when('pull_request_review_thread', () => undefined),
					Match.when(
						(event) => event.endsWith('comment'),
						() => payload.comment,
					),
					Match.orElse(() => issue),
				)
				const contentAction = ['opened', 'created', 'edited', 'submitted'].includes(payload.action)
				const mentioned =
					contentAction &&
					mention !== undefined &&
					mention.test(content?.body ?? '') &&
					(payload.action !== 'edited' ||
						(payload.changes?.body !== undefined && !mention.test(payload.changes.body.from ?? '')))
				yield* ingress.acceptActivity({
					event: normalized,
					mentioned,
					own: isOwn(payload.sender) || (contentAction && isOwn(content?.user)),
				})
				return HttpServerResponse.empty({ status: 200 })
			})
			const accept = Effect.fn('github.webhook.accept')(function* ({
				event,
				deliveryId,
				bytes,
			}: {
				readonly event: string
				readonly deliveryId: string
				readonly bytes: Uint8Array
			}) {
				if (
					![
						'issues',
						'issue_comment',
						'pull_request',
						'pull_request_review',
						'pull_request_review_comment',
						'pull_request_review_thread',
					].includes(event)
				)
					return HttpServerResponse.empty({ status: 200 })
				const text = yield* Effect.try({
					try: () => new TextDecoder('utf-8', { fatal: true }).decode(bytes),
					catch: () => GitHubWebhookError.make({ reason: 'decode' }),
				})
				const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Payload))(text).pipe(
					Effect.mapError(() => GitHubWebhookError.make({ reason: 'decode' })),
				)
				if (!credentials.acceptsInstallation({ installationId: payload.installation.id }))
					return yield* GitHubWebhookError.make({ reason: 'installation' })
				const issue = event.startsWith('pull_request') ? payload.pull_request : payload.issue
				if (issue === undefined || (event === 'issue_comment' && payload.comment === undefined))
					return yield* GitHubWebhookError.make({ reason: 'decode' })
				return yield* acceptActivity({ event, deliveryId, payload, issue })
			})
			return HttpRouter.addAll(
				[
					HttpRouter.route('POST', GitHubWebhookPath, (request) =>
						Effect.gen(function* () {
							const signature = request.headers['x-hub-signature-256']
							const deliveryId = request.headers['x-github-delivery']
							const event = request.headers['x-github-event']
							if (
								signature === undefined ||
								deliveryId === undefined ||
								!/^[a-zA-Z0-9-]{1,128}$/.test(deliveryId) ||
								event === undefined
							)
								return yield* GitHubWebhookError.make({ reason: 'signature' })
							const bytes = yield* request.stream.pipe(
								Stream.runFoldEffect(
									() => {
										const chunks: Array<Uint8Array> = []
										return { chunks, size: 0 }
									},
									(body, chunk) => {
										if (body.size + chunk.byteLength > config.maxBodyBytes)
											return Effect.fail(GitHubWebhookError.make({ reason: 'capacity' }))
										if (chunk.byteLength > 0) body.chunks.push(chunk)
										body.size += chunk.byteLength
										return Effect.succeed(body)
									},
								),
								Effect.map(({ chunks, size }) => {
									const bytes = new Uint8Array(size)
									let offset = 0
									for (const chunk of chunks) {
										bytes.set(chunk, offset)
										offset += chunk.byteLength
									}
									return bytes
								}),
								Effect.catchTag('HttpServerError', () =>
									Effect.fail(GitHubWebhookError.make({ reason: 'decode' })),
								),
							)
							yield* crypto.verifyWebhook({ secret: config.signingSecret, body: bytes, signature })
							return yield* accept({ event, deliveryId, bytes })
						}).pipe(
							Effect.catchTags({
								GitHubWebhookError: (error) =>
									Effect.succeed(
										HttpServerResponse.empty({
											status: Match.value(error.reason).pipe(
												Match.when('signature', () => 401),
												Match.when('installation', () => 403),
												Match.when('capacity', () => 413),
												Match.when('decode', () => 400),
												Match.when('crypto', () => 500),
												Match.exhaustive,
											),
										}),
									),
								GitHubIngressError: (error) =>
									Effect.succeed(
										HttpServerResponse.empty({ status: error.reason === 'unexpected' ? 500 : 503 }),
									),
							}),
							Effect.withSpan('github.webhook'),
						),
					),
				],
				{ prefix: mountPath },
			)
		}),
	)

export const GitHubRoutes = {
	webhookPath: GitHubWebhookPath,
	mountedWebhookPath: (mountPath: string) => HttpRouter.prefixPath(GitHubWebhookPath, mountPath),
	layer,
	layerMounted: (mountPath: string, options: GitHubRoutesOptions) => layer(options, mountPath),
	layerConfig: Layer.unwrap(
		Effect.gen(function* () {
			const signingSecret = yield* Config.redacted('GITHUB_WEBHOOK_SECRET')
			const botLogin = yield* Config.string('GITHUB_BOT_LOGIN')
			return layer({ signingSecret, maxBodyBytes: 256_000, botLogin })
		}),
	),
	layerConfigMounted: (mountPath: string) =>
		Layer.unwrap(
			Effect.gen(function* () {
				const signingSecret = yield* Config.redacted('GITHUB_WEBHOOK_SECRET')
				const botLogin = yield* Config.string('GITHUB_BOT_LOGIN')
				return layer({ signingSecret, maxBodyBytes: 256_000, botLogin }, mountPath)
			}),
		),
}
