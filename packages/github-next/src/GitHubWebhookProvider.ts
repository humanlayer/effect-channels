import {
	DeliveryAdmission,
	ProviderWebhookEvent,
	ProviderWebhookEvents,
	ProviderWebhookIgnored,
	WebhookAuthenticationError,
	WebhookPayloadInvalidError,
	type ProviderWebhookOutcome,
	type WebhookProvider,
} from '@humanlayer/channels-delivery-next'
import { Crypto, Effect, Match, Redacted, Schema } from 'effect'

import { githubDiscussionResourceId } from './GitHubIdentity'
import {
	GitHubSupportedWebhook,
	type GitHubSupportedWebhook as GitHubSupportedWebhookType,
	GitHubWebhookEnvelope,
	GitHubWebhookHeaders,
} from './GitHubWebhookSchemas'
import { verifyGitHubWebhookSignature } from './GitHubWebhookSignature'

export type GitHubWebhookProviderOptions = {
	readonly namespace: string
	readonly webhookSecret: Redacted.Redacted<string>
}

const SupportedGitHubEvent = Schema.Literals([
	'issues',
	'issue_comment',
	'pull_request',
	'pull_request_review',
	'pull_request_review_comment',
	'pull_request_review_thread',
	'check_run',
])
type SupportedGitHubEvent = typeof SupportedGitHubEvent.Type

const actionsByEvent = {
	issues: ['opened', 'edited', 'closed', 'reopened', 'assigned', 'unassigned', 'labeled', 'unlabeled'],
	issue_comment: ['created', 'edited', 'deleted'],
	pull_request: [
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
	],
	pull_request_review: ['submitted', 'edited', 'dismissed'],
	pull_request_review_comment: ['created', 'edited', 'deleted'],
	pull_request_review_thread: ['resolved', 'unresolved'],
	check_run: ['completed'],
} satisfies Record<SupportedGitHubEvent, ReadonlyArray<string>>

const isSupportedEvent = Schema.is(SupportedGitHubEvent)

const makeGitHubWebhookEvent = (
	options: GitHubWebhookProviderOptions,
	deliveryId: string,
	webhook: GitHubSupportedWebhookType,
) =>
	Match.value(webhook).pipe(
		Match.when({ event: 'check_run' }, ({ payload }): ProviderWebhookOutcome => {
			const pullRequest = payload.check_run.pull_requests[0]
			if (pullRequest === undefined) {
				return ProviderWebhookIgnored.make({})
			}
			const makeAdmission = (association: (typeof payload.check_run.pull_requests)[number]) =>
				DeliveryAdmission.make({
					namespace: options.namespace,
					provider: 'github',
					installationId: String(payload.installation.id),
					resourceId: githubDiscussionResourceId({
						repositoryId: payload.repository.id,
						kind: 'pull-request',
						number: association.number,
					}),
					eventId: `${deliveryId}:pull-request:${payload.repository.id}:${association.number}`,
					payload: webhook,
				})
			const first = makeAdmission(pullRequest)
			const rest = payload.check_run.pull_requests.slice(1).map(makeAdmission)
			return rest.length === 0
				? ProviderWebhookEvent.make({ event: first })
				: ProviderWebhookEvents.make({ events: [first, ...rest] })
		}),
		Match.orElse(({ payload }): ProviderWebhookOutcome => {
			const discussion = 'pull_request' in payload ? payload.pull_request : payload.issue
			const kind = 'pull_request' in payload || discussion.pull_request !== undefined ? 'pull-request' : 'issue'
			return ProviderWebhookEvent.make({
				event: DeliveryAdmission.make({
					namespace: options.namespace,
					provider: 'github',
					installationId: String(payload.installation.id),
					resourceId: githubDiscussionResourceId({
						repositoryId: payload.repository.id,
						kind,
						number: discussion.number,
					}),
					eventId: deliveryId,
					payload: webhook,
				}),
			})
		}),
	)

export const makeGitHubWebhookProvider = (options: GitHubWebhookProviderOptions): WebhookProvider<Crypto.Crypto> => ({
	providerName: 'github',
	handle: (input) =>
		Effect.gen(function* () {
			const headers = yield* Schema.decodeUnknownEffect(GitHubWebhookHeaders)(input.headers).pipe(
				Effect.mapError(() => WebhookAuthenticationError.make({ reason: 'invalid_signature_headers' })),
			)
			yield* verifyGitHubWebhookSignature({
				body: input.body,
				signature: headers['x-hub-signature-256'],
				webhookSecret: options.webhookSecret,
			})
			const event = headers['x-github-event']
			if (!isSupportedEvent(event)) return ProviderWebhookIgnored.make({})

			const unknownPayload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
				new TextDecoder().decode(input.body),
			).pipe(Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_json' })))
			const action = yield* Schema.decodeUnknownEffect(GitHubWebhookEnvelope)(unknownPayload).pipe(
				Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: 'invalid_event_envelope' })),
			)
			if (!actionsByEvent[event].includes(action.action)) {
				return ProviderWebhookIgnored.make({})
			}
			const decoded = yield* Schema.decodeUnknownEffect(GitHubSupportedWebhook)(
				{
					event,
					payload: unknownPayload,
				},
				{ onExcessProperty: 'preserve' },
			).pipe(Effect.mapError(() => WebhookPayloadInvalidError.make({ reason: `invalid_${event}` })))
			if (decoded.event === 'check_run' && decoded.payload.check_run.pull_requests.length === 0) {
				yield* Effect.logInfo('GitHub check run has no pull request association').pipe(
					Effect.annotateLogs({
						reason: 'no_pull_request_association',
						association_count: decoded.payload.check_run.pull_requests.length,
					}),
				)
			}
			return makeGitHubWebhookEvent(options, headers['x-github-delivery'], decoded)
		}),
})
