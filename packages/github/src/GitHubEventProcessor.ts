/** GitHub post-admission batch identity validation, normalization, and callback routing. */
import {
	deliveryMailboxKey,
	type DeliveryAdmission,
	type DeliveryAdmissionBatch,
	type DeliveryCallbackResult,
	type DeliveryContext,
	MailboxSubscriptions,
	PreparedDeliveryCallback,
	PreparedDeliveryInvocation,
	type ProviderDeliveryExecution,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery'
import { Array as Arr, Data, Effect, Match, Option, Predicate, Schema } from 'effect'

import { reviewComment } from './api/GitHubApiProjections'
import { GitHubApi } from './GitHubApi'
import {
	GitHubIssueAssigned,
	GitHubIssueClosed,
	GitHubIssueCommentCreated,
	GitHubIssueCommentDeleted,
	GitHubIssueCommentEdited,
	GitHubIssueCreated,
	type GitHubIssueEvent,
	GitHubIssueEdited,
	GitHubIssueLabeled,
	GitHubIssueMentioned,
	GitHubIssueOpened,
	GitHubIssueReopened,
	GitHubIssueUnassigned,
	GitHubIssueUnlabeled,
	type GitHubMentioned,
	GitHubPrAssigned,
	GitHubPrCheckCompleted,
	GitHubPrClosed,
	GitHubPrCommentCreated,
	GitHubPrCommentDeleted,
	GitHubPrCommentEdited,
	GitHubPrConvertedToDraft,
	GitHubPrCreated,
	type GitHubPrEvent,
	GitHubPrEdited,
	GitHubPrLabeled,
	GitHubPrMentioned,
	GitHubPrMerged,
	GitHubPrOpened,
	GitHubPrReadyForReview,
	GitHubPrReopened,
	GitHubPrReviewCommentCreated,
	GitHubPrReviewCommentDeleted,
	GitHubPrReviewCommentEdited,
	GitHubPrReviewDismissed,
	GitHubPrReviewEdited,
	GitHubPrReviewRequestRemoved,
	GitHubPrReviewRequested,
	GitHubPrReviewSubmitted,
	GitHubPrReviewThreadResolved,
	GitHubPrReviewThreadUnresolved,
	GitHubPrSynchronized,
	GitHubPrUnassigned,
	GitHubPrUnlabeled,
	GitHubSubscribedIssueEvents,
	GitHubSubscribedPrEvents,
} from './GitHubCallbackEvents'
import {
	type GitHubCallbackError,
	GitHubCallbackName,
	type GitHubCallbackOperations,
	GitHubCallbacks,
} from './GitHubCallbacks'
import {
	GitHubActivationTarget,
	GitHubActivationTargetJson,
	GitHubDeliveryDestination,
	GitHubDeliveryDestinationJson,
	gitHubReactionTargets,
	gitHubSupportedOperations,
	gitHubPresentationVersion,
} from './GitHubDeliveryDestination'
import { githubDiscussionResourceId, GitHubId } from './GitHubIdentity'
import {
	GitHubDiscussionRef,
	GitHubEventId,
	GitHubIssueCommentRef,
	GitHubIssueRef,
	GitHubLabel,
	GitHubParticipant,
	GitHubPullRequestRef,
	GitHubReview,
	GitHubReviewCommentRef,
	GitHubReviewRef,
	GitHubReviewThread,
	GitHubTeam,
} from './GitHubModels'
import { GitHubIssue, GitHubIssueComment, GitHubPullRequest } from './GitHubResources'
import type {
	GitHubIssueCommentWebhook,
	GitHubIssuesWebhook,
	GitHubPullRequestReviewCommentWebhook,
	GitHubPullRequestReviewThreadWebhook,
	GitHubPullRequestReviewWebhook,
	GitHubPullRequestWebhook,
	GitHubSupportedWebhook as GitHubSupportedWebhookType,
} from './GitHubWebhookSchemas'
import { GitHubSupportedWebhook } from './GitHubWebhookSchemas'

export const GitHubBotConfiguration = Schema.Struct({
	mentionNames: Schema.Array(Schema.NonEmptyString),
	botUserId: GitHubId,
})
export interface GitHubBotConfiguration extends Schema.Schema.Type<typeof GitHubBotConfiguration> {}

export type GitHubEventProcessorOptions = {
	readonly namespace: string
	readonly bot: GitHubBotConfiguration
}

const GitHubResourceAddress = Schema.Struct({
	repositoryId: GitHubId,
	kind: Schema.Literals(['issue', 'pull-request']),
	number: GitHubId,
})
type GitHubResourceAddress = typeof GitHubResourceAddress.Type

type NormalizedIssueEvent = {
	readonly kind: 'issue'
	readonly event: GitHubIssueOpened | GitHubIssueEvent
	readonly mentionsBot: boolean
}

type NormalizedPrEvent = {
	readonly kind: 'pull-request'
	readonly event: GitHubPrOpened | GitHubPrEvent
	readonly mentionsBot: boolean
}

type NormalizedEvent = NormalizedIssueEvent | NormalizedPrEvent

const invalidPayload = () => ProviderEventInvalid.make({ provider: 'github', reason: 'invalid_payload' })
const identityMismatch = () => ProviderEventInvalid.make({ provider: 'github', reason: 'identity_mismatch' })
const providerFailure = (safeCode: string) =>
	ProviderEventExecutionFailed.make({ provider: 'github', retryable: true, safeCode })
const nonRetryableFailure = (safeCode: string) =>
	ProviderEventExecutionFailed.make({ provider: 'github', retryable: false, safeCode })

const decodeGitHubWebhook = (admission: DeliveryAdmission) =>
	Schema.decodeUnknownEffect(GitHubSupportedWebhook)(admission.payload).pipe(
		Effect.tapError((error) => Effect.logError('Stored GitHub webhook could not be decoded', error)),
		Effect.mapError(invalidPayload),
	)

const parseResourceAddress = (resourceId: string) =>
	Effect.gen(function* () {
		const parts = resourceId.split(':')
		if (parts.length !== 5 || parts[0] !== 'github' || parts[1] !== 'v1') return yield* identityMismatch()
		const repository = parts[2]
		const kind = parts[3]
		const number = parts[4]
		if (repository === undefined || kind === undefined || number === undefined) return yield* identityMismatch()
		return yield* Schema.decodeUnknownEffect(GitHubResourceAddress)({
			repositoryId: Number(repository),
			kind,
			number: Number(number),
		}).pipe(Effect.mapError(identityMismatch))
	})

const participant = (user: { readonly id: GitHubId; readonly login: string; readonly type?: string }) =>
	GitHubParticipant.make({ id: user.id, login: user.login, type: user.type ?? 'User' })

const label = (value: {
	readonly id?: GitHubId
	readonly name: string
	readonly color: string
	readonly description?: string | null
}) => GitHubLabel.make({ ...value, description: value.description ?? null })

const team = (value: { readonly id: GitHubId; readonly name: string; readonly slug: string }) => GitHubTeam.make(value)

const issueRef = (payload: GitHubIssuesWebhook | GitHubIssueCommentWebhook) =>
	GitHubIssueRef.make({
		installationId: payload.installation.id,
		repositoryId: payload.repository.id,
		owner: payload.repository.owner.login,
		repository: payload.repository.name,
		number: payload.issue.number,
	})

const pullRequestRef = (
	payload:
		| GitHubPullRequestWebhook
		| GitHubPullRequestReviewWebhook
		| GitHubPullRequestReviewCommentWebhook
		| GitHubPullRequestReviewThreadWebhook,
) =>
	GitHubPullRequestRef.make({
		installationId: payload.installation.id,
		repositoryId: payload.repository.id,
		owner: payload.repository.owner.login,
		repository: payload.repository.name,
		number: payload.pull_request.number,
	})

const webhookAddress = (
	webhook: GitHubSupportedWebhookType,
	mailboxAddress: GitHubResourceAddress,
): GitHubResourceAddress =>
	Match.value(webhook).pipe(
		Match.when({ event: 'issues' }, ({ payload }) => ({
			repositoryId: payload.repository.id,
			kind: 'issue' as const,
			number: payload.issue.number,
		})),
		Match.when({ event: 'issue_comment' }, ({ payload }) => ({
			repositoryId: payload.repository.id,
			kind: payload.issue.pull_request === undefined ? ('issue' as const) : ('pull-request' as const),
			number: payload.issue.number,
		})),
		Match.when({ event: 'check_run' }, ({ payload }) => ({
			repositoryId: payload.repository.id,
			kind: 'pull-request' as const,
			number: mailboxAddress.number,
		})),
		Match.orElse(({ payload }) => ({
			repositoryId: payload.repository.id,
			kind: 'pull-request' as const,
			number: payload.pull_request.number,
		})),
	)

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

const mentionsBot = (body: string | null, mentionNames: ReadonlyArray<string>): boolean => {
	if (body === null) return false
	return mentionNames.some((name) =>
		new RegExp(`(^|[^A-Za-z0-9_])@${escapeRegExp(name)}(?![A-Za-z0-9_-])`, 'i').test(body),
	)
}

const makeIssueComment = (payload: GitHubIssueCommentWebhook, discussion: GitHubDiscussionRef) =>
	GitHubIssueComment.make({
		ref: GitHubIssueCommentRef.make({
			discussion,
			id: payload.comment.id,
		}),
		body: payload.comment.body,
		url: payload.comment.html_url,
		author: payload.comment.user === null ? null : participant(payload.comment.user),
	})

const normalizeReviewState = (state: GitHubPullRequestReviewWebhook['review']['state']) =>
	Match.value(state).pipe(
		Match.when('approved', () => 'approved' as const),
		Match.when('APPROVED', () => 'approved' as const),
		Match.when('changes_requested', () => 'changes_requested' as const),
		Match.when('CHANGES_REQUESTED', () => 'changes_requested' as const),
		Match.when('commented', () => 'commented' as const),
		Match.when('COMMENTED', () => 'commented' as const),
		Match.when('dismissed', () => 'dismissed' as const),
		Match.when('DISMISSED', () => 'dismissed' as const),
		Match.when('pending', () => 'pending' as const),
		Match.when('PENDING', () => 'pending' as const),
		Match.exhaustive,
	)

const makeReview = (payload: GitHubPullRequestReviewWebhook, ref: GitHubPullRequestRef) =>
	GitHubReview.make({
		ref: GitHubReviewRef.make({ pullRequest: ref, id: payload.review.id, nodeId: payload.review.node_id }),
		body: payload.review.body,
		author: payload.review.user === null ? null : participant(payload.review.user),
		state: normalizeReviewState(payload.review.state),
		commitId: payload.review.commit_id,
		url: payload.review.html_url,
	})

const normalizeWebhook = (
	webhook: GitHubSupportedWebhookType,
	admission: DeliveryAdmission,
	address: GitHubResourceAddress,
	mailboxKey: string,
	bot: GitHubBotConfiguration,
): NormalizedEvent | null => {
	const eventId = GitHubEventId.make(admission.eventId)
	return Match.value(webhook).pipe(
		Match.when({ event: 'issues' }, ({ payload }): NormalizedEvent | null => {
			if (payload.sender.id === bot.botUserId) return null
			const ref = issueRef(payload)
			const issue = GitHubIssue.make({ ref, mailboxKey })
			const actor = participant(payload.sender)
			const event = Match.value(payload.action).pipe(
				Match.when('opened', () =>
					GitHubIssueOpened.make({
						eventId,
						issue,
						actor,
						title: payload.issue.title,
						body: payload.issue.body,
					}),
				),
				Match.when('edited', () => GitHubIssueEdited.make({ eventId, issue, actor })),
				Match.when('closed', () => GitHubIssueClosed.make({ eventId, issue, actor })),
				Match.when('reopened', () => GitHubIssueReopened.make({ eventId, issue, actor })),
				Match.when('assigned', () =>
					GitHubIssueAssigned.make({
						eventId,
						issue,
						actor,
						assignee:
							payload.assignee === undefined || payload.assignee === null
								? null
								: participant(payload.assignee),
					}),
				),
				Match.when('unassigned', () =>
					GitHubIssueUnassigned.make({
						eventId,
						issue,
						actor,
						assignee:
							payload.assignee === undefined || payload.assignee === null
								? null
								: participant(payload.assignee),
					}),
				),
				Match.when('labeled', () =>
					GitHubIssueLabeled.make({
						eventId,
						issue,
						actor,
						label: payload.label === undefined || payload.label === null ? null : label(payload.label),
					}),
				),
				Match.when('unlabeled', () =>
					GitHubIssueUnlabeled.make({
						eventId,
						issue,
						actor,
						label: payload.label === undefined || payload.label === null ? null : label(payload.label),
					}),
				),
				Match.exhaustive,
			)
			return {
				kind: 'issue',
				event,
				mentionsBot: Schema.is(GitHubIssueOpened)(event) && mentionsBot(event.body, bot.mentionNames),
			}
		}),
		Match.when({ event: 'issue_comment' }, ({ payload }): NormalizedEvent | null => {
			if (payload.sender.id === bot.botUserId || payload.comment.user?.id === bot.botUserId) return null
			const actor = participant(payload.sender)
			if (address.kind === 'issue') {
				const ref = issueRef(payload)
				const issue = GitHubIssue.make({ ref, mailboxKey })
				const comment = makeIssueComment(payload, GitHubDiscussionRef.cases.Issue.make({ ref }))
				const event = Match.value(payload.action).pipe(
					Match.when('created', () => GitHubIssueCommentCreated.make({ eventId, issue, actor, comment })),
					Match.when('edited', () => GitHubIssueCommentEdited.make({ eventId, issue, actor, comment })),
					Match.when('deleted', () => GitHubIssueCommentDeleted.make({ eventId, issue, actor, comment })),
					Match.exhaustive,
				)
				return {
					kind: 'issue',
					event,
					mentionsBot: payload.action === 'created' && mentionsBot(comment.body, bot.mentionNames),
				}
			}
			const ref = GitHubPullRequestRef.make({ ...issueRef(payload) })
			const pullRequest = GitHubPullRequest.make({ ref, mailboxKey })
			const comment = makeIssueComment(payload, GitHubDiscussionRef.cases.PullRequest.make({ ref }))
			const event = Match.value(payload.action).pipe(
				Match.when('created', () => GitHubPrCommentCreated.make({ eventId, pullRequest, actor, comment })),
				Match.when('edited', () => GitHubPrCommentEdited.make({ eventId, pullRequest, actor, comment })),
				Match.when('deleted', () => GitHubPrCommentDeleted.make({ eventId, pullRequest, actor, comment })),
				Match.exhaustive,
			)
			return {
				kind: 'pull-request',
				event,
				mentionsBot: payload.action === 'created' && mentionsBot(comment.body, bot.mentionNames),
			}
		}),
		Match.when({ event: 'pull_request' }, ({ payload }): NormalizedEvent | null => {
			if (payload.sender.id === bot.botUserId) return null
			const ref = pullRequestRef(payload)
			const pullRequest = GitHubPullRequest.make({ ref, mailboxKey })
			const actor = participant(payload.sender)
			const event = Match.value(payload.action).pipe(
				Match.when('opened', () =>
					GitHubPrOpened.make({
						eventId,
						pullRequest,
						actor,
						title: payload.pull_request.title,
						body: payload.pull_request.body,
					}),
				),
				Match.when('edited', () => GitHubPrEdited.make({ eventId, pullRequest, actor })),
				Match.when('closed', () =>
					payload.pull_request.merged === true
						? GitHubPrMerged.make({ eventId, pullRequest, actor })
						: GitHubPrClosed.make({ eventId, pullRequest, actor }),
				),
				Match.when('reopened', () => GitHubPrReopened.make({ eventId, pullRequest, actor })),
				Match.when('synchronize', () => GitHubPrSynchronized.make({ eventId, pullRequest, actor })),
				Match.when('review_requested', () =>
					GitHubPrReviewRequested.make({
						eventId,
						pullRequest,
						actor,
						reviewer:
							payload.requested_reviewer === undefined || payload.requested_reviewer === null
								? null
								: participant(payload.requested_reviewer),
						team:
							payload.requested_team === undefined || payload.requested_team === null
								? null
								: team(payload.requested_team),
					}),
				),
				Match.when('review_request_removed', () =>
					GitHubPrReviewRequestRemoved.make({
						eventId,
						pullRequest,
						actor,
						reviewer:
							payload.requested_reviewer === undefined || payload.requested_reviewer === null
								? null
								: participant(payload.requested_reviewer),
						team:
							payload.requested_team === undefined || payload.requested_team === null
								? null
								: team(payload.requested_team),
					}),
				),
				Match.when('assigned', () =>
					GitHubPrAssigned.make({
						eventId,
						pullRequest,
						actor,
						assignee:
							payload.assignee === undefined || payload.assignee === null
								? null
								: participant(payload.assignee),
					}),
				),
				Match.when('unassigned', () =>
					GitHubPrUnassigned.make({
						eventId,
						pullRequest,
						actor,
						assignee:
							payload.assignee === undefined || payload.assignee === null
								? null
								: participant(payload.assignee),
					}),
				),
				Match.when('labeled', () =>
					GitHubPrLabeled.make({
						eventId,
						pullRequest,
						actor,
						label: payload.label === undefined || payload.label === null ? null : label(payload.label),
					}),
				),
				Match.when('unlabeled', () =>
					GitHubPrUnlabeled.make({
						eventId,
						pullRequest,
						actor,
						label: payload.label === undefined || payload.label === null ? null : label(payload.label),
					}),
				),
				Match.when('converted_to_draft', () => GitHubPrConvertedToDraft.make({ eventId, pullRequest, actor })),
				Match.when('ready_for_review', () => GitHubPrReadyForReview.make({ eventId, pullRequest, actor })),
				Match.exhaustive,
			)
			return {
				kind: 'pull-request',
				event,
				mentionsBot: Schema.is(GitHubPrOpened)(event) && mentionsBot(event.body, bot.mentionNames),
			}
		}),
		Match.when({ event: 'pull_request_review' }, ({ payload }): NormalizedEvent | null => {
			if (payload.sender.id === bot.botUserId) return null
			const ref = pullRequestRef(payload)
			const pullRequest = GitHubPullRequest.make({ ref, mailboxKey })
			const actor = participant(payload.sender)
			const review = makeReview(payload, ref)
			const event = Match.value(payload.action).pipe(
				Match.when('submitted', () => GitHubPrReviewSubmitted.make({ eventId, pullRequest, actor, review })),
				Match.when('edited', () => GitHubPrReviewEdited.make({ eventId, pullRequest, actor, review })),
				Match.when('dismissed', () => GitHubPrReviewDismissed.make({ eventId, pullRequest, actor, review })),
				Match.exhaustive,
			)
			return { kind: 'pull-request', event, mentionsBot: false }
		}),
		Match.when({ event: 'pull_request_review_comment' }, ({ payload }): NormalizedEvent | null => {
			if (payload.sender.id === bot.botUserId || payload.comment.user?.id === bot.botUserId) return null
			const ref = pullRequestRef(payload)
			const pullRequest = GitHubPullRequest.make({ ref, mailboxKey })
			const actor = participant(payload.sender)
			const comment = reviewComment(ref, payload.comment)
			const event = Match.value(payload.action).pipe(
				Match.when('created', () =>
					GitHubPrReviewCommentCreated.make({ eventId, pullRequest, actor, comment }),
				),
				Match.when('edited', () => GitHubPrReviewCommentEdited.make({ eventId, pullRequest, actor, comment })),
				Match.when('deleted', () =>
					GitHubPrReviewCommentDeleted.make({ eventId, pullRequest, actor, comment }),
				),
				Match.exhaustive,
			)
			return {
				kind: 'pull-request',
				event,
				mentionsBot: payload.action === 'created' && mentionsBot(comment.body, bot.mentionNames),
			}
		}),
		Match.when({ event: 'pull_request_review_thread' }, ({ payload }): NormalizedEvent | null => {
			if (payload.sender?.id === bot.botUserId) return null
			const ref = pullRequestRef(payload)
			const pullRequest = GitHubPullRequest.make({ ref, mailboxKey })
			const actor = participant(
				payload.sender ??
					payload.pull_request.user ??
					payload.thread.comments[0]?.user ?? { id: bot.botUserId, login: 'github', type: 'Bot' },
			)
			const thread = GitHubReviewThread.make({
				nodeId: payload.thread.node_id,
				comments: payload.thread.comments.map((comment) =>
					GitHubReviewCommentRef.make({ pullRequest: ref, id: comment.id }),
				),
			})
			const event = Match.value(payload.action).pipe(
				Match.when('resolved', () =>
					GitHubPrReviewThreadResolved.make({ eventId, pullRequest, actor, thread }),
				),
				Match.when('unresolved', () =>
					GitHubPrReviewThreadUnresolved.make({ eventId, pullRequest, actor, thread }),
				),
				Match.exhaustive,
			)
			return { kind: 'pull-request', event, mentionsBot: false }
		}),
		Match.when({ event: 'check_run' }, ({ payload }): NormalizedEvent | null => {
			const association = payload.check_run.pull_requests.find(({ number }) => number === address.number)
			if (association === undefined) return null
			const ref = GitHubPullRequestRef.make({
				installationId: payload.installation.id,
				repositoryId: payload.repository.id,
				owner: payload.repository.owner.login,
				repository: payload.repository.name,
				number: association.number,
			})
			const pullRequest = GitHubPullRequest.make({ ref, mailboxKey })
			return {
				kind: 'pull-request',
				mentionsBot: false,
				event: GitHubPrCheckCompleted.make({
					eventId,
					pullRequest,
					actor: participant(payload.sender),
					checkRunId: payload.check_run.id,
					name: payload.check_run.name,
					status: payload.check_run.status,
					conclusion: payload.check_run.conclusion,
					detailsUrl: payload.check_run.details_url,
					headSha: payload.check_run.head_sha,
					checkSuiteId: payload.check_run.check_suite?.id ?? null,
					startedAt: payload.check_run.started_at,
					completedAt: payload.check_run.completed_at,
				}),
			}
		}),
		Match.exhaustive,
	)
}

const runCallback = <R>(effect: Effect.Effect<DeliveryCallbackResult, { readonly retryable: boolean }, R>) =>
	effect.pipe(
		Effect.mapError((error) =>
			ProviderEventExecutionFailed.make({
				provider: 'github',
				retryable: error.retryable,
				safeCode: 'callback_failed',
			}),
		),
		Effect.as(ProviderEventHandled.make({})),
	)

const isIssueMentionTrigger = Schema.is(Schema.Union([GitHubIssueOpened, GitHubIssueCommentCreated]))
const isPrMentionTrigger = Schema.is(
	Schema.Union([GitHubPrOpened, GitHubPrCommentCreated, GitHubPrReviewCommentCreated]),
)

const decodeGitHubBatch = Effect.fn('github.decode_event_batch')(function* (
	options: GitHubEventProcessorOptions,
	admissions: DeliveryAdmissionBatch,
) {
	const first = admissions[0]
	const address = yield* parseResourceAddress(first.resourceId)
	const webhooks = yield* Effect.forEach(admissions, decodeGitHubWebhook)

	for (let index = 0; index < admissions.length; index += 1) {
		const admission = admissions[index]
		const webhook = webhooks[index]
		if (admission === undefined || webhook === undefined) return yield* identityMismatch()
		if (
			webhook.event === 'check_run' &&
			!webhook.payload.check_run.pull_requests.some(({ number }) => number === address.number)
		) {
			return yield* invalidPayload()
		}
		const actual = webhookAddress(webhook, address)
		if (
			admission.namespace !== options.namespace ||
			admission.provider !== 'github' ||
			admission.installationId !== first.installationId ||
			admission.resourceId !== first.resourceId ||
			githubDiscussionResourceId(address) !== admission.resourceId ||
			admission.installationId !== String(webhook.payload.installation.id) ||
			actual.repositoryId !== address.repositoryId ||
			actual.kind !== address.kind ||
			actual.number !== address.number
		)
			return yield* identityMismatch()
	}
	return { first, address, webhooks }
})

/** The normalized events of one batch, which is always about one issue or one pull request. */
type GitHubBatchEvents = Data.TaggedEnum<{
	Issue: { readonly events: ReadonlyArray<NormalizedIssueEvent> }
	PullRequest: { readonly events: ReadonlyArray<NormalizedPrEvent> }
}>
const GitHubBatchEvents = Data.taggedEnum<GitHubBatchEvents>()

/** The callback a batch runs, with the event value it receives. */
type GitHubInvocation = Data.TaggedEnum<{
	IssueCreated: { readonly event: GitHubIssueCreated }
	PrCreated: { readonly event: GitHubPrCreated }
	Mentioned: { readonly event: GitHubMentioned }
	SubscribedIssueEvents: { readonly event: GitHubSubscribedIssueEvents }
	SubscribedPrEvents: { readonly event: GitHubSubscribedPrEvents }
}>
const GitHubInvocation = Data.taggedEnum<GitHubInvocation>()

const invocationCallbackName = GitHubInvocation.$match({
	IssueCreated: (): GitHubCallbackName => 'onIssueCreated',
	PrCreated: (): GitHubCallbackName => 'onPrCreated',
	Mentioned: (): GitHubCallbackName => 'onMentioned',
	SubscribedIssueEvents: (): GitHubCallbackName => 'onSubscribedIssueEvents',
	SubscribedPrEvents: (): GitHubCallbackName => 'onSubscribedPrEvents',
})

const isIssueOpened = Schema.is(GitHubIssueOpened)
const isPrOpened = Schema.is(GitHubPrOpened)

const buildSubscribedIssueEvents = (issueEvents: ReadonlyArray<NormalizedIssueEvent>) => {
	const issue = issueEvents[0]?.event.issue
	const events = issueEvents
		.map(({ event }) => event)
		.filter((event): event is GitHubIssueEvent => !isIssueOpened(event))
	const firstEvent = events[0]
	if (issue === undefined || firstEvent === undefined) return Option.none<GitHubInvocation>()
	return Option.some(
		GitHubInvocation.SubscribedIssueEvents({
			event: GitHubSubscribedIssueEvents.make({ issue, events: [firstEvent, ...events.slice(1)] }),
		}),
	)
}

/**
 * A mention starts at the mentioning event, or at the opening event before it when `onIssueCreated` is
 * configured, so the callback sees how the issue began.
 */
const buildIssueMention = (issueEvents: ReadonlyArray<NormalizedIssueEvent>, includeOpened: boolean) => {
	const issue = issueEvents[0]?.event.issue
	const mentionIndex = issueEvents.findIndex(({ mentionsBot: mentioned }) => mentioned)
	const trigger = issueEvents[mentionIndex]
	if (issue === undefined || trigger === undefined || !isIssueMentionTrigger(trigger.event)) {
		return Option.none<GitHubInvocation>()
	}
	const openedIndex = includeOpened ? issueEvents.findIndex(({ event }) => isIssueOpened(event)) : -1
	const start = openedIndex >= 0 ? Math.min(openedIndex, mentionIndex) : mentionIndex
	const events = issueEvents
		.slice(start)
		.filter((_, index) => start + index !== mentionIndex)
		.map(({ event }) => event)
	return Option.some(
		GitHubInvocation.Mentioned({ event: GitHubIssueMentioned.make({ issue, trigger: trigger.event, events }) }),
	)
}

const buildIssueCreated = (issueEvents: ReadonlyArray<NormalizedIssueEvent>) => {
	const issue = issueEvents[0]?.event.issue
	const openedIndex = issueEvents.findIndex(({ event }) => isIssueOpened(event))
	const trigger = issueEvents[openedIndex]
	if (issue === undefined || trigger === undefined || !isIssueOpened(trigger.event)) {
		return Option.none<GitHubInvocation>()
	}
	const events = issueEvents
		.slice(openedIndex + 1)
		.map(({ event }) => event)
		.filter((event): event is GitHubIssueEvent => !isIssueOpened(event))
	return Option.some(
		GitHubInvocation.IssueCreated({ event: GitHubIssueCreated.make({ issue, trigger: trigger.event, events }) }),
	)
}

const buildSubscribedPrEvents = (prEvents: ReadonlyArray<NormalizedPrEvent>) => {
	const pullRequest = prEvents[0]?.event.pullRequest
	const events = prEvents.map(({ event }) => event).filter((event): event is GitHubPrEvent => !isPrOpened(event))
	const firstEvent = events[0]
	if (pullRequest === undefined || firstEvent === undefined) return Option.none<GitHubInvocation>()
	return Option.some(
		GitHubInvocation.SubscribedPrEvents({
			event: GitHubSubscribedPrEvents.make({ pullRequest, events: [firstEvent, ...events.slice(1)] }),
		}),
	)
}

/** Like {@link buildIssueMention}, for a pull request and `onPrCreated`. */
const buildPrMention = (prEvents: ReadonlyArray<NormalizedPrEvent>, includeOpened: boolean) => {
	const pullRequest = prEvents[0]?.event.pullRequest
	const mentionIndex = prEvents.findIndex(({ mentionsBot: mentioned }) => mentioned)
	const trigger = prEvents[mentionIndex]
	if (pullRequest === undefined || trigger === undefined || !isPrMentionTrigger(trigger.event)) {
		return Option.none<GitHubInvocation>()
	}
	const openedIndex = includeOpened ? prEvents.findIndex(({ event }) => isPrOpened(event)) : -1
	const start = openedIndex >= 0 ? Math.min(openedIndex, mentionIndex) : mentionIndex
	const events = prEvents
		.slice(start)
		.filter((_, index) => start + index !== mentionIndex)
		.map(({ event }) => event)
	return Option.some(
		GitHubInvocation.Mentioned({
			event: GitHubPrMentioned.make({ pullRequest, trigger: trigger.event, events }),
		}),
	)
}

const buildPrCreated = (prEvents: ReadonlyArray<NormalizedPrEvent>) => {
	const pullRequest = prEvents[0]?.event.pullRequest
	const openedIndex = prEvents.findIndex(({ event }) => isPrOpened(event))
	const trigger = prEvents[openedIndex]
	if (pullRequest === undefined || trigger === undefined || !isPrOpened(trigger.event)) {
		return Option.none<GitHubInvocation>()
	}
	const events = prEvents
		.slice(openedIndex + 1)
		.map(({ event }) => event)
		.filter((event): event is GitHubPrEvent => !isPrOpened(event))
	return Option.some(
		GitHubInvocation.PrCreated({ event: GitHubPrCreated.make({ pullRequest, trigger: trigger.event, events }) }),
	)
}

/** Rebuild the event value for a callback an earlier attempt chose. */
const buildInvocation = (
	callback: GitHubCallbackName,
	batch: GitHubBatchEvents,
	includeOpened: boolean,
): Option.Option<GitHubInvocation> =>
	GitHubBatchEvents.$match(batch, {
		Issue: ({ events }) =>
			Match.value(callback).pipe(
				Match.when('onIssueCreated', () => buildIssueCreated(events)),
				Match.when('onMentioned', () => buildIssueMention(events, includeOpened)),
				Match.when('onSubscribedIssueEvents', () => buildSubscribedIssueEvents(events)),
				Match.whenOr('onPrCreated', 'onSubscribedPrEvents', () => Option.none<GitHubInvocation>()),
				Match.exhaustive,
			),
		PullRequest: ({ events }) =>
			Match.value(callback).pipe(
				Match.when('onPrCreated', () => buildPrCreated(events)),
				Match.when('onMentioned', () => buildPrMention(events, includeOpened)),
				Match.when('onSubscribedPrEvents', () => buildSubscribedPrEvents(events)),
				Match.whenOr('onIssueCreated', 'onSubscribedIssueEvents', () => Option.none<GitHubInvocation>()),
				Match.exhaustive,
			),
	})

/** The ordered callbacks a new batch runs, or why it runs none. */
type CallbackSelection = Data.TaggedEnum<{
	Selected: { readonly invocations: readonly [GitHubInvocation, ...Array<GitHubInvocation>] }
	Ignored: { readonly reason: string }
}>
const CallbackSelection = Data.taggedEnum<CallbackSelection>()

const ignored = (reason: string) => CallbackSelection.Ignored({ reason })

const fromBuilt = (invocations: ReadonlyArray<GitHubInvocation>, reason: string) =>
	Arr.isReadonlyArrayNonEmpty(invocations) ? CallbackSelection.Selected({ invocations }) : ignored(reason)

/**
 * Freeze creation first, then the mention or subscription route. A batch that mentions the bot runs
 * `onMentioned`, even in a subscribed discussion, so every mention gets the same access check and activity;
 * other batches in a subscribed discussion run its subscribed-events callback. Subscription changes made
 * by creation cannot change this batch's continuation. Unconfigured callbacks are skipped.
 */
const selectInvocation = <R>(input: {
	readonly callbacks: GitHubCallbackOperations<R>
	readonly batch: GitHubBatchEvents
	readonly subscribed: boolean
}): CallbackSelection => {
	const { callbacks, subscribed } = input
	return GitHubBatchEvents.$match(input.batch, {
		Issue: ({ events }) => {
			if (Arr.isReadonlyArrayEmpty(events)) return ignored('no_relevant_event')
			const mentioned = events.some(({ mentionsBot: mention }) => mention)
			const opened = events.some(({ event }) => isIssueOpened(event))
			const creation = Predicate.isNotUndefined(callbacks.onIssueCreated)
				? Option.toArray(buildIssueCreated(events))
				: []
			const continuation =
				mentioned && Predicate.isNotUndefined(callbacks.onMentioned)
					? Option.toArray(buildIssueMention(events, creation.length > 0))
					: subscribed && Predicate.isNotUndefined(callbacks.onSubscribedIssueEvents)
						? Option.toArray(buildSubscribedIssueEvents(events))
						: []
			return fromBuilt(
				[...creation, ...continuation],
				subscribed && !mentioned
					? 'no_relevant_event'
					: !opened && !mentioned
						? 'no_activation_event'
						: 'callback_not_configured',
			)
		},
		PullRequest: ({ events }) => {
			if (Arr.isReadonlyArrayEmpty(events)) return ignored('no_relevant_event')
			const mentioned = events.some(({ mentionsBot: mention }) => mention)
			const opened = events.some(({ event }) => isPrOpened(event))
			const creation = Predicate.isNotUndefined(callbacks.onPrCreated)
				? Option.toArray(buildPrCreated(events))
				: []
			const continuation =
				mentioned && Predicate.isNotUndefined(callbacks.onMentioned)
					? Option.toArray(buildPrMention(events, creation.length > 0))
					: subscribed && Predicate.isNotUndefined(callbacks.onSubscribedPrEvents)
						? Option.toArray(buildSubscribedPrEvents(events))
						: []
			return fromBuilt(
				[...creation, ...continuation],
				subscribed && !mentioned
					? 'no_relevant_event'
					: !opened && !mentioned
						? 'no_activation_event'
						: 'callback_not_configured',
			)
		},
	})
}

const isCallbackConfigured = <R>(callbacks: GitHubCallbackOperations<R>, callback: GitHubCallbackName) =>
	Predicate.isNotUndefined(callbacks[callback])

const invocationDestination = GitHubInvocation.$match({
	IssueCreated: ({ event }) => GitHubDeliveryDestination.cases.GitHubIssue.make({ issue: event.issue.ref }),
	PrCreated: ({ event }) =>
		GitHubDeliveryDestination.cases.GitHubPullRequest.make({ pullRequest: event.pullRequest.ref }),
	Mentioned: ({ event }) =>
		Match.value(event).pipe(
			Match.tagsExhaustive({
				GitHubIssueMentioned: ({ issue }) =>
					GitHubDeliveryDestination.cases.GitHubIssue.make({ issue: issue.ref }),
				GitHubPrMentioned: ({ pullRequest }) =>
					GitHubDeliveryDestination.cases.GitHubPullRequest.make({ pullRequest: pullRequest.ref }),
			}),
		),
	SubscribedIssueEvents: ({ event }) => GitHubDeliveryDestination.cases.GitHubIssue.make({ issue: event.issue.ref }),
	SubscribedPrEvents: ({ event }) =>
		GitHubDeliveryDestination.cases.GitHubPullRequest.make({ pullRequest: event.pullRequest.ref }),
})

const invocationActivationTarget = GitHubInvocation.$match({
	IssueCreated: ({ event }) => Option.some(GitHubActivationTarget.cases.GitHubIssue.make({ issue: event.issue.ref })),
	PrCreated: ({ event }) =>
		Option.some(GitHubActivationTarget.cases.GitHubPullRequest.make({ pullRequest: event.pullRequest.ref })),
	Mentioned: ({ event }) =>
		Option.some(
			Match.value(event.trigger).pipe(
				Match.tagsExhaustive({
					GitHubIssueOpened: ({ issue }) =>
						GitHubActivationTarget.cases.GitHubIssue.make({ issue: issue.ref }),
					GitHubIssueCommentCreated: ({ comment }) =>
						GitHubActivationTarget.cases.GitHubIssueComment.make({ comment: comment.ref }),
					GitHubPrOpened: ({ pullRequest }) =>
						GitHubActivationTarget.cases.GitHubPullRequest.make({ pullRequest: pullRequest.ref }),
					GitHubPrCommentCreated: ({ comment }) =>
						GitHubActivationTarget.cases.GitHubIssueComment.make({ comment: comment.ref }),
					GitHubPrReviewCommentCreated: ({ comment }) =>
						GitHubActivationTarget.cases.GitHubReviewComment.make({ comment: comment.ref }),
				}),
			),
		),
	SubscribedIssueEvents: () => Option.none<GitHubActivationTarget>(),
	SubscribedPrEvents: () => Option.none<GitHubActivationTarget>(),
})

/** The saved form of an invocation: callback name, issue or pull request, and what started it. */
const preparedInvocation = Effect.fn('github.prepared_invocation')(function* (invocation: GitHubInvocation) {
	const destination = yield* Schema.encodeEffect(GitHubDeliveryDestinationJson)(invocationDestination(invocation))
	const target = invocationActivationTarget(invocation)
	const activationTarget = yield* Effect.transposeOption(
		Option.map(target, Schema.encodeEffect(GitHubActivationTargetJson)),
	)
	return PreparedDeliveryCallback.make({
		name: invocationCallbackName(invocation),
		presentationVersion: gitHubPresentationVersion,
		destination,
		...Option.match(activationTarget, { onNone: () => ({}), onSome: (target) => ({ activationTarget: target }) }),
		supportedOperations: gitHubSupportedOperations(target),
		reactionTargets: gitHubReactionTargets(target),
	})
})

/** Save the callback choice before any application code runs, so every retry runs the same callback. */
const prepareInvocation = Effect.fn('github.prepare_delivery')(function* (
	execution: ProviderDeliveryExecution,
	invocations: readonly [GitHubInvocation, ...Array<GitHubInvocation>],
) {
	const steps = yield* Effect.forEach(invocations, preparedInvocation).pipe(
		Effect.tapError((error) => Effect.logError('GitHub delivery destination could not be encoded', error)),
		Effect.mapError(() => nonRetryableFailure('delivery_destination_unencodable')),
	)
	if (!Arr.isReadonlyArrayNonEmpty(steps)) return yield* nonRetryableFailure('prepared_callback_missing')
	return yield* execution.prepare(PreparedDeliveryInvocation.make({ callbacks: steps })).pipe(
		Effect.tapError((error) =>
			Effect.logError('GitHub delivery preparation failed', error).pipe(
				Effect.annotateLogs({ deliveryId: execution.deliveryId, callbacks: steps.map(({ name }) => name) }),
			),
		),
		Effect.catchTags({
			DeliveryPreparationUnavailable: () => Effect.fail(providerFailure('delivery_prepare_unavailable')),
			DeliveryPreparationConflict: () => Effect.fail(nonRetryableFailure('delivery_prepare_conflict')),
		}),
	)
})

const invokeCallback = <R>(
	callbacks: GitHubCallbackOperations<R>,
	invocation: GitHubInvocation,
	delivery: DeliveryContext,
): Option.Option<Effect.Effect<DeliveryCallbackResult, GitHubCallbackError, R>> =>
	GitHubInvocation.$match(invocation, {
		IssueCreated: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onIssueCreated), (callback) => callback(event, delivery)),
		PrCreated: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onPrCreated), (callback) => callback(event, delivery)),
		Mentioned: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onMentioned), (callback) => callback(event, delivery)),
		SubscribedIssueEvents: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onSubscribedIssueEvents), (callback) =>
				callback(event, delivery),
			),
		SubscribedPrEvents: ({ event }) =>
			Option.map(Option.fromUndefinedOr(callbacks.onSubscribedPrEvents), (callback) => callback(event, delivery)),
	})

const runInvocation = <R>(
	callbacks: GitHubCallbackOperations<R>,
	invocation: GitHubInvocation,
	delivery: DeliveryContext,
) =>
	Option.match(invokeCallback(callbacks, invocation, delivery), {
		onNone: () => Effect.fail(nonRetryableFailure('prepared_callback_missing')),
		onSome: runCallback,
	})

/** Run the callback an earlier attempt saved, without choosing again. */
const runPreparedInvocation = Effect.fn('github.run_prepared_invocation')(function* <R>(input: {
	readonly callbacks: GitHubCallbackOperations<R>
	readonly prepared: PreparedDeliveryInvocation
	readonly callbackIndex: number
	readonly batch: GitHubBatchEvents
	readonly delivery: DeliveryContext
}) {
	const step = input.prepared.callbacks[input.callbackIndex]
	if (Predicate.isUndefined(step)) return yield* nonRetryableFailure('prepared_callback_missing')
	const annotations = { deliveryId: input.delivery.deliveryId, callback: step.name }
	const callback = yield* Schema.decodeUnknownEffect(GitHubCallbackName)(step.name).pipe(
		Effect.tapError((error) =>
			Effect.logError('Prepared GitHub callback is unknown', error).pipe(Effect.annotateLogs(annotations)),
		),
		Effect.mapError(() => nonRetryableFailure('prepared_callback_missing')),
	)
	if (!isCallbackConfigured(input.callbacks, callback)) {
		yield* Effect.logError('Prepared GitHub callback is no longer configured').pipe(
			Effect.annotateLogs(annotations),
		)
		return yield* nonRetryableFailure('prepared_callback_missing')
	}
	const includeOpened = input.prepared.callbacks.some(
		({ name }) => name === 'onIssueCreated' || name === 'onPrCreated',
	)
	const invocation = buildInvocation(callback, input.batch, includeOpened)
	if (Option.isNone(invocation)) {
		yield* Effect.logError('Prepared GitHub callback cannot be rebuilt from its batch').pipe(
			Effect.annotateLogs(annotations),
		)
		return yield* nonRetryableFailure('prepared_callback_unbuildable')
	}
	return yield* runInvocation(input.callbacks, invocation.value, input.delivery)
})

const processGitHubBatch = <R>(options: GitHubEventProcessorOptions, callbacks: GitHubCallbackOperations<R>) =>
	Effect.fn('github.process_event_batch')(function* (
		admissions: DeliveryAdmissionBatch,
		execution: ProviderDeliveryExecution,
	) {
		yield* GitHubApi
		const { first, address, webhooks } = yield* decodeGitHubBatch(options, admissions)
		const mailboxKey = deliveryMailboxKey(first)
		const normalized = webhooks
			.map((webhook, index) => {
				const admission = admissions[index]
				return admission === undefined
					? null
					: normalizeWebhook(webhook, admission, address, mailboxKey, options.bot)
			})
			.filter(Predicate.isNotNullish)
		const batch =
			address.kind === 'issue'
				? GitHubBatchEvents.Issue({
						events: normalized.filter((entry): entry is NormalizedIssueEvent => entry.kind === 'issue'),
					})
				: GitHubBatchEvents.PullRequest({
						events: normalized.filter((entry): entry is NormalizedPrEvent => entry.kind === 'pull-request'),
					})

		if (Option.isSome(execution.prepared)) {
			return yield* runPreparedInvocation({
				callbacks,
				prepared: execution.prepared.value,
				callbackIndex: execution.callbackIndex,
				batch,
				delivery: execution.context,
			})
		}
		if (Arr.isReadonlyArrayEmpty(normalized)) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })

		const subscribed = yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey }),
		).pipe(
			Effect.tapError((error) => Effect.logError('GitHub subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)

		return yield* CallbackSelection.$match(selectInvocation({ callbacks, batch, subscribed }), {
			Ignored: ({ reason }) => Effect.succeed(ProviderEventIgnored.make({ reason })),
			Selected: ({ invocations }) =>
				prepareInvocation(execution, invocations).pipe(
					Effect.flatMap((prepared) =>
						runPreparedInvocation({
							callbacks,
							prepared,
							callbackIndex: execution.callbackIndex,
							batch,
							delivery: execution.context,
						}),
					),
				),
		})
	})

export function makeGitHubEventProcessor(
	options: GitHubEventProcessorOptions,
): ProviderEventProcessor<GitHubCallbacks | GitHubApi | MailboxSubscriptions>
export function makeGitHubEventProcessor<R>(
	options: GitHubEventProcessorOptions,
	callbacks: GitHubCallbackOperations<R>,
): ProviderEventProcessor<GitHubApi | MailboxSubscriptions | R>
export function makeGitHubEventProcessor<R>(
	options: GitHubEventProcessorOptions,
	callbacks?: GitHubCallbackOperations<R>,
) {
	return {
		namespace: options.namespace,
		providerName: 'github',
		process: Predicate.isUndefined(callbacks)
			? (admissions: DeliveryAdmissionBatch, execution: ProviderDeliveryExecution) =>
					Effect.flatMap(GitHubCallbacks, (service) =>
						processGitHubBatch(options, service)(admissions, execution),
					)
			: processGitHubBatch(options, callbacks),
	}
}
