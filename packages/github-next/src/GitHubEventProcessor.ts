/** GitHub post-admission batch identity validation, normalization, and callback routing. */
import {
	deliveryMailboxKey,
	type DeliveryAdmission,
	type DeliveryAdmissionBatch,
	MailboxSubscriptions,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
	type ProviderEventProcessor,
} from '@humanlayer/channels-delivery-next'
import { Array as Arr, Effect, Match, Predicate, Schema } from 'effect'

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
import { GitHubCallbacks } from './GitHubCallbacks'
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

const decodeGitHubWebhook = (admission: DeliveryAdmission) =>
	Schema.decodeUnknownEffect(GitHubSupportedWebhook)(admission.payload, { onExcessProperty: 'preserve' }).pipe(
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

const runCallback = (effect: Effect.Effect<void, { readonly retryable: boolean }>) =>
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

const dispatchIssueEvents = Effect.fn('github.dispatch_issue_events')(function* (
	issueEvents: ReadonlyArray<NormalizedIssueEvent>,
	subscribed: boolean,
) {
	const callbacks = yield* GitHubCallbacks
	const issue = issueEvents[0]?.event.issue
	if (issue === undefined) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })
	if (subscribed) {
		if (Predicate.isUndefined(callbacks.onSubscribedIssueEvents))
			return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
		const events = issueEvents
			.map(({ event }) => event)
			.filter((event): event is GitHubIssueEvent => !Schema.is(GitHubIssueOpened)(event))
		const firstEvent = events[0]
		if (firstEvent === undefined) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })
		return yield* runCallback(
			callbacks.onSubscribedIssueEvents(
				GitHubSubscribedIssueEvents.make({ issue, events: [firstEvent, ...events.slice(1)] }),
			),
		)
	}

	const actualMentionIndex = issueEvents.findIndex(({ mentionsBot: mentioned }) => mentioned)
	const actualOpenedIndex = issueEvents.findIndex(({ event }) => Schema.is(GitHubIssueOpened)(event))
	const mentionIndex = Predicate.isUndefined(callbacks.onMentioned) ? -1 : actualMentionIndex
	const openedIndex = Predicate.isUndefined(callbacks.onIssueCreated) ? -1 : actualOpenedIndex
	if (mentionIndex >= 0 && !Predicate.isUndefined(callbacks.onMentioned)) {
		const trigger = issueEvents[mentionIndex]
		if (trigger === undefined || !isIssueMentionTrigger(trigger.event))
			return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		const start = openedIndex >= 0 ? Math.min(openedIndex, mentionIndex) : mentionIndex
		const events = issueEvents
			.slice(start)
			.filter((_, index) => start + index !== mentionIndex)
			.map(({ event }) => event)
		return yield* runCallback(
			callbacks.onMentioned(GitHubIssueMentioned.make({ issue, trigger: trigger.event, events })),
		)
	}
	if (openedIndex >= 0 && !Predicate.isUndefined(callbacks.onIssueCreated)) {
		const trigger = issueEvents[openedIndex]
		if (trigger === undefined || !Schema.is(GitHubIssueOpened)(trigger.event))
			return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		const events = issueEvents
			.slice(openedIndex + 1)
			.map(({ event }) => event)
			.filter((event): event is GitHubIssueEvent => !Schema.is(GitHubIssueOpened)(event))
		return yield* runCallback(
			callbacks.onIssueCreated(GitHubIssueCreated.make({ issue, trigger: trigger.event, events })),
		)
	}
	return ProviderEventIgnored.make({
		reason: actualOpenedIndex < 0 && actualMentionIndex < 0 ? 'no_activation_event' : 'callback_not_configured',
	})
})

const dispatchPullRequestEvents = Effect.fn('github.dispatch_pull_request_events')(function* (
	prEvents: ReadonlyArray<NormalizedPrEvent>,
	subscribed: boolean,
) {
	const callbacks = yield* GitHubCallbacks
	const pullRequest = prEvents[0]?.event.pullRequest
	if (pullRequest === undefined) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })
	if (subscribed) {
		if (Predicate.isUndefined(callbacks.onSubscribedPrEvents))
			return ProviderEventIgnored.make({ reason: 'callback_not_configured' })
		const events = prEvents
			.map(({ event }) => event)
			.filter((event): event is GitHubPrEvent => !Schema.is(GitHubPrOpened)(event))
		const firstEvent = events[0]
		if (firstEvent === undefined) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })
		return yield* runCallback(
			callbacks.onSubscribedPrEvents(
				GitHubSubscribedPrEvents.make({ pullRequest, events: [firstEvent, ...events.slice(1)] }),
			),
		)
	}

	const actualMentionIndex = prEvents.findIndex(({ mentionsBot: mentioned }) => mentioned)
	const actualOpenedIndex = prEvents.findIndex(({ event }) => Schema.is(GitHubPrOpened)(event))
	const mentionIndex = Predicate.isUndefined(callbacks.onMentioned) ? -1 : actualMentionIndex
	const openedIndex = Predicate.isUndefined(callbacks.onPrCreated) ? -1 : actualOpenedIndex
	if (mentionIndex >= 0 && !Predicate.isUndefined(callbacks.onMentioned)) {
		const trigger = prEvents[mentionIndex]
		if (trigger === undefined || !isPrMentionTrigger(trigger.event))
			return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		const start = openedIndex >= 0 ? Math.min(openedIndex, mentionIndex) : mentionIndex
		const events = prEvents
			.slice(start)
			.filter((_, index) => start + index !== mentionIndex)
			.map(({ event }) => event)
		return yield* runCallback(
			callbacks.onMentioned(GitHubPrMentioned.make({ pullRequest, trigger: trigger.event, events })),
		)
	}
	if (openedIndex >= 0 && !Predicate.isUndefined(callbacks.onPrCreated)) {
		const trigger = prEvents[openedIndex]
		if (trigger === undefined || !Schema.is(GitHubPrOpened)(trigger.event))
			return ProviderEventIgnored.make({ reason: 'no_activation_event' })
		const events = prEvents
			.slice(openedIndex + 1)
			.map(({ event }) => event)
			.filter((event): event is GitHubPrEvent => !Schema.is(GitHubPrOpened)(event))
		return yield* runCallback(
			callbacks.onPrCreated(GitHubPrCreated.make({ pullRequest, trigger: trigger.event, events })),
		)
	}
	return ProviderEventIgnored.make({
		reason: actualOpenedIndex < 0 && actualMentionIndex < 0 ? 'no_activation_event' : 'callback_not_configured',
	})
})

const processGitHubBatch = (options: GitHubEventProcessorOptions) =>
	Effect.fn('github.process_event_batch')(function* (admissions: DeliveryAdmissionBatch) {
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
		if (Arr.isReadonlyArrayEmpty(normalized)) return ProviderEventIgnored.make({ reason: 'no_relevant_event' })

		const subscribed = yield* Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
			subscriptions.isSubscribed({ mailboxKey }),
		).pipe(
			Effect.tapError((error) => Effect.logError('GitHub subscription lookup failed', error)),
			Effect.mapError(() => providerFailure('subscription_lookup_failed')),
		)

		if (address.kind === 'issue') {
			return yield* dispatchIssueEvents(
				normalized.filter((entry): entry is NormalizedIssueEvent => entry.kind === 'issue'),
				subscribed,
			)
		}
		return yield* dispatchPullRequestEvents(
			normalized.filter((entry): entry is NormalizedPrEvent => entry.kind === 'pull-request'),
			subscribed,
		)
	})

export const makeGitHubEventProcessor = (
	options: GitHubEventProcessorOptions,
): ProviderEventProcessor<GitHubCallbacks | GitHubApi | MailboxSubscriptions> => ({
	namespace: options.namespace,
	providerName: 'github',
	process: processGitHubBatch(options),
})
