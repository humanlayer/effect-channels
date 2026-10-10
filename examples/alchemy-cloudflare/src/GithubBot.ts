/**
 * The bot: its providers, callbacks and event processing settings.
 * The Worker and the Durable Object both build their half from this one value.
 */
import { ChannelsCloudflare } from '@humanlayer/channels-alchemy-cloudflare'
import {
	DebounceDeliveryMode,
	type DeliveryContext,
	type DeliveryHandoffError,
	type MailboxSubscriptionError,
	type MailboxSubscriptions,
} from '@humanlayer/channels-delivery'
import {
	GitHubApi,
	GitHubBot,
	GitHubContent,
	GitHubDiscussionRef,
	GitHubId,
	GitHubReaction,
	GitHubReactionTarget,
	hasGitHubAccess,
	type GitHubApiError,
	type GitHubCallbackHandlers,
	type GitHubCheckConclusion,
	type GitHubIssueComment,
	type GitHubIssueCreated,
	type GitHubMentioned,
	type GitHubParticipant,
	type GitHubPrCheckCompleted,
	type GitHubPrCreated,
	type GitHubPrEvent,
	type GitHubRepositoryRef,
	type GitHubReviewComment,
} from '@humanlayer/channels-github'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Array as Arr, Config, Effect, Match, Predicate, Schema } from 'effect'

import { AgentSessions } from './AgentSessionDO'
import { AutoLabel, type AutoLabelError } from './AutoLabel'
import { AgentSessionMessage } from './DeliveryTurn'
import { RequestComment } from './DiscussionContext'

export const maintainerOnlyNotice =
	'This agent can only be invoked by maintainers (users with write access or higher to this repository).'

const gitHubMentionDiscussion = (event: GitHubMentioned) =>
	Match.value(event).pipe(
		Match.tagsExhaustive({
			GitHubIssueMentioned: ({ issue }) => issue,
			GitHubPrMentioned: ({ pullRequest }) => pullRequest,
		}),
	)

/**
 * check if a user has mention access. if so, send eyes when we start working.
 * If not, send a thumbs down - AND, if not posted already in the issue/pr, a comment indicating it can only be used by maintainers
 */
export const respondToMentionAccess = Effect.fn('bot.github.respondToMentionAccess')(function* (
	event: GitHubMentioned,
) {
	const discussion = gitHubMentionDiscussion(event)
	const access = yield* discussion.fetchUserAccess(event.trigger.actor.login).pipe(
		Effect.catchIf(
			(error) => error.reason === 'not_found',
			(error) =>
				Effect.logInfo('GitHub could not resolve the mention author; denying access', error).pipe(
					Effect.as('none' as const),
				),
		),
	)
	const allowed = hasGitHubAccess({ access, minimum: 'write' })
	const target = Match.value(event.trigger).pipe(
		Match.tagsExhaustive({
			GitHubIssueOpened: ({ issue }) =>
				GitHubReactionTarget.cases.Discussion.make({
					discussion: GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref }),
				}),
			GitHubPrOpened: ({ pullRequest }) =>
				GitHubReactionTarget.cases.Discussion.make({
					discussion: GitHubDiscussionRef.cases.PullRequest.make({ ref: pullRequest.ref }),
				}),
			GitHubIssueCommentCreated: ({ comment }) =>
				GitHubReactionTarget.cases.Comment.make({ comment: comment.ref }),
			GitHubPrCommentCreated: ({ comment }) => GitHubReactionTarget.cases.Comment.make({ comment: comment.ref }),
			GitHubPrReviewCommentCreated: ({ comment }) =>
				GitHubReactionTarget.cases.Comment.make({ comment: comment.ref }),
		}),
	)
	const api = yield* GitHubApi
	yield* api.addReaction({ target, reaction: GitHubReaction.make(allowed ? 'eyes' : '-1') })
	if (allowed) return true

	yield* Effect.logWarning('Bot mention denied: the author lacks write access').pipe(
		Effect.annotateLogs({ actor: event.trigger.actor.login, access }),
	)
	const botUserId = yield* Config.schema(GitHubId, 'GITHUB_BOT_USER_ID')
	const comments = yield* discussion.listComments()
	const alreadyNotified = comments.some(
		(comment) => comment.author?.id === botUserId && comment.body === maintainerOnlyNotice,
	)
	if (!alreadyNotified) yield* discussion.postComment(GitHubContent.make({ markdown: maintainerOnlyNotice }))
	return false
})

/** Lines of text, without the missing ones, as one string. */
const joinText = (lines: ReadonlyArray<string | null>) => lines.filter(Predicate.isNotNull).join('\n')

/** One comment of the request: which comment it is, its own text, and how the agent is shown it. */
interface RequestItem {
	readonly comment: RequestComment
	readonly body: string
	readonly text: string
}

const authorOf = (author: GitHubParticipant | null) => (author === null ? 'a deleted user' : `@${author.login}`)

const commentItem = (comment: GitHubIssueComment): RequestItem => ({
	comment: RequestComment.cases.Comment.make({ id: comment.ref.id }),
	body: comment.body,
	text: `${authorOf(comment.author)} commented (comment ${comment.ref.id}):\n${comment.body}`,
})

/** A line comment with where it is in the diff and the thread to reply in: GitHub threads replies under the first. */
const lineCommentItem = (comment: GitHubReviewComment): RequestItem => {
	const line = comment.line ?? comment.startLine
	const where = Predicate.isNullish(line) ? comment.path : `${comment.path}:${line}`
	const thread = comment.inReplyToId ?? comment.ref.id
	return {
		comment: RequestComment.cases.ReviewComment.make({ id: comment.ref.id }),
		body: comment.body,
		text: `${authorOf(comment.author)} commented on ${where} (line comment ${comment.ref.id}, thread ${thread}):\n\`\`\`diff\n${comment.diffHunk}\n\`\`\`\n${comment.body}`,
	}
}

/**
 * The comments a mention's batch posted, the mentioning one first, each once. Other events add nothing to
 * the request.
 */
const requestItems = (event: GitHubMentioned): ReadonlyArray<RequestItem> =>
	Arr.dedupeWith(
		[event.trigger, ...event.events].flatMap((item): ReadonlyArray<RequestItem> => {
			if (
				Predicate.isTagged(item, 'GitHubIssueCommentCreated') ||
				Predicate.isTagged(item, 'GitHubPrCommentCreated')
			)
				return [commentItem(item.comment)]
			if (Predicate.isTagged(item, 'GitHubPrReviewCommentCreated')) return [lineCommentItem(item.comment)]
			return []
		}),
		(a, b) => a.comment._tag === b.comment._tag && a.comment.id === b.comment.id,
	)

/**
 * The agent's request. A lone comment is its text, and an opened issue or pull request is its title and
 * description. Otherwise, such as several comments posted together or a line comment, each comment is listed
 * with what the agent needs to answer it, and the agent is told to answer each line comment in its own thread.
 */
export const gitHubRequest = (event: GitHubMentioned) => {
	const items = requestItems(event)
	const opened = Match.value(event.trigger).pipe(
		Match.tags({
			GitHubIssueOpened: ({ title, body }) => joinText([title, body]),
			GitHubPrOpened: ({ title, body }) => joinText([title, body]),
		}),
		Match.orElse(() => null),
	)
	const [first] = items
	if (Predicate.isNull(opened) && items.length === 1 && Predicate.isTagged(first?.comment, 'Comment'))
		return first.body
	if (Arr.isReadonlyArrayEmpty(items)) return opened ?? ''
	const guide = items.some(({ comment }) => Predicate.isTagged(comment, 'ReviewComment'))
		? 'Answer each line comment in its own review thread with github_post_comment, setting reply_to to its thread. Your final answer is posted on the pull request, so keep it to a short summary.'
		: 'Answer them in your final answer.'
	return joinText([
		opened,
		`<system-information>These comments were posted together, and they are the request. ${guide}</system-information>`,
		items.map(({ text }) => text).join('\n\n---\n\n'),
	])
}

/** Check results that need the agent: the check failed, or could not run. */
const FAILED_CONCLUSIONS: ReadonlySet<GitHubCheckConclusion> = new Set([
	'failure',
	'timed_out',
	'action_required',
	'startup_failure',
])

const isFailedCheck = (event: GitHubPrEvent): event is GitHubPrCheckCompleted =>
	Predicate.isTagged(event, 'GitHubPrCheckCompleted') && FAILED_CONCLUSIONS.has(event.conclusion)

/** The failed checks among a batch's events that ran on `headSha`, the pull request's latest commit, each once. */
export const failedChecksOnHead = (events: ReadonlyArray<GitHubPrEvent>, headSha: string) =>
	Arr.dedupeWith(
		events.filter(isFailedCheck).filter((check) => check.headSha === headSha),
		(a, b) => a.checkRunId === b.checkRunId,
	)

/** The agent's request when checks fail on the latest commit of a pull request it follows. */
export const checkFailurePrompt = (checks: ReadonlyArray<GitHubPrCheckCompleted>, headSha: string) =>
	[
		`<system-information>Checks failed on this pull request's latest commit, ${headSha.slice(0, 7)}. Nobody mentioned you; these failures are the request.</system-information>`,
		'',
		...checks.map(
			(check) =>
				`- \`${check.name}\`: ${check.conclusion.replace('_', ' ')} (check run ${check.checkRunId})${Predicate.isNull(check.detailsUrl) ? '' : `, ${check.detailsUrl}`}`,
		),
		'',
		'Find out why with github_check_failure. If the cause is clear and the fix belongs in this pull request, fix it, check it as far as you can, and push. Otherwise explain the cause and what to change. Your answer is posted on the pull request.',
	].join('\n')

/**
 * An authorized mention subscribes the discussion, so later comments and checks reach this mailbox, and
 * hands the delivery to the discussion's AgentSession, which finishes it when its turn ends. Until then the
 * mailbox holds later events back.
 */

const githubRepositoryLogAnnotations = (ref: GitHubRepositoryRef) => ({
	'github.owner': ref.owner,
	'github.repository': ref.repository,
	'github.repository_id': ref.repositoryId,
	'github.installation_id': ref.installationId,
})

/**
 * GitHub App callbacks. Creation labels public submissions; mentions require write access or higher.
 * `GitHubApiLive`, the default, reads the App ID and private key.
 */
export const githubHandlers = {
	/** Here is where you would e.g. do code review / automatic triage */
	onIssueCreated: (event: GitHubIssueCreated, context: DeliveryContext) =>
		Effect.gen(function* () {
			const labeler = yield* AutoLabel
			yield* labeler.apply({
				discussion: event.issue,
				kind: 'issue',
				title: event.trigger.title,
				body: event.trigger.body,
			})
		}).pipe(
			Effect.annotateLogs({
				...githubRepositoryLogAnnotations(event.issue.ref),
				'github.issue_number': event.issue.ref.number,
				'github.event_id': event.trigger.eventId,
				'delivery.id': context.deliveryId,
			}),
		),
	onPrCreated: (event: GitHubPrCreated, context: DeliveryContext) =>
		Effect.gen(function* () {
			const labeler = yield* AutoLabel
			yield* labeler.apply({
				discussion: event.pullRequest,
				kind: 'pull_request',
				title: event.trigger.title,
				body: event.trigger.body,
			})
		}).pipe(
			Effect.annotateLogs({
				...githubRepositoryLogAnnotations(event.pullRequest.ref),
				'github.pr_number': event.pullRequest.ref.number,
				'github.event_id': event.trigger.eventId,
				'delivery.id': context.deliveryId,
			}),
		),

	onMentioned: (event: GitHubMentioned, context: DeliveryContext) => {
		const discussion = gitHubMentionDiscussion(event)
		return Effect.gen(function* () {
			if (!(yield* respondToMentionAccess(event))) return

			yield* Effect.logInfo('Authorized mention received')
			yield* discussion.subscribe()
			const message = yield* Schema.encodeEffect(AgentSessionMessage)(
				AgentSessionMessage.make({
					prompt: gitHubRequest(event),
					githubDiscussion: discussion,
					requestComments: requestItems(event).map(({ comment }) => comment),
					deliveryId: context.deliveryId,
					accessToken: context.accessToken,
				}),
			).pipe(Effect.orDie)
			const agentSessions = yield* AgentSessions
			yield* agentSessions.getByName(discussion.mailboxKey).send(message)
			return yield* context.handoff()
		}).pipe(
			Effect.annotateLogs({
				...githubRepositoryLogAnnotations(discussion.ref),
				'github.discussion_kind': discussion._tag,
				'github.discussion_number': discussion.ref.number,
				'github.event_id': event.trigger.eventId,
				'github.actor': event.trigger.actor.login,
				'delivery.id': context.deliveryId,
			}),
		)
	},
	onSubscribedPrEvents: (event, context) =>
		Effect.gen(function* () {
			if (!event.events.some(isFailedCheck)) return
			const { headSha } = yield* event.pullRequest.fetchInfo()
			const failed = failedChecksOnHead(event.events, headSha)
			if (Arr.isReadonlyArrayEmpty(failed)) {
				yield* Effect.logInfo('Failed checks ignored: not on the latest commit')
				return
			}
			yield* Effect.logInfo('Checks failed on the latest commit').pipe(
				Effect.annotateLogs({ checks: failed.map(({ name }) => name).join(', '), head_sha: headSha }),
			)
			const message = yield* Schema.encodeEffect(AgentSessionMessage)(
				AgentSessionMessage.make({
					prompt: checkFailurePrompt(failed, headSha),
					githubDiscussion: event.pullRequest,
					requestComments: [],
					deliveryId: context.deliveryId,
					accessToken: context.accessToken,
				}),
			).pipe(Effect.orDie)
			const agentSessions = yield* AgentSessions
			yield* agentSessions.getByName(event.pullRequest.mailboxKey).send(message)
			return yield* context.handoff()
		}).pipe(
			Effect.annotateLogs({
				...githubRepositoryLogAnnotations(event.pullRequest.ref),
				'github.pr_number': event.pullRequest.ref.number,
				'delivery.id': context.deliveryId,
			}),
		),
} satisfies GitHubCallbackHandlers<
	GitHubApiError | AutoLabelError | Config.ConfigError | MailboxSubscriptionError | DeliveryHandoffError,
	GitHubApi | AutoLabel | RuntimeContext | AgentSessions | MailboxSubscriptions
>

const github = GitHubBot.make<
	GitHubApiError | AutoLabelError | Config.ConfigError | MailboxSubscriptionError | DeliveryHandoffError,
	GitHubApi | AutoLabel | RuntimeContext | AgentSessions | MailboxSubscriptions
>({
	webhookSecret: Config.Redacted('GITHUB_WEBHOOK_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({ quietPeriodMs: 3_000, maxWaitMs: 3_000 }),
	bot: Config.all({
		mentionNames: Config.String('GITHUB_BOT_MENTION_NAME').pipe(Config.map((name) => [name])),
		botUserId: Config.schema(GitHubId, 'GITHUB_BOT_USER_ID'),
	}),
	handlers: githubHandlers,
})

export const bot = ChannelsCloudflare.make({
	namespace: 'humanlayer-channels-app',
	providers: [github],
	eventProcessing: {
		concurrency: 1,
		leaseMs: 10_000,
		maxAttempts: 5,
	},
})
