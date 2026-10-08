/**
 * The bot: its providers, callbacks and event processing settings.
 * The Worker and the Durable Object both build their half from this one value.
 */
import { ChannelsCloudflare } from '@humanlayer/channels-alchemy-cloudflare'
import { DebounceDeliveryMode, type DeliveryContext } from '@humanlayer/channels-delivery'
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
	type GitHubIssueCreated,
	type GitHubMentioned,
	type GitHubPrCreated,
	type GitHubRepositoryRef,
} from '@humanlayer/channels-github'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Config, Effect, Match, Predicate } from 'effect'

import { AutoLabel, type AutoLabelError } from './AutoLabel'

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

/** Lines of text, without the missing ones, as one string to look for a handoff command in. */
const joinText = (lines: ReadonlyArray<string | null>) => lines.filter(Predicate.isNotNull).join('\n')

/** The text of what mentioned the bot: the comment, or the issue or pull request that was opened. */
const gitHubMentionText = (event: GitHubMentioned) =>
	Match.value(event.trigger).pipe(
		Match.tagsExhaustive({
			GitHubIssueOpened: ({ title, body }) => joinText([title, body]),
			GitHubPrOpened: ({ title, body }) => joinText([title, body]),
			GitHubIssueCommentCreated: ({ comment }) => comment.body,
			GitHubPrCommentCreated: ({ comment }) => comment.body,
			GitHubPrReviewCommentCreated: ({ comment }) => comment.body,
		}),
	)

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
export const githubHandlers: GitHubCallbackHandlers<
	GitHubApiError | AutoLabelError | Config.ConfigError,
	GitHubApi | AutoLabel | RuntimeContext
> = {
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

			const mentionText = gitHubMentionText(event)
			/** We handle Issue & PR mentions separately  */
			yield* Match.value(event).pipe(
				Match.tagsExhaustive({
					GitHubIssueMentioned: (issueEvent) =>
						Effect.gen(function* () {
							yield* Effect.logInfo('Authorized issue mention received')
						}).pipe(
							Effect.annotateLogs({
								'github.issue_number': issueEvent.issue.ref.number,
								'github.issue_event': issueEvent.trigger.eventId,
								'github.actor': issueEvent.trigger.actor.login,
							}),
						),
					GitHubPrMentioned: (prEvent) =>
						Effect.gen(function* () {
							yield* Effect.logInfo('Authorized PR mention received')
						}).pipe(
							Effect.annotateLogs({
								'github.pr_number': prEvent.pullRequest.ref.number,
								'github.pr_event': prEvent.trigger.eventId,
								'github.actor': prEvent.trigger.actor.login,
							}),
						),
				}),
			)
		}).pipe(
			Effect.annotateLogs({
				...githubRepositoryLogAnnotations(discussion.ref),
				'github.discussion_number': discussion.ref.number,
				'github.event_id': event.trigger.eventId,
				'github.actor': event.trigger.actor.login,
				'delivery.id': context.deliveryId,
			}),
		)
	},
	onSubscribedPrEvents: (event, context) =>
		Effect.gen(function* () {
			/** TODO if the issue(s) are failing CI checks then we shoudl like address them */
		}).pipe(
			Effect.annotateLogs({
				...githubRepositoryLogAnnotations(event.pullRequest.ref),
				'github.pr_number': event.pullRequest.ref.number,
				'delivery.id': context.deliveryId,
			}),
		),
}

const github = GitHubBot.make({
	webhookSecret: Config.Redacted('GITHUB_WEBHOOK_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({ quietPeriodMs: 10_000, maxWaitMs: 10_000 }),
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
