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
	type GitHubIssueCreated,
	type GitHubMentioned,
	type GitHubPrCreated,
} from '@humanlayer/channels-github'
import { Config, Effect, Match, Predicate } from 'effect'

export const maintainerOnlyNotice =
	'This agent can only be invoked by maintainers (users with write access or higher to this repository).'

/**
 * check if a user has mention access. if so, send eyes when we start working.
 * If not, send a thumbs down - AND, if not posted already in the issue/pr, a comment indicating it can only be used by maintainers
 */
export const respondToMentionAccess = Effect.fn('bot.github.respondToMentionAccess')(function* (
	event: GitHubMentioned,
) {
	const discussion = Match.value(event).pipe(
		Match.tagsExhaustive({
			GitHubIssueMentioned: ({ issue }) => issue,
			GitHubPrMentioned: ({ pullRequest }) => pullRequest,
		}),
	)
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

/**
 * GitHub App callbacks. `GitHubApiLive`, the default, reads the App ID and private key. Each callback first
 * checks that the mention author has write access or higher, reacting and explaining denied access.
 */
export const githubHandlers = {
	/** Here is where you would e.g. do code review / automatic triage */
	onIssueCreated: (event: GitHubIssueCreated, context: DeliveryContext) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`New issue created! Preparing auto-label`)
			const issueText = event.trigger.body
			if (Predicate.isNull(issueText)) {
				yield* Effect.logWarning('GithubIssueOpened event has a null body; skipping')
				return
			}

			/** TODO label the PR */
		}).pipe(
			Effect.annotateLogs({
				'github.issue_number': event.issue.ref.number,
			}),
		),
	onPrCreated: (event: GitHubPrCreated, context: DeliveryContext) =>
		Effect.gen(function* () {
			yield* Effect.logInfo(`New PR created! Preparing auto-label`)
			const prText = event.trigger.body
			if (Predicate.isNull(prText)) {
				yield* Effect.logWarning('GithubPrOpened event has a null body; skipping')
				return
			}
			/** TODO auto-label the PR */
		}).pipe(
			Effect.annotateLogs({
				'github.pr_number': event.pullRequest.ref.number,
			}),
		),

	onMentioned: (event: GitHubMentioned, context: DeliveryContext) =>
		Effect.gen(function* () {
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
		}),
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
