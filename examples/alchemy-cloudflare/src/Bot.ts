/**
 * The bot: its providers, callbacks and event processing settings.
 * The Worker and the Durable Object both build their half from this one value.
 */
import { ChannelsCloudflare } from '@humanlayer/channels-alchemy-cloudflare'
import { DebounceDeliveryMode, ExternalLink, type DeliveryContext } from '@humanlayer/channels-delivery-next'
import { GitHubBot, GitHubContent, GitHubId, GitHubReaction } from '@humanlayer/channels-github-next'
import {
	LinearAuth,
	LinearBot,
	LinearOrganizationId,
	LinearUserId,
	type LinearAgentSessionCreated,
	type LinearAgentSessionPrompted,
	type LinearIssueCreated,
} from '@humanlayer/channels-linear-next'
import { SlackBot, SlackContent, SlackReaction, type SlackNewMention } from '@humanlayer/channels-slack-next'
import { Config, Effect, Option, Predicate, Duration, Redacted } from 'effect'

import { FakeRemoteAgent } from './FakeRemoteAgentDO'
import { FlakySlackApiLive } from './FlakySlackApi'
import { parseHandoffCommand, parseHandoffText, parseIssueHandoffText, type HandoffCommand } from './HandoffCommand'

/**
 * Start a job for this delivery on the fake remote agent. The delivery ID is the job's idempotency key,
 * so a callback retry gets the job already started. Answers the job, with its run-log URL.
 */
const startRemoteJob = Effect.fn('example.start_remote_job')(function* (
	delivery: DeliveryContext,
	command: HandoffCommand,
) {
	const agents = yield* FakeRemoteAgent
	return yield* agents.getByName(delivery.deliveryId).start({
		deliveryId: delivery.deliveryId,
		accessToken: Redacted.value(delivery.accessToken),
		delaySeconds: command.delaySeconds,
		flakyOutput: command.flakyOutput,
		askForInput: command.askForInput,
	})
})

/** The reply to any other mention. */
const replyToMention = Effect.fn('example.slack.reply_to_mention')(function* (event: SlackNewMention) {
	yield* event.thread.startTyping()
	yield* Effect.sleep(2_000)
	yield* event.thread.post(SlackContent.make({ markdown: 'Subscribed! subsequent messages will be logged' }))
})

/**
 * `@bot handoff [seconds] [flaky]`: start a job on the remote agent, say so in the thread, and hand the
 * delivery off. The callback then returns; the remote agent completes the delivery through the delivery
 * API with a final message, which the bot posts to the thread. `flaky` makes that post fail for a while.
 */
const handOffMention = Effect.fn('example.slack.hand_off_mention')(function* (
	event: SlackNewMention,
	delivery: DeliveryContext,
	command: HandoffCommand,
) {
	const { delaySeconds, flakyOutput } = command
	yield* startRemoteJob(delivery, command)
	const flakyNote = flakyOutput ? ' Slack will refuse the final message for 20s, then it retries.' : ''
	yield* event.thread.post(
		SlackContent.make({
			markdown: `Handed off \`${delivery.deliveryId}\`. Finishing in ${delaySeconds}s.${flakyNote}`,
		}),
	)
	return yield* delivery.handoff()
})

/**
 * Slack with placeholder callbacks. `FlakySlackApiLive` is `SlackApiLive`, which reads `SLACK_BOT_TOKEN`,
 * except that it refuses a `flaky` handoff's final message for a while.
 */
const slack = SlackBot.make({
	signingSecret: Config.redacted('SLACK_SIGNING_SECRET'),
	slackApi: FlakySlackApiLive,
	deliveryMode: DebounceDeliveryMode.make({ quietPeriodMs: 2_000, maxWaitMs: 10_000 }),
	handlers: {
		onNewMention: (event, delivery) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Slack new mention received').pipe(
					Effect.annotateLogs({
						team_id: event.thread.ref.teamId,
						channel_id: event.thread.ref.channelId,
						thread_ts: event.thread.ref.threadTs,
						event_count: event.events.length + 1,
					}),
				)
				if (!(yield* event.thread.isSubscribed())) yield* event.thread.subscribe()
				return yield* Option.match(parseHandoffCommand(event.trigger.content), {
					onNone: () => replyToMention(event),
					onSome: (command) => handOffMention(event, delivery, command),
				})
			}),
		onSubscribedThreadEvents: (event) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Slack subscribed thread events received').pipe(
					Effect.annotateLogs({
						team_id: event.thread.ref.teamId,
						channel_id: event.thread.ref.channelId,
						thread_ts: event.thread.ref.threadTs,
						event_count: event.events.length,
					}),
				)
				for (const threadEvent of event.events) {
					if (Predicate.isTagged(threadEvent, 'SlackMessageReceived')) {
						yield* threadEvent.message.addReaction(SlackReaction.make('eyes'))
					}
				}
			}),
	},
})

/** GitHub App callbacks. `GitHubApiLive`, the default, reads the App ID and private key. */
const github = GitHubBot.make({
	webhookSecret: Config.redacted('GITHUB_WEBHOOK_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({ quietPeriodMs: 2_000, maxWaitMs: 10_000 }),
	bot: Config.all({
		mentionNames: Config.string('GITHUB_BOT_MENTION_NAME').pipe(Config.map((name) => [name])),
		botUserId: Config.schema(GitHubId, 'GITHUB_BOT_USER_ID'),
	}),
	handlers: {
		onIssueCreated: (event) =>
			Effect.gen(function* () {
				const [issue, comments] = yield* Effect.all([event.issue.fetchInfo(), event.issue.listComments()])
				yield* Effect.logInfo('GitHub issue created').pipe(
					Effect.annotateLogs({
						repository_id: event.issue.ref.repositoryId,
						issue_number: event.issue.ref.number,
						title: issue.title,
						comment_count: comments.length,
						trigger: event.trigger._tag,
						event_count: event.events.length + 1,
					}),
				)
				yield* event.issue.subscribe()
				const comment = yield* event.issue.postComment(
					GitHubContent.make({ markdown: 'Subscribed! Subsequent issue activity will be logged.' }),
				)
				yield* comment.addReaction(GitHubReaction.make('eyes'))
			}),
		onPrCreated: (event) =>
			Effect.gen(function* () {
				const [pullRequest, comments, reviews, reviewComments] = yield* Effect.all([
					event.pullRequest.fetchInfo(),
					event.pullRequest.listComments(),
					event.pullRequest.listReviews(),
					event.pullRequest.listReviewComments(),
				])
				yield* Effect.logInfo('GitHub pull request created').pipe(
					Effect.annotateLogs({
						repository_id: event.pullRequest.ref.repositoryId,
						pull_request_number: event.pullRequest.ref.number,
						title: pullRequest.title,
						comment_count: comments.length,
						review_count: reviews.length,
						review_comment_count: reviewComments.length,
						trigger: event.trigger._tag,
						event_count: event.events.length + 1,
					}),
				)
				yield* event.pullRequest.subscribe()
				const comment = yield* event.pullRequest.postComment(
					GitHubContent.make({ markdown: 'Subscribed! Subsequent pull request activity will be logged.' }),
				)
				yield* comment.addReaction(GitHubReaction.make('eyes'))
			}),
		onMentioned: (event) =>
			Effect.gen(function* () {
				const isIssue = Predicate.isTagged(event, 'GitHubIssueMentioned')
				const discussion = isIssue ? event.issue : event.pullRequest
				yield* Effect.logInfo('GitHub bot mentioned').pipe(
					Effect.annotateLogs({
						discussion_kind: isIssue ? 'issue' : 'pull_request',
						repository_id: discussion.ref.repositoryId,
						discussion_number: discussion.ref.number,
						trigger: event.trigger._tag,
						event_count: event.events.length + 1,
					}),
				)
				if (!(yield* discussion.isSubscribed())) yield* discussion.subscribe()
				const comment = yield* discussion.postComment(
					GitHubContent.make({ markdown: 'Mention received; this thread is subscribed.' }),
				)
				yield* comment.addReaction(GitHubReaction.make('eyes'))
			}),
		onSubscribedIssueEvents: (event) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('GitHub subscribed issue events received').pipe(
					Effect.annotateLogs({
						repository_id: event.issue.ref.repositoryId,
						issue_number: event.issue.ref.number,
						event_count: event.events.length,
					}),
				)
				for (const issueEvent of event.events) {
					if (Predicate.isTagged(issueEvent, 'GitHubIssueCommentCreated')) {
						yield* issueEvent.comment.addReaction(GitHubReaction.make('eyes'))
					}
				}
			}),
		onSubscribedPrEvents: (event) =>
			Effect.gen(function* () {
				const [comments, reviewComments] = yield* Effect.all([
					event.pullRequest.listComments(),
					event.pullRequest.listReviewComments(),
				])
				yield* Effect.logInfo('GitHub subscribed pull request events received').pipe(
					Effect.annotateLogs({
						repository_id: event.pullRequest.ref.repositoryId,
						pull_request_number: event.pullRequest.ref.number,
						event_count: event.events.length,
						comment_count: comments.length,
						review_comment_count: reviewComments.length,
					}),
				)
				for (const pullRequestEvent of event.events) {
					if (
						Predicate.isTagged(pullRequestEvent, 'GitHubPrCommentCreated') ||
						Predicate.isTagged(pullRequestEvent, 'GitHubPrReviewCommentCreated')
					) {
						yield* pullRequestEvent.comment.addReaction(GitHubReaction.make('eyes'))
					}
				}
			}),
	},
})

/**
 * A Linear session turn that asks for `handoff [seconds] [ask]`: start a job on the fake remote agent and
 * hand the turn off with a link to the job's run log, which Linear shows on the session. The automatic
 * thought has already answered Linear within its 10 seconds. The remote agent then shows its activity
 * as ephemeral thoughts, posts one lasting thought, and ends the turn with a response, or with a
 * question for `ask`. Stop in the session makes it end the turn with an error.
 */
const handOffSessionTurn = Effect.fn('example.linear.hand_off_session_turn')(function* (
	event: LinearAgentSessionCreated | LinearAgentSessionPrompted,
	delivery: DeliveryContext,
	command: HandoffCommand,
) {
	const started = yield* startRemoteJob(delivery, command)
	yield* Effect.logInfo('Linear session turn handed off').pipe(
		Effect.annotateLogs({
			agent_session_id: event.session.ref.sessionId,
			delivery_id: delivery.deliveryId,
			delay_seconds: command.delaySeconds,
			ask_for_input: command.askForInput,
		}),
	)
	return yield* delivery.handoff({
		links: [ExternalLink.make({ label: 'Fake remote agent run log', url: started.runLogUrl })],
	})
})

/** A Linear issue that asked for `issue-handoff [seconds]`: start a job and hand the delivery off. Its output is comments. */
const handOffIssue = Effect.fn('example.linear.hand_off_issue')(function* (
	event: LinearIssueCreated,
	delivery: DeliveryContext,
	command: HandoffCommand,
) {
	yield* startRemoteJob(delivery, command)
	yield* Effect.logInfo('Linear issue delivery handed off').pipe(
		Effect.annotateLogs({ issue_id: event.issue.ref.issueId, delivery_id: delivery.deliveryId }),
	)
	return yield* delivery.handoff()
})

/** Lines of text, without the missing ones, as one string to look for a handoff command in. */
const joinText = (lines: ReadonlyArray<string | null>) => lines.filter(Predicate.isNotNull).join('\n')

/** Linear Application callbacks for one explicitly configured workspace. */
const linear = LinearBot.make({
	webhookSecret: Config.redacted('LINEAR_WEBHOOK_SECRET'),
	bot: Config.all({
		organizationId: Config.schema(LinearOrganizationId, 'LINEAR_ORGANIZATION_ID'),
		appUserId: Config.schema(LinearUserId, 'LINEAR_APP_USER_ID'),
	}),
	auth: LinearAuth.fromEnvironment,
	handlers: {
		onAgentSessionCreated: (event, delivery) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Linear agent session created').pipe(
					Effect.annotateLogs({
						organization_id: event.session.ref.organizationId,
						agent_session_id: event.session.ref.sessionId,
						issue_id: event.issue.ref.issueId,
						issue_identifier: event.issue.identifier,
						delivery_id: event.deliveryId,
					}),
				)
				if (!(yield* event.issue.isSubscribed())) yield* event.issue.subscribe()
				/** The command may be in the mentioning comment (part of the prompt context) or the issue itself. */
				const command = parseHandoffText(
					joinText([event.promptContext, event.issue.title, event.issue.description]),
				)
				return yield* Option.match(command, {
					onSome: (handoff) => handOffSessionTurn(event, delivery, handoff),
					onNone: () =>
						Effect.gen(function* () {
							yield* Effect.sleep(Duration.seconds(2))
							yield* event.session.thought('thinking about session created...')
							yield* Effect.sleep(Duration.seconds(2))
							yield* event.session.respond(
								'The example agent received this session and completed its callback.',
							)
						}),
				})
			}),
		onAgentSessionPrompted: (event, delivery) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Linear agent session prompted').pipe(
					Effect.annotateLogs({
						organization_id: event.session.ref.organizationId,
						agent_session_id: event.session.ref.sessionId,
						issue_id: event.issue.ref.issueId,
						issue_identifier: event.issue.identifier,
						prompt_activity_id: event.prompt.id,
						prompt_signal: event.prompt.signal ?? 'none',
						delivery_id: event.deliveryId,
					}),
				)
				/**
				 * Stop was handled before this ran: it marked the handed-off turn, and the remote agent
				 * ended that turn with an error. There is nothing left to stop.
				 */
				if (event.prompt.signal === 'stop') {
					return yield* Effect.logInfo('Linear stop prompt reached its callback; nothing is left to stop')
				}
				return yield* Option.match(parseHandoffText(event.prompt.body), {
					onSome: (handoff) => handOffSessionTurn(event, delivery, handoff),
					onNone: () =>
						Effect.gen(function* () {
							yield* Effect.sleep(Duration.seconds(2))
							yield* event.session.thought('Thinking about session continuation...')
							yield* Effect.sleep(Duration.seconds(2))
							yield* event.session.respond(
								'The example agent received the follow-up prompt and completed its callback.',
							)
						}),
				})
			}),
		onIssueCreated: (event, delivery) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Linear issue created').pipe(
					Effect.annotateLogs({
						organization_id: event.issue.ref.organizationId,
						issue_id: event.issue.ref.issueId,
						issue_identifier: event.issue.identifier,
						event_id: event.trigger.eventId,
					}),
				)
				yield* event.issue.subscribe()
				return yield* Option.match(parseIssueHandoffText(joinText([event.issue.title, event.issue.description])), {
					onSome: (handoff) => handOffIssue(event, delivery, handoff),
					onNone: () => Effect.void,
				})
			}),
		onSubscribedEvent: (event) =>
			Effect.logInfo('Linear subscribed issue events received').pipe(
				Effect.annotateLogs({
					organization_id: event.issue.ref.organizationId,
					issue_id: event.issue.ref.issueId,
					issue_identifier: event.issue.identifier,
					event_count: event.events.length,
					event_tags: event.events.map((issueEvent) => issueEvent._tag).join(','),
					issue_change_count: event.events.reduce(
						(count, issueEvent) =>
							Predicate.isTagged(issueEvent, 'LinearIssueUpdated')
								? count + issueEvent.changes.length
								: count,
						0,
					),
					issue_change_tags: event.events
						.flatMap((issueEvent) =>
							Predicate.isTagged(issueEvent, 'LinearIssueUpdated')
								? issueEvent.changes.map((change) => change._tag)
								: [],
						)
						.join(','),
				}),
			),
	},
})

export const bot = ChannelsCloudflare.make({
	namespace: 'alchemy-cloudflare-example',
	providers: [slack, github, linear],
	eventProcessing: { concurrency: 1, maxAttempts: 5, leaseMs: 30_000 },
})
