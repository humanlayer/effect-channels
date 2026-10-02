/**
 * Whom the example bot listens to. On GitHub, only people with write access or higher to the repository;
 * on Slack, only people from the installation's own workspace, which matters in Slack Connect channels.
 * Everyone else is ignored without a comment, reply, or reaction. The libraries do not enforce this.
 */
import {
	type GitHubAccessLevel,
	type GitHubApiError,
	type GitHubIssue,
	type GitHubIssueEvent,
	type GitHubIssueOpened,
	type GitHubPrEvent,
	type GitHubPrOpened,
	type GitHubPullRequest,
	hasGitHubAccess,
} from '@humanlayer/channels-github-next'
import type { SlackMessage } from '@humanlayer/channels-slack-next'
import { Array as Arr, Effect, Predicate } from 'effect'

/** The lowest GitHub access whose events the example acts on. */
export const minimumGitHubAccess: GitHubAccessLevel = 'write'

/** An issue or pull request event, with the person who caused it. */
export type GitHubAuthoredEvent = GitHubIssueOpened | GitHubIssueEvent | GitHubPrOpened | GitHubPrEvent

/**
 * A login GitHub does not know as a user (a bot account, or a placeholder) has no access. Other lookup
 * failures stay failures.
 */
const countNotFoundAsNoAccess = <R>(effect: Effect.Effect<GitHubAccessLevel, GitHubApiError, R>) =>
	effect.pipe(
		Effect.catchIf(
			(error) => error.reason === 'not_found',
			(error) =>
				Effect.logInfo('GitHub does not know this author as a user; counting it as no access', error).pipe(
					Effect.as<GitHubAccessLevel>('none'),
				),
		),
	)

/**
 * The events whose author has write access or higher, in their order. Each distinct author is looked up once
 * per call, and each dropped event is logged. An author GitHub answers `not_found` for counts as `none`; any
 * other failed lookup fails the call, so no event gets through unchecked.
 */
export const eventsFromGitHubWriters = Effect.fn('example.github.events_from_writers')(function* <
	Event extends GitHubAuthoredEvent,
>(input: { readonly discussion: GitHubIssue | GitHubPullRequest; readonly events: ReadonlyArray<Event> }) {
	const { discussion, events } = input
	const logins = Arr.dedupe(events.map((event) => event.actor.login))
	const accessByLogin = new Map(
		yield* Effect.forEach(logins, (login) =>
			discussion.fetchUserAccess(login).pipe(
				countNotFoundAsNoAccess,
				Effect.map((access) => [login, access] as const),
			),
		),
	)
	const allowed: Array<Event> = []
	for (const event of events) {
		const access = accessByLogin.get(event.actor.login) ?? 'none'
		if (hasGitHubAccess({ access, minimum: minimumGitHubAccess })) {
			allowed.push(event)
			continue
		}
		yield* Effect.logInfo('Example ignored GitHub author without write access').pipe(
			Effect.annotateLogs({
				login: event.actor.login,
				access,
				event: event._tag,
				repository_id: discussion.ref.repositoryId,
				discussion_number: discussion.ref.number,
			}),
		)
	}
	return allowed
})

/** Whether one event's author has write access or higher. Logs the event when not. */
export const isFromGitHubWriter = (input: {
	readonly discussion: GitHubIssue | GitHubPullRequest
	readonly event: GitHubAuthoredEvent
}) =>
	eventsFromGitHubWriters({ discussion: input.discussion, events: [input.event] }).pipe(
		Effect.map(Arr.isReadonlyArrayNonEmpty),
	)

/** Whether a Slack message's author is from the installation's workspace. No `user_team` counts as local. */
export const isFromInstallationWorkspace = (message: SlackMessage): boolean =>
	Predicate.isUndefined(message.authorTeamId) || message.authorTeamId === message.ref.teamId

/** Whether a Slack message's author is from the installation's workspace. Logs the message when not. */
export const isFromSlackWorkspace = Effect.fn('example.slack.is_from_workspace')(function* (message: SlackMessage) {
	if (isFromInstallationWorkspace(message)) return true
	yield* Effect.logInfo('Example ignored Slack author from another workspace').pipe(
		Effect.annotateLogs({
			team_id: message.ref.teamId,
			author_team_id: message.authorTeamId ?? 'none',
			user_id: message.author.userId,
			channel_id: message.ref.channelId,
			message_ts: message.ref.messageTs,
		}),
	)
	return false
})
