import type { HandlerContext } from '@humanlayer/channels-delivery'
import { GitHub, GitHubSubscriptions, type GitHubActivityEvent } from '@humanlayer/channels-github'
import { MarkdownContent, type MessageEvent } from '@humanlayer/channels-slack'
import { Config, Effect, Match, Predicate } from 'effect'

export const githubNamespace = 'organization-github'

export const slackMention = Effect.fn('example.organization.slack_mention')(function* (
	event: MessageEvent,
	context: HandlerContext<MessageEvent>,
) {
	if (event.message.text.trim().toLowerCase() !== 'unsubscribe') yield* event.thread.subscribe()
	yield* slackReply(event, context)
})

export const slackReply = Effect.fn('example.organization.slack_reply')(function* (
	event: MessageEvent,
	context: HandlerContext<MessageEvent>,
) {
	if (event.message.text.trim().toLowerCase() === 'unsubscribe') {
		yield* event.thread.unsubscribe()
		yield* event.thread.post(MarkdownContent.make({ markdown: 'Unsubscribed. Mention me to engage again.' }))
		return
	}
	yield* event.thread.post(MarkdownContent.make({ markdown: `Organization: ${context.organizationId}` }))
})

const githubText = (event: GitHubActivityEvent) =>
	Match.value(event).pipe(
		Match.when({ event: 'issues', action: Match.is('opened', 'edited') }, (event) => event.issue.body),
		Match.when({ event: 'pull_request', action: Match.is('opened', 'edited') }, (event) => event.pull_request.body),
		Match.when(
			{ event: Match.is('issue_comment', 'pull_request_review_comment'), action: Match.is('created', 'edited') },
			(event) => event.comment.body,
		),
		Match.when(
			{ event: 'pull_request_review', action: Match.is('submitted', 'edited') },
			(event) => event.review.body,
		),
		Match.orElse(() => null),
	)

const githubUnsubscribe = Effect.fn('example.organization.github_unsubscribe')(function* (event: GitHubActivityEvent) {
	const text = githubText(event)?.trim() ?? ''
	const login = yield* Config.string('GITHUB_BOT_LOGIN')
	const prefix = `@${login}`.toLowerCase()
	const remainder = text.slice(prefix.length)
	const command =
		text.toLowerCase().startsWith(prefix) && remainder.trimStart() !== remainder ? remainder.trim() : text
	if (command.toLowerCase() !== 'unsubscribe') return false
	yield* (yield* GitHubSubscriptions).unsubscribe({ namespace: githubNamespace, resource: event.resource })
	yield* (yield* GitHub).createComment({ issue: event.resource, body: 'Unsubscribed. Mention me to engage again.' })
	return true
})

export const githubMention = Effect.fn('example.organization.github_mention')(function* (
	event: GitHubActivityEvent,
	context: HandlerContext<GitHubActivityEvent>,
) {
	if (yield* githubUnsubscribe(event)) return
	yield* (yield* GitHubSubscriptions).subscribe({ namespace: githubNamespace, resource: event.resource })
	yield* githubReply(event, context)
})

export const githubFollowup = Effect.fn('example.organization.github_followup')(function* (
	event: GitHubActivityEvent,
	context: HandlerContext<GitHubActivityEvent>,
) {
	const text = githubText(event)
	if (event.sender?.type !== 'User' || !Predicate.isString(text) || text.trim() === '') return
	if (yield* githubUnsubscribe(event)) return
	yield* githubReply(event, context)
})

export const githubReply = Effect.fn('example.organization.github_reply')(function* (
	event: GitHubActivityEvent,
	context: HandlerContext<GitHubActivityEvent>,
) {
	yield* (yield* GitHub).createComment({ issue: event.resource, body: `Organization: ${context.organizationId}` })
})
