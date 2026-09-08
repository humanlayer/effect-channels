import { GitHub, GitHubSubscriptions, type GitHubActivityEvent } from '@humanlayer/channels-github'
import {
	PlainTextContent,
	Slack,
	SlackTeamId,
	SlackChannelId,
	slackChannelRef,
	type MessageEvent,
} from '@humanlayer/channels-slack'
import { Config, Effect } from 'effect'

export const respond = Effect.fn('example.combined.github_reply')(function* (event: GitHubActivityEvent) {
	const subscriptions = yield* GitHubSubscriptions
	yield* subscriptions.subscribe({ namespace: 'combined-github', resource: event.resource })
	const github = yield* GitHub
	yield* github.createComment({
		issue: event.resource,
		body: `Received ${event.event} for #${event.resource.number}.`,
	})
})
export const notifySlack = Effect.fn('example.github.notify_slack')(function* (event: GitHubActivityEvent) {
	const teamId = yield* Config.schema(SlackTeamId, 'SLACK_TEAM_ID')
	const channelId = yield* Config.schema(SlackChannelId, 'SLACK_NOTIFICATION_CHANNEL_ID')
	const slack = yield* Slack
	yield* slack.postToChannel({
		channel: slackChannelRef(teamId, channelId),
		content: PlainTextContent.make({
			text: `GitHub mention on ${event.resource.kind} #${event.resource.number} in ${event.resource.repository.owner}/${event.resource.repository.name}`,
		}),
	})
})

const reply = Effect.fn('example.combined.slack_reply')(function* ({ thread, message }: MessageEvent) {
	yield* thread.post(PlainTextContent.make({ text: `Durable echo: ${message.text}` }))
})
export const slackHandlers = {
	onNewMention: Effect.fn('example.combined.slack_mention')(function* (event: MessageEvent) {
		yield* event.thread.subscribe()
		yield* reply(event)
	}),
	onSubscribedMessage: reply,
	onDirectMessage: reply,
}
