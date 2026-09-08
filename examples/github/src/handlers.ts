import { GitHub, GitHubSubscriptions, type GitHubActivityEvent } from '@humanlayer/channels-github'
import { Effect } from 'effect'

export const respond = Effect.fn('example.github.respond')(function* (event: GitHubActivityEvent) {
	const subscriptions = yield* GitHubSubscriptions
	yield* subscriptions.subscribe({ namespace: 'github-example', resource: event.resource })
	const github = yield* GitHub
	yield* github.createComment({
		issue: event.resource,
		body: `Received ${event.event} for #${event.resource.number}.`,
	})
})
