import {
	GitHub,
	GitHubSubscriptions,
	type GitHubActivityEvent,
	type GitHubCreationEvent,
} from '@humanlayer/channels-github'
import { Effect, Match } from 'effect'

export const namespace = 'github-example'

export const observeCreation = Effect.fn('example.github.observe_creation')(function* (event: GitHubCreationEvent) {
	yield* Effect.logInfo('Creation observed; mention the bot to follow', {
		event: event.event,
		number: event.resource.number,
	})
})

export const observeActivity = Effect.fn('example.github.observe_activity')(function* (event: GitHubActivityEvent) {
	const detail = Match.value(event).pipe(
		Match.discriminatorsExhaustive('event')({
			issues: () => ({}),
			issue_comment: (event) => ({ commentId: event.comment.id }),
			pull_request: (event) =>
				Match.value(event).pipe(
					Match.when({ action: 'closed' }, (event) => ({ merged: event.pull_request.merged })),
					Match.when({ action: 'synchronize' }, (event) => ({ before: event.before, after: event.after })),
					Match.orElse(() => ({})),
				),
			pull_request_review: (event) => ({ reviewId: event.review.id, state: event.review.state }),
			pull_request_review_comment: (event) => ({ commentId: event.comment.id }),
			pull_request_review_thread: (event) => ({ threadId: event.thread.node_id }),
		}),
	)
	yield* Effect.logInfo('Followed GitHub activity', {
		event: event.event,
		action: event.action,
		number: event.resource.number,
		...detail,
	})
})

export const respond = Effect.fn('example.github.respond')(function* (event: GitHubActivityEvent) {
	const subscriptions = yield* GitHubSubscriptions
	yield* subscriptions.subscribe({ namespace, resource: event.resource })
	const github = yield* GitHub
	yield* github.createComment({
		issue: event.resource,
		body: `Received ${event.event} for #${event.resource.number}.`,
	})
})
