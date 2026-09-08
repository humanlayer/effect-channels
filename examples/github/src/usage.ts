import {
	GitHub,
	GitHubSubscriptions,
	type GitHubDiscussionRef,
	type GitHubReactionRef,
	type GitHubReactionTarget,
} from '@humanlayer/channels-github'
import { Effect } from 'effect'

import { namespace } from './handlers.js'

export const stopFollowing = Effect.fn('example.github.stop_following')(function* (resource: GitHubDiscussionRef) {
	const subscriptions = yield* GitHubSubscriptions
	const input = { namespace, resource }
	const wasSubscribed = yield* subscriptions.isSubscribed(input)
	yield* subscriptions.unsubscribe(input)
	return wasSubscribed
})

export const acknowledge = Effect.fn('example.github.acknowledge')(function* (target: GitHubReactionTarget) {
	const github = yield* GitHub
	return yield* github.addReaction({ target, content: 'eyes' })
})

export const listAcknowledgements = Effect.fn('example.github.list_acknowledgements')(function* (
	target: GitHubReactionTarget,
) {
	const github = yield* GitHub
	return yield* github.listReactions({ target, content: 'eyes', page: 1, perPage: 20 })
})

export const removeAcknowledgement = Effect.fn('example.github.remove_acknowledgement')(function* (
	reaction: GitHubReactionRef,
) {
	const github = yield* GitHub
	yield* github.removeReaction({ reaction })
})
