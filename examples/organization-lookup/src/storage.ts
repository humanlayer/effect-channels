import { layer as deliveryMemory } from '@humanlayer/channels-delivery/memory'
import { subscriptions as githubSubscriptions } from '@humanlayer/channels-github/memory'
import { subscriptions as slackSubscriptions } from '@humanlayer/channels-slack/memory'
import { Layer } from 'effect'

export const storage = Layer.mergeAll(
	deliveryMemory({ maxMailboxes: 10_000 }),
	slackSubscriptions(),
	githubSubscriptions(),
)
