import { DeliveryPolicy } from '@humanlayer/channels-delivery'
import { GitHubBot } from '@humanlayer/channels-github'
import { Slack, SlackBot } from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'

import { notifySlack, respond, slackHandlers } from './handlers.js'

const policy = DeliveryPolicy.make({
	mode: 'serial',
	maxPayloadBytes: 512_000,
	maxEnvelopes: 100,
	maxOutcomes: 1_000,
	retentionMs: 86_400_000,
	maxAttempts: 3,
	retryBaseMs: 100,
	retryMaxMs: 30_000,
	leaseMs: 30_000,
	heartbeatMs: 5_000,
	conflictRetries: 10,
})
export const slackBot = SlackBot.make({ namespace: 'combined-slack', handlers: slackHandlers })
export const githubBot = GitHubBot.make({
	namespace: 'combined-github',
	policy,
	runner: { scanLimit: 100, concurrency: 8, pollMs: 25 },
	handlers: [
		{ id: 'respond', onMention: respond, onSubscribedEvent: () => Effect.void },
		{ id: 'notify-slack', onMention: notifySlack },
	],
})
export const application = Layer.merge(slackBot.layer, githubBot.layer.pipe(Layer.provide(Slack.layerFromStore)))
