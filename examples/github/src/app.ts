import { DeliveryPolicy } from '@humanlayer/channels-delivery'
import { GitHubBot } from '@humanlayer/channels-github'
import { Effect } from 'effect'

import { respond } from './handlers.js'

export const policy = DeliveryPolicy.make({
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
export const runner = { scanLimit: 100, concurrency: 8, pollMs: 25 }
export const bot = GitHubBot.make({
	namespace: 'github-example',
	policy,
	runner,
	activityHandlers: [{ id: 'respond', onMention: respond, onSubscribedEvent: () => Effect.void }],
})
export const application = bot.layer
