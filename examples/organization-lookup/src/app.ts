import { DeliveryPolicy } from '@humanlayer/channels-delivery'
import { GitHubBot } from '@humanlayer/channels-github'
import { SlackBot } from '@humanlayer/channels-slack'
import { Layer } from 'effect'

import { githubFollowup, githubMention, githubNamespace, slackMention, slackReply } from './handlers.js'
import { organizations } from './organizations.js'

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

export const slackBot = SlackBot.make({
	namespace: 'organization-slack',
	policy,
	handlers: { onNewMention: slackMention, onSubscribedMessage: slackReply },
})

export const githubBot = GitHubBot.make({
	namespace: githubNamespace,
	policy,
	runner: { scanLimit: 100, concurrency: 8, pollMs: 25 },
	handlers: [{ id: 'reply', onMention: githubMention, onSubscribedEvent: githubFollowup }],
})

export const application = Layer.merge(slackBot.layer, githubBot.layer).pipe(Layer.provide(organizations))
