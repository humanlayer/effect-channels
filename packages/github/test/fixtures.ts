import { DeliveryPolicy } from '@humanlayer/channels-delivery'
import { Layer } from 'effect'

import { GitHubCredentials, GitHubIssueEvent } from '../src/index.js'

export const policy = DeliveryPolicy.make({
	mode: 'queue',
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

export const user = { id: 3, login: 'alice', type: 'User' }
export const event = {
	event: 'issues',
	action: 'opened',
	deliveryId: 'event-a',
	resource: {
		kind: 'github.issue',
		repository: { kind: 'github.repository', installationId: 100, id: 20, owner: 'alice', name: 'project' },
		number: 1,
	},
	sender: user,
	issue: {
		id: 30,
		number: 1,
		title: 'Hello',
		body: null,
		user,
		state: 'open',
		html_url: 'https://github.test/alice/project/issues/1',
	},
} satisfies GitHubIssueEvent
export const routeCredentials = Layer.mock(GitHubCredentials, {
	apiUrl: 'https://api.github.test',
	botUserId: 99,
	acceptsInstallation: ({ installationId }) => installationId === 100,
})
