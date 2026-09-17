import { assert, it } from '@effect/vitest'
import { Effect, Layer } from 'effect'

import { SlackApiError, SlackTransportError, slackErrorRetryability } from '../src/Errors'
import { ThreadId } from '../src/index'
import { Slack } from '../src/Slack'
import { stubSlackClientLayer } from './support'

it('classifies Slack transport and API errors for provider-neutral retry handling', () => {
	assert.strictEqual(
		slackErrorRetryability(SlackTransportError.make({ operation: 'conversations.replies' })),
		'retryable',
	)
	for (const code of ['ratelimited', 'internal_error', 'fatal_error', 'service_unavailable']) {
		assert.strictEqual(slackErrorRetryability(SlackApiError.make({ operation: 'test', code })), 'retryable')
	}
	for (const code of [
		'missing_scope',
		'not_authed',
		'invalid_auth',
		'token_revoked',
		'not_in_channel',
		'channel_not_found',
		'ordinary_rejection',
	]) {
		assert.strictEqual(slackErrorRetryability(SlackApiError.make({ operation: 'test', code })), 'non_retryable')
	}
})

it.effect('preserves missing_scope as non-retryable through Slack history narrowing', () =>
	Effect.gen(function* () {
		const provider = yield* Slack
		const failure = yield* Effect.flip(
			provider.messages({ threadId: ThreadId.make('slack:v1:T_TEST:C_TEST:100.1') }),
		)
		assert.strictEqual(failure._tag, 'HistoryFailed')
		assert.strictEqual(failure.retryability, 'non_retryable')
	}).pipe(
		Effect.provide(
			Slack.layer.pipe(
				Layer.provide(
					stubSlackClientLayer({
						replies: () =>
							Effect.fail(
								SlackApiError.make({ operation: 'conversations.replies', code: 'missing_scope' }),
							),
					}),
				),
			),
		),
	),
)
