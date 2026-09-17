import { Effect, Layer, Predicate, type Redacted, Schema } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import { SlackMessageTs } from './SlackIdentity'
import { SlackReactionThreadResolutionUnavailable, SlackReactionThreadResolver } from './SlackReactionThreadResolver'

const SlackHistoryResponse = Schema.Struct({
	ok: Schema.Boolean,
	error: Schema.optionalKey(Schema.String),
	messages: Schema.optionalKey(
		Schema.Array(
			Schema.Struct({
				ts: SlackMessageTs,
				thread_ts: Schema.optionalKey(SlackMessageTs),
			}),
		),
	),
})

const unavailable = (reason: 'transport' | 'slack_api' | 'invalid_response') =>
	new SlackReactionThreadResolutionUnavailable({ reason })

/** Finds the thread root for the message referenced by a Slack reaction event. */
export const SlackReactionThreadResolverLive = (botToken: Redacted.Redacted<string>) =>
	Layer.effect(
		SlackReactionThreadResolver,
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient

			return SlackReactionThreadResolver.of({
				resolve: Effect.fn('slack.reaction.resolve_thread')(function* (input) {
					const request = HttpClientRequest.get('https://slack.com/api/conversations.history', {
						urlParams: {
							channel: input.channelId,
							oldest: input.messageTs,
							latest: input.messageTs,
							inclusive: true,
							limit: 1,
						},
					}).pipe(HttpClientRequest.bearerToken(botToken))
					const response = yield* client
						.execute(request)
						.pipe(Effect.mapError(() => unavailable('transport')))
					if (response.status < 200 || response.status >= 300) {
						return yield* unavailable('slack_api')
					}
					const body = yield* response.json.pipe(
						Effect.flatMap(Schema.decodeUnknownEffect(SlackHistoryResponse)),
						Effect.mapError(() => unavailable('invalid_response')),
					)
					if (!body.ok) return yield* unavailable('slack_api')
					const message = body.messages?.find(({ ts }) => ts === input.messageTs)
					if (Predicate.isUndefined(message)) return yield* unavailable('invalid_response')
					return message.thread_ts ?? message.ts
				}),
			})
		}),
	).pipe(Layer.provide(FetchHttpClient.layer))
