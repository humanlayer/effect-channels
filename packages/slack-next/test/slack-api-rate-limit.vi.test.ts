import { describe, it } from '@effect/vitest'
import { ConfigProvider, Effect, Fiber, Layer, Ref } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { SlackApi } from '../src/SlackApi'
import { SlackApiLiveBase } from '../src/SlackApiLive'
import { SlackChannelId, SlackTeamId } from '../src/SlackIdentity'
import { SlackChannelRef } from '../src/SlackModels'

const channel = SlackChannelRef.make({
	teamId: SlackTeamId.make('T_RATE_LIMIT'),
	channelId: SlackChannelId.make('C_RATE_LIMIT'),
	isDm: false,
})

const configLayer = ConfigProvider.layer(
	ConfigProvider.fromUnknown({
		SLACK_BOT_TOKEN: 'xoxb-test-token',
		SLACK_BOT_USER_ID: 'U_BOT',
	}),
)

describe('SlackApi rate limits', () => {
	it.effect('waits for Retry-After before retrying a 429 response', ({ expect }) =>
		Effect.gen(function* () {
			const attempts = yield* Ref.make(0)
			const httpClient = HttpClient.make((request) =>
				Ref.updateAndGet(attempts, (count) => count + 1).pipe(
					Effect.map((attempt) =>
						HttpClientResponse.fromWeb(
							request,
							attempt === 1
								? Response.json(
										{ ok: false, error: 'ratelimited' },
										{ status: 429, headers: { 'retry-after': '2' } },
									)
								: Response.json({
										ok: true,
										channel: { id: channel.channelId, name: 'rate-limit-test', num_members: 3 },
									}),
						),
					),
				),
			)
			const layer = SlackApiLiveBase.pipe(
				Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), configLayer)),
			)
			const operation = Effect.flatMap(SlackApi, (api) => api.getChannelInfo({ channel })).pipe(
				Effect.provide(layer),
			)

			const fiber = yield* Effect.forkChild(operation)
			yield* TestClock.adjust('2 seconds')
			const result = yield* Fiber.join(fiber)

			expect(result.name).toBe('rate-limit-test')
			expect(yield* Ref.get(attempts)).toBe(2)
		}),
	)

	it.effect('does not retry a non-rate-limit HTTP failure', ({ expect }) =>
		Effect.gen(function* () {
			const attempts = yield* Ref.make(0)
			const httpClient = HttpClient.make((request) =>
				Ref.update(attempts, (count) => count + 1).pipe(
					Effect.as(HttpClientResponse.fromWeb(request, Response.json({}, { status: 500 }))),
				),
			)
			const layer = SlackApiLiveBase.pipe(
				Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), configLayer)),
			)
			const error = yield* Effect.flip(
				Effect.flatMap(SlackApi, (api) => api.getChannelInfo({ channel })).pipe(Effect.provide(layer)),
			)

			expect(error.message).toBe('Slack returned HTTP 500')
			expect(yield* Ref.get(attempts)).toBe(1)
		}),
	)

	it.effect('stops after three rate-limit retries', ({ expect }) =>
		Effect.gen(function* () {
			const attempts = yield* Ref.make(0)
			const httpClient = HttpClient.make((request) =>
				Ref.update(attempts, (count) => count + 1).pipe(
					Effect.as(
						HttpClientResponse.fromWeb(
							request,
							Response.json(
								{ ok: false, error: 'ratelimited' },
								{ status: 429, headers: { 'retry-after': '1' } },
							),
						),
					),
				),
			)
			const layer = SlackApiLiveBase.pipe(
				Layer.provide(Layer.merge(Layer.succeed(HttpClient.HttpClient, httpClient), configLayer)),
			)
			const operation = Effect.flip(
				Effect.flatMap(SlackApi, (api) => api.getChannelInfo({ channel })).pipe(Effect.provide(layer)),
			)

			const fiber = yield* Effect.forkChild(operation)
			yield* TestClock.adjust('4 seconds')
			const error = yield* Fiber.join(fiber)

			expect(error.message).toBe('Slack rate limit persisted after retries')
			expect(yield* Ref.get(attempts)).toBe(4)
		}),
	)
})
