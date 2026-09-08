import { NodeCrypto } from '@effect/platform-node'
import { subscriptions } from '@humanlayer/channels-github/memory'
import { SlackConnection, SlackConnectionCredentials, SlackTeamId } from '@humanlayer/channels-slack'
import { layer as storage } from '@humanlayer/channels-slack/memory'
import { Effect, Layer, Queue, Redacted } from 'effect'
import { FetchHttpClient, HttpClient, HttpClientRequest } from 'effect/unstable/http'

import {
	SlackEmulator,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorBotId,
} from '../../../packages/slack/test/integration/support/SlackEmulator.js'

export {
	SlackEmulator,
	HistoryResponse,
	PostedMessageResponse,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from '../../../packages/slack/test/integration/support/SlackEmulator.js'

export const makeExampleTestTransport = Effect.gen(function* () {
	const emulator = yield* SlackEmulator
	const posts = yield* Queue.unbounded<void>()
	const http = Layer.effect(
		HttpClient.HttpClient,
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient
			return HttpClient.make((request) =>
				client
					.execute(
						HttpClientRequest.setUrl(
							request,
							request.url.replace('https://slack.com/api', `${emulator.emulator.url}/api`),
						),
					)
					.pipe(
						Effect.tap(() =>
							request.url.endsWith('/chat.postMessage') ? Queue.offer(posts, undefined) : Effect.void,
						),
					),
			)
		}),
	).pipe(Layer.provide(FetchHttpClient.layer))
	const stores = storage({
		connections: [
			{
				workspaceId: SlackTeamId.make(emulator.teamId),
				connection: SlackConnection.make({
					credentials: SlackConnectionCredentials.make({
						botToken: Redacted.make(slackEmulatorBotToken),
						botUserId: slackEmulatorBotUserId,
						botId: slackEmulatorBotId,
					}),
				}),
			},
		],
	})
	const transport = Layer.mergeAll(stores, subscriptions(), http)
	return { transport: Layer.merge(transport, NodeCrypto.layer), posts }
})
