import { NodeCrypto } from '@effect/platform-node'
import {
	SlackClient,
	SlackTenantCredentials,
	SlackConnection,
	SlackConnectionCredentials,
} from '@humanlayer/channels-slack'
import { ConfigProvider, Effect, Layer, Queue, Redacted } from 'effect'
import { FetchHttpClient, HttpClient } from 'effect/unstable/http'

import {
	SlackEmulator,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorBotId,
	slackEmulatorSigningSecret,
} from '../../../packages/slack/test/integration/support/SlackEmulator.ts'

export {
	SlackEmulator,
	HistoryResponse,
	PostedMessageResponse,
	OpenConversationResponse,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
} from '../../../packages/slack/test/integration/support/SlackEmulator.ts'

export const makeExampleTestTransport = Effect.gen(function* () {
	const emulator = yield* SlackEmulator
	const posts = yield* Queue.unbounded<void>()
	const http = Layer.effect(
		HttpClient.HttpClient,
		Effect.gen(function* () {
			const client = yield* HttpClient.HttpClient
			return HttpClient.make((request) =>
				client
					.execute(request)
					.pipe(
						Effect.tap(() =>
							request.url.endsWith('/chat.postMessage') ? Queue.offer(posts, undefined) : Effect.void,
						),
					),
			)
		}),
	).pipe(Layer.provide(FetchHttpClient.layer))
	const credentials = SlackTenantCredentials.layerWithLookup({
		loadConnection: ({ workspaceId }) =>
			Effect.succeed(
				workspaceId === emulator.teamId
					? SlackConnection.make({
							credentials: SlackConnectionCredentials.make({
								botToken: Redacted.make(slackEmulatorBotToken),
								botUserId: slackEmulatorBotUserId,
								botId: slackEmulatorBotId,
							}),
						})
					: undefined,
			),
	})
	const transport = SlackClient.layerWith({ apiOrigin: new URL(`${emulator.emulator.url}/api`) }).pipe(
		Layer.provideMerge(credentials),
		Layer.provide(http),
	)
	const config = ConfigProvider.layer(
		ConfigProvider.fromUnknown({ SLACK_SIGNING_SECRET: slackEmulatorSigningSecret }),
	)
	return { transport: Layer.merge(transport, NodeCrypto.layer), config, posts }
})
