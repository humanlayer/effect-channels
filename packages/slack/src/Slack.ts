import { PostFailed, SentMessage, StatusFailed, Thread, UnknownTenant, unimplemented } from '@humanlayer/channels'
import { Context, Effect, Layer } from 'effect'

import type { SlackApiError, SlackTransportError } from './Errors.ts'
import type {
	SlackApiInput,
	SlackApiResponse,
	SlackCreateThreadInput,
	SlackEphemeralInput,
	SlackNativePostInput,
	SlackSessionStatusInput,
} from './Schema.ts'
import { SlackClient } from './SlackClient.ts'

export class Slack extends Context.Service<
	Slack,
	{
		readonly createThread: (input: SlackCreateThreadInput) => Effect.Effect<Thread, UnknownTenant | PostFailed>
		readonly post: (input: SlackNativePostInput) => Effect.Effect<SentMessage, UnknownTenant | PostFailed>
		readonly setSessionStatus: (input: SlackSessionStatusInput) => Effect.Effect<void, UnknownTenant | StatusFailed>
		readonly postEphemeral: (input: SlackEphemeralInput) => Effect.Effect<SentMessage, UnknownTenant | PostFailed>
		readonly api: (
			input: SlackApiInput,
		) => Effect.Effect<SlackApiResponse, UnknownTenant | SlackTransportError | SlackApiError>
	}
>()('channels/Slack') {
	static readonly layer = Layer.effect(
		Slack,
		Effect.map(SlackClient, () =>
			Slack.of({
				createThread: () => unimplemented('Slack.createThread'),
				post: () => unimplemented('Slack.post'),
				setSessionStatus: () => unimplemented('Slack.setSessionStatus'),
				postEphemeral: () => unimplemented('Slack.postEphemeral'),
				api: () => unimplemented('Slack.api'),
			}),
		),
	)
}
