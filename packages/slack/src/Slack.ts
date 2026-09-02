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
import { SlackThreadRef } from './Schema.ts'
import { SlackClient } from './SlackClient.ts'
import { encodeSlackThreadId } from './SlackThreadId.ts'

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
		Effect.gen(function* () {
			const client = yield* SlackClient
			const setSessionStatus = Effect.fn('slack.set_session_status')(function* (input: SlackSessionStatusInput) {
				const threadId = encodeSlackThreadId(
					SlackThreadRef.make({
						teamId: input.teamId,
						channelId: input.channelId,
						threadTs: input.threadTs,
					}),
				)
				yield* Effect.annotateCurrentSpan({
					provider: 'slack',
					tenant: input.teamId,
					thread_id: threadId,
					operation: 'set_session_status',
				})
				return yield* client.setSessionStatus(input).pipe(
					Effect.tapError((error) =>
						Effect.logError('Slack session status failed', error).pipe(
							Effect.annotateLogs({ provider: 'slack', thread_id: threadId }),
						),
					),
					Effect.catchTags({
						SlackTransportError: () =>
							Effect.fail(
								StatusFailed.make({
									provider: 'slack',
									threadId,
									message: 'Slack transport failed',
								}),
							),
						SlackApiError: () =>
							Effect.fail(
								StatusFailed.make({
									provider: 'slack',
									threadId,
									message: 'Slack API rejected the session status',
								}),
							),
					}),
				)
			})
			return Slack.of({
				createThread: () => unimplemented('Slack.createThread'),
				post: () => unimplemented('Slack.post'),
				setSessionStatus,
				postEphemeral: () => unimplemented('Slack.postEphemeral'),
				api: () => unimplemented('Slack.api'),
			})
		}),
	)
}
