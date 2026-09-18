/**
 * Application-owned Slack processor with placeholder callbacks.
 */
import { ProviderEventDispatcherLive } from '@humanlayer/channels-delivery-next'
import { makeSlackEventProcessor, SlackApiLive, SlackContent, SlackReaction } from '@humanlayer/channels-slack-next'
import { Effect, Layer, Predicate } from 'effect'

import { applicationNamespace } from './config'

/**
 * Application-owned Slack processor with placeholder callbacks.
 */
const slackProcessor = makeSlackEventProcessor({
	namespace: applicationNamespace,
	handlers: {
		onNewMention: (event) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Slack new mention received').pipe(
					Effect.annotateLogs({
						team_id: event.thread.ref.teamId,
						channel_id: event.thread.ref.channelId,
						thread_ts: event.thread.ref.threadTs,
						event_count: event.events.length + 1,
					}),
				)
				if (!(yield* event.thread.isSubscribed())) yield* event.thread.subscribe()
				yield* event.thread.startTyping()
				yield* Effect.sleep(2_000)
				yield* event.thread.post(
					SlackContent.make({ markdown: 'Subscribed! subsequent messages will be logged' }),
				)
			}),
		onSubscribedThreadEvents: (event) =>
			Effect.gen(function* () {
				yield* Effect.logInfo('Slack subscribed thread events received').pipe(
					Effect.annotateLogs({
						team_id: event.thread.ref.teamId,
						channel_id: event.thread.ref.channelId,
						thread_ts: event.thread.ref.threadTs,
						event_count: event.events.length,
					}),
				)
				for (const threadEvent of event.events) {
					if (Predicate.isTagged(threadEvent, 'SlackMessageReceived')) {
						yield* threadEvent.message.addReaction(SlackReaction.make('eyes'))
					}
				}
			}),
	},
})

export const SlackApiAlchemyCloudflare = SlackApiLive.pipe(
	Layer.tapError((error) => Effect.logError('Slack API configuration is invalid', error)),
	Layer.orDie,
)

export const ProviderEventDispatcherSlack = ProviderEventDispatcherLive([slackProcessor]).pipe(
	Layer.provide(SlackApiAlchemyCloudflare),
)
