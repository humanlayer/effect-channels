/**
 * Application-owned Slack processor with placeholder callbacks.
 */
import { processProviderEvent, ProviderEventDispatcher } from '@humanlayer/channels-delivery-next'
import {
	makeSlackEventProcessor,
	SlackApi,
	SlackApiLive,
	SlackContent,
	SlackReaction,
	SlackSubscriptions,
} from '@humanlayer/channels-slack-next'
import { Effect, Layer, Predicate } from 'effect'

import { applicationNamespace } from './config'
import { SlackSubscriptionsDurableObject } from './SlackSubscriptions'

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

const ProviderEventDispatcherAlchemyCloudflareBase = Layer.effect(
	ProviderEventDispatcher,
	Effect.gen(function* () {
		const slackApi = yield* SlackApi
		const slackSubscriptions = yield* SlackSubscriptions

		return ProviderEventDispatcher.of({
			process: (admissions) =>
				processProviderEvent([
					{
						...slackProcessor,
						process: (batch) =>
							slackProcessor
								.process(batch)
								.pipe(
									Effect.provideService(SlackApi, slackApi),
									Effect.provideService(SlackSubscriptions, slackSubscriptions),
								),
					},
				])(admissions),
		})
	}),
)

export const SlackApiAlchemyCloudflare = SlackApiLive.pipe(
	Layer.tapError((error) => Effect.logError('Slack API configuration is invalid', error)),
	Layer.orDie,
)

export const ProviderEventDispatcherAlchemyCloudflare = ProviderEventDispatcherAlchemyCloudflareBase.pipe(
	Layer.provide(SlackApiAlchemyCloudflare),
	Layer.provide(SlackSubscriptionsDurableObject),
)
