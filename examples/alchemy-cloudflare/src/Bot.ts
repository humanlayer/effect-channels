/**
 * The bot: its providers, callbacks and event processing settings.
 * The Worker and the Durable Object both build their half from this one value.
 */
import { ChannelsCloudflare } from '@humanlayer/channels-alchemy-cloudflare'
import { DebounceDeliveryMode } from '@humanlayer/channels-delivery-next'
import { SlackBot, SlackContent, SlackReaction } from '@humanlayer/channels-slack-next'
import { Config, Effect, Predicate } from 'effect'

/** Slack with placeholder callbacks. `SlackApiLive`, the default, reads `SLACK_BOT_TOKEN`. */
const slack = SlackBot.make({
	signingSecret: Config.redacted('SLACK_SIGNING_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({ quietPeriodMs: 2_000, maxWaitMs: 10_000 }),
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

export const bot = ChannelsCloudflare.make({
	namespace: 'alchemy-cloudflare-example',
	providers: [slack],
	eventProcessing: { concurrency: 1, maxAttempts: 5, leaseMs: 30_000 },
})
