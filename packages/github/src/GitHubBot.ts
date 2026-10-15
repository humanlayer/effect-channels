/**
 * This file defines `GitHubBot.make`: the GitHub provider as `Channels.make` takes it.
 */
import { type ChannelsProvider, type DeliveryMode } from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'
import type { Config, Redacted } from 'effect'

import type { GitHubApi } from './GitHubApi'
import { type GitHubCallbackHandlers } from './GitHubCallbacks'
import { makeGitHubOutputProcessor } from './GitHubDeliveryOutput'
import { GitHubBotConfiguration, makeGitHubEventProcessor } from './GitHubEventProcessor'
import { makeGitHubWebhookProvider } from './GitHubWebhookProvider'

/**
 * @property webhookSecret - read when the bot is built, so the bot itself can be declared at the top of a module
 * @property deliveryMode - when an issue's or pull request's mailbox runs and what each batch holds
 * @property handlers - the callbacks; the application provides `GitHubApi` (usually `GitHubApiLive`) and
 * anything else they need, and the storage supplies `MailboxSubscriptions`
 * @property bot - the names and user id that count as a mention of this bot
 */
export type MakeOptions<E, R> = {
	readonly webhookSecret: Config.Config<Redacted.Redacted<string>>
	readonly deliveryMode: DeliveryMode
	readonly bot: GitHubBotConfiguration | Config.Config<GitHubBotConfiguration>
	readonly handlers: GitHubCallbackHandlers<E, R>
}

export const make = <E, R>(
	options: MakeOptions<E, R>,
): ChannelsProvider<{
	readonly build: never
	readonly process: R | GitHubApi
	readonly error: Config.ConfigError
}> => {
	const readBotConfiguration = Schema.is(GitHubBotConfiguration)(options.bot)
		? Effect.succeed(options.bot)
		: options.bot

	return {
		providerName: 'github',
		deliveryMode: options.deliveryMode,
		webhookProvider: Effect.fn('github.bot.build_webhook_provider')(function* ({ namespace }) {
			const webhookSecret = yield* options.webhookSecret
			/**
			 * Split hosts build only this half in the Worker construction phase. Resolve the callback half's
			 * configuration here too, so Alchemy can discover and bind it for the Durable Object runtime.
			 */
			yield* readBotConfiguration
			return makeGitHubWebhookProvider({ namespace, webhookSecret })
		}),
		eventProcessor: Effect.fn('github.bot.build_event_processor')(function* ({ namespace }) {
			const bot = yield* readBotConfiguration
			return makeGitHubEventProcessor({ namespace, bot, handlers: options.handlers })
		}),
		/** Sends a handed-off delivery's output: comments, and the `eyes` reaction while it works. */
		outputProcessor: ({ namespace }) => makeGitHubOutputProcessor({ namespace }),
	}
}
