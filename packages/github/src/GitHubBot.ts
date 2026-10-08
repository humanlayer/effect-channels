/**
 * This file defines `GitHubBot.make`: the GitHub provider as `Channels.make` takes it.
 */
import {
	type ChannelsProvider,
	type DeliveryMode,
	type DeliveryAdmissionBatch,
	type ProviderDeliveryExecution,
} from '@humanlayer/channels-delivery'
import { Effect, Layer, Predicate, Schema } from 'effect'
import type { Config, Redacted } from 'effect'

import { GitHubApi } from './GitHubApi'
import { GitHubApiLive } from './GitHubApiLive'
import { GitHubCallbacks, type GitHubCallbackHandlers, makeGitHubCallbacks } from './GitHubCallbacks'
import { makeGitHubOutputProcessor } from './GitHubDeliveryOutput'
import { GitHubBotConfiguration, makeGitHubEventProcessor } from './GitHubEventProcessor'
import { makeGitHubWebhookProvider } from './GitHubWebhookProvider'

/**
 * @property webhookSecret - read when the bot is built, so the bot itself can be declared at the top of a module
 * @property deliveryMode - when an issue's or pull request's mailbox runs and what each batch holds
 * @property handlers - the callbacks; the bot supplies `GitHubApi` to them, the storage supplies
 * `MailboxSubscriptions`, and anything else they need comes from the application
 * @property bot - the names and user id that count as a mention of this bot
 * @property gitHubApi - how the bot talks to GitHub; defaults to `GitHubApiLive`, which reads the GitHub App
 * id and private key from the configuration
 */
export type MakeOptions<E, R, ApiError, ApiRequirements> = {
	readonly webhookSecret: Config.Config<Redacted.Redacted<string>>
	readonly deliveryMode: DeliveryMode
	readonly bot: GitHubBotConfiguration | Config.Config<GitHubBotConfiguration>
	readonly handlers: GitHubCallbackHandlers<E, R>
	readonly gitHubApi?: Layer.Layer<GitHubApi, ApiError, ApiRequirements>
}

export const make = <E, R, ApiError = never, ApiRequirements = never>(
	options: MakeOptions<E, R, ApiError, ApiRequirements>,
): ChannelsProvider<{
	readonly build: ApiRequirements
	readonly process: Exclude<R, GitHubApi>
	readonly runtime: R | GitHubApi
	readonly error: Config.ConfigError | ApiError
}> => {
	const callbacks = GitHubCallbacks.layer(options.handlers)
	const readBotConfiguration = Schema.is(GitHubBotConfiguration)(options.bot)
		? Effect.succeed(options.bot)
		: options.bot
	const buildGitHubApi = Predicate.isUndefined(options.gitHubApi)
		? Layer.build(GitHubApiLive)
		: Layer.build(options.gitHubApi)

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
			yield* buildGitHubApi
			return makeGitHubWebhookProvider({ namespace, webhookSecret })
		}),
		eventProcessor: Effect.fn('github.bot.build_event_processor')(function* ({ namespace }) {
			const bot = yield* readBotConfiguration
			const gitHubApi = yield* buildGitHubApi
			const eventProcessor = makeGitHubEventProcessor({ namespace, bot })
			return {
				namespace: eventProcessor.namespace,
				providerName: eventProcessor.providerName,
				/** The callbacks are wrapped per batch because they read the services of the running batch. */
				process: (admissions: DeliveryAdmissionBatch, execution: ProviderDeliveryExecution) =>
					eventProcessor
						.process(admissions, execution)
						.pipe(Effect.provide(callbacks), Effect.provide(gitHubApi)),
			}
		}),
		/** Sends a handed-off delivery's output: comments, and the `eyes` reaction while it works. */
		outputProcessor: Effect.fn('github.bot.build_output_processor')(function* ({ namespace }) {
			const gitHubApi = yield* buildGitHubApi
			const outputProcessor = yield* makeGitHubOutputProcessor({ namespace })
			return {
				...outputProcessor,
				process: (attempt) => outputProcessor.process(attempt).pipe(Effect.provide(gitHubApi)),
			}
		}),
		runtimeEventProcessor: Effect.fn('github.bot.build_runtime_event_processor')(function* ({ namespace }) {
			const bot = yield* readBotConfiguration
			return makeGitHubEventProcessor({ namespace, bot }, makeGitHubCallbacks(options.handlers))
		}),
		runtimeOutputProcessor: ({ namespace }) => makeGitHubOutputProcessor({ namespace }),
	}
}
