/**
 * This file defines `GitHubBot.make`: the GitHub provider as `Channels.make` takes it.
 */
import {
	ChannelsProviderUnavailable,
	type ChannelsProvider,
	type DeliveryMode,
	type DeliveryAdmissionBatch,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Predicate, Schema } from 'effect'
import type { Config, Redacted } from 'effect'

import { GitHubApi } from './GitHubApi'
import { GitHubApiLive } from './GitHubApiLive'
import { GitHubCallbacks, type GitHubCallbackHandlers } from './GitHubCallbacks'
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

const unavailable = <A, E, R>(step: string, effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError((error) =>
			Effect.logError('GitHub bot could not be built', error).pipe(Effect.annotateLogs({ step })),
		),
		Effect.mapError(() => ChannelsProviderUnavailable.make({ provider: 'github' })),
	)

export const make = <E, R, ApiError = never, ApiRequirements = never>(
	options: MakeOptions<E, R, ApiError, ApiRequirements>,
): ChannelsProvider<{ readonly build: ApiRequirements; readonly process: Exclude<R, GitHubApi> }> => {
	const callbacks = GitHubCallbacks.layer(options.handlers)
	const readBotConfiguration = Schema.is(GitHubBotConfiguration)(options.bot)
		? Effect.succeed(options.bot)
		: unavailable('read_bot_configuration', options.bot)
	const buildGitHubApi = Predicate.isUndefined(options.gitHubApi)
		? unavailable('build_github_api', Layer.build(GitHubApiLive))
		: unavailable('build_github_api', Layer.build(options.gitHubApi))

	return {
		providerName: 'github',
		deliveryMode: options.deliveryMode,
		webhookProvider: ({ namespace }) =>
			Effect.gen(function* () {
				const webhookSecret = yield* unavailable('read_webhook_secret', options.webhookSecret)
				/**
				 * Split hosts build only this half in the Worker construction phase. Resolve the callback half's
				 * configuration here too, so Alchemy can discover and bind it for the Durable Object runtime.
				 */
				yield* readBotConfiguration
				yield* buildGitHubApi
				return makeGitHubWebhookProvider({ namespace, webhookSecret })
			}).pipe(Effect.withSpan('github.bot.build_webhook_provider')),
		eventProcessor: ({ namespace }) =>
			Effect.gen(function* () {
				const bot = yield* readBotConfiguration
				const gitHubApi = yield* buildGitHubApi
				const eventProcessor = makeGitHubEventProcessor({ namespace, bot })
				return {
					namespace: eventProcessor.namespace,
					providerName: eventProcessor.providerName,
					/** The callbacks are wrapped per batch because they read the services of the running batch. */
					process: (admissions: DeliveryAdmissionBatch) =>
						eventProcessor.process(admissions).pipe(Effect.provide(callbacks), Effect.provide(gitHubApi)),
				}
			}).pipe(Effect.withSpan('github.bot.build_event_processor')),
	}
}
