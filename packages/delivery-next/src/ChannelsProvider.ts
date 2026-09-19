/**
 * This file defines what a provider package hands to `Channels.make`.
 *
 * A provider such as Slack or GitHub has two halves: one that checks and parses webhooks, and one that
 * runs the application's callbacks. Each half arrives with the provider's own services, such as its API
 * client, already supplied. What it still needs comes from the host: `Crypto` for the webhook half and
 * `MailboxSubscriptions` for the callback half.
 */
import { Schema } from 'effect'
import type { Crypto, Effect, Scope } from 'effect'

import type { DeliveryMode } from './MailboxPolicy'
import type { MailboxSubscriptions } from './MailboxSubscriptions'
import type { ProviderEventProcessor } from './ProviderEventProcessing'
import type { WebhookProvider } from './ProviderWebhooks'

/** The provider could not be built, for example because a secret is missing from the configuration. */
export class ChannelsProviderUnavailable extends Schema.TaggedError<ChannelsProviderUnavailable>()(
	'ChannelsProviderUnavailable',
	{ provider: Schema.NonEmptyString },
) {}

export type ChannelsProviderBuildInput = {
	readonly namespace: string
}

/**
 * What a provider needs from the application.
 *
 * @property build - needed to build either half, such as what a custom API layer requires
 * @property process - needed by the application's callbacks while a batch runs
 */
export type ChannelsProviderRequirements = {
	readonly build: unknown
	readonly process: unknown
}

/**
 * One provider as `Channels.make` sees it.
 *
 * The halves are built apart because some hosts run them apart: a Cloudflare Worker builds only
 * the webhook half, and its Durable Object builds only the callback half.
 *
 * @property deliveryMode - when this provider's mailboxes run and what each batch holds
 * @property webhookProvider - reads the provider's configuration and builds the half that checks webhooks
 * @property eventProcessor - builds the half that runs the application's callbacks
 */
export type ChannelsProvider<
	R extends ChannelsProviderRequirements = { readonly build: never; readonly process: never },
> = {
	readonly providerName: string
	readonly deliveryMode: DeliveryMode
	readonly webhookProvider: (
		input: ChannelsProviderBuildInput,
	) => Effect.Effect<WebhookProvider<Crypto.Crypto>, ChannelsProviderUnavailable, R['build'] | Scope.Scope>
	readonly eventProcessor: (
		input: ChannelsProviderBuildInput,
	) => Effect.Effect<
		ProviderEventProcessor<R['process'] | MailboxSubscriptions>,
		ChannelsProviderUnavailable,
		R['build'] | Scope.Scope
	>
}
