/**
 * This file defines what a provider package hands to `Channels.make`.
 *
 * A provider such as Slack or GitHub has two halves: one that checks and parses webhooks, and one that
 * runs the application's callbacks. A provider may also send saved output, such as a handed-off
 * delivery's final message; that half runs beside the callback half.
 *
 * A provider builds none of its own services. What each half needs, such as the provider's API client,
 * stays in its requirements, and the application provides it once.
 */
import type { Crypto, Effect, Scope } from 'effect'

import type { DeliveryMode } from './MailboxPolicy'
import type { MailboxSubscriptions } from './MailboxSubscriptions'
import type { ProviderEventProcessor } from './ProviderEventProcessing'
import type { ProviderOutputProcessor } from './ProviderOutput'
import type { WebhookProvider } from './ProviderWebhooks'

export type ChannelsProviderBuildInput = {
	readonly namespace: string
}

/**
 * What a provider needs from the application, and how building it can fail.
 *
 * @property build - needed to build any half, such as what the webhook half calls while it checks a webhook
 * @property process - needed while a batch's callbacks run or output is sent, such as the provider's API
 * client and whatever the application's callbacks need
 * @property error - why building a half failed, such as a `ConfigError` for a missing secret
 */
export type ChannelsProviderRequirements = {
	readonly build: unknown
	readonly process: unknown
	readonly error: unknown
}

/**
 * One provider as `Channels.make` sees it.
 *
 * The halves are built apart because some hosts run them apart: a Cloudflare Worker builds only
 * the webhook half, and its Durable Object builds only the callback and output halves. The callback
 * and output halves leave `process` in their requirements, so a host can supply it once when it
 * builds processing, or, like a Durable Object alarm, while each call runs.
 *
 * @property deliveryMode - when this provider's mailboxes run and what each batch holds
 * @property webhookProvider - reads the provider's configuration and builds the half that checks webhooks
 * @property eventProcessor - builds the half that runs the application's callbacks
 * @property outputProcessor - builds the half that sends saved output. A provider without one fails
 * its output, but its deliveries still end.
 */
export type ChannelsProvider<
	R extends ChannelsProviderRequirements = { readonly build: never; readonly process: never; readonly error: never },
> = {
	readonly providerName: string
	readonly deliveryMode: DeliveryMode
	readonly webhookProvider: (
		input: ChannelsProviderBuildInput,
	) => Effect.Effect<WebhookProvider<Crypto.Crypto>, R['error'], R['build'] | Scope.Scope>
	readonly eventProcessor: (
		input: ChannelsProviderBuildInput,
	) => Effect.Effect<
		ProviderEventProcessor<R['process'] | MailboxSubscriptions>,
		R['error'],
		R['build'] | Scope.Scope
	>
	readonly outputProcessor?: (
		input: ChannelsProviderBuildInput,
	) => Effect.Effect<ProviderOutputProcessor<R['process']>, R['error'], R['build'] | Scope.Scope>
}
