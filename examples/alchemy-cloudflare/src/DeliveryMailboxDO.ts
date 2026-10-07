import * as Cloudflare from 'alchemy/Cloudflare'
import { Crypto, Effect, Layer } from 'effect'

import { bot } from './Bot'

/** The mailbox's RPC methods and alarm. */
type MailboxMethods = Effect.Success<ReturnType<typeof bot.mailbox>>

/** The application-owned mailbox Durable Object: one per Slack thread. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox, MailboxMethods>()('DeliveryMailbox') {}

const { processing, deliveryControl, processingBackend, subscriptions, deliveryControlBackend, storage } =
	bot.layers.mailbox

/** The mailbox services for each Durable Object instance. Replace any leaf layer here in tests. */
const MailboxLive = Layer.merge(processing, deliveryControl).pipe(
	Layer.provide(Layer.mergeAll(processingBackend, subscriptions, deliveryControlBackend)),
	Layer.provideMerge(storage),
)

/**
 * Alchemy's outer phase resolves this instance's state reference and the shared Worker services.
 * Its inner phase receives RuntimeContext and builds the handlers for this Durable Object instance.
 */
const MailboxImplementation = Effect.gen(function* () {
	const state = yield* Cloudflare.DurableObjectState
	const crypto = yield* Crypto.Crypto
	return { state, crypto }
}).pipe(
	Effect.map(({ state, crypto }) => {
		const MailboxInstanceLive = MailboxLive.pipe(
			Layer.provide(
				Layer.mergeAll(
					Layer.succeed(Cloudflare.DurableObjectState, state),
					Layer.succeed(Crypto.Crypto, crypto),
				),
			),
		)
		return bot.mailbox({ rearmAfterMs: 1_000 }).pipe(Effect.provide(MailboxInstanceLive), Effect.orDie)
	}),
)

/**
 * The mailbox's implementation. Its layer requires what the bot's callbacks need, such as `Crypto`
 * and the `FakeRemoteAgent` namespace; the host Worker provides them.
 */
export const DeliveryMailboxDOLive = DeliveryMailbox.make(MailboxImplementation)
