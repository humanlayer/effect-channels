import * as Cloudflare from 'alchemy/Cloudflare'
import { Crypto, Effect, Layer } from 'effect'

import { AutoLabel } from './AutoLabel'
import { bot } from './Bot'

/** The mailbox's RPC methods and alarm. */
type MailboxMethods = Effect.Success<ReturnType<typeof bot.mailbox>>

/** The application-owned mailbox Durable Object: one per GitHub issue or pull request. */
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
	const autoLabel = yield* AutoLabel
	return { state, crypto, autoLabel }
}).pipe(
	Effect.map(({ state, crypto, autoLabel }) => {
		const MailboxInstanceLive = MailboxLive.pipe(
			Layer.provide(
				Layer.mergeAll(
					Layer.succeed(Cloudflare.DurableObjectState, state),
					Layer.succeed(Crypto.Crypto, crypto),
					Layer.succeed(AutoLabel, autoLabel),
				),
			),
		)
		return bot.mailbox({ rearmAfterMs: 1_000 }).pipe(Effect.provide(MailboxInstanceLive), Effect.orDie)
	}),
)

/**
 * The mailbox's implementation. The host Worker provides crypto and the Workers AI labeler
 * before this constructor captures the callbacks' services.
 */
export const DeliveryMailboxDOLive = DeliveryMailbox.make(MailboxImplementation)
