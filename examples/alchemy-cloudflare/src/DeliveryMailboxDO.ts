import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer } from 'effect'

import { bot } from './GithubBot'

/** The mailbox's RPC methods and alarm. */
type MailboxMethods = Effect.Success<ReturnType<typeof bot.mailbox>>

/** The application-owned mailbox Durable Object: one per GitHub issue or pull request. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox, MailboxMethods>()('DeliveryMailbox') {}

const { deliveryControl, processingBackend, subscriptions, deliveryControlBackend, storage } = bot.layers.mailbox

/** The mailbox services for each Durable Object instance. Replace any leaf layer here in tests. */
const MailboxBackendsLive = Layer.mergeAll(processingBackend, subscriptions, deliveryControlBackend).pipe(
	Layer.provideMerge(storage),
)
const MailboxLive = Layer.merge(deliveryControl, MailboxBackendsLive).pipe(
	Layer.provide(MailboxBackendsLive),
)

/**
 * Construct the mailbox once for this Durable Object instance. Alchemy supplies RuntimeContext when
 * it evaluates the returned inner Effect and when it runs the returned methods.
 */
const MailboxImplementation = bot.mailbox({ rearmAfterMs: 1_000 }).pipe(
	Effect.provide(MailboxLive),
	Effect.map(Effect.succeed),
	Effect.orDie,
)

/**
 * The mailbox's implementation. The Worker entrypoint provides MailboxLive's leaf dependencies.
 */
export const DeliveryMailboxDOLive = DeliveryMailbox.make(MailboxImplementation)
