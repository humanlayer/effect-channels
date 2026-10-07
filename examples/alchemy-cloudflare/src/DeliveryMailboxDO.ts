import * as Cloudflare from 'alchemy/Cloudflare'
import type { Effect } from 'effect'

import { bot } from './Bot'

/** The mailbox's RPC methods and alarm. */
type MailboxMethods = Effect.Success<Effect.Success<ReturnType<typeof bot.mailbox>>>

/** The application-owned mailbox Durable Object: one per Slack thread. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox, MailboxMethods>()('DeliveryMailbox') {}

/**
 * The mailbox's implementation. Its layer requires what the bot's callbacks need, such as `Crypto`
 * and the `FakeRemoteAgent` namespace; the host Worker provides them.
 */
export const DeliveryMailboxDOLive = DeliveryMailbox.make(bot.mailbox({ rearmAfterMs: 1_000 }))
