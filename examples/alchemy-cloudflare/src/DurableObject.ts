import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect } from 'effect'

import { bot } from './Bot'

/** The application-owned mailbox Durable Object: one per Slack thread. */
export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox>()(
	'DeliveryMailbox',
	Effect.succeed(bot.mailbox({ rearmAfterMs: 1_000 })),
) {}
