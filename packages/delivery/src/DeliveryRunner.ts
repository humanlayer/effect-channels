import { Clock, Effect, Schema } from 'effect'

import { MailboxReadiness } from './MailboxStore'

export const DeliveryPassOptions = Schema.Struct({
	scanLimit: Schema.Int.check(Schema.isGreaterThan(0)),
	concurrency: Schema.Int.check(Schema.isGreaterThan(0)),
})
export interface DeliveryPassOptions extends Schema.Schema.Type<typeof DeliveryPassOptions> {}

/** @internal Shared finite ready-mailbox pass used by direct and polling execution. */
export const runDeliveryPass = Effect.fn('delivery.runner.pass')(function* <A, E, R>(input: {
	readonly prefix: string
	readonly options: DeliveryPassOptions
	readonly process: (key: string) => Effect.Effect<A, E, R>
}) {
	const readiness = yield* MailboxReadiness
	const now = yield* Clock.currentTimeMillis
	const keys = yield* readiness.scanReady({ prefix: input.prefix, now, limit: input.options.scanLimit })
	return yield* Effect.forEach(keys, input.process, { concurrency: input.options.concurrency })
})
