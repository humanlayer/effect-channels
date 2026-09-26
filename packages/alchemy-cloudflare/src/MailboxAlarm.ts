/**
 * This file defines the Durable Object alarm handler for a mailbox.
 *
 * The alarm is the only thing that wakes a Durable Object mailbox, and Cloudflare clears it before
 * the handler runs. Every store write puts it back, so a pass that writes nothing leaves the mailbox
 * with work due and nothing to wake it: a claim the storage refused, a look that failed, or an alarm
 * that fired a moment early. The handler therefore checks the alarm after every pass.
 */
import { MailboxProcessing } from '@humanlayer/channels-delivery-next'
import { Cause, Clock, Effect, Predicate, Schema } from 'effect'

import { DurableMailboxState, mailboxStateKey } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'

export type MailboxAlarmHandlerOptions = {
	/** How soon to look again at a mailbox that was due but could not be worked on. */
	readonly rearmAfterMs: number
}

/** Build the alarm handler over the current Durable Object's storage and its mailbox processing. */
export const makeMailboxAlarmHandler = (options: MailboxAlarmHandlerOptions) =>
	Effect.gen(function* () {
		const processing = yield* MailboxProcessing
		const storage = yield* MailboxStorage

		const rearmWhenDueWithoutAlarm = Effect.gen(function* () {
			const stored = yield* storage.get(mailboxStateKey)
			if (Predicate.isUndefined(stored)) return
			const { readyAt } = yield* Schema.decodeUnknownEffect(DurableMailboxState)(stored)
			if (Predicate.isNull(readyAt)) return
			if (Predicate.isNotNull(yield* storage.getAlarm)) return
			const now = yield* Clock.currentTimeMillis
			yield* storage.setAlarm(Math.max(readyAt, now + options.rearmAfterMs))
			yield* Effect.logWarning('Mailbox was due with no alarm set; alarm put back').pipe(
				Effect.annotateLogs({ ready_at: readyAt }),
			)
		})

		return Effect.fn('delivery.cloudflare.mailbox_alarm')(function* () {
			yield* processing.processReady.pipe(
				Effect.asVoid,
				Effect.catchCauseIf(
					(cause) => !Cause.hasInterruptsOnly(cause),
					(cause) => Effect.logError('Mailbox alarm pass failed', cause),
				),
			)
			yield* rearmWhenDueWithoutAlarm
		})
	})
