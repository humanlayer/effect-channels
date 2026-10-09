/**
 * This file defines the Durable Object alarm handler for a mailbox.
 *
 * The alarm is the only thing that wakes a Durable Object mailbox, and Cloudflare clears it before
 * the handler runs. Every store write puts it back, so a pass that writes nothing leaves the mailbox
 * with work due and nothing to wake it: a claim the storage refused, a look that failed, or an alarm
 * that fired a moment early. The handler therefore checks the alarm after its passes.
 *
 * An alarm written during the handler for a time already past does not survive it: seen live, a
 * delivery whose result arrived while an earlier output was being sent set the alarm to the result's
 * past `readyAt` when that output settled, and the mailbox never woke again. So the handler keeps
 * passing while a pass finds work, and when it ends it moves any alarm at or before now forward.
 */
import { MailboxProcessing, type MailboxProcessingOperations } from '@humanlayer/channels-delivery'
import { Cause, Clock, Effect, Predicate, Schema } from 'effect'

import { DurableMailboxState, mailboxStateKey } from './MailboxState'
import { MailboxStorage } from './MailboxStorage'

export type MailboxAlarmHandlerOptions = {
	/** How soon to look again at a mailbox that was due but could not be worked on. */
	readonly rearmAfterMs: number
}

/** The most processing passes one alarm runs. Work still due after them waits for the next alarm. */
export const MAX_PASSES_PER_ALARM = 20

/** Build the alarm handler over the current Durable Object's storage and its mailbox processing. */
export const makeMailboxAlarmHandlerWith = <R>(
	options: MailboxAlarmHandlerOptions,
	processing: MailboxProcessingOperations<R>,
) =>
	Effect.gen(function* () {
		const storage = yield* MailboxStorage

		/**
		 * Leave the mailbox with an alarm it will keep: when work is due and the alarm is missing, or set
		 * for a time already past, set it to `readyAt`, but no sooner than `rearmAfterMs` from now.
		 */
		const rearmWhenDueWithoutLiveAlarm = Effect.gen(function* () {
			const stored = yield* storage.get(mailboxStateKey)
			if (Predicate.isUndefined(stored)) return
			const { readyAt } = (yield* Schema.decodeUnknownEffect(DurableMailboxState)(stored)).deliveries
			if (Predicate.isNull(readyAt)) return
			const alarm = yield* storage.getAlarm
			const now = yield* Clock.currentTimeMillis
			if (Predicate.isNotNull(alarm) && alarm > now) return
			yield* storage.setAlarm(Math.max(readyAt, now + options.rearmAfterMs))
			yield* Effect.logWarning('Mailbox was due with no live alarm; alarm put back').pipe(
				Effect.annotateLogs({ ready_at: readyAt, alarm: alarm ?? 'none' }),
			)
		})

		/** One pass. Reports whether it found work, so the handler knows to look again. */
		const pass = processing.processReady.pipe(
			Effect.map(({ claimed, output }) => claimed > 0 || output > 0),
			Effect.catchCauseIf(
				(cause) => !Cause.hasInterruptsOnly(cause),
				(cause) => Effect.logError('Mailbox alarm pass failed', cause).pipe(Effect.as(false)),
			),
		)

		return Effect.fn('delivery.cloudflare.mailbox_alarm')(function* () {
			/** Work one step can make due at once, such as the output after a settled one, runs in this alarm. */
			for (let passes = 0; passes < MAX_PASSES_PER_ALARM; passes++) {
				if (!(yield* pass)) break
			}
			yield* rearmWhenDueWithoutLiveAlarm
		})
	})

export const makeMailboxAlarmHandler = (options: MailboxAlarmHandlerOptions) =>
	Effect.flatMap(MailboxProcessing, (processing) => makeMailboxAlarmHandlerWith(options, processing))
