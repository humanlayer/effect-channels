/**
 * This file defines delivery modes and the one function that applies them.
 *
 * A delivery mode answers two questions about a mailbox with waiting events:
 * is it time to run yet, and which waiting events form the next batch?
 *
 * Nothing here touches storage. The stores report what is waiting; `decideMailboxClaim`
 * decides; the stores then claim exactly what they were told to claim.
 */
import { Data, Match, Schema } from 'effect'

export const Timestamp = Schema.Finite.pipe(Schema.brand('Timestamp'))
export type Timestamp = typeof Timestamp.Type

const PositiveMilliseconds = Schema.Int.check(Schema.isGreaterThan(0)).check(
	Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)

/** Run as soon as anything is waiting, and take everything that is waiting. */
export const QueueDeliveryMode = Schema.TaggedStruct('Queue', {})

/** Run as soon as anything is waiting, and take only the oldest waiting event. */
export const SerialDeliveryMode = Schema.TaggedStruct('Serial', {})

/**
 * Wait until events stop arriving, then take everything that is waiting.
 *
 * @property quietPeriodMs - how long the mailbox must be quiet before it runs
 * @property maxWaitMs - the longest the oldest waiting event may wait, however busy the mailbox is
 */
export const DebounceDeliveryMode = Schema.TaggedStruct('Debounce', {
	quietPeriodMs: PositiveMilliseconds,
	maxWaitMs: Schema.optionalKey(PositiveMilliseconds),
})

/**
 * Wait a fixed window after the first waiting event, then take everything that is waiting.
 *
 * @property windowMs - how long to collect events after the first one arrives
 */
export const BurstDeliveryMode = Schema.TaggedStruct('Burst', {
	windowMs: PositiveMilliseconds,
})

export const DeliveryMode = Schema.Union([
	QueueDeliveryMode,
	SerialDeliveryMode,
	DebounceDeliveryMode,
	BurstDeliveryMode,
])
export type DeliveryMode = typeof DeliveryMode.Type

export const MailboxSequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).check(
	Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
export type MailboxSequence = typeof MailboxSequence.Type

/**
 * What a store reports about the events waiting in one idle mailbox.
 *
 * Sequence numbers only ever grow within a mailbox, so "claim up to sequence N"
 * names an exact set of events even if more arrive after the report was read.
 */
export const WaitingEvents = Schema.Struct({
	count: Schema.Int.check(Schema.isGreaterThan(0)),
	firstSequence: MailboxSequence,
	firstArrivedAt: Timestamp,
	lastSequence: MailboxSequence,
	lastArrivedAt: Timestamp,
})
export type WaitingEvents = typeof WaitingEvents.Type

export type MailboxClaimDecision = Data.TaggedEnum<{
	/** Claim the waiting events whose sequence is at or below `upToSequence`. */
	ClaimUpTo: { readonly upToSequence: MailboxSequence }
	/** Not yet. Look at this mailbox again at `until`. */
	WaitUntil: { readonly until: Timestamp }
}>
export const MailboxClaimDecision = Data.taggedEnum<MailboxClaimDecision>()

export type DecideMailboxClaimInput = {
	readonly waiting: WaitingEvents
	readonly mode: DeliveryMode
	readonly now: Timestamp
}

const claimAllWhenDue = (input: {
	readonly dueAt: number
	readonly waiting: WaitingEvents
	readonly now: Timestamp
}) =>
	input.dueAt > input.now
		? MailboxClaimDecision.WaitUntil({ until: Timestamp.make(input.dueAt) })
		: MailboxClaimDecision.ClaimUpTo({ upToSequence: input.waiting.lastSequence })

/** Decide whether an idle mailbox with waiting events runs now, and with which events. */
export const decideMailboxClaim = ({ waiting, mode, now }: DecideMailboxClaimInput): MailboxClaimDecision =>
	Match.value(mode).pipe(
		Match.tagsExhaustive({
			Queue: () => MailboxClaimDecision.ClaimUpTo({ upToSequence: waiting.lastSequence }),
			Serial: () => MailboxClaimDecision.ClaimUpTo({ upToSequence: waiting.firstSequence }),
			Debounce: ({ quietPeriodMs, maxWaitMs }) =>
				claimAllWhenDue({
					dueAt: Math.min(
						waiting.lastArrivedAt + quietPeriodMs,
						waiting.firstArrivedAt + (maxWaitMs ?? Number.POSITIVE_INFINITY),
					),
					waiting,
					now,
				}),
			Burst: ({ windowMs }) => claimAllWhenDue({ dueAt: waiting.firstArrivedAt + windowMs, waiting, now }),
		}),
	)
