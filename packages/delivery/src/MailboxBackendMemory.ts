/**
 * In-memory mailbox store, for tests and local development. Everything is lost when the process stops.
 *
 * It implements the same services a real store implements (MailboxDelivery, MailboxProcessingBackend,
 * and DeliveryControlBackend) and must pass the same backend contract, so the shared processing code
 * can run against realistic mailbox behaviour without a database.
 */
import { Array as Arr, Clock, Context, Effect, Layer, Match, Option, Predicate, SynchronizedRef } from 'effect'

import { DeliveryPreparationConflict } from './DeliveryContext'
import { DeliveryControlBackend, DeliveryNotFound, type DeliveryStatus } from './DeliveryControl'
import {
	DEFAULT_RETRY_AFTER_MS,
	activeDeliveryWork,
	applyDeliverySlotMutation,
	claimDeliveryOutput,
	claimFrozenBatch,
	emptyDeliverySlot,
	handOffDeliverySlot,
	prepareDeliverySlot,
	readDeliverySlotStatus,
	recordDeliveryAttempt,
	renewDeliveryClaim,
	renewDeliveryOutput,
	requestDeliveryInterrupt,
	settleDeliveryOutput,
	startDeliveryBatch,
	type DeliverySlot,
} from './DeliveryLifecycle'
import { makeDeliveryId } from './DeliveryReference'
import { DeliveryReceipt, MailboxDelivery, deliveryMailboxKey, type DeliveryAdmission } from './MailboxDelivery'
import { Timestamp } from './MailboxPolicy'
import {
	ClaimedMailboxBatch,
	MailboxProcessingBackend,
	type ClaimedDeliveryOutput,
	MailboxProcessingClaimLost,
	OutputReadyMailbox,
	RecoverableMailbox,
	WaitingMailbox,
	toClaimedDeliveryOutput,
	toClaimedMailboxBatch,
	type ClaimMailbox,
	type ReadyMailbox,
} from './MailboxProcessing'
import { DeliveryAdmissionBatch } from './ProviderEventProcessing'

type WaitingEvent = { readonly sequence: number; readonly arrivedAt: number; readonly admission: DeliveryAdmission }

type MemoryMailbox = {
	readonly mailboxKey: string
	readonly provider: string
	readonly nextSequence: number
	readonly waiting: ReadonlyArray<WaitingEvent>
	readonly deliveries: DeliverySlot
}

type MemoryStore = {
	readonly eventIds: ReadonlySet<string>
	readonly mailboxes: ReadonlyMap<string, MemoryMailbox>
	readonly claimsMade: number
}

const withMailbox = (store: MemoryStore, mailbox: MemoryMailbox): MemoryStore => ({
	...store,
	mailboxes: new Map([...store.mailboxes, [mailbox.mailboxKey, mailbox]]),
})

const toBatch = (events: ReadonlyArray<WaitingEvent>) => {
	const [first, ...rest] = events.map(({ admission }) => admission)
	return first === undefined ? null : DeliveryAdmissionBatch.make([first, ...rest])
}

export const MailboxBackendMemory = Layer.effectContext(
	Effect.gen(function* () {
		const state = yield* SynchronizedRef.make<MemoryStore>({
			eventIds: new Set(),
			mailboxes: new Map(),
			claimsMade: 0,
		})

		/**
		 * Apply one change to one mailbox's deliveries, atomically. A missing mailbox fails with
		 * `onMissing`; a refused change fails with its refusal and stores nothing.
		 */
		const updateDeliveries = <A, E1, E2>(
			mailboxKey: string,
			onMissing: E1,
			change: (
				mailbox: MemoryMailbox,
				now: number,
			) => Effect.Effect<{ readonly slot: DeliverySlot; readonly value: A }, E2>,
		) =>
			Effect.gen(function* () {
				const now = yield* Clock.currentTimeMillis
				return yield* SynchronizedRef.modifyEffect(
					state,
					(store): Effect.Effect<readonly [A, MemoryStore], E1 | E2> => {
						const current = store.mailboxes.get(mailboxKey)
						if (current === undefined) return Effect.fail(onMissing)
						return change(current, now).pipe(
							Effect.map(
								({ slot, value }) =>
									[value, withMailbox(store, { ...current, deliveries: slot })] as const,
							),
						)
					},
				)
			})

		const deliver = (admission: DeliveryAdmission) =>
			Effect.gen(function* () {
				const now = yield* Clock.currentTimeMillis
				const mailboxKey = deliveryMailboxKey(admission)
				const eventKey = `${admission.namespace}|${admission.provider}|${admission.eventId}`
				const accepted = yield* SynchronizedRef.modify(state, (store) => {
					if (store.eventIds.has(eventKey)) return [false, store]
					const current: MemoryMailbox = store.mailboxes.get(mailboxKey) ?? {
						mailboxKey,
						provider: admission.provider,
						nextSequence: 0,
						waiting: [],
						deliveries: emptyDeliverySlot,
					}
					const interrupted =
						admission.interrupt === true
							? requestDeliveryInterrupt(current.deliveries, now)
							: current.deliveries
					const next: MemoryMailbox = {
						...current,
						nextSequence: current.nextSequence + 1,
						waiting: [...current.waiting, { sequence: current.nextSequence, arrivedAt: now, admission }],
						deliveries:
							interrupted.active === null
								? { ...interrupted, readyAt: Timestamp.make(now) }
								: interrupted,
					}
					return [true, { ...withMailbox(store, next), eventIds: new Set([...store.eventIds, eventKey]) }]
				})
				return DeliveryReceipt.make({ mailboxKey, accepted })
			})

		const findReadyMailboxes = Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const store = yield* SynchronizedRef.get(state)
			return [...store.mailboxes.values()]
				.filter(({ deliveries }) => deliveries.readyAt !== null && deliveries.readyAt <= now)
				.flatMap((mailbox): ReadonlyArray<ReadyMailbox> => {
					if (mailbox.deliveries.active !== null) {
						return activeDeliveryWork(mailbox.deliveries.active) === 'Output'
							? [OutputReadyMailbox.make({ mailboxKey: mailbox.mailboxKey })]
							: [RecoverableMailbox.make({ mailboxKey: mailbox.mailboxKey })]
					}
					const first = mailbox.waiting[0]
					const last = mailbox.waiting.at(-1)
					if (first === undefined || last === undefined) return []
					return [
						WaitingMailbox.make({
							mailboxKey: mailbox.mailboxKey,
							provider: mailbox.provider,
							waiting: {
								count: mailbox.waiting.length,
								firstSequence: first.sequence,
								firstArrivedAt: Timestamp.make(first.arrivedAt),
								lastSequence: last.sequence,
								lastArrivedAt: Timestamp.make(last.arrivedAt),
							},
						}),
					]
				})
		})

		const claimMailbox = (input: ClaimMailbox) =>
			Effect.gen(function* () {
				const now = yield* Clock.currentTimeMillis
				return yield* SynchronizedRef.modify(
					state,
					(store): readonly [Option.Option<ClaimedMailboxBatch>, MemoryStore] => {
						const current = store.mailboxes.get(input.mailboxKey)
						if (current === undefined) return [Option.none(), store]
						const { deliveries } = current
						if (deliveries.readyAt === null || deliveries.readyAt > now) return [Option.none(), store]
						const claimId = `claim-${store.claimsMade + 1}`
						const claimsMade = { claimsMade: store.claimsMade + 1 }
						return Match.value(input).pipe(
							Match.tagsExhaustive({
								ClaimWaitingEvents: ({ upToSequence, batchId, accessToken, leaseMs }) => {
									const admissions = toBatch(
										current.waiting.filter(({ sequence }) => sequence <= upToSequence),
									)
									if (admissions === null) return [Option.none(), store] as const
									const started = startDeliveryBatch(deliveries, {
										batchId,
										accessToken,
										admissions,
										claimId,
										leaseMs,
										now,
									})
									if (started === null) return [Option.none(), store] as const
									const next: MemoryMailbox = {
										...current,
										waiting: current.waiting.filter(({ sequence }) => sequence > upToSequence),
										deliveries: started.slot,
									}
									return [
										Option.some(
											toClaimedMailboxBatch({
												mailboxKey: current.mailboxKey,
												active: started.claimed,
												claimId,
											}),
										),
										{ ...withMailbox(store, next), ...claimsMade },
									] as const
								},
								ClaimFrozenBatch: ({ leaseMs }) => {
									const { slot, claimed } = claimFrozenBatch(deliveries, {
										claimId,
										leaseMs,
										now,
										hasWaiting: Arr.isReadonlyArrayNonEmpty(current.waiting),
									})
									const nextStore = {
										...withMailbox(store, { ...current, deliveries: slot }),
										...claimsMade,
									}
									return claimed === null
										? ([Option.none(), nextStore] as const)
										: ([
												Option.some(
													toClaimedMailboxBatch({
														mailboxKey: current.mailboxKey,
														active: claimed,
														claimId,
													}),
												),
												nextStore,
											] as const)
								},
							}),
						)
					},
				)
			})

		const claimLost = (mailboxKey: string, claimId: string) =>
			new MailboxProcessingClaimLost({ mailboxKey, claimId })

		const findDeliveries = (mailboxKey: string) =>
			SynchronizedRef.get(state).pipe(Effect.map((store) => store.mailboxes.get(mailboxKey)))

		const controlBackend = DeliveryControlBackend.of({
			readDeliveryStatus: (input) =>
				Effect.gen(function* () {
					const now = yield* Clock.currentTimeMillis
					const mailbox = yield* findDeliveries(input.reference.mailboxKey)
					if (mailbox === undefined) return yield* new DeliveryNotFound()
					return yield* readDeliverySlotStatus(mailbox.deliveries, { ...input, now }) satisfies Effect.Effect<
						DeliveryStatus,
						DeliveryNotFound
					>
				}),
			applyDeliveryMutation: (input) =>
				updateDeliveries(input.reference.mailboxKey, new DeliveryNotFound(), (mailbox, now) =>
					applyDeliverySlotMutation(mailbox.deliveries, {
						...input,
						now,
						hasWaiting: Arr.isReadonlyArrayNonEmpty(mailbox.waiting),
					}).pipe(Effect.map(({ slot, receipt }) => ({ slot, value: receipt }))),
				),
		})

		return Context.make(MailboxDelivery, MailboxDelivery.of({ deliver })).pipe(
			Context.add(DeliveryControlBackend, controlBackend),
			Context.add(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					findReadyMailboxes,
					claimMailbox,
					deferMailbox: (input) =>
						SynchronizedRef.update(state, (store) => {
							const current = store.mailboxes.get(input.mailboxKey)
							if (current === undefined || current.deliveries.active !== null) return store
							if (current.waiting.at(-1)?.sequence !== input.lastSequenceSeen) return store
							return withMailbox(store, {
								...current,
								deliveries: { ...current.deliveries, readyAt: input.until },
							})
						}),
					renewClaim: (input) =>
						updateDeliveries(input.mailboxKey, claimLost(input.mailboxKey, input.claimId), (mailbox, now) =>
							renewDeliveryClaim(mailbox.deliveries, { ...input, now }).pipe(
								Effect.map((slot) => ({ slot, value: undefined })),
								Effect.catchTag('ClaimNotOwned', () =>
									Effect.fail(claimLost(input.mailboxKey, input.claimId)),
								),
							),
						),
					recordProcessingAttemptResult: (input) =>
						updateDeliveries(
							input.claim.mailboxKey,
							claimLost(input.claim.mailboxKey, input.claim.claimId),
							(mailbox) =>
								recordDeliveryAttempt(mailbox.deliveries, {
									claimId: input.claim.claimId,
									succeeded: Predicate.isTagged(input.result, 'Completed'),
									retryAfterMs: Match.value(input.result).pipe(
										Match.tag(
											'RetryableFailure',
											({ retryAfterMs }) => retryAfterMs ?? DEFAULT_RETRY_AFTER_MS,
										),
										Match.orElse(() => null),
									),
									now: input.finishedAt,
									hasWaiting: Arr.isReadonlyArrayNonEmpty(mailbox.waiting),
								}).pipe(
									Effect.map((slot) => ({ slot, value: undefined })),
									Effect.catchTag('ClaimNotOwned', () =>
										Effect.fail(claimLost(input.claim.mailboxKey, input.claim.claimId)),
									),
								),
						),
					prepareDelivery: (input) =>
						updateDeliveries(input.mailboxKey, claimLost(input.mailboxKey, input.claimId), (mailbox) =>
							prepareDeliverySlot(mailbox.deliveries, input).pipe(
								Effect.map(({ slot, prepared }) => ({ slot, value: prepared })),
								Effect.catchTags({
									ClaimNotOwned: () => Effect.fail(claimLost(input.mailboxKey, input.claimId)),
									PreparationMismatch: ({ batchId }) =>
										Effect.fail(
											new DeliveryPreparationConflict({
												deliveryId: makeDeliveryId({
													mailboxKey: input.mailboxKey,
													batchId,
													callbackIndex: mailbox.deliveries.active?.callbackIndex ?? 0,
												}),
											}),
										),
								}),
							),
						),
					handOffDelivery: (input) =>
						updateDeliveries(input.mailboxKey, claimLost(input.mailboxKey, input.claimId), (mailbox) =>
							handOffDeliverySlot(mailbox.deliveries, input).pipe(
								Effect.map((slot) => ({ slot, value: undefined })),
								Effect.catchTag('ClaimNotOwned', () =>
									Effect.fail(claimLost(input.mailboxKey, input.claimId)),
								),
							),
						),
					claimDeliveryOutput: (input) =>
						Effect.gen(function* () {
							const now = yield* Clock.currentTimeMillis
							return yield* SynchronizedRef.modify(
								state,
								(store): readonly [Option.Option<ClaimedDeliveryOutput>, MemoryStore] => {
									const current = store.mailboxes.get(input.mailboxKey)
									if (current === undefined) return [Option.none(), store]
									const claimId = `output-claim-${store.claimsMade + 1}`
									const { slot, claimed } = claimDeliveryOutput(current.deliveries, {
										claimId,
										leaseMs: input.leaseMs,
										now,
										idempotencyKey: input.idempotencyKey,
									})
									if (claimed === null) return [Option.none(), store]
									return [
										Option.some(
											toClaimedDeliveryOutput({
												mailboxKey: input.mailboxKey,
												claimId,
												...claimed,
											}),
										),
										{
											...withMailbox(store, { ...current, deliveries: slot }),
											claimsMade: store.claimsMade + 1,
										},
									]
								},
							)
						}),
					renewDeliveryOutput: (input) =>
						updateDeliveries(input.mailboxKey, claimLost(input.mailboxKey, input.claimId), (mailbox, now) =>
							renewDeliveryOutput(mailbox.deliveries, { ...input, now }).pipe(
								Effect.map((slot) => ({ slot, value: undefined })),
								Effect.catchTag('ClaimNotOwned', () =>
									Effect.fail(claimLost(input.mailboxKey, input.claimId)),
								),
							),
						),
					settleDeliveryOutput: (input) =>
						updateDeliveries(input.mailboxKey, claimLost(input.mailboxKey, input.claimId), (mailbox) =>
							settleDeliveryOutput(mailbox.deliveries, {
								...input,
								now: input.settledAt,
								hasWaiting: Arr.isReadonlyArrayNonEmpty(mailbox.waiting),
							}).pipe(
								Effect.map((slot) => ({ slot, value: undefined })),
								Effect.catchTag('ClaimNotOwned', () =>
									Effect.fail(claimLost(input.mailboxKey, input.claimId)),
								),
							),
						),
				}),
			),
		)
	}),
)
