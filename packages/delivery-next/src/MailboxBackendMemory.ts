/**
 * In-memory mailbox store, for tests and local development. Everything is lost when the process stops.
 *
 * It implements the same two services a real store implements (MailboxDelivery and
 * MailboxProcessingBackend) and must pass the same backend contract, so the shared processing
 * code can run against realistic mailbox behaviour without a database.
 */
import { Array as Arr, Clock, Context, Effect, Layer, Match, Option, Ref } from 'effect'

import { DeliveryReceipt, MailboxDelivery, deliveryMailboxKey, type DeliveryAdmission } from './MailboxDelivery'
import { Timestamp } from './MailboxPolicy'
import {
	ClaimedMailboxBatch,
	MailboxProcessingBackend,
	MailboxProcessingClaimLost,
	RecoverableMailbox,
	WaitingMailbox,
	type ClaimMailbox,
	type ReadyMailbox,
} from './MailboxProcessing'
import { DeliveryAdmissionBatch } from './ProviderEventProcessing'

type WaitingEvent = { readonly sequence: number; readonly arrivedAt: number; readonly admission: DeliveryAdmission }

type MemoryMailbox = {
	readonly mailboxKey: string
	readonly provider: string
	readonly status: 'idle' | 'active' | 'retry'
	readonly nextSequence: number
	readonly waiting: ReadonlyArray<WaitingEvent>
	readonly frozenBatch: DeliveryAdmissionBatch | null
	readonly claimId: string | null
	readonly attempt: number
	readonly readyAt: number | null
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
		const state = yield* Ref.make<MemoryStore>({ eventIds: new Set(), mailboxes: new Map(), claimsMade: 0 })

		const deliver = (admission: DeliveryAdmission) =>
			Effect.gen(function* () {
				const now = yield* Clock.currentTimeMillis
				const mailboxKey = deliveryMailboxKey(admission)
				const eventKey = `${admission.namespace}|${admission.provider}|${admission.eventId}`
				const accepted = yield* Ref.modify(state, (store) => {
					if (store.eventIds.has(eventKey)) return [false, store]
					const current: MemoryMailbox = store.mailboxes.get(mailboxKey) ?? {
						mailboxKey,
						provider: admission.provider,
						status: 'idle',
						nextSequence: 0,
						waiting: [],
						frozenBatch: null,
						claimId: null,
						attempt: 0,
						readyAt: null,
					}
					const next: MemoryMailbox = {
						...current,
						nextSequence: current.nextSequence + 1,
						waiting: [...current.waiting, { sequence: current.nextSequence, arrivedAt: now, admission }],
						readyAt: current.status === 'idle' ? now : current.readyAt,
					}
					return [true, { ...withMailbox(store, next), eventIds: new Set([...store.eventIds, eventKey]) }]
				})
				return DeliveryReceipt.make({ mailboxKey, accepted })
			})

		const findReadyMailboxes = Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const store = yield* Ref.get(state)
			return [...store.mailboxes.values()]
				.filter((mailbox) => mailbox.readyAt !== null && mailbox.readyAt <= now)
				.flatMap((mailbox): ReadonlyArray<ReadyMailbox> => {
					if (mailbox.status !== 'idle') return [RecoverableMailbox.make({ mailboxKey: mailbox.mailboxKey })]
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
				return yield* Ref.modify(state, (store): readonly [Option.Option<ClaimedMailboxBatch>, MemoryStore] => {
					const current = store.mailboxes.get(input.mailboxKey)
					if (current === undefined || current.readyAt === null || current.readyAt > now) {
						return [Option.none(), store]
					}
					const claimId = `claim-${store.claimsMade + 1}`
					const selection = Match.value(input).pipe(
						Match.tagsExhaustive({
							ClaimWaitingEvents: ({ upToSequence }) => {
								if (current.status !== 'idle') return null
								const taken = current.waiting.filter(({ sequence }) => sequence <= upToSequence)
								const admissions = toBatch(taken)
								return admissions === null
									? null
									: {
											admissions,
											attempt: 1,
											waiting: current.waiting.filter(({ sequence }) => sequence > upToSequence),
										}
							},
							ClaimFrozenBatch: () =>
								current.status === 'idle' || current.frozenBatch === null
									? null
									: {
											admissions: current.frozenBatch,
											attempt: current.attempt + 1,
											waiting: current.waiting,
										},
						}),
					)
					if (selection === null) return [Option.none(), store]
					const next: MemoryMailbox = {
						...current,
						status: 'active',
						waiting: selection.waiting,
						frozenBatch: selection.admissions,
						claimId,
						attempt: selection.attempt,
						readyAt: now + input.leaseMs,
					}
					return [
						Option.some(
							ClaimedMailboxBatch.make({
								mailboxKey: current.mailboxKey,
								claimId,
								attempt: selection.attempt,
								admissions: selection.admissions,
							}),
						),
						{ ...withMailbox(store, next), claimsMade: store.claimsMade + 1 },
					]
				})
			})

		const ownClaim = (store: MemoryStore, mailboxKey: string, claimId: string) => {
			const current = store.mailboxes.get(mailboxKey)
			return current !== undefined && current.status === 'active' && current.claimId === claimId ? current : null
		}

		const claimLostUnless = (owned: boolean, mailboxKey: string, claimId: string) =>
			owned ? Effect.void : Effect.fail(new MailboxProcessingClaimLost({ mailboxKey, claimId }))

		return Context.make(MailboxDelivery, MailboxDelivery.of({ deliver })).pipe(
			Context.add(
				MailboxProcessingBackend,
				MailboxProcessingBackend.of({
					findReadyMailboxes,
					claimMailbox,
					deferMailbox: (input) =>
						Ref.update(state, (store) => {
							const current = store.mailboxes.get(input.mailboxKey)
							if (current === undefined || current.status !== 'idle') return store
							if (current.waiting.at(-1)?.sequence !== input.lastSequenceSeen) return store
							return withMailbox(store, { ...current, readyAt: input.until })
						}),
					renewClaim: (input) =>
						Effect.gen(function* () {
							const now = yield* Clock.currentTimeMillis
							const owned = yield* Ref.modify(state, (store) => {
								const current = ownClaim(store, input.mailboxKey, input.claimId)
								return current === null
									? [false, store]
									: [true, withMailbox(store, { ...current, readyAt: now + input.leaseMs })]
							})
							yield* claimLostUnless(owned, input.mailboxKey, input.claimId)
						}),
					recordProcessingAttemptResult: (input) =>
						Effect.gen(function* () {
							const owned = yield* Ref.modify(state, (store) => {
								const current = ownClaim(store, input.claim.mailboxKey, input.claim.claimId)
								if (current === null) return [false, store]
								const settled: MemoryMailbox = {
									...current,
									status: 'idle',
									claimId: null,
									frozenBatch: null,
									attempt: 0,
									readyAt: Arr.isReadonlyArrayNonEmpty(current.waiting) ? input.finishedAt : null,
								}
								const next = Match.value(input.result).pipe(
									Match.tag('RetryableFailure', ({ retryAfterMs }): MemoryMailbox => ({
										...current,
										status: 'retry',
										claimId: null,
										readyAt: input.finishedAt + (retryAfterMs ?? 1_000),
									})),
									Match.orElse(() => settled),
								)
								return [true, withMailbox(store, next)]
							})
							yield* claimLostUnless(owned, input.claim.mailboxKey, input.claim.claimId)
						}),
				}),
			),
		)
	}),
)
