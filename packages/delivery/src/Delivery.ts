import { Cause, Clock, Effect, Exit, Fiber, Schedule, Schema } from 'effect'

import { DeliveryPolicy } from './DeliveryPolicy.ts'
import type { EventDefinition } from './EventDefinition.ts'
import {
	ActiveBatch,
	activeBatches,
	CurrentMailboxState,
	currentMailbox,
	emptyMailbox,
	Envelope,
	eventIdentity,
	mailboxKey,
	mailboxPrefix,
	MailboxAddress,
	MailboxState,
	Outcome,
} from './Mailbox.ts'
import { MailboxReadiness, MailboxStore } from './MailboxStore.ts'

export class DeliveryError extends Schema.TaggedError<DeliveryError>()('DeliveryError', {
	reason: Schema.Literals(['conflict', 'capacity', 'payload', 'definition', 'stale', 'configuration']),
}) {}

export class HandlerFailure extends Schema.TaggedError<HandlerFailure>()('HandlerFailure', {
	retryable: Schema.Boolean,
}) {}

export type HandlerContext<A> = { readonly skipped: ReadonlyArray<A> }

type Transition<A> = {
	readonly state: CurrentMailboxState
	readonly result: A
}

const transition = <A>(input: {
	readonly key: string
	readonly retries: number
	readonly calculate: (
		state: CurrentMailboxState,
		revision: number,
		now: number,
	) => Effect.Effect<Transition<A>, DeliveryError>
}) =>
	Effect.gen(function* () {
		const store = yield* MailboxStore
		for (let attempt = 0; attempt <= input.retries; attempt++) {
			const current = yield* store.loadMailbox({ key: input.key })
			const now = yield* Clock.currentTimeMillis
			const next = yield* input.calculate(
				currentMailbox(current?.state ?? emptyMailbox()),
				(current?.revision ?? -1) + 1,
				now,
			)
			const committed = yield* store.commitMailbox({
				key: input.key,
				expectedRevision: current?.revision ?? null,
				nextState: next.state,
			})
			if (committed === 'committed') return next.result
		}
		return yield* DeliveryError.make({ reason: 'conflict' })
	})

const cancellationPending = (state: MailboxState, outcome: Outcome) => {
	const target = outcome.cancellationTarget
	if (target === null) return false
	return activeBatches(state).some((batch) =>
		target === undefined
			? batch.cancelled
			: eventIdentity(batch.envelopes[0]) === target.identity &&
				batch.envelopes[0].acceptedAt === target.acceptedAt,
	)
}
const retained = (state: MailboxState, now: number) =>
	state.outcomes.filter(
		(outcome) => outcome.expiresAt > now || (outcome.kind === 'control' && cancellationPending(state, outcome)),
	)
const envelopes = (state: MailboxState) => [
	...state.pending,
	...activeBatches(state).flatMap((batch) => batch.envelopes),
	...state.failed.flatMap((batch) => batch.envelopes),
]
const known = (state: MailboxState, identity: string) =>
	state.outcomes.some((outcome) => outcome.identity === identity) ||
	envelopes(state).some((event) => eventIdentity(event) === identity)

export type HandlerRegistration<Event extends Schema.Constraint, Resource extends Schema.Constraint, R> = {
	readonly namespace: string
	readonly handlerId: string
	readonly definition: EventDefinition<Event, Resource>
	readonly policy: DeliveryPolicy
	readonly handler: (
		event: Event['Type'],
		context: HandlerContext<Event['Type']>,
	) => Effect.Effect<void, HandlerFailure, R>
}

const RegistrationIdentity = Schema.Struct({
	namespace: Schema.NonEmptyString,
	handlerId: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	name: Schema.NonEmptyString,
	version: Schema.NonEmptyString,
})

const RunnerOptions = Schema.Struct({
	scanLimit: Schema.Int.check(Schema.isGreaterThan(0)),
	concurrency: Schema.Int.check(Schema.isGreaterThan(0)),
	pollMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type RunnerOptions = typeof RunnerOptions.Type

export const bind = <Event extends Schema.Constraint, Resource extends Schema.Constraint, R>(
	registration: HandlerRegistration<Event, Resource, R>,
) => {
	const definition = registration.definition
	const policy = registration.policy
	const concurrency = policy.mode === 'concurrent' ? policy.maxConcurrency : 1
	const withBatches = (state: CurrentMailboxState, batches: ReadonlyArray<ActiveBatch>): CurrentMailboxState => ({
		...state,
		active: batches[0] ?? null,
		additionalActive: batches.slice(1),
	})
	const scheduled = (state: CurrentMailboxState): CurrentMailboxState => {
		const batches = activeBatches(state)
		const deadlines = batches.map((batch) => batch.leaseUntil)
		if (state.pending.length > 0 && batches.length < concurrency && state.pendingReadyAt !== null)
			deadlines.push(state.pendingReadyAt)
		return {
			...state,
			readyAt: deadlines.length === 0 ? null : Math.min(...deadlines),
			burstDraining: state.burstDraining && (batches.length > 0 || state.pending.length > 0),
		}
	}
	const eventCodec = Schema.fromJsonString(Schema.toCodecJson(definition.event))
	const resourceCodec = Schema.fromJsonString(Schema.toCodecJson(definition.resource))
	const prefix = mailboxPrefix({ ...registration, provider: definition.provider })
	const configured = Effect.all([
		DeliveryPolicy.makeEffect(policy),
		RegistrationIdentity.makeEffect({ ...registration, ...definition }),
	]).pipe(Effect.mapError(() => DeliveryError.make({ reason: 'configuration' })))
	const decode = (envelope: Envelope) =>
		Effect.gen(function* () {
			if (envelope.definition !== definition.name || envelope.version !== definition.version)
				return yield* DeliveryError.make({ reason: 'definition' })
			const event = yield* Schema.decodeEffect(eventCodec)(envelope.payload).pipe(
				Effect.tapError(() =>
					Effect.logError('Delivery event payload failed schema decoding').pipe(
						Effect.annotateLogs({ definition: envelope.definition, version: envelope.version }),
					),
				),
				Effect.mapError(() => DeliveryError.make({ reason: 'payload' })),
			)
			yield* Schema.decodeEffect(resourceCodec)(envelope.resource).pipe(
				Effect.tapError(() =>
					Effect.logError('Delivery resource failed schema decoding').pipe(
						Effect.annotateLogs({ definition: envelope.definition, version: envelope.version }),
					),
				),
				Effect.mapError(() => DeliveryError.make({ reason: 'payload' })),
			)
			return event
		})

	const keyForResource = Effect.fn('delivery.key_for_resource')(function* (input: {
		readonly installation: string
		readonly resource: Resource['Type']
	}) {
		yield* configured
		yield* Schema.encodeEffect(resourceCodec)(input.resource).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'payload' })),
		)
		const address = yield* MailboxAddress.makeEffect({
			...registration,
			provider: definition.provider,
			installation: input.installation,
			resourceKey: definition.resourceKey(input.resource),
		}).pipe(Effect.mapError(() => DeliveryError.make({ reason: 'definition' })))
		return mailboxKey(address)
	})

	const identify = Effect.fn('delivery.identify')(function* (input: { readonly event: Event['Type'] }) {
		const identity = definition.identify(input.event)
		const key = yield* keyForResource({
			installation: identity.installation,
			resource: identity.resource,
		})
		return { identity, key }
	})

	const keyFor = Effect.fn('delivery.key_for')(function* (input: { readonly event: Event['Type'] }) {
		return (yield* identify(input)).key
	})

	const admit = Effect.fn('delivery.admit')(function* (input: { readonly event: Event['Type'] }) {
		const identified = yield* identify(input)
		const identity = identified.identity
		const payload = yield* Schema.encodeEffect(eventCodec)(input.event).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'payload' })),
		)
		const resource = yield* Schema.encodeEffect(resourceCodec)(identity.resource).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'payload' })),
		)
		const key = identified.key
		const now = yield* Clock.currentTimeMillis
		const envelope = yield* Envelope.makeEffect({
			definition: definition.name,
			version: definition.version,
			eventId: identity.eventId,
			resource,
			payload,
			acceptedAt: now,
		}).pipe(Effect.mapError(() => DeliveryError.make({ reason: 'definition' })))
		const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Envelope))(envelope).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'payload' })),
		)
		if (new TextEncoder().encode(encoded + key).byteLength > policy.maxPayloadBytes)
			return yield* DeliveryError.make({ reason: 'capacity' })
		const accepted = yield* transition({
			key,
			retries: policy.conflictRetries,
			calculate: (current, _revision, now) => {
				const state = { ...current, outcomes: retained(current, now) }
				if (known(state, eventIdentity(envelope))) return Effect.succeed({ state, result: false })
				if (policy.mode === 'drop' && (state.pending.length > 0 || activeBatches(state).length > 0)) {
					if (state.outcomes.length + envelopes(state).length >= policy.maxOutcomes)
						return Effect.fail(DeliveryError.make({ reason: 'capacity' }))
					return Effect.succeed({
						state: {
							...state,
							outcomes: [
								...state.outcomes,
								Outcome.make({
									identity: eventIdentity(envelope),
									kind: 'dropped',
									expiresAt: now + policy.retentionMs,
								}),
							],
						},
						result: true,
					})
				}
				if (
					envelopes(state).length >= policy.maxEnvelopes ||
					state.outcomes.length + envelopes(state).length >= policy.maxOutcomes
				)
					return Effect.fail(DeliveryError.make({ reason: 'capacity' }))
				const pendingReadyAt =
					policy.mode === 'debounce'
						? now + policy.quietPeriodMs
						: (state.pendingReadyAt ??
							(policy.mode === 'burst' && !state.burstDraining ? now + policy.windowMs : now))
				return Effect.succeed({
					state: scheduled({ ...state, pending: [...state.pending, envelope], pendingReadyAt }),
					result: true,
				})
			},
		})
		return { key, accepted }
	})

	const claim = Effect.fn('delivery.claim')(function* (input: { readonly key: string }) {
		yield* configured
		if (!input.key.startsWith(prefix)) return yield* DeliveryError.make({ reason: 'definition' })
		return yield* transition<ActiveBatch | undefined>({
			key: input.key,
			retries: policy.conflictRetries,
			calculate: (state, revision, now) => {
				if (state.readyAt === null || state.readyAt > now) return Effect.succeed({ state, result: undefined })
				const batches = activeBatches(state)
				const batch = batches.find((batch) => batch.leaseUntil <= now)
				if (
					batch === undefined &&
					(batches.length >= concurrency || state.pendingReadyAt === null || state.pendingReadyAt > now)
				)
					return Effect.succeed({ state: scheduled(state), result: undefined })
				const selected =
					batch?.envelopes ??
					(policy.mode === 'serial' || policy.mode === 'concurrent'
						? state.pending.slice(0, 1)
						: state.pending)
				const first = selected[0]
				if (first === undefined)
					return Effect.succeed({ state: { ...state, readyAt: null }, result: undefined })
				const active = ActiveBatch.make({
					envelopes: [first, ...selected.slice(1)],
					attempt: (batch?.attempt ?? 0) + 1,
					owner: revision,
					leaseUntil: now + policy.leaseMs,
					cancelled: batch?.cancelled ?? false,
				})
				const pending = batch === undefined ? state.pending.slice(selected.length) : state.pending
				return Effect.succeed({
					state: scheduled(
						withBatches(
							{
								...state,
								pending,
								pendingReadyAt: pending.length === 0 ? null : state.pendingReadyAt,
								burstDraining: policy.mode === 'burst' || state.burstDraining,
							},
							batch === undefined
								? [...batches, active]
								: batches.map((entry) => (entry === batch ? active : entry)),
						),
					),
					result: active,
				})
			},
		})
	})

	const owned = (state: MailboxState, batch: ActiveBatch, now: number) => {
		return activeBatches(state).find(
			(active) => active.owner === batch.owner && active.owner !== null && active.leaseUntil > now,
		)
	}

	const finish = Effect.fn('delivery.finish')(function* (input: {
		readonly key: string
		readonly batch: ActiveBatch
		readonly outcome: 'completed' | 'failed' | 'retry'
	}) {
		yield* transition({
			key: input.key,
			retries: policy.conflictRetries,
			calculate: (state, _revision, now) => {
				const batch = owned(state, input.batch, now)
				if (batch === undefined) return Effect.fail(DeliveryError.make({ reason: 'stale' }))
				const batches = activeBatches(state)
				const outcome = batch.cancelled ? 'cancelled' : input.outcome
				if (outcome === 'retry' && batch.attempt < policy.maxAttempts) {
					const readyAt = now + Math.min(policy.retryMaxMs, policy.retryBaseMs * 2 ** (batch.attempt - 1))
					return Effect.succeed({
						state: scheduled(
							withBatches(
								state,
								batches.map((entry) =>
									entry === batch ? { ...batch, owner: null, leaseUntil: readyAt } : entry,
								),
							),
						),
						result: undefined,
					})
				}
				const kind = outcome === 'retry' ? 'failed' : outcome
				const outcomes = batch.envelopes.map((event) =>
					Outcome.make({ identity: eventIdentity(event), kind, expiresAt: now + policy.retentionMs }),
				)
				return Effect.succeed({
					state: scheduled(
						withBatches(
							{
								...state,
								failed: kind === 'failed' ? [...state.failed, batch] : state.failed,
								outcomes: [...retained(state, now), ...outcomes],
							},
							batches.filter((entry) => entry !== batch),
						),
					),
					result: undefined,
				})
			},
		})
	})

	const cancelActive = Effect.fn('delivery.cancel_active')(function* (input: {
		readonly key: string
		readonly controlId: string
		readonly eventId?: string
	}) {
		yield* configured
		if (!input.key.startsWith(prefix)) return yield* DeliveryError.make({ reason: 'definition' })
		yield* Schema.NonEmptyString.makeEffect(input.controlId).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'definition' })),
		)
		if (input.eventId !== undefined)
			yield* Schema.NonEmptyString.makeEffect(input.eventId).pipe(
				Effect.mapError(() => DeliveryError.make({ reason: 'definition' })),
			)
		if (
			new TextEncoder().encode(input.controlId + input.key + (input.eventId ?? '')).byteLength >
			policy.maxPayloadBytes
		)
			return yield* DeliveryError.make({ reason: 'capacity' })
		const store = yield* MailboxStore
		const snapshot = yield* store.loadMailbox(input)
		const target =
			snapshot === undefined
				? undefined
				: activeBatches(snapshot.state).find(
						(batch) =>
							input.eventId === undefined ||
							batch.envelopes.some(
								(event) => event.definition === definition.name && event.eventId === input.eventId,
							),
					)
		const identity = `control:${input.controlId}`
		return yield* transition({
			key: input.key,
			retries: policy.conflictRetries,
			calculate: (current, _revision, now) => {
				const state = { ...current, outcomes: retained(current, now) }
				if (state.outcomes.some((outcome) => outcome.identity === identity))
					return Effect.succeed({ state, result: false })
				if (state.outcomes.length + envelopes(state).length >= policy.maxOutcomes)
					return Effect.fail(DeliveryError.make({ reason: 'capacity' }))
				const batches = activeBatches(state)
				const targeted =
					target === undefined
						? undefined
						: batches.find(
								(batch) =>
									batch.owner === target.owner &&
									batch.attempt === target.attempt &&
									batch.envelopes[0].acceptedAt === target.envelopes[0].acceptedAt &&
									eventIdentity(batch.envelopes[0]) === eventIdentity(target.envelopes[0]),
							)
				return Effect.succeed({
					state: {
						...withBatches(
							state,
							batches.map((batch) => (batch === targeted ? { ...batch, cancelled: true } : batch)),
						),
						outcomes: [
							...state.outcomes,
							Outcome.make({
								identity,
								kind: 'control',
								expiresAt: now + policy.retentionMs,
								cancellationTarget:
									targeted === undefined
										? null
										: {
												identity: eventIdentity(targeted.envelopes[0]),
												acceptedAt: targeted.envelopes[0].acceptedAt,
											},
							}),
						],
					},
					result: targeted !== undefined,
				})
			},
		})
	})

	const processMailbox = Effect.fn('delivery.process_mailbox')(function* (input: { readonly key: string }) {
		const batch = yield* claim(input)
		if (batch === undefined) return false
		const execute = Effect.gen(function* () {
			if (batch.cancelled) return
			if (batch.attempt > policy.maxAttempts) return yield* HandlerFailure.make({ retryable: false })
			const events = yield* Effect.forEach(batch.envelopes, decode)
			const event = events.at(-1)
			if (event === undefined) return yield* DeliveryError.make({ reason: 'payload' })
			yield* registration.handler(event, { skipped: events.slice(0, -1) })
		}).pipe(Effect.scoped)
		const renew = transition({
			key: input.key,
			retries: policy.conflictRetries,
			calculate: (state, _revision, now) => {
				const current = owned(state, batch, now)
				if (current === undefined) return Effect.fail(DeliveryError.make({ reason: 'stale' }))
				const active = { ...current, leaseUntil: now + policy.leaseMs }
				return Effect.succeed({
					state: scheduled(
						withBatches(
							state,
							activeBatches(state).map((entry) => (entry === current ? active : entry)),
						),
					),
					result: active.cancelled,
				})
			},
		})
		let cancellationRequested = false
		const result = yield* Effect.scoped(
			Effect.gen(function* () {
				const task = yield* execute.pipe(Effect.forkScoped)
				const monitor = Effect.gen(function* () {
					while (true) {
						yield* Effect.sleep(policy.heartbeatMs)
						if ((yield* renew) && !cancellationRequested) {
							cancellationRequested = true
							yield* Fiber.interrupt(task).pipe(Effect.forkScoped)
						}
					}
				})
				return yield* Effect.raceFirst(Fiber.join(task), monitor).pipe(Effect.exit)
			}),
		)
		if (Exit.isSuccess(result) || (cancellationRequested && Cause.hasInterruptsOnly(result.cause))) {
			yield* finish({ ...input, batch, outcome: 'completed' })
			return true
		}
		if (Cause.hasInterruptsOnly(result.cause)) return yield* Effect.failCause(result.cause)
		yield* Effect.logError('delivery attempt failed', result.cause)
		if (Cause.hasDies(result.cause)) {
			yield* finish({ ...input, batch, outcome: 'failed' })
			return yield* Effect.failCause(result.cause)
		}
		return yield* Effect.failCause(result.cause).pipe(
			Effect.catchTag('HandlerFailure', (failure) =>
				finish({ ...input, batch, outcome: failure.retryable ? 'retry' : 'failed' }).pipe(Effect.as(true)),
			),
			Effect.catchTag('DeliveryError', (error) =>
				error.reason === 'stale'
					? Effect.fail(error)
					: finish({ ...input, batch, outcome: 'failed' }).pipe(Effect.andThen(Effect.fail(error))),
			),
		)
	})

	const awaitInactive = Effect.fn('delivery.await_inactive')(function* (input: { readonly key: string }) {
		yield* configured
		if (!input.key.startsWith(prefix)) return yield* DeliveryError.make({ reason: 'definition' })
		const store = yield* MailboxStore
		yield* store.loadMailbox(input).pipe(
			Effect.map((snapshot) => snapshot !== undefined && activeBatches(snapshot.state).length > 0),
			Effect.repeat({ while: (active) => active, schedule: Schedule.spaced(policy.heartbeatMs) }),
		)
	})

	const awaitCancellation = Effect.fn('delivery.await_cancellation')(function* (input: {
		readonly key: string
		readonly controlId: string
	}) {
		yield* configured
		if (!input.key.startsWith(prefix)) return yield* DeliveryError.make({ reason: 'definition' })
		yield* Schema.NonEmptyString.makeEffect(input.controlId).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'definition' })),
		)
		const store = yield* MailboxStore
		yield* store.loadMailbox(input).pipe(
			Effect.map((snapshot) => {
				if (snapshot === undefined) return false
				const control = snapshot.state.outcomes.find(
					(outcome) => outcome.kind === 'control' && outcome.identity === `control:${input.controlId}`,
				)
				return control !== undefined && cancellationPending(snapshot.state, control)
			}),
			Effect.repeat({ while: (pending) => pending, schedule: Schedule.spaced(policy.heartbeatMs) }),
		)
	})

	const run = Effect.fn('delivery.run')(function* (input: RunnerOptions) {
		yield* configured
		yield* RunnerOptions.makeEffect(input).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'configuration' })),
		)
		const readiness = yield* MailboxReadiness
		const running = new Map<string, number>()
		let runningCount = 0
		const pass = Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const keys = yield* readiness.scanReady({ prefix, now, limit: input.scanLimit })
			for (const key of keys) {
				if (runningCount >= input.concurrency) break
				if ((running.get(key) ?? 0) >= concurrency) continue
				running.set(key, (running.get(key) ?? 0) + 1)
				runningCount++
				yield* processMailbox({ key }).pipe(
					Effect.catchCauseIf(
						(cause) => !Cause.hasInterruptsOnly(cause),
						(cause) => Effect.logError('delivery mailbox processing failed', cause),
					),
					Effect.ensuring(
						Effect.sync(() => {
							runningCount--
							const remaining = (running.get(key) ?? 1) - 1
							if (remaining === 0) running.delete(key)
							else running.set(key, remaining)
						}),
					),
					Effect.forkScoped,
				)
			}
		})
		yield* pass.pipe(Effect.repeat(Schedule.spaced(input.pollMs)), Effect.scoped)
	})

	return { keyForResource, keyFor, admit, processMailbox, cancelActive, awaitInactive, awaitCancellation, run }
}
