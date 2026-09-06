import { Cause, Clock, Effect, Exit, Fiber, Schedule, Schema } from 'effect'

import { DeliveryPolicy } from './DeliveryPolicy.ts'
import type { EventDefinition } from './EventDefinition.ts'
import {
	ActiveBatch,
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
	readonly state: MailboxState
	readonly result: A
}

const transition = <A>(input: {
	readonly key: string
	readonly retries: number
	readonly calculate: (
		state: MailboxState,
		revision: number,
		now: number,
	) => Effect.Effect<Transition<A>, DeliveryError>
}) =>
	Effect.gen(function* () {
		const store = yield* MailboxStore
		for (let attempt = 0; attempt <= input.retries; attempt++) {
			const current = yield* store.loadMailbox({ key: input.key })
			const now = yield* Clock.currentTimeMillis
			const next = yield* input.calculate(current?.state ?? emptyMailbox(), (current?.revision ?? -1) + 1, now)
			const committed = yield* store.commitMailbox({
				key: input.key,
				expectedRevision: current?.revision ?? null,
				nextState: next.state,
			})
			if (committed === 'committed') return next.result
		}
		return yield* DeliveryError.make({ reason: 'conflict' })
	})

const retained = (state: MailboxState, now: number) => state.outcomes.filter((outcome) => outcome.expiresAt > now)
const envelopes = (state: MailboxState) => [
	...state.pending,
	...(state.active?.envelopes ?? []),
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
				if (
					envelopes(state).length >= policy.maxEnvelopes ||
					state.outcomes.length + envelopes(state).length >= policy.maxOutcomes
				)
					return Effect.fail(DeliveryError.make({ reason: 'capacity' }))
				return Effect.succeed({
					state: { ...state, pending: [...state.pending, envelope], readyAt: state.readyAt ?? now },
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
				const batch = state.active
				if (batch !== null && batch.owner !== null && batch.leaseUntil > now)
					return Effect.succeed({ state, result: undefined })
				const selected =
					batch?.envelopes ?? (policy.mode === 'queue' ? state.pending : state.pending.slice(0, 1))
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
				return Effect.succeed({
					state: {
						...state,
						active,
						pending: batch === null ? state.pending.slice(selected.length) : state.pending,
						readyAt: active.leaseUntil,
					},
					result: active,
				})
			},
		})
	})

	const owned = (state: MailboxState, batch: ActiveBatch, now: number) => {
		const active = state.active
		return active !== null && active.owner === batch.owner && active.leaseUntil > now
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
				if (!owned(state, input.batch, now)) return Effect.fail(DeliveryError.make({ reason: 'stale' }))
				const batch = state.active
				if (batch === null) return Effect.fail(DeliveryError.make({ reason: 'stale' }))
				const outcome = batch.cancelled ? 'cancelled' : input.outcome
				if (outcome === 'retry' && batch.attempt < policy.maxAttempts) {
					const readyAt = now + Math.min(policy.retryMaxMs, policy.retryBaseMs * 2 ** (batch.attempt - 1))
					return Effect.succeed({
						state: { ...state, active: { ...batch, owner: null, leaseUntil: readyAt }, readyAt },
						result: undefined,
					})
				}
				const kind = outcome === 'retry' ? 'failed' : outcome
				const outcomes = batch.envelopes.map((event) =>
					Outcome.make({ identity: eventIdentity(event), kind, expiresAt: now + policy.retentionMs }),
				)
				return Effect.succeed({
					state: {
						...state,
						active: null,
						failed: kind === 'failed' ? [...state.failed, batch] : state.failed,
						outcomes: [...retained(state, now), ...outcomes],
						readyAt: state.pending.length > 0 ? now : null,
					},
					result: undefined,
				})
			},
		})
	})

	const cancelActive = Effect.fn('delivery.cancel_active')(function* (input: {
		readonly key: string
		readonly controlId: string
	}) {
		yield* configured
		if (!input.key.startsWith(prefix)) return yield* DeliveryError.make({ reason: 'definition' })
		yield* Schema.NonEmptyString.makeEffect(input.controlId).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'definition' })),
		)
		if (new TextEncoder().encode(input.controlId + input.key).byteLength > policy.maxPayloadBytes)
			return yield* DeliveryError.make({ reason: 'capacity' })
		const store = yield* MailboxStore
		const target = (yield* store.loadMailbox(input))?.state.active
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
				const targeted =
					target !== undefined &&
					target !== null &&
					state.active !== null &&
					state.active.owner === target.owner &&
					eventIdentity(state.active.envelopes[0]) === eventIdentity(target.envelopes[0])
				const active = targeted && state.active !== null ? { ...state.active, cancelled: true } : state.active
				return Effect.succeed({
					state: {
						...state,
						active,
						outcomes: [
							...state.outcomes,
							Outcome.make({ identity, kind: 'control', expiresAt: now + policy.retentionMs }),
						],
					},
					result: targeted,
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
				if (!owned(state, batch, now) || state.active === null)
					return Effect.fail(DeliveryError.make({ reason: 'stale' }))
				const active = { ...state.active, leaseUntil: now + policy.leaseMs }
				return Effect.succeed({
					state: { ...state, active, readyAt: active.leaseUntil },
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
		while (true) {
			const snapshot = yield* store.loadMailbox(input)
			if (snapshot?.state.active === null || snapshot === undefined) return
			yield* Effect.sleep(policy.heartbeatMs)
		}
	})

	const run = Effect.fn('delivery.run')(function* (input: RunnerOptions) {
		yield* configured
		yield* RunnerOptions.makeEffect(input).pipe(
			Effect.mapError(() => DeliveryError.make({ reason: 'configuration' })),
		)
		const readiness = yield* MailboxReadiness
		const running = new Set<string>()
		const pass = Effect.gen(function* () {
			const now = yield* Clock.currentTimeMillis
			const keys = yield* readiness.scanReady({ prefix, now, limit: input.scanLimit })
			for (const key of keys) {
				if (running.size >= input.concurrency) break
				if (running.has(key)) continue
				running.add(key)
				yield* processMailbox({ key }).pipe(
					Effect.catchCauseIf(
						(cause) => !Cause.hasInterruptsOnly(cause),
						(cause) => Effect.logError('delivery mailbox processing failed', cause),
					),
					Effect.ensuring(Effect.sync(() => running.delete(key))),
					Effect.forkScoped,
				)
			}
		})
		yield* pass.pipe(Effect.repeat(Schedule.spaced(input.pollMs)), Effect.scoped)
	})

	return { keyForResource, keyFor, admit, processMailbox, cancelActive, awaitInactive, run }
}
