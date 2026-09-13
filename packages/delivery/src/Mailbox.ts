import { Array as Arr, Schema } from 'effect'

import { DeliveryOperation } from './DeliveryOperation.js'
import { DeliveryId } from './protocol.js'

export const Envelope = Schema.Struct({
	definition: Schema.NonEmptyString,
	version: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	resource: Schema.String,
	payload: Schema.String,
	acceptedAt: Schema.Finite,
	organizationId: Schema.optionalKey(Schema.NonEmptyString),
})
export type Envelope = typeof Envelope.Type

export const LocalDeliveryStage = Schema.TaggedStruct('Local', {
	owner: Schema.NullOr(Schema.Natural),
	leaseUntil: Schema.Finite,
	terminalOutcome: Schema.optionalKey(Schema.Literals(['completed', 'failed'])),
})

export const ExternalDeliveryStage = Schema.TaggedStruct('External', {
	cleanupOwner: Schema.NullOr(Schema.Natural),
	cleanupLeaseUntil: Schema.NullOr(Schema.Finite),
	handedOffAt: Schema.Finite,
	terminalOutcome: Schema.optionalKey(Schema.Literals(['completed', 'failed'])),
})

export const ActiveBatch = Schema.Struct({
	envelopes: Schema.NonEmptyArray(Envelope),
	attempt: Schema.Natural,
	owner: Schema.NullOr(Schema.Natural),
	leaseUntil: Schema.Finite,
	cancelled: Schema.Boolean,
	deliveryId: Schema.optionalKey(DeliveryId),
	stage: Schema.optionalKey(Schema.Union([LocalDeliveryStage, ExternalDeliveryStage])),
})
export type ActiveBatch = typeof ActiveBatch.Type

export const Outcome = Schema.Struct({
	identity: Schema.String,
	kind: Schema.Literals(['completed', 'cancelled', 'failed', 'control', 'dropped']),
	expiresAt: Schema.Finite,
	deliveryId: Schema.optionalKey(DeliveryId),
	organizationId: Schema.optionalKey(Schema.NonEmptyString),
	definition: Schema.optionalKey(Schema.NonEmptyString),
	version: Schema.optionalKey(Schema.NonEmptyString),
	eventId: Schema.optionalKey(Schema.NonEmptyString),
	resource: Schema.optionalKey(Schema.String),
	payload: Schema.optionalKey(Schema.String),
	cancellationTarget: Schema.optionalKey(
		Schema.NullOr(
			Schema.Struct({
				identity: Schema.String,
				acceptedAt: Schema.Finite,
			}),
		),
	),
})
export type Outcome = typeof Outcome.Type

const fields = {
	pending: Schema.Array(Envelope),
	active: Schema.NullOr(ActiveBatch),
	failed: Schema.Array(ActiveBatch),
	outcomes: Schema.Array(Outcome),
	readyAt: Schema.NullOr(Schema.Finite),
}

const LegacyMailboxState = Schema.Struct({ ...fields, version: Schema.Literal(1) })

const V2MailboxState = Schema.Struct({
	...fields,
	version: Schema.Literal(2),
	additionalActive: Schema.Array(ActiveBatch),
	pendingReadyAt: Schema.NullOr(Schema.Finite),
	burstDraining: Schema.Boolean,
})

export const IngressAttribution = Schema.Struct({ organizationId: Schema.NonEmptyString })
export interface IngressAttribution extends Schema.Schema.Type<typeof IngressAttribution> {}

export const CurrentMailboxState = Schema.Struct({
	...V2MailboxState.fields,
	version: Schema.Literals([3, 4, 5]),
	attribution: Schema.optionalKey(IngressAttribution),
	operations: Schema.optionalKey(Schema.Array(DeliveryOperation)),
	maxOutcomes: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
})
export type CurrentMailboxState = typeof CurrentMailboxState.Type

export const MailboxState = Schema.Union([LegacyMailboxState, V2MailboxState, CurrentMailboxState])
export type MailboxState = typeof MailboxState.Type

/** Upgrade under revision fencing. Stop old writers before the first v3 write. */
export const currentMailbox = (state: MailboxState): CurrentMailboxState =>
	state.version !== 1
		? {
				...state,
				version: 5,
				operations: state.version === 2 ? [] : (state.operations ?? []),
			}
		: {
				...state,
				version: 5,
				additionalActive: [],
				pendingReadyAt: state.pending[0]?.acceptedAt ?? null,
				burstDraining: false,
				operations: [],
			}

export const activeBatches = (state: MailboxState): ReadonlyArray<ActiveBatch> => [
	...(state.active === null ? [] : [state.active]),
	...(state.version !== 1 ? state.additionalActive : []),
]

export const deliveryIds = (state: MailboxState): ReadonlyArray<DeliveryId> =>
	Arr.dedupe([
		...activeBatches(state).flatMap((batch) => (batch.deliveryId === undefined ? [] : [batch.deliveryId])),
		...state.outcomes.flatMap((outcome) => (outcome.deliveryId === undefined ? [] : [outcome.deliveryId])),
	])

export const MailboxSnapshot = Schema.Struct({ revision: Schema.Natural, state: MailboxState })
export type MailboxSnapshot = typeof MailboxSnapshot.Type

export const emptyMailbox = (): CurrentMailboxState =>
	CurrentMailboxState.make({
		version: 5,
		pending: [],
		active: null,
		additionalActive: [],
		pendingReadyAt: null,
		burstDraining: false,
		failed: [],
		outcomes: [],
		readyAt: null,
		operations: [],
	})

export const eventIdentity = (envelope: Envelope) =>
	`${envelope.definition.length}:${envelope.definition}${envelope.eventId.length}:${envelope.eventId}`

export const cancellationPending = (state: MailboxState, outcome: Outcome) => {
	const target = outcome.cancellationTarget
	if (target === null) return false
	return activeBatches(state).some((batch) =>
		target === undefined
			? batch.cancelled
			: eventIdentity(batch.envelopes[0]) === target.identity &&
				batch.envelopes[0].acceptedAt === target.acceptedAt,
	)
}

export const retainedOutcomes = (state: MailboxState, now: number) =>
	state.outcomes.filter(
		(outcome) => outcome.expiresAt > now || (outcome.kind === 'control' && cancellationPending(state, outcome)),
	)

export const mailboxEnvelopes = (state: MailboxState) => [
	...state.pending,
	...activeBatches(state).flatMap((batch) => batch.envelopes),
	...state.failed.flatMap((batch) => batch.envelopes),
]

/** Records retained by a mailbox and charged against its configured capacity. */
export const mailboxCapacityUsage = (state: MailboxState) =>
	state.outcomes.length +
	mailboxEnvelopes(state).length +
	(state.version === 1 || state.version === 2 ? 0 : (state.operations?.length ?? 0))

export const MailboxAddress = Schema.Struct({
	namespace: Schema.NonEmptyString,
	handlerId: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	installation: Schema.NonEmptyString,
	resourceKey: Schema.String,
})
export type MailboxAddress = typeof MailboxAddress.Type

const segment = (value: string) => `${value.length}:${value}`

export const mailboxPrefix = (input: Pick<MailboxAddress, 'namespace' | 'handlerId' | 'provider'>) =>
	`delivery:v1:${[input.namespace, input.handlerId, input.provider].map(segment).join('')}`

export const mailboxKey = (input: MailboxAddress) =>
	`${mailboxPrefix(input)}${segment(input.installation)}${segment(input.resourceKey)}`

export const parseMailboxAddress = (key: string): MailboxAddress | undefined => {
	const prefix = 'delivery:v1:'
	if (!key.startsWith(prefix)) return undefined
	const values: Array<string> = []
	let remaining = key.slice(prefix.length)
	for (let index = 0; index < 5; index++) {
		const separator = remaining.indexOf(':')
		if (separator <= 0) return undefined
		const length = Number(remaining.slice(0, separator))
		if (!Number.isSafeInteger(length) || length < 0) return undefined
		const start = separator + 1
		const value = remaining.slice(start, start + length)
		if (value.length !== length) return undefined
		values.push(value)
		remaining = remaining.slice(start + length)
	}
	if (remaining.length !== 0) return undefined
	const [namespace, handlerId, provider, installation, resourceKey] = values
	if (
		namespace === undefined ||
		handlerId === undefined ||
		provider === undefined ||
		installation === undefined ||
		resourceKey === undefined
	)
		return undefined
	const address = { namespace, handlerId, provider, installation, resourceKey }
	return Schema.is(MailboxAddress)(address) && mailboxKey(address) === key ? address : undefined
}
