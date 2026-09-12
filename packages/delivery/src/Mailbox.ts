import { Schema } from 'effect'

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

export const ActiveBatch = Schema.Struct({
	envelopes: Schema.NonEmptyArray(Envelope),
	attempt: Schema.Natural,
	owner: Schema.NullOr(Schema.Natural),
	leaseUntil: Schema.Finite,
	cancelled: Schema.Boolean,
})
export type ActiveBatch = typeof ActiveBatch.Type

export const Outcome = Schema.Struct({
	identity: Schema.String,
	kind: Schema.Literals(['completed', 'cancelled', 'failed', 'control', 'dropped']),
	expiresAt: Schema.Finite,
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
	version: Schema.Literal(3),
	attribution: Schema.optionalKey(IngressAttribution),
})
export type CurrentMailboxState = typeof CurrentMailboxState.Type

export const MailboxState = Schema.Union([LegacyMailboxState, V2MailboxState, CurrentMailboxState])
export type MailboxState = typeof MailboxState.Type

/** Upgrade under revision fencing. Stop old writers before the first v3 write. */
export const currentMailbox = (state: MailboxState): CurrentMailboxState =>
	state.version !== 1
		? { ...state, version: 3 }
		: {
				...state,
				version: 3,
				additionalActive: [],
				pendingReadyAt: state.pending[0]?.acceptedAt ?? null,
				burstDraining: false,
			}

export const activeBatches = (state: MailboxState): ReadonlyArray<ActiveBatch> => [
	...(state.active === null ? [] : [state.active]),
	...(state.version !== 1 ? state.additionalActive : []),
]

export const MailboxSnapshot = Schema.Struct({ revision: Schema.Natural, state: MailboxState })
export type MailboxSnapshot = typeof MailboxSnapshot.Type

export const emptyMailbox = (): CurrentMailboxState =>
	CurrentMailboxState.make({
		version: 3,
		pending: [],
		active: null,
		additionalActive: [],
		pendingReadyAt: null,
		burstDraining: false,
		failed: [],
		outcomes: [],
		readyAt: null,
	})

export const eventIdentity = (envelope: Envelope) =>
	`${envelope.definition.length}:${envelope.definition}${envelope.eventId.length}:${envelope.eventId}`

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
