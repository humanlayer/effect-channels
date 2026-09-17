import { Context, Effect, Layer, Schema } from 'effect'

import { emptyMailbox, IngressAttribution } from './Mailbox'
import { MailboxStore, MailboxStoreError } from './MailboxStore'

export const AttributionIdentity = Schema.Struct({
	namespace: Schema.NonEmptyString,
	provider: Schema.NonEmptyString,
	installation: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
})
export interface AttributionIdentity extends Schema.Schema.Type<typeof AttributionIdentity> {}

export const SaveIngressAttribution = Schema.Struct({ ...AttributionIdentity.fields, ...IngressAttribution.fields })
export interface SaveIngressAttribution extends Schema.Schema.Type<typeof SaveIngressAttribution> {}

const reject = (input: {
	readonly operation: 'load' | 'commit'
	readonly classification:
		| 'invalid_identity'
		| 'invalid_attribution'
		| 'incompatible_record'
		| 'missing_attribution'
		| 'missing_winner'
}) =>
	Effect.logError('Delivery attribution rejected', input).pipe(
		Effect.andThen(Effect.fail(MailboxStoreError.make({ operation: input.operation }))),
	)

export const ingressAttributionKey = (input: AttributionIdentity) =>
	`delivery-attribution:v1:${JSON.stringify([input.namespace, input.provider, input.installation, input.eventId])}`

export const loadIngressAttribution = Effect.fn('delivery.attribution.load')(function* (input: AttributionIdentity) {
	const identity = yield* Schema.decodeEffect(AttributionIdentity)(input).pipe(
		Effect.catchTag('SchemaError', () => reject({ operation: 'load', classification: 'invalid_identity' })),
	)
	const store = yield* MailboxStore
	const snapshot = yield* store.loadMailbox({ key: ingressAttributionKey(identity) })
	if (snapshot === undefined) return undefined
	if (snapshot.state.version !== 3 && snapshot.state.version !== 4 && snapshot.state.version !== 5)
		return yield* reject({ operation: 'load', classification: 'incompatible_record' })
	if (snapshot.state.attribution === undefined)
		return yield* reject({ operation: 'load', classification: 'missing_attribution' })
	return snapshot.state.attribution
})

export const saveIngressAttribution = Effect.fn('delivery.attribution.save')(function* (input: SaveIngressAttribution) {
	const identity = yield* Schema.decodeEffect(AttributionIdentity)(input).pipe(
		Effect.catchTag('SchemaError', () => reject({ operation: 'commit', classification: 'invalid_identity' })),
	)
	const attribution = yield* Schema.decodeEffect(IngressAttribution)(input).pipe(
		Effect.catchTag('SchemaError', () => reject({ operation: 'commit', classification: 'invalid_attribution' })),
	)
	const store = yield* MailboxStore
	const committed = yield* store.commitMailbox({
		key: ingressAttributionKey(identity),
		expectedRevision: null,
		nextState: { ...emptyMailbox(), attribution },
	})
	if (committed === 'committed') return attribution
	const winner = yield* loadIngressAttribution(identity)
	if (winner === undefined) return yield* reject({ operation: 'load', classification: 'missing_winner' })
	return winner
})

export class IngressAttributionStore extends Context.Service<
	IngressAttributionStore,
	{
		readonly load: (input: AttributionIdentity) => Effect.Effect<IngressAttribution | undefined, MailboxStoreError>
		readonly save: (input: SaveIngressAttribution) => Effect.Effect<IngressAttribution, MailboxStoreError>
	}
>()('delivery/IngressAttributionStore') {
	/** Persists attribution records through the supplied MailboxStore. */
	static readonly layerMailboxStore = Layer.effect(
		IngressAttributionStore,
		Effect.gen(function* () {
			const store = yield* MailboxStore
			return IngressAttributionStore.of({
				load: (input) => loadIngressAttribution(input).pipe(Effect.provideService(MailboxStore, store)),
				save: (input) => saveIngressAttribution(input).pipe(Effect.provideService(MailboxStore, store)),
			})
		}),
	)
}
