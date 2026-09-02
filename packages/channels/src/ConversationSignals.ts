import { Context, Effect, Layer, PubSub, Stream } from 'effect'

import type { ConversationSignalError } from './Errors.ts'
import { unimplemented } from './internal/unimplemented.ts'
import type { ConversationSignal } from './Operations.ts'

export class ConversationSignals extends Context.Service<
	ConversationSignals,
	{
		readonly publish: (signal: ConversationSignal) => Effect.Effect<void, ConversationSignalError>
		readonly events: Stream.Stream<ConversationSignal, ConversationSignalError>
	}
>()('channels/ConversationSignals') {
	static readonly layerMemory = Layer.effect(
		ConversationSignals,
		Effect.map(PubSub.unbounded<ConversationSignal>(), (pubsub) =>
			ConversationSignals.of({
				publish: (signal) => PubSub.publish(pubsub, signal).pipe(Effect.asVoid),
				events: Stream.fromPubSub(pubsub),
			}),
		),
	)

	static readonly layerDistributed = Layer.effect(
		ConversationSignals,
		unimplemented('ConversationSignals.layerDistributed'),
	)
}
