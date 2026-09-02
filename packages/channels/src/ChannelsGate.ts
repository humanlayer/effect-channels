import { Context, Effect, Layer } from 'effect'

import type { GateError } from './Errors.ts'
import type { GateCheck } from './Operations.ts'

export class ChannelsGate extends Context.Service<
	ChannelsGate,
	{
		readonly allowed: (input: GateCheck) => Effect.Effect<boolean, GateError>
	}
>()('channels/ChannelsGate') {
	static readonly layerAllowAll = Layer.succeed(
		ChannelsGate,
		ChannelsGate.of({ allowed: () => Effect.succeed(true) }),
	)

	static make(allowed: (input: GateCheck) => Effect.Effect<boolean, GateError>) {
		return Layer.succeed(ChannelsGate, ChannelsGate.of({ allowed }))
	}
}
