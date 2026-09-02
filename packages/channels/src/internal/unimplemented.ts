import { Effect } from 'effect'

export const unimplemented = (operation: string): Effect.Effect<never> =>
	Effect.die(new Error(`channels: ${operation} is intentionally unimplemented`))
