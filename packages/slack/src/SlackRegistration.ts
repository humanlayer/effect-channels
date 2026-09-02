import { ProviderRegistry } from '@humanlayer/channels'
import { Effect, Layer } from 'effect'

import { SlackProvider } from './SlackProvider.ts'

export const SlackRegistration = {
	layer: Layer.effectDiscard(
		Effect.gen(function* () {
			const registry = yield* ProviderRegistry
			const provider = yield* SlackProvider
			yield* registry.register(provider)
		}),
	),
}
