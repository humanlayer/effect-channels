import { Context, Effect, Layer } from 'effect'

import { ChannelProvider } from './ChannelProvider.ts'
import { ProviderAlreadyRegistered, UnknownProvider } from './Errors.ts'
import type { ProviderByChannelInput, ProviderByNameInput, ProviderByThreadIdInput } from './Operations.ts'
import { providerNames, type ProviderName } from './Schema.ts'

export const providerNameFromThreadId = (threadId: string): ProviderName | undefined => {
	for (const provider of providerNames) {
		if (threadId.startsWith(`${provider}:`)) {
			return provider
		}
	}
	return undefined
}

export class ProviderRegistry extends Context.Service<
	ProviderRegistry,
	{
		readonly register: (provider: ChannelProvider) => Effect.Effect<void, ProviderAlreadyRegistered>
		readonly byName: (input: ProviderByNameInput) => Effect.Effect<ChannelProvider, UnknownProvider>
		readonly byThreadId: (input: ProviderByThreadIdInput) => Effect.Effect<ChannelProvider, UnknownProvider>
		readonly byChannel: (input: ProviderByChannelInput) => Effect.Effect<ChannelProvider, UnknownProvider>
	}
>()('channels/ProviderRegistry') {
	static readonly layer = Layer.sync(ProviderRegistry, () => {
		const providers = new Map<ProviderName, ChannelProvider>()
		const byName = (input: ProviderByNameInput) =>
			Effect.suspend(() => {
				const provider = providers.get(input.provider)
				return provider === undefined
					? Effect.fail(UnknownProvider.make({ provider: input.provider }))
					: Effect.succeed(provider)
			})
		return ProviderRegistry.of({
			register: (provider) =>
				Effect.suspend(() => {
					if (providers.has(provider.name)) {
						return Effect.fail(ProviderAlreadyRegistered.make({ provider: provider.name }))
					}
					providers.set(provider.name, provider)
					return Effect.void
				}),
			byName,
			byThreadId: (input) => {
				const provider = providerNameFromThreadId(input.threadId)
				return provider === undefined
					? Effect.fail(UnknownProvider.make({ provider: input.threadId }))
					: byName({ provider })
			},
			byChannel: (input) => byName({ provider: input.channel.provider }),
		})
	})
}
