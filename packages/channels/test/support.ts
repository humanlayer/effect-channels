import { Layer } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

import {
	Channels,
	ChannelsGate,
	ChannelsObserver,
	ConversationCoordinator,
	ConversationSignals,
	Organizations,
	ProviderRegistry,
	Subscriptions,
} from '../src/index.ts'

const persistence = Persistence.layerMemory
const subscriptions = Subscriptions.layer.pipe(Layer.provide(persistence))

export const CoreDependencies = Layer.mergeAll(
	ConversationCoordinator.layerMemory(),
	ConversationSignals.layerMemory,
	ProviderRegistry.layer,
	Organizations.layerDefault,
	ChannelsGate.layerAllowAll,
	ChannelsObserver.layerLogger,
	subscriptions,
)

export const ChannelsLayer = Channels.layer().pipe(Layer.provide(CoreDependencies))
