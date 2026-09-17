import { layerMailboxStoreServices } from '@humanlayer/channels-delivery'
import { layer as delivery } from '@humanlayer/channels-delivery/redis'
import { Layer } from 'effect'

import { connections } from './redis/connections'
import { subscriptions } from './redis/subscriptions'

export { connections, subscriptions }
export const layer = Layer.mergeAll(connections, subscriptions, layerMailboxStoreServices.pipe(Layer.provide(delivery)))
