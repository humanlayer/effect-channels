import { layerMailboxStoreServices } from '@humanlayer/channels-delivery'
import { layer as delivery } from '@humanlayer/channels-delivery/postgres'
import { Layer } from 'effect'

import { connections } from './postgres/connections'
import { subscriptions } from './postgres/subscriptions'

export { connections, subscriptions }
export const layer = Layer.mergeAll(connections, subscriptions, layerMailboxStoreServices.pipe(Layer.provide(delivery)))
