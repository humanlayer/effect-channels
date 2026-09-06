import { layer as delivery } from '@humanlayer/channels-delivery/redis'
import { Layer } from 'effect'

import { connections } from './redis/connections.ts'
import { subscriptions } from './redis/subscriptions.ts'

export { connections, subscriptions }
export const layer = Layer.mergeAll(connections, subscriptions, delivery)
