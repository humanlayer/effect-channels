import { layer as delivery } from '@humanlayer/channels-delivery/redis'
import { Layer } from 'effect'

import { connections } from './redis/connections.js'
import { subscriptions } from './redis/subscriptions.js'

export { connections, subscriptions }
export const layer = Layer.mergeAll(connections, subscriptions, delivery)
