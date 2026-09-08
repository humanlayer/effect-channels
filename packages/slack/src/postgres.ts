import { layer as delivery } from '@humanlayer/channels-delivery/postgres'
import { Layer } from 'effect'

import { connections } from './postgres/connections.js'
import { subscriptions } from './postgres/subscriptions.js'

export { connections, subscriptions }
export const layer = Layer.mergeAll(connections, subscriptions, delivery)
