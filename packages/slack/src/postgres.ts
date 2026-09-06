import { layer as delivery } from '@humanlayer/channels-delivery/postgres'
import { Layer } from 'effect'

import { connections } from './postgres/connections.ts'
import { subscriptions } from './postgres/subscriptions.ts'

export { connections, subscriptions }
export const layer = Layer.mergeAll(connections, subscriptions, delivery)
