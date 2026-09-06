import { layer } from '@humanlayer/channels-slack/postgres'
import { Layer } from 'effect'

import { postgresClient } from './postgres-client.ts'

export const storage = layer.pipe(Layer.provide(postgresClient))
