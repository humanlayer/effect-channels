import * as PostgresClient from '@humanlayer/channels-slack/postgres/client'
import { Config } from 'effect'

export const postgresClient = PostgresClient.layerConfig({ url: Config.redacted('DATABASE_URL') })
