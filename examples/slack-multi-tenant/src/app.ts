import { SlackBot } from '@humanlayer/channels-slack'

import { handlers } from './handlers.ts'

export const bot = SlackBot.memory({ namespace: 'slack-multi-tenant', handlers })
export const application = bot.layer
