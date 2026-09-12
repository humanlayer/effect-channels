import { SlackBot } from '@humanlayer/channels-slack'

import { handlers } from './handlers.js'

export const bot = SlackBot.make({ namespace: 'slack-multi-tenant', handlers })
export const application = bot.layer
