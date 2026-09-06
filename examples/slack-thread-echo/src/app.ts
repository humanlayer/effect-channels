import { SlackBot } from '@humanlayer/channels-slack'

import { handlers } from './handlers.ts'

export const bot = SlackBot.memory({ namespace: 'slack-thread-echo', handlers })
export const application = bot.layer
