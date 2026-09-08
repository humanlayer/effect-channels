import { SlackBot } from '@humanlayer/channels-slack'

import { handlers } from './handlers.js'

export const bot = SlackBot.make({ namespace: 'slack-thread-echo', handlers })
export const application = bot.layer
