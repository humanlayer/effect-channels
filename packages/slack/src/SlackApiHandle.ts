/**
 * A second handle on the `SlackApi` service. It shares the service's key, so it reads the same slot in
 * context. Slack files and messages read the service through it: importing `SlackApi` itself would form an
 * import cycle, since `SlackApi`'s constructor builds those files and messages.
 */
import { Context } from 'effect'

import type { SlackApi } from './SlackApi'
import type { SlackApiOperations } from './SlackApiContract'

export const slackApiKey = '@humanlayer/channels-slack/SlackApi'

export const SlackApiHandle = Context.Service<SlackApi, SlackApiOperations>(slackApiKey)
