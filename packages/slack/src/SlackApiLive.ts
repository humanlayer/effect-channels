import { Layer } from 'effect'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

import { SlackHttpClient } from './api/SlackHttpClient'
import { SlackApi } from './SlackApi'

export { SlackPostMessageRequest, SlackPostMessageResponse } from './SlackApiWire'

/** Live Slack API implementation. Slack transport, decoding, pagination, and credentials stay private. */
export const SlackApiLiveBase = SlackApi.layer.pipe(Layer.provide(SlackHttpClient.layer))

/** Slack API implementation with the standard Fetch transport. */
export const SlackApiLive = SlackApiLiveBase.pipe(Layer.provide(FetchHttpClient.layer))
