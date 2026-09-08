import * as Redis from 'effect/unstable/persistence/Redis'

import type { SubscriptionInput } from '../Operations.js'
import type { ResolveSlackDirectMessageRoute } from '../SlackSubscriptions.js'
import { routeKey, subscriptionKey } from './keys.js'

export const subscribe = Redis.script(
	(input: SubscriptionInput) => [subscriptionKey(input), String(30 * 24 * 60 * 60 * 1000)],
	{
		numberOfKeys: 1,
		lua: `
local previous = redis.call('GET', KEYS[1])
if previous and previous ~= '1' then return redis.error_reply('SLACK_SUBSCRIPTION_VALUE') end
redis.call('SET', KEYS[1], '1', 'PX', ARGV[1])
if previous then return 0 else return 1 end
`,
	},
).withReturnType<unknown>()

interface ResolveScriptInput extends ResolveSlackDirectMessageRoute {
	readonly rootedJson: string
	readonly proactiveJson: string
	readonly fallbackJson: string
}

export const resolve = Redis.script(
	(input: ResolveScriptInput) => [
		routeKey(input),
		subscriptionKey({ threadId: input.rootedThread.id }),
		subscriptionKey({ threadId: input.proactiveThread.id }),
		input.rootedJson,
		input.proactiveJson,
		input.fallbackJson,
		String(24 * 60 * 60 * 1000),
	],
	{
		numberOfKeys: 3,
		lua: `
local frozen = redis.call('GET', KEYS[1])
if frozen then return frozen end
local rooted = redis.call('GET', KEYS[2])
local proactive = redis.call('GET', KEYS[3])
if (rooted and rooted ~= '1') or (proactive and proactive ~= '1') then
  return redis.error_reply('SLACK_SUBSCRIPTION_VALUE')
end
local route = ARGV[3]
if rooted then route = ARGV[1] elseif proactive then route = ARGV[2] end
redis.call('SET', KEYS[1], route, 'PX', ARGV[4])
return route
`,
	},
).withReturnType<unknown>()
