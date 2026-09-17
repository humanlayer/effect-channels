import * as Redis from 'effect/unstable/persistence/Redis'

import { deliveryLocatorKey, readyKeys, recordKey } from './keys'

type CommitScriptInput = {
	readonly key: string
	readonly expectedRevision: number | null
	readonly revision: number
	readonly json: string
	readonly readyAt: number | null
	readonly deliveryIds: ReadonlyArray<string>
}

export const commit = Redis.script(
	(input: CommitScriptInput) => [
		recordKey(input),
		...readyKeys(input),
		...(input.deliveryIds.length === 0 ? [] : [deliveryLocatorKey]),
		input.expectedRevision === null ? '' : String(input.expectedRevision),
		String(input.revision),
		input.json,
		input.readyAt === null ? '' : String(input.readyAt),
		JSON.stringify(input.key),
		JSON.stringify(input.deliveryIds),
	],
	{
		numberOfKeys: (input) => input.key.length + 2 + (input.deliveryIds.length === 0 ? 0 : 1),
		lua: `
local record_type = redis.call('TYPE', KEYS[1]).ok
if record_type ~= 'none' and record_type ~= 'hash' then
  return redis.error_reply('DELIVERY_RECORD_TYPE')
end
local current = redis.call('HGET', KEYS[1], 'revision')
if record_type == 'hash' and not current then
  return redis.error_reply('DELIVERY_RECORD_REVISION')
end
if ARGV[1] == '' then
  if current then return 0 end
elseif current ~= ARGV[1] then
  return 0
end
local ready_end = #KEYS
if ARGV[6] ~= '[]' then
  ready_end = #KEYS - 1
  local locator_type = redis.call('TYPE', KEYS[#KEYS]).ok
  if locator_type ~= 'none' and locator_type ~= 'hash' then
    return redis.error_reply('DELIVERY_LOCATOR_TYPE')
  end
  local delivery_ids = cjson.decode(ARGV[6])
  for _, delivery_id in ipairs(delivery_ids) do
    local existing = redis.call('HGET', KEYS[#KEYS], delivery_id)
    if existing and existing ~= ARGV[5] then
      return redis.error_reply('DELIVERY_LOCATOR_CONFLICT')
    end
  end
end
for i = 2, ready_end do
  local index_type = redis.call('TYPE', KEYS[i]).ok
  if index_type ~= 'none' and index_type ~= 'zset' then
    return redis.error_reply('DELIVERY_INDEX_TYPE')
  end
end
redis.call('HSET', KEYS[1], 'revision', ARGV[2], 'snapshot', ARGV[3])
for i = 2, ready_end do
  if ARGV[4] == '' then
    redis.call('ZREM', KEYS[i], ARGV[5])
  else
    redis.call('ZADD', KEYS[i], ARGV[4], ARGV[5])
  end
end
if ARGV[6] ~= '[]' then
  local delivery_ids = cjson.decode(ARGV[6])
  for _, delivery_id in ipairs(delivery_ids) do
    redis.call('HSET', KEYS[#KEYS], delivery_id, ARGV[5])
  end
end
return 1
`,
	},
).withReturnType<unknown>()
