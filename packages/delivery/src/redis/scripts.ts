import * as Redis from 'effect/unstable/persistence/Redis'

import { readyKeys, recordKey } from './keys.ts'

type CommitScriptInput = {
	readonly key: string
	readonly expectedRevision: number | null
	readonly revision: number
	readonly json: string
	readonly readyAt: number | null
}

export const commit = Redis.script(
	(input: CommitScriptInput) => [
		recordKey(input),
		...readyKeys(input),
		input.expectedRevision === null ? '' : String(input.expectedRevision),
		String(input.revision),
		input.json,
		input.readyAt === null ? '' : String(input.readyAt),
		JSON.stringify(input.key),
	],
	{
		numberOfKeys: (input) => input.key.length + 2,
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
for i = 2, #KEYS do
  local index_type = redis.call('TYPE', KEYS[i]).ok
  if index_type ~= 'none' and index_type ~= 'zset' then
    return redis.error_reply('DELIVERY_INDEX_TYPE')
  end
end
redis.call('HSET', KEYS[1], 'revision', ARGV[2], 'snapshot', ARGV[3])
for i = 2, #KEYS do
  if ARGV[4] == '' then
    redis.call('ZREM', KEYS[i], ARGV[5])
  else
    redis.call('ZADD', KEYS[i], ARGV[4], ARGV[5])
  end
end
return 1
`,
	},
).withReturnType<unknown>()
