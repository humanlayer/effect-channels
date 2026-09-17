import { Predicate } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { mailboxEventsKey, mailboxPendingKey, mailboxStateKey, readyMailboxesKey } from './keys'

export const admit = Redis.script(
	(input: {
		readonly mailboxKey: string
		readonly eventId: string
		readonly admissionJson: string
		readonly now: number
	}) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		mailboxEventsKey(input.mailboxKey),
		input.mailboxKey,
		input.eventId,
		input.admissionJson,
		input.now,
	],
	{
		numberOfKeys: 4,
		lua: `
if redis.call('SISMEMBER', KEYS[4], ARGV[2]) == 1 then return 0 end
redis.call('SADD', KEYS[4], ARGV[2])
redis.call('RPUSH', KEYS[3], ARGV[3])
local status = redis.call('HGET', KEYS[2], 'status') or 'idle'
if status == 'idle' then
  redis.call('HSET', KEYS[2], 'status', 'idle', 'attempt', '0')
  redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
end
return 1
`,
	},
).withReturnType<unknown>()

export const claim = Redis.script(
	(input: {
		readonly mailboxKey: string
		readonly claimId: string
		readonly now: number
		readonly recoveryAt: number
	}) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		input.mailboxKey,
		input.claimId,
		input.now,
		input.recoveryAt,
	],
	{
		numberOfKeys: 3,
		lua: `
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[3]) then return false end
local status = redis.call('HGET', KEYS[2], 'status') or 'idle'
if status ~= 'idle' and status ~= 'retry' and status ~= 'active' then return false end
local batch_json
local admissions
local attempt
if status == 'retry' or status == 'active' then
  batch_json = redis.call('HGET', KEYS[2], 'batch')
  if not batch_json then return redis.error_reply('DELIVERY_RETRY_BATCH_MISSING') end
  admissions = cjson.decode(batch_json)
  attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0') + 1
else
  local pending = redis.call('LRANGE', KEYS[3], 0, -1)
  if #pending == 0 then
    redis.call('ZREM', KEYS[1], ARGV[1])
    return false
  end
  admissions = {}
  for index, encoded in ipairs(pending) do admissions[index] = cjson.decode(encoded) end
  batch_json = cjson.encode(admissions)
  redis.call('DEL', KEYS[3])
  attempt = 1
end
redis.call('HSET', KEYS[2], 'status', 'active', 'claim_id', ARGV[2], 'attempt', attempt,
  'batch', batch_json, 'ready_at', ARGV[4])
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
return cjson.encode({ attempt = attempt, admissions = admissions })
`,
	},
).withReturnType<unknown>()

export const recordResult = Redis.script(
	(input: {
		readonly mailboxKey: string
		readonly claimId: string
		readonly resultJson: string
		readonly retryAt: number | null
		readonly finishedAt: number
	}) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		input.mailboxKey,
		input.claimId,
		input.resultJson,
		Predicate.isNull(input.retryAt) ? '' : input.retryAt,
		input.finishedAt,
	],
	{
		numberOfKeys: 3,
		lua: `
if redis.call('HGET', KEYS[2], 'status') ~= 'active' or
   redis.call('HGET', KEYS[2], 'claim_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[2], 'last_result', ARGV[3])
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[2], 'status', 'retry', 'claim_id', '', 'ready_at', ARGV[4])
  redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
else
  redis.call('HSET', KEYS[2], 'status', 'idle', 'claim_id', '', 'attempt', '0', 'batch', '', 'ready_at', '')
  if redis.call('LLEN', KEYS[3]) > 0 then
    redis.call('HSET', KEYS[2], 'ready_at', ARGV[5])
    redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
  else
    redis.call('ZREM', KEYS[1], ARGV[1])
  end
end
return 1
`,
	},
).withReturnType<unknown>()
