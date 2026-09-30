/**
 * The Lua scripts behind the Redis mailbox store.
 *
 * The scripts know nothing about delivery modes. They report what is waiting and take exactly
 * what they are told to take. Every time they see arrives as an argument; none reads the Redis clock.
 *
 * A waiting event is one pending-list entry: `<sequence>|<arrivedAt>|<admission JSON>`.
 * The scripts cut entries apart and join admissions as plain text, so an admission never passes
 * through cjson, which would turn `[]` into `{}` and round large numbers. The saved preparation is
 * opaque text to the scripts for the same reason.
 *
 * Besides its status and lease, a mailbox's state hash holds its frozen batch while one exists:
 * `batch` (admissions JSON), `batch_id`, `access_token`, and `prepared` once a claim has prepared it.
 */
import { Predicate } from 'effect'
import * as Redis from 'effect/unstable/persistence/Redis'

import { mailboxEventsKey, mailboxPendingKey, mailboxStateKey, readyMailboxesKey } from './Keys'

/** Splits one pending entry into its sequence, arrival time and admission JSON. */
const parseEntry = `
local function parse_entry(entry)
  local first = string.find(entry, '|', 1, true)
  local second = string.find(entry, '|', first + 1, true)
  return string.sub(entry, 1, first - 1), string.sub(entry, first + 1, second - 1), string.sub(entry, second + 1)
end
`

export const admit = Redis.script(
	(input: {
		readonly mailboxKey: string
		readonly provider: string
		readonly eventId: string
		readonly admissionJson: string
		readonly now: number
	}) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		mailboxEventsKey(input.mailboxKey),
		input.mailboxKey,
		input.provider,
		input.eventId,
		input.admissionJson,
		input.now,
	],
	{
		numberOfKeys: 4,
		lua: `
if redis.call('SADD', KEYS[4], ARGV[3]) == 0 then return 0 end
local sequence = redis.call('HINCRBY', KEYS[2], 'next_sequence', 1) - 1
redis.call('RPUSH', KEYS[3], string.format('%.0f', sequence) .. '|' .. ARGV[5] .. '|' .. ARGV[4])
local status = redis.call('HGET', KEYS[2], 'status')
if not status then
  redis.call('HSET', KEYS[2], 'status', 'idle', 'attempt', '0', 'provider', ARGV[2])
  status = 'idle'
end
if status == 'idle' then
  redis.call('HSET', KEYS[2], 'ready_at', ARGV[5])
  redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
end
return 1
`,
	},
).withReturnType<unknown>()

/**
 * Reports one due mailbox: `{ status, provider, count, firstSequence, firstArrivedAt, lastSequence, lastArrivedAt }`.
 * The waiting fields are empty strings unless the mailbox is idle.
 * Returns false when the mailbox is not due. An idle mailbox with nothing waiting leaves the ready set.
 */
export const look = Redis.script(
	(input: { readonly mailboxKey: string; readonly now: number }) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		input.mailboxKey,
		input.now,
	],
	{
		numberOfKeys: 3,
		lua: `${parseEntry}
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then return false end
local status = redis.call('HGET', KEYS[2], 'status') or 'idle'
local provider = redis.call('HGET', KEYS[2], 'provider') or ''
if status ~= 'idle' then return { status, provider, '0', '', '', '', '' } end
local count = redis.call('LLEN', KEYS[3])
if count == 0 then
  redis.call('HDEL', KEYS[2], 'ready_at')
  redis.call('ZREM', KEYS[1], ARGV[1])
  return false
end
local first_sequence, first_arrived_at = parse_entry(redis.call('LINDEX', KEYS[3], 0))
local last_sequence, last_arrived_at = parse_entry(redis.call('LINDEX', KEYS[3], -1))
return { status, provider, tostring(count), first_sequence, first_arrived_at, last_sequence, last_arrived_at }
`,
	},
).withReturnType<unknown>()

/**
 * Freezes a batch and starts its lease. Returns `{ claimId, attempt, batch JSON, batchId, accessToken, prepared }`,
 * with `prepared` empty until a claim has saved one, or false when the mailbox is not due or not in the state
 * the caller named.
 *
 * With `newBatch` it takes waiting events from an idle mailbox, oldest first, up to its sequence, and saves the
 * new batch's ID and token. Without it, it takes the frozen batch of an active or retry mailbox again, keeping
 * its ID, token and preparation. A frozen batch saved before batches had IDs gets one on its next claim.
 */
export const claim = Redis.script(
	(input: {
		readonly mailboxKey: string
		readonly claimNonce: string
		readonly now: number
		readonly leaseUntil: number
		readonly newBatch: {
			readonly upToSequence: number
			readonly batchId: string
			readonly accessToken: string
		} | null
	}) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		input.mailboxKey,
		input.claimNonce,
		input.now,
		input.leaseUntil,
		Predicate.isNull(input.newBatch) ? '' : input.newBatch.upToSequence,
		Predicate.isNull(input.newBatch) ? '' : input.newBatch.batchId,
		Predicate.isNull(input.newBatch) ? '' : input.newBatch.accessToken,
	],
	{
		numberOfKeys: 3,
		lua: `${parseEntry}
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[3]) then return false end
local status = redis.call('HGET', KEYS[2], 'status') or 'idle'
local batch_json
local attempt
local batch_id
local access_token
local prepared = ''
if ARGV[5] ~= '' then
  if status ~= 'idle' then return false end
  local up_to = tonumber(ARGV[5])
  local admissions = {}
  while true do
    local entry = redis.call('LINDEX', KEYS[3], 0)
    if not entry then break end
    local sequence, _, admission = parse_entry(entry)
    if tonumber(sequence) > up_to then break end
    redis.call('LPOP', KEYS[3])
    admissions[#admissions + 1] = admission
  end
  if #admissions == 0 then return false end
  batch_json = '[' .. table.concat(admissions, ',') .. ']'
  attempt = 1
  batch_id = ARGV[6]
  access_token = ARGV[7]
  redis.call('HDEL', KEYS[2], 'prepared')
else
  if status ~= 'active' and status ~= 'retry' then return false end
  batch_json = redis.call('HGET', KEYS[2], 'batch')
  if not batch_json then return false end
  attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0') + 1
  batch_id = redis.call('HGET', KEYS[2], 'batch_id')
  access_token = redis.call('HGET', KEYS[2], 'access_token')
  if not batch_id or not access_token then
    local seed = redis.sha1hex(ARGV[1] .. '|' .. ARGV[2] .. '|' .. batch_json)
    batch_id = 'legacy-' .. string.sub(seed, 1, 32)
    access_token = redis.sha1hex(seed .. '|' .. ARGV[3])
  end
  prepared = redis.call('HGET', KEYS[2], 'prepared') or ''
end
local claim_id = ARGV[2] .. '-' .. string.format('%.0f', redis.call('HINCRBY', KEYS[2], 'claims_made', 1))
redis.call('HSET', KEYS[2], 'status', 'active', 'claim_id', claim_id, 'attempt', attempt,
  'batch', batch_json, 'batch_id', batch_id, 'access_token', access_token, 'ready_at', ARGV[4])
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
return { claim_id, attempt, batch_json, batch_id, access_token, prepared }
`,
	},
).withReturnType<unknown>()

/** Moves an idle mailbox's wake-up time, unless an event newer than `lastSequenceSeen` has arrived. */
export const defer = Redis.script(
	(input: { readonly mailboxKey: string; readonly until: number; readonly lastSequenceSeen: number }) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		input.mailboxKey,
		input.until,
		input.lastSequenceSeen,
	],
	{
		numberOfKeys: 3,
		lua: `${parseEntry}
if (redis.call('HGET', KEYS[2], 'status') or 'idle') ~= 'idle' then return 0 end
local last = redis.call('LINDEX', KEYS[3], -1)
if not last then return 0 end
local last_sequence = parse_entry(last)
if tonumber(last_sequence) ~= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', KEYS[2], 'ready_at', ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`,
	},
).withReturnType<unknown>()

/** Moves the lease of a live claim forward. Returns 0 when the claim is no longer the caller's. */
export const renew = Redis.script(
	(input: { readonly mailboxKey: string; readonly claimId: string; readonly leaseUntil: number }) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		input.mailboxKey,
		input.claimId,
		input.leaseUntil,
	],
	{
		numberOfKeys: 2,
		lua: `
if redis.call('HGET', KEYS[2], 'status') ~= 'active' or
   redis.call('HGET', KEYS[2], 'claim_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[2], 'ready_at', ARGV[3])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
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
redis.call('HDEL', KEYS[2], 'claim_id')
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[2], 'status', 'retry', 'ready_at', ARGV[4])
  redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
  return 1
end
redis.call('HSET', KEYS[2], 'status', 'idle', 'attempt', '0')
redis.call('HDEL', KEYS[2], 'batch', 'batch_id', 'access_token', 'prepared')
if redis.call('LLEN', KEYS[3]) > 0 then
  redis.call('HSET', KEYS[2], 'ready_at', ARGV[5])
  redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
else
  redis.call('HDEL', KEYS[2], 'ready_at')
  redis.call('ZREM', KEYS[1], ARGV[1])
end
return 1
`,
	},
).withReturnType<unknown>()

/**
 * Saves the preparation of the batch a running claim owns, unless one is saved already.
 * Returns `{ saved preparation JSON, batchId }`, or false when the claim is no longer the caller's.
 * The caller compares the saved preparation with its own.
 */
export const prepare = Redis.script(
	(input: { readonly mailboxKey: string; readonly claimId: string; readonly preparedJson: string }) => [
		mailboxStateKey(input.mailboxKey),
		input.claimId,
		input.preparedJson,
	],
	{
		numberOfKeys: 1,
		lua: `
if redis.call('HGET', KEYS[1], 'status') ~= 'active' or
   redis.call('HGET', KEYS[1], 'claim_id') ~= ARGV[1] then return false end
local batch_id = redis.call('HGET', KEYS[1], 'batch_id')
if not batch_id then return false end
local saved = redis.call('HGET', KEYS[1], 'prepared')
if not saved then
  redis.call('HSET', KEYS[1], 'prepared', ARGV[2])
  saved = ARGV[2]
end
return { saved, batch_id }
`,
	},
).withReturnType<unknown>()
