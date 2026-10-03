/**
 * The Lua scripts behind the Redis mailbox store.
 *
 * The scripts hold no delivery rules. The shared `DeliveryLifecycle` decides every change in
 * TypeScript; the scripts read a mailbox in one step (`load`) and write the decided change in one
 * step, only if nothing changed the mailbox since it was read (`commit`). Every change bumps the
 * mailbox's `version` field, which is what `commit` compares. `admit` and `defer`, the two changes
 * that need no lifecycle transition, bump it too, so a change decided before them is decided again.
 * Every time the scripts see arrives as an argument; none reads the Redis clock.
 *
 * A waiting event is one pending-list entry: `<sequence>|<arrivedAt>|<admission JSON>`.
 * The scripts cut entries apart and join them as plain text, so an admission never passes through
 * cjson, which would turn `[]` into `{}` and round large numbers. Stored JSON is opaque text to the
 * scripts for the same reason.
 *
 * A mailbox's state hash holds its scheduling fields (`status`, `ready_at`, `version`, counters) and
 * its active delivery: `batch` (admissions JSON), `batch_id`, `access_token`, `prepared`, `claim_id`,
 * `attempt`, `stage`, and `delivery`, the JSON of everything else the lifecycle keeps.
 */
import { Data, Predicate } from 'effect'
import * as Redis from 'effect/persistence/Redis'

import { mailboxEventsKey, mailboxPendingKey, mailboxRetainedKey, mailboxStateKey, readyMailboxesKey } from './Keys'

/** Splits one pending entry into its sequence, arrival time and admission JSON. */
const parseEntry = `
local function parse_entry(entry)
  local first = string.find(entry, '|', 1, true)
  local second = string.find(entry, '|', first + 1, true)
  return string.sub(entry, 1, first - 1), string.sub(entry, first + 1, second - 1), string.sub(entry, second + 1)
end
`

/**
 * Accepts an event once and wakes an idle mailbox. Returns 1 when accepted, 0 for a repeat.
 * An interrupting event goes through `commit` instead, so it marks the active delivery in the same write.
 */
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
redis.call('HINCRBY', KEYS[2], 'version', 1)
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
 * Reports one due mailbox: `{ status, provider, count, firstSequence, firstArrivedAt, lastSequence, lastArrivedAt, stage }`.
 * The waiting fields are empty strings unless the mailbox is idle; `stage` is empty unless it is not,
 * and for a batch saved before stages were stored.
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
if status ~= 'idle' then
  return { status, provider, '0', '', '', '', '', redis.call('HGET', KEYS[2], 'stage') or '' }
end
local count = redis.call('LLEN', KEYS[3])
if count == 0 then
  redis.call('HDEL', KEYS[2], 'ready_at')
  redis.call('ZREM', KEYS[1], ARGV[1])
  return false
end
local first_sequence, first_arrived_at = parse_entry(redis.call('LINDEX', KEYS[3], 0))
local last_sequence, last_arrived_at = parse_entry(redis.call('LINDEX', KEYS[3], -1))
return { status, provider, tostring(count), first_sequence, first_arrived_at, last_sequence, last_arrived_at, '' }
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
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`,
	},
).withReturnType<unknown>()

/**
 * Reads a mailbox in one step: `{ the named state fields, waiting count, retained JSON or false, waiting admissions }`.
 * The admissions are those at or below `pendingUpTo`, oldest first, and none when it is null.
 * Returns false when there is no such mailbox.
 *
 * A frozen batch saved before batches had IDs gets an ID and token here, once, and the version moves
 * so no change decided without them can be written.
 */
export const load = Redis.script(
	(input: {
		readonly mailboxKey: string
		readonly pendingUpTo: number | null
		readonly legacySeed: string
		readonly now: number
		readonly fields: ReadonlyArray<string>
	}) => [
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		mailboxRetainedKey(input.mailboxKey),
		input.mailboxKey,
		Predicate.isNull(input.pendingUpTo) ? '' : input.pendingUpTo,
		input.legacySeed,
		input.now,
		...input.fields,
	],
	{
		numberOfKeys: 3,
		lua: `${parseEntry}
if redis.call('EXISTS', KEYS[1]) == 0 then return false end
local status = redis.call('HGET', KEYS[1], 'status') or 'idle'
local batch_json = redis.call('HGET', KEYS[1], 'batch')
if status ~= 'idle' and batch_json and
   (not redis.call('HGET', KEYS[1], 'batch_id') or not redis.call('HGET', KEYS[1], 'access_token')) then
  local seed = redis.sha1hex(ARGV[1] .. '|' .. ARGV[3] .. '|' .. batch_json)
  redis.call('HSET', KEYS[1], 'batch_id', 'legacy-' .. string.sub(seed, 1, 32),
    'access_token', redis.sha1hex(seed .. '|' .. ARGV[4]))
  redis.call('HINCRBY', KEYS[1], 'version', 1)
end
local pending = {}
if ARGV[2] ~= '' then
  local up_to = tonumber(ARGV[2])
  for _, entry in ipairs(redis.call('LRANGE', KEYS[2], 0, -1)) do
    local sequence, _, admission = parse_entry(entry)
    if tonumber(sequence) > up_to then break end
    pending[#pending + 1] = admission
  end
end
return {
  redis.call('HMGET', KEYS[1], unpack(ARGV, 5)),
  redis.call('LLEN', KEYS[2]),
  redis.call('GET', KEYS[3]),
  pending,
}
`,
	},
).withReturnType<unknown>()

/** What a change does to a mailbox's finished deliveries. */
export type RetainedWrite = Data.TaggedEnum<{
	Keep: {}
	Replace: { readonly json: string; readonly ttlMs: number }
	Remove: {}
}>
export const RetainedWrite = Data.taggedEnum<RetainedWrite>()

/**
 * A change decided in TypeScript, written as it was decided.
 *
 * @property expectedVersion - the version `load` read; the write happens only if it is still current
 * @property readyAt - the mailbox's new due time, or null to take it out of the ready set
 * @property popWaiting - how many waiting entries a new batch took from the front of the list
 * @property retained - the finished deliveries: left alone, replaced with a lifetime, or removed
 * @property admission - an event to accept in the same write; a repeat writes nothing
 * @property set - state fields to set
 * @property remove - state fields to remove
 */
export type CommitInput = {
	readonly mailboxKey: string
	readonly provider: string
	readonly expectedVersion: number
	readonly readyAt: number | null
	readonly popWaiting: number
	readonly retained: RetainedWrite
	readonly admission: { readonly eventId: string; readonly arrivedAt: number; readonly json: string } | null
	readonly set: ReadonlyArray<readonly [string, string]>
	readonly remove: ReadonlyArray<string>
}

const retainedArguments = RetainedWrite.$match({
	Keep: () => ['keep', '', 0] as const,
	Replace: ({ json, ttlMs }) => ['replace', json, ttlMs] as const,
	Remove: () => ['remove', '', 0] as const,
})

/** Writes a decided change. Returns `ok`, `conflict` when the mailbox changed since it was read, or `duplicate` for a repeated admission. */
export const commit = Redis.script(
	(input: CommitInput) => [
		readyMailboxesKey,
		mailboxStateKey(input.mailboxKey),
		mailboxPendingKey(input.mailboxKey),
		mailboxRetainedKey(input.mailboxKey),
		mailboxEventsKey(input.mailboxKey),
		input.mailboxKey,
		input.expectedVersion,
		Predicate.isNull(input.readyAt) ? '' : input.readyAt,
		input.popWaiting,
		...retainedArguments(input.retained),
		input.provider,
		input.admission?.eventId ?? '',
		input.admission?.arrivedAt ?? '',
		input.admission?.json ?? '',
		input.set.length,
		...input.set.flat(),
		...input.remove,
	],
	{
		numberOfKeys: 5,
		lua: `
local version = tonumber(redis.call('HGET', KEYS[2], 'version') or '0')
if version ~= tonumber(ARGV[2]) then return 'conflict' end
if ARGV[9] ~= '' then
  if redis.call('SADD', KEYS[5], ARGV[9]) == 0 then return 'duplicate' end
  local sequence = redis.call('HINCRBY', KEYS[2], 'next_sequence', 1) - 1
  redis.call('RPUSH', KEYS[3], string.format('%.0f', sequence) .. '|' .. ARGV[10] .. '|' .. ARGV[11])
end
local popped = tonumber(ARGV[4])
if popped > 0 then redis.call('LPOP', KEYS[3], popped) end
if ARGV[8] ~= '' then redis.call('HSETNX', KEYS[2], 'provider', ARGV[8]) end
local set_count = tonumber(ARGV[12])
local first_removed = 13 + set_count * 2
if set_count > 0 then redis.call('HSET', KEYS[2], unpack(ARGV, 13, first_removed - 1)) end
if first_removed <= #ARGV then redis.call('HDEL', KEYS[2], unpack(ARGV, first_removed)) end
redis.call('HSET', KEYS[2], 'version', tostring(version + 1))
if ARGV[3] == '' then
  redis.call('HDEL', KEYS[2], 'ready_at')
  redis.call('ZREM', KEYS[1], ARGV[1])
else
  redis.call('HSET', KEYS[2], 'ready_at', ARGV[3])
  redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
end
if ARGV[5] == 'replace' then
  redis.call('SET', KEYS[4], ARGV[6], 'PX', ARGV[7])
elseif ARGV[5] == 'remove' then
  redis.call('DEL', KEYS[4])
end
return 'ok'
`,
	},
).withReturnType<unknown>()
