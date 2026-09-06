# Shared delivery — memory, Postgres and Redis

`@humanlayer/channels-delivery` is the shared mailbox implementation used by
native Slack ingress. Memory, Postgres and Redis Layers implement the same atomic
store contracts; delivery policy stays in one engine. The old Channels and app packages
have been removed. The package name and production defaults remain provisional.

## Boundaries

- Root exports contain Effect schemas, typed handler bindings, and ambient
  `MailboxStore` / `MailboxReadiness` service contracts. No backend, provider,
  HTTP client, database driver, or runner starts during import.
- `/memory` supplies both services from one Layer acquisition. Every load
  decodes a fresh schema-backed snapshot. Conditional creation/update and
  readiness publication happen in the same synchronous mutation.
- `bind` retains the event-codec-to-handler relationship in its closure.
  `admit({ event })` encodes the event and resource, calculates a collision-safe
  key, and conditionally persists acceptance without executing the handler.
- `processMailbox({ key })` claims and awaits one invocation. Queue mode selects
  the newest pending event and supplies earlier events in `context.skipped`.
  Internal serial mode invokes every event separately. Each invocation owns a
  scope; its registered finalizers finish before the attempt is recorded complete.
- `run({ scanLimit, concurrency, pollMs })` is a long-running Effect, not an
  automatically started background task. The host must supervise it within its
  scope. Scan limits and concurrency are explicit. Storage errors, defects, and
  incompatible payloads surface to the host; the host must decide how to report
  and restart a failed readiness scan. Individual processing failures are logged
  and do not stop unrelated mailboxes; their persisted state controls recovery.
- `cancelActive({ key, controlId, eventId? })` records a deduplicated control transition,
  freezing the target before conditional-commit retries. It cannot retarget a
  successor merely because the original attempt completed during a conflict.
  Lease renewal continues while the cancelled handler's finalizers are running.
  `eventId` optionally selects the active batch containing that caller-owned event
  ID for this binding's definition, including a frozen retry or a coalesced/skipped
  event. Callers obtain the key from `admit`, `keyFor`, or `keyForResource`; no
  storage read or attempt-owner knowledge is needed. The engine snapshots and
  fences the attempt internally: completion or retry cannot redirect an in-flight
  control to a replacement attempt, even for the same event ID.
  Without `eventId`, the first retained active batch is targeted, not every concurrent
  invocation. A missing/stale target still records the control identity, so replay
  cannot target future work. Slack Stop uses this default per message handler.
  Pending-only, completed, and unknown event IDs do not target active peers.
  Reusing a control ID does not retarget it, even if a different event ID is supplied.
- `awaitCancellation({ key, controlId })` waits for that control's recorded batch
  to retire, not for the mailbox to become idle. Slack's Stop callback uses this
  barrier: unrelated concurrent work and later batches may keep running. The
  control outcome stores the target envelope identity and admission time (or
  explicit `null` for no target), so callback retries and runtime reconstruction
  use the same target across lease recovery. Finalizers complete before the
  engine retires the batch; lease renewal during cleanup is unchanged.
  Control outcomes stay retained, and count against capacity, while their target
  remains active—even past normal retention expiry. Missing/expired retired
  controls and explicit no-target controls impose no barrier. `awaitInactive`
  retains its separate, mailbox-wide meaning for callers that need it.
  Both barriers check immediately and repeat with a heartbeat-spaced Schedule
  while work remains; storage failures propagate and waiting is interruptible.
  Older stored controls without target metadata remain readable and conservatively
  wait only for cancelled active batches, never unrelated uncancelled work; their
  exact historical target cannot be reconstructed. All writers must use the same
  revision of the code to avoid stripping the new optional metadata.

## Delivery modes

All modes commit before acknowledgment and leave execution to the host. Configure
the common limits plus one of these mode-specific settings:

| Policy                                      | Behavior                                                                                                                                                                                           |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `{ mode: 'queue' }`                         | Immediately eligible when idle. Claim the latest pending event with earlier pending events in `context.skipped`. Never interrupt active work. Arrivals before the first claim may coalesce too.    |
| `{ mode: 'concurrent', maxConcurrency: 2 }` | Each event has its own invocation and no skipped context. Shared persisted active batches enforce the per-mailbox bound across runners. No completion ordering guarantee.                          |
| `{ mode: 'debounce', quietPeriodMs: 1500 }` | Each new eligible admission resets a persisted pending deadline. Duplicates do not reset it. Run after both the quiet period and active completion; continuous arrivals can postpone indefinitely. |
| `{ mode: 'drop' }`                          | Pending (including ready-but-unclaimed), active or retrying work is busy. Record a `dropped` dedupe outcome for new busy arrivals before acknowledgment; never execute those arrivals.             |
| `{ mode: 'burst', windowMs: 1500 }`         | First arrival starts a fixed persisted window, not extended by later arrivals. After the first claim, drain queue-style immediately until idle. The next idle arrival starts a new window.         |

The internal `serial` policy still delivers every lifecycle event separately.
All numeric mode settings must be positive integers; there are no implicit window
or concurrency defaults. `SlackBot.make({ policy: ... })` accepts these settings
with its existing common-limit defaults. Slack lifecycle registrations always use
serial delivery, regardless of the configured message mode.

Retries reuse the frozen batch and skipped context, not newer pending work. A
retrying batch reserves a concurrent slot until completion or terminal failure.
Runner `concurrency` independently bounds that host's processing fibers; the
policy's `maxConcurrency` bounds shared mailbox attempts. A runner can process
multiple attempts in one mailbox, adding at most one per key per scan pass.
Handlers and finalizers are awaited before freeing a slot.

`admit().accepted` means a **new durable admission outcome**, including deliberate
drop, not a promise of handler execution. Duplicate admissions return `false`.
Dropped identities count against `maxOutcomes` and expire under `retentionMs`;
capacity exhaustion rejects admission rather than acknowledging an unrecorded drop.

## Persisted mailbox upgrade (v1 → v2)

New code reads both mailbox document versions. Its next conditional commit writes
v2 in the **same key/table/Redis namespace**, preserving revision fencing and all
pending, active, failed and outcome records. No SQL or Redis policy/migration script
changes are needed: those backends store the document and readiness atomically.
V2 adds `pendingReadyAt`, `burstDraining`, and `additionalActive`; `active` remains
the first batch. Use `activeBatches(state)` to inspect all concurrent batches.
V1 queue/serial work retains its existing eligibility and frozen retry deadlines.

Upgrade procedure: pause ingress, stop **all** old workers/writers, back up the
store, then start the new code with the same handler IDs, definitions, keys and
queue/serial policies before resuming ingress. Existing work need not be discarded
or drained to read it. Change a handler's mode only when its mailbox work is idle,
and use the same configuration on all hosts. Old binaries reject v2; mixed-version
operation and in-place rollback after a v2 write are unsupported. To roll back,
stop new writers and reconcile/export post-upgrade admissions before restoring a
backup—blind restoration would lose accepted work. No upgrade or live store
operation is performed by the test suite.

## Guarantees and limits

The `/memory` adapter is **volatile**, not disk persistence. Logical worker reconstruction
over the same acquired Layer can recover a frozen batch after its lease expires;
reconstructing the Layer or restarting the process loses all records.

`DeliveryPolicy` requires explicit payload-byte, envelope-count, outcome-count,
retention, attempt, backoff, lease, heartbeat, and conflict limits. There are no
approved production defaults. The tests use deliberately small settings.

Accepted pending/active events are never evicted on overflow. Failed batches
are retained for inspection through `loadMailbox` and count against capacity.
Completed/cancelled dedupe outcomes expire lazily during later operations.
Active, pending, and retained failed events continue to deduplicate irrespective
of outcome expiry. Capacity accounting reserves room for terminal outcomes and
is deliberately conservative for failed batches.

Memory's `maxMailboxes` is a lifetime bound per acquired Layer; idle mailbox
records are not deleted or recycled. Reaching it rejects new mailbox creation.
There is not yet an administrative failed-work replay/pruning API. Do not edit
state or clear the Layer to free capacity in an application that must retain
accepted work.

Retryable handler failures use bounded exponential backoff and preserve the
original batch. New admissions remain pending. Exhausted/non-retryable batches
are retained as failed and do not prevent newer work from becoming ready.
Defects are surfaced and the batch is retained as failed, not automatically retried.
Host interruption leaves the lease recoverable. Pure interruption is not logged as
a processing error. A failed readiness scan still requires host supervision.

Ownership fencing protects mailbox state, **not external side effects**. An
expired or interrupted handler may already have posted a message; retry can
repeat that effect. External operations need their own idempotency strategy.
All hosts sharing a distributed backend need compatible clocks and the same
registration/policy configuration. Real backend contracts verify atomicity and
logical reconstruction; they do not certify power-loss or replica failover.

Mailbox keys use length-prefixed namespace, handler, provider, installation, and
canonical resource-key segments. Definition version lives on the envelope, not
in the mailbox key. Incompatible definition versions and corrupt payloads fail
explicitly and retain their batch. These formats are **not** compatible with the
legacy Channels SQL tables; no legacy data migration or live operation occurred.

## Slack integration

Slack owns its native event codecs, subscriptions, and stable routing records.
Each explicitly named handler gets an independent mailbox. Required-handler
fan-out must finish admission before HTTP success; provider retry fills any
missing admissions without repeating the committed ones. Lifecycle events use
serial delivery instead of message coalescing. Stop is a targeted control
transition. The consuming example owns the HTTP server and explicit runner.

Normal tests exercise memory Layers, actual Slack clients/routes, Emulate, and
narrow SQL/Redis command seams. Optional backend suites separately exercise real
storage. Neither suite establishes disk-crash or Cloudflare guarantees.

## Optional storage Layers

```ts
import { layer as storage } from '@humanlayer/channels-delivery/postgres'
import { layerConfig } from '@humanlayer/channels-delivery/postgres/client'
import { Config, Layer } from 'effect'

const durable = storage.pipe(Layer.provide(layerConfig({ url: Config.redacted('DATABASE_URL') })))
```

`/postgres` and `/redis` export Layer **values**, each providing `MailboxStore`
and `MailboxReadiness`. They depend on ambient `SqlClient.SqlClient` or neutral
`Redis.Redis`; they do not construct pools/clients. Only `/postgres/client` and
`/redis/client` require optional platform peers. Postgres migrations are bundled,
versioned and acquisition-time, serialized before migration-table bootstrap.
Snapshot and readiness updates commit together; failed CAS changes neither.

Redis uses atomic scripts and literal-prefix readiness indexes, with no TTL on
delivery records. Every touched key occupies the fixed `{mailboxes}` slot.
This avoids namespace starvation but costs O(key length) index memberships and
O(key length squared) aggregate prefix-key bytes. Keep identities compact.
The initial verified topology is standalone Redis, not sharded Redis Cluster.
Configure **noeviction**, headroom, persistence/replication and backups. Script
success is not a disk acknowledgement; scripts cannot roll back arbitrary
resource errors. See [backend details](./test-backends/README.md).

Root, memory, backend and client entries export built ESM and declarations.
`bun run verify:exports` checks isolated packed consumers both without optional
peers and with them, browser emitted-code isolation, strict declarations,
cross-entry service identity and a single Effect runtime. Root/memory imports
neither require nor acquire a database client.

`bun run test:backend:postgres` / `bun run test:backend:redis` use fresh scoped
Docker containers for delivery and Slack separately. The normal test graph never
activates these from an ambient database URL.

Effect dependencies are pinned to rc.112. The root catalog reserves Alchemy
`2.0.0-beta.76`, whose published Effect peers require at least rc.112. Alchemy
has not been installed or built, so its full transitive graph is not yet verified.
