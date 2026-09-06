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
- `cancelActive({ key, controlId })` records a deduplicated control transition,
  freezing the target before conditional-commit retries. It cannot retarget a
  successor merely because the original attempt completed during a conflict.
  Lease renewal continues while the cancelled handler's finalizers are running.

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
