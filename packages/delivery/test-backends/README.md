# Optional delivery backend verification

These suites are **not run by the default test configuration**. They require new,
disposable local containers and explicitly opt in through
`DELIVERY_BACKEND_TEST_CONFIRM=disposable`. They never read `DATABASE_URL`,
`REDIS_URL`; Vite env-file loading is disabled. Connections use fixed loopback ports and test credentials.
They refuse a nonempty Redis or an already migrated Postgres database. Do not
point existing services or port forwards at those ports. Tests leave their data
for inspection; remove the disposable containers afterward.

Launchers such as Bun may load env files before starting a command. Isolation does
not rely on an empty environment: application database URLs are ignored, fixtures
reject existing state, and the wrapper owns fresh containers on reserved test ports.

Both real backend suites passed during Phase 2 integration. Fake command tests in
`../test/*Adapter.test.ts` establish SQL/Redis command construction, decoding and
safe error narrowing, **not** database atomicity or disk durability. The Redis
test seam uses a covariant `Context.Key` view of the actual `Redis.Redis` key with
unknown command replies; the production adapter decodes those replies. It does
not evaluate Lua or reproduce Effect's script cache. Only the optional real Redis
suite exercises `Redis.make`/NodeRedis, Lua execution, and NOSCRIPT reload.

## Dependencies and commands

The manifest declares optional peers and development dependencies for
`@effect/sql-pg@4.0.0-rc.112`, `@effect/platform-node@4.0.0-rc.112`, and
`redis` (node-redis; installed platform peer range `>=5.0.0 <7.0.0`). PgClient
brings its own `pg` driver dependencies; strict TypeScript consumers also install
the optional `@types/pg` peer. Tests use the existing
`@effect/vitest`, `effect`, and `vite-plus`; no Testcontainers package is required.

Prefer root `bun run test:backend:postgres` / `bun run test:backend:redis`.
They own fresh containers per suite and clean them up, including anonymous volumes,
on success/failure/interruption. A local Unix-socket Docker daemon is required;
remote contexts and occupied ports are refused. For direct runs instead:

```sh
./node_modules/.bin/tsc --noEmit -p packages/delivery/test-backends/tsconfig.json

docker run --rm -d --name delivery-postgres-test \
  -p 127.0.0.1:55432:5432 \
  -e POSTGRES_USER=delivery_test -e POSTGRES_PASSWORD=delivery_test \
  -e POSTGRES_DB=delivery_adapter_test postgres:17-alpine
docker exec delivery-postgres-test pg_isready -h 127.0.0.1 -U delivery_test -d delivery_adapter_test
DELIVERY_BACKEND_TEST_CONFIRM=disposable ./node_modules/.bin/vp test \
  --config packages/delivery/test-backends/vite.postgres.config.ts
docker stop delivery-postgres-test

docker run --rm -d --name delivery-redis-test \
  -p 127.0.0.1:56379:6379 redis:7-alpine \
  redis-server --appendonly yes --appendfsync always --maxmemory-policy noeviction
docker exec delivery-redis-test redis-cli ping
DELIVERY_BACKEND_TEST_CONFIRM=disposable ./node_modules/.bin/vp test \
  --config packages/delivery/test-backends/vite.redis.config.ts
docker stop delivery-redis-test
```

Wait for each explicit health check to succeed before running its suite. The
tests have a 30-second test timeout. Bound each verification command externally
to 120 seconds. Do not run both suites against reused containers: each expects a
fresh store. Redis tests issue **SCRIPT FLUSH** on that disposable server.

The shared contract exercises conditional creation, competing CAS, snapshot and
readiness agreement, null readiness removal, stale-revision rejection, literal
wildcards and unrelated earlier work. It also exercises the actual shared engine
for stale attempt fencing and interrupted-work recovery across fresh store Layer
acquisitions. Postgres additionally tests concurrent first startup and migration
idempotency. Redis additionally checks every index membership, no TTL, preflight
of wrong-type index keys, and script reload. This is logical reconstruction, not
process-crash, power-loss, replica-failover, or Redis Cluster certification.

## Public integration surface

| Subpath            | Exports                                           | Ambient dependency             |
| ------------------ | ------------------------------------------------- | ------------------------------ |
| `/postgres`        | `layer`, `migrate`, `PostgresInitializationError` | `SqlClient.SqlClient`          |
| `/postgres/client` | upstream `layer`, `layerConfig`                   | none; creates scoped PgClient  |
| `/redis`           | `layer`                                           | neutral `Redis.Redis`          |
| `/redis/client`    | upstream `layer`, `layerConfig`                   | none; creates scoped NodeRedis |

Both store Layers provide **MailboxStore and MailboxReadiness**. `layer` is a
Layer value, not a factory. Client `layer`/`layerConfig` are upstream functions,
not combined storage constructors. Provide one client to all relevant storage
Layers; the store captures that acquired client for subsequent operations. Root
and memory must not reexport these modules. Optional drivers occur only in the
two client source modules. All four entrypoints have ESM/declaration exports.

Postgres acquisition runs `migrate`. It takes a transaction advisory lock before
creating the migration table or invoking `Migrator.make`. The migration table is
bootstrapped under the lock because rc.112's generic Postgres runner probes
`::regclass`; probing an absent relation inside the outer transaction would abort
that transaction even if its Effect failure were caught. Bundled versioned
`Migrator.fromRecord` effects then run under the same transaction, with the
generic runner's nested transaction/savepoint. Fixed `humanlayer_delivery_v1_*`
tables and advisory-lock identifiers are owned by this adapter. The historical
Postgres tables are untouched and are not automatically compatible. The old
package is retired to Git history at `e7894f0` / `f27947c`; see the root README
for the required old-data drain/export planning.

## Readiness and Redis deployment assumptions

Postgres commits snapshot state and `ready_at` in **one conditional statement**.
Readiness uses a parameterized, escaped LIKE prefix with filtering before LIMIT;
`%`, `_`, `!`, and backslashes are literal input. Readiness/key partial indexes
support the query. A failed CAS cannot change readiness.

Redis maintains a sorted set for **each literal UTF-16 prefix** of a mailbox key,
including the empty and full prefixes. This includes the registration prefix
produced by `mailboxPrefix`, while preserving the stable contract's arbitrary
prefix behavior. A scan is a single `ZRANGEBYSCORE ... LIMIT 0 n` on the exact
prefix's index, never an unbounded SCAN, global due scan with post-filtering, or
an offset that can starve a namespace behind unrelated keys. Prefix keys encode
UTF-16 units as hex; index members are JSON strings, preserving delimiters,
braces, Unicode and lone surrogates without Redis glob semantics.

The tradeoff is write/storage amplification: O(key length) sorted-set
memberships per ready mailbox, with O(key length squared) aggregate index-key
bytes for a newly introduced key. Keep application namespace/handler/resource
identities compact. The adapter imposes no new admission or delivery policy.
Inactive mailboxes retain their record but lose all readiness memberships;
empty sorted sets disappear naturally. No snapshot or ready index gets a TTL.

Every key uses the fixed `{mailboxes}` hash tag, so each multi-key script is
same-slot even when identities contain braces. This is deliberately a **single
slot throughput domain**, not a sharded queue. Supported initial transport is
standalone Redis through NodeRedis's node-redis client. The neutral seam can
accept another implementation, but a cluster-aware client's routing, script
loading, retries and failover require separate verification.

Lua checks revision and all key types before mutating revision/snapshot and every
ready index in one script. Scripts are storage operations only; queue, retry,
lease and cancellation decisions stay in the shared engine. The adapter never
expires or evicts records. Configure **noeviction**, adequate headroom, appropriate
AOF/replication/backups, and exclusive ownership of the versioned keyspace.
Redis scripts do not provide general rollback after arbitrary command/resource
errors; type preflight prevents known wrong-type partial writes. Successful
commands are not themselves a disk or replica acknowledgement guarantee. Choose
your persistence/failover acknowledgement policy outside this adapter. Both
backends require compatible host clocks and delivery policies, and neither can
fence external provider side effects after a stale handler's lease expires.

## Verification scope

Default delivery tests include seven command-seam cases. Each optional real backend
file runs one composite contract containing the atomicity/recovery assertions
described above. Fresh Postgres and Redis runs both passed during integration;
this is not evidence for power-loss, process-kill or cluster/failover behavior.
Packed imports, declarations, browser isolation and dependency identity are checked
separately by `bun run verify:exports`.
