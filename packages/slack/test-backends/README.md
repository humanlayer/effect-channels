# Optional Slack backend verification

These suites are excluded from the default test graph. **Do not run against an
existing service.** They require `DELIVERY_BACKEND_TEST_CONFIRM=disposable`, the
same fixed loopback ports and test credentials as the delivery suites. They read
neither `DATABASE_URL` nor `REDIS_URL`; Vite env-file loading is disabled. Postgres requires no existing
tables in the current schema; Redis requires `DBSIZE = 0`. Neither suite clears
an existing database to make it pass. Each needs its own fresh disposable
container, not a container previously used by the delivery suite. Redis tests
issue `SCRIPT FLUSH` on that disposable server. Both real suites passed during
Phase 2 integration.

Bun may load env files before launching a command; that is not an isolation
guarantee. Safety comes from ignoring application URLs, checking fresh state and
owning disposable containers, not from assuming ambient variables are absent.

## Dependencies and entrypoints

Built ESM/declaration entrypoints and optional peers are configured for
`@effect/sql-pg@4.0.0-rc.112`, `@effect/platform-node@4.0.0-rc.112` and
`redis` (NodeRedis's node-redis peer, `>=5.0.0 <7.0.0`) dependencies as appropriate.
PgClient owns its driver dependencies; strict TS consumers also install the optional
`@types/pg` peer. All Effect packages must remain rc.112.

| Source entrypoint        | Exports                                 | Required ambient capability |
| ------------------------ | --------------------------------------- | --------------------------- |
| `src/postgres.ts`        | `connections`, `subscriptions`, `layer` | `SqlClient.SqlClient`       |
| `src/redis.ts`           | `connections`, `subscriptions`, `layer` | neutral `Redis.Redis`       |
| `src/postgres/client.ts` | upstream `layer`, `layerConfig`         | creates scoped PgClient     |
| `src/redis/client.ts`    | upstream `layer`, `layerConfig`         | creates scoped NodeRedis    |

The storage exports are Layer **values**, not factories. `connections` provides
`SlackConnectionStore`, `subscriptions` provides `SlackSubscriptions`, and
`layer` merges both **and shared delivery store/readiness**. Root imports do not
reexport these adapters or clients.
Both Postgres layers acquire the shared migration layer (memoized once when
merged). Acquisition failure is `SlackConnectionStoreError` with operation
`initialize`; runtime operations expose only their stable domain errors. No
optional service lookup or application-graph dependency is used.

## Commands (opt-in only)

Prefer root `bun run test:backend:postgres` / `bun run test:backend:redis`, which
create and clean a fresh container per suite. The wrapper requires local Unix-socket
Docker, rejects remote contexts/occupied ports, and removes owned containers and
anonymous volumes on shutdown. Direct commands, if needed:

```sh
./node_modules/.bin/tsc --noEmit -p packages/slack/test-backends/tsconfig.json

docker run --rm -d --name slack-postgres-test \
  -p 127.0.0.1:55432:5432 \
  -e POSTGRES_USER=delivery_test -e POSTGRES_PASSWORD=delivery_test \
  -e POSTGRES_DB=delivery_adapter_test postgres:17-alpine
docker exec slack-postgres-test pg_isready -h 127.0.0.1 -U delivery_test -d delivery_adapter_test
DELIVERY_BACKEND_TEST_CONFIRM=disposable ./node_modules/.bin/vp test \
  --config packages/slack/test-backends/vite.postgres.config.ts
docker stop slack-postgres-test

docker run --rm -d --name slack-redis-test \
  -p 127.0.0.1:56379:6379 redis:7-alpine \
  redis-server --appendonly yes --appendfsync always --maxmemory-policy noeviction
docker exec slack-redis-test redis-cli ping
DELIVERY_BACKEND_TEST_CONFIRM=disposable ./node_modules/.bin/vp test \
  --config packages/slack/test-backends/vite.redis.config.ts
docker stop slack-redis-test
```

Wait for the explicit health check to succeed. Never redirect these ports to
existing databases. Suites leave their data for inspection; remove the
containers afterward. Each test has a 30-second timeout; bound verification
commands externally to 120 seconds.

## Storage behavior and assumptions

Postgres owns only `humanlayer_slack_v1_connections`, `_subscriptions`, `_routes`,
`_migrations`, and their new indexes. It does not read, alter, migrate or delete
historical/example data. Acquisition takes a transaction advisory lock **before**
bootstrapping the migration table and invoking bundled `Migrator.make/fromRecord`.
This avoids rc.112's absent-table `::regclass` probe aborting a transaction during
concurrent first startup. Migration statements capture safe SQL reason metadata
before the generic runner wraps a failure as a defect.

Connections have no TTL or cache. Atomic upsert replaces the entire connection;
remove is idempotent. A private persistence-only JSON codec uses
`RedactedFromValue` for the token, retaining the existing domain schema's
`disallowJsonEncode`. Tokens are encoded only for a write and decoded/redacted
before returning. Storage itself contains plaintext credentials: use appropriate
database access control, transport encryption and at-rest protection. Never log
query parameters or command arguments from client instrumentation.

Subscriptions refresh a 30-day TTL on subscribe. Routes freeze the complete
`ThreadRef` and `subscribed` decision for one day, without refreshing on replay.
Initially, a live rooted subscription beats a proactive subscription; otherwise
proactive wins if subscribed; otherwise the rooted route is unsubscribed.
Subscription changes never invalidate a live frozen route. SQL uses server
`statement_timestamp()` and Redis uses server key TTLs, avoiding host-clock
disagreement. PostgreSQL uses a single conflict-update statement to select and
return the winning frozen route, not a read/insert/do-nothing race. The stored
subscription `created` boolean reports the atomic creation-versus-refresh result.

Each Postgres subscription operation first deletes **at most 128 expired rows
per table**, using expiry indexes and `FOR UPDATE SKIP LOCKED`. Logical expiry
is enforced in every lookup even if cleanup has a backlog. Idle databases may
retain expired rows until subsequent activity. Redis uses one direct key per
subscription and route; there is no prefix index or global scan. Its route Lua
script accesses exactly three keys, all using the fixed `{state}` hash tag. Key
components encode UTF-16 units, avoiding delimiter/brace/Unicode collisions.
This is deliberately one hash slot, not a sharded throughput design. The
initial transport is standalone NodeRedis; no Redis Cluster/failover certification
is claimed. Configure noeviction and appropriate AOF, replication, backups and
capacity. Successful commands are not a power-loss durability guarantee.

## What verification establishes

Default `test/*Adapter.test.ts` suites use the actual `SqlClient.make` statement
compiler/connection seam and a covariant view of the actual neutral `Redis.Redis`
service key. They record commands and supply scripted unknown replies. They test
command construction, migrations ordering, bounded cleanup commands, codecs,
redaction and safe error narrowing—not SQL constraints or Lua execution.
The Redis seam does not reproduce script caching or claim database behavior.

Only the optional real suites establish backend atomicity: concurrent subscribe
has one creator, competing route resolvers return the identical frozen winner,
initial rooted precedence, proactive/fallback freezing across later subscription
changes, cross-runtime credential rotation/removal, fresh-layer reconstruction,
expiry, and stored end state. Redis uses two separately acquired clients and
exercises real Lua and NOSCRIPT reload. Postgres exercises concurrent first
migrations, idempotency and bounded retention. These are logical reconstruction
tests, not process-crash, power-loss or replica-failover tests.

`BotContract.ts` additionally runs the same `SlackBot.make` definition over each
real storage bundle. One runtime admits three mentions without executing handlers;
a reconstructed runtime processes the newest plus two skipped events, resolves
authors through one cached HTTP lookup, subscribes durably and posts via native
Slack. Only external Slack HTTP is substituted. Signed hosting remains covered
by the database-free Emulate tests; this contract starts at normalized admission.

Default seam suites contribute eight cases. Each optional backend file runs one
composite contract; count these as two backend tests, not dozens of independent
test cases. Package/declaration isolation has its own `verify:exports` command.
