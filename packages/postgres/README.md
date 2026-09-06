# Transitional legacy Postgres storage

This package preserves the existing SQL implementation while Phase 1 removes
`packages/channels`. It depends on native Slack types, **not** the deleted Channels
facade. Neither Slack nor shared delivery imports this package.

This is **not** the Phase 2 Postgres backend for shared delivery. It does not
implement `MailboxStore`, and no Redis or new delivery/backend subpath export is
provided here. Do not supply `layerFromClient` to a new Slack application expecting
durable shared delivery; that application must explicitly supply compatible stores.

## Retained ownership

- `ConversationCoordinator` / `ConversationCoordinatorPostgres.ts`: legacy SQL
  admission, FIFO delivery, leases/heartbeats, durable dedupe, cancellation,
  retries, and stale-owner guards. `layerPostgres` uses an ambient Effect SqlClient.
- `LegacySubscriptions`: access to the old `channels-subscriptions` persistence
  namespace. It is not `SlackSubscriptions` and does not implement stable DM-route
  storage. It is available for transitional callers/drain work only.
- `UserProfileCachePostgres`: the existing disposable profile cache, using the
  native Slack cache service. Its retention here does not make it mandatory.
- `layer`, `layerConfig`, and `layerFromClient`: existing composition of legacy
  coordinator, Effect SQL persistence, and profile cache.

The coordinator's event codec now accepts native Slack message/update/delete,
reaction, and Stop events. Old universal assignment/action/command placeholders
are not supported. No new code claims compatibility with arbitrary historical
provider events.

## Existing data

No table is dropped or renamed by this code transfer. The old table names,
subscription namespace, and SQL lease/dedupe semantics are retained. Shared
delivery's keys/state are different; do not point it at old rows or automatically
delete old pending work.

Before switching a deployment, separately authorize and plan an offline drain or
export/import: stop old ingress, inventory pending mailbox rows and subscriptions,
back up the old namespace, drain supported Slack events with the legacy runner (or
export unsupported payloads unchanged), verify pending counts, then switch ingress
to the new storage. Copying legacy rows into new delivery state is not a migration.
No live data operation was performed as part of this source transfer.

## Optional regression tests

`test/ConversationCoordinator.postgres.test.ts` retains durable dedupe/FIFO,
non-retryable advancement, two-owner exclusion, cross-thread concurrency,
heartbeat renewal, capped retries, killed-owner recovery, and stale-token guards.
`test/ChannelsPostgres.test.ts` retains composition, shared legacy subscriptions,
and shared cache/expiry tests, now using isolated test schemas.

These files are excluded by the normal root test configuration. They were not run
during Phase 1 retirement: no live or disposable database was accessed. They need
an explicitly selected optional test configuration and an authorized disposable
Postgres instance. A set `DATABASE_URL` alone must not activate them in the normal
suite.
