# SQL

`@humanlayer/channels-sql` stores [Channels](https://github.com/humanlayer/effect-channels#readme) mailboxes in Postgres.

```sh
npm install @humanlayer/channels-sql @humanlayer/channels-delivery effect @effect/sql-pg
```

```ts
const bot = Channels.make({
	namespace: 'my-app',
	providers: [slack],
	eventProcessing: { concurrency: 8, maxAttempts: 5, leaseMs: 30_000 },
	storage: ChannelsSql.make({ claimLimit: 50, runMigrations: true, polling: { intervalMs: 1_000 } }),
})
```

Provide a `SqlClient`, such as `PgClient.layerConfig(...)` from `@effect/sql-pg`. With `runMigrations: true` the store creates its tables on start. Postgres has nothing to wake processing, so it polls.
