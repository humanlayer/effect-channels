# Redis

`@humanlayer/channels-redis` stores [Channels](https://github.com/humanlayer/effect-channels#readme) mailboxes in Redis.

```sh
npm install @humanlayer/channels-redis @humanlayer/channels-delivery effect
```

```ts
const bot = Channels.make({
	namespace: 'my-app',
	providers: [slack],
	eventProcessing: { concurrency: 8, maxAttempts: 5, leaseMs: 30_000 },
	storage: ChannelsRedis.make({ claimLimit: 50, polling: { intervalMs: 1_000 } }),
})
```

Provide Effect's `Redis` service, such as `NodeRedis.layer(...)` from `@effect/platform-node`.
