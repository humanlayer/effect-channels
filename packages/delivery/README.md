# Delivery

`@humanlayer/channels-delivery` is the core of [Channels](https://github.com/humanlayer/effect-channels#readme). It saves provider webhooks into mailboxes, runs your callbacks in order with retries, and lets a callback hand its delivery to a remote worker that finishes it over the delivery API.

```sh
npm install @humanlayer/channels-delivery effect
```

It includes `Channels.make`, the delivery modes, in-memory storage for tests (`ChannelsMemory.make`), the delivery API routes, and `makeDeliveryClient` for remote workers. Add a provider ([Slack](https://www.npmjs.com/package/@humanlayer/channels-slack), [GitHub](https://www.npmjs.com/package/@humanlayer/channels-github), [Linear](https://www.npmjs.com/package/@humanlayer/channels-linear)) and a store ([Postgres](https://www.npmjs.com/package/@humanlayer/channels-sql), [Redis](https://www.npmjs.com/package/@humanlayer/channels-redis), [Cloudflare](https://www.npmjs.com/package/@humanlayer/channels-alchemy-cloudflare)).

See the [main README](https://github.com/humanlayer/effect-channels#readme) for a full walkthrough.
