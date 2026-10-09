# Alchemy Cloudflare

`@humanlayer/channels-alchemy-cloudflare` runs [Channels](https://github.com/humanlayer/effect-channels#readme) on Cloudflare with [Alchemy](https://alchemy.run): a Worker takes webhooks and serves the delivery API, and one Durable Object per mailbox stores its events and runs your callbacks from its alarm.

```sh
npm install @humanlayer/channels-alchemy-cloudflare @humanlayer/channels-delivery effect alchemy
```

```ts
export const bot = ChannelsCloudflare.make({ namespace: 'my-app', providers: [slack], eventProcessing })

export class DeliveryMailbox extends Cloudflare.DurableObject<DeliveryMailbox, MailboxMethods>()('DeliveryMailbox') {}
```

`bot.layers.mailbox` and `bot.layers.worker` contain the default leaf layers. Compose them at the Durable Object and Worker entrypoints, where any leaf can be replaced for tests or another host implementation. The Durable Object's alarm belongs to the mailbox; don't use it for anything else. See [`examples/alchemy-cloudflare`](https://github.com/humanlayer/effect-channels/tree/main/examples/alchemy-cloudflare) for the complete composition.
