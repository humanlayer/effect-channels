# channels

- [`examples/slack-thread-echo`](./examples/slack-thread-echo/) — minimal single-workspace Slack application.
- [`examples/slack-multi-tenant`](./examples/slack-multi-tenant/) — a provider-owned Slack `loadConnection` callback for multiple workspaces.

## Slack provider testing

Slack integration tests are emulator-first and run as part of the ordinary test suite:

```bash
vp test
```

This is the default, credential-free test command for the whole repository. It uses memory storage and local provider emulators, does not read `.env`, does not contact live providers, does not require Docker or Postgres, and excludes infrastructure-only Postgres suites. Test discovery is restricted to this repository's package and example test directories, so dependency tests under `node_modules` are never collected.

The app suite starts `emulate@0.11.0` through a scoped Effect test Layer, points the production `SlackClient` at the emulator's `/api` origin, and closes both the application runtime and emulator deterministically. It exercises signed webhook admission, public and private channel mentions and subscribed follow-ups, mentions inside integration-authored threads, bot-loop suppression, provider-backed history in both directions, pagination signals, `allMessages`, channel/thread metadata and listing, participants, user hydration, and unknown/disabled connection routing. Assertions read Slack state through the emulator's HTTP APIs rather than its internal store.

The emulator's high-level API emits ordinary Slack `message` events for writes but does not expose an `app_mention` simulator. Mention tests therefore create the message through the emulator first, then construct and sign the corresponding `app_mention` delivery from the returned channel, timestamp, text, and author fields. This keeps provider state authoritative while using the signed webhook only as the inbound delivery driver.

The emulator currently models one Slack workspace per server and does not implement `agents.sessions.setStatus`. True two-workspace callback routing remains covered by the connection-layer and Postgres example tests; typing/status request encoding remains covered by the exact Slack client tests. The emulator's `conversations.replies` implementation also returns a complete thread rather than implementing Slack's timestamp pagination, so exact multi-page cursor behavior remains in the Slack client contract tests while the emulator suite verifies both directions and the backward-page continuation signal.

When `DATABASE_URL` is available, an isolated-schema integration suite sends a signed mention through one complete application runtime and its human follow-up through a second runtime sharing Postgres. It verifies durable subscription transfer, single-owner FIFO handling, and both production Slack replies in the original emulator thread. The schema, application runtimes, SQL pools, and emulator are all scoped and finalized by Effect.

`bun run test:live:slack` is an optional minimal smoke check against a real Slack workspace. It is not required for CI or normal development, and recording files are not part of the test workflow.
