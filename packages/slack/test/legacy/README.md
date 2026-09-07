# Historical Channels regression migration

These are tests of **native Slack and shared delivery**, not a compatibility
Channels implementation. Fixtures live here; the normal Slack suite also includes migrated emulator tests.

| Historical file                                        | Destination / retained behavior                                                                                                                                                                                                                          |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Schema.test.ts`                                       | Same name: schema-backed handles reconstruct behavior.                                                                                                                                                                                                   |
| `UserProfileCache.test.ts`                             | Same name: positive/negative cache, expiry, capacity.                                                                                                                                                                                                    |
| `Channels.status-cleanup.test.ts`                      | `Slack.status-cleanup.test.ts`: interrupted/rejected posts, no typing, once-per-typing cleanup.                                                                                                                                                          |
| `Channels.history-stream.test.ts`                      | `Slack.history-stream.test.ts`: lazy I/O, one-page calls, both directions, complete pagination, resumed cursor and per-page limits, channel messages/thread lists. Real Slack over a supplied SlackClient seam.                                          |
| `Channels.participants.test.ts`                        | `Slack.participants.test.ts`: complete history, uniqueness, current-author seed, exclusion of bots/self, empty history.                                                                                                                                  |
| `Channels.context.test.ts`                             | `Slack.context.test.ts`: native bounded history composition, preceding-channel chronology, preserving all authors, actionable history failures.                                                                                                          |
| `Channels.threading.test.ts`                           | `Slack.threading.test.ts`: explicit subscribe/unsubscribe, root/reply routing, other-bot eligibility, own-bot suppression, unrelated threads, TTL renewal/expiry.                                                                                        |
| `Channels.lifecycle.test.ts`                           | `Slack.lifecycle.test.ts`: updates/deletes/typed reactions, DM lifecycle remapping, own-event suppression before routing, rooted Stop cancels proactive DM work. Reaction filtering is application-owned.                                                |
| `Channels.dedup-retry.test.ts`                         | `Slack.dedup-retry.test.ts`: duplicate admission, backoff/retry, non-retryable metadata and advancement.                                                                                                                                                 |
| `ConversationCoordinator.memory.test.ts`               | Capped-backoff/serial progression in `Slack.dedup-retry.test.ts`; frozen batches, bounded failed retention, stale completion, two runners, recovery and cancellation in `packages/delivery/test/Delivery.test.ts`. No second memory coordinator remains. |
| `UserDirectory.test.ts`                                | `SlackUserDirectory.test.ts`: handler hydration, history hydration composition, installation/user isolation, retryable fallback, negative cache.                                                                                                         |
| `Channels.root-roundtrip.test.ts`                      | `Slack.root-roundtrip.test.ts`: signed root mention, duplicate ACK, explicit subscription, actual outgoing Slack HTTP request. Emulator integration lives in `packages/slack/test/integration`.                                                          |
| `ConversationCoordinator.postgres.test.ts` and support | Historical-only in Git at `e7894f0` / `f27947c`; the SQL coordinator suite and child-owner resource are intentionally retired. See the coverage boundaries below.                                                                                        |
| `Channels.ha.test.ts`                                  | The obsolete Channels graph and historical SQL two-owner test are retired. Shared delivery tests cover new memory ownership/concurrency, not parity with the old SQL graph.                                                                              |
| `Channels.dm-observer.test.ts`                         | Native `SlackDirectMessages.test.ts` and `SlackEphemeral.test.ts` retain canonical DM identity, native/fallback outcomes, and provider errors. Observer/gate assertions below intentionally retire.                                                      |

## Intentional obsolete assertions

- Gate/organization/`TenantDisabled` denial, including gate failures during DM
  fallback: excluded from Phase 1 rather than reimplemented in Slack.
- `ChannelsObserver` report record shapes and artificial operation-report order:
  no universal observer facade survives. Actual DM/ephemeral outcomes and errors
  remain covered, not merely report callbacks.
- `ServiceSurface.test.ts`: registry `UnknownProvider`, universal subject,
  assignment/action/command/pattern placeholders, distributed-signal placeholder,
  unimplemented debounce/concurrent/interrupt constructors, and the generic
  `unimplemented()` defect string. None is a native Slack contract or a reason to
  start later phases.
- Cross-provider GitHub profile-cache entries in the universal directory: replaced
  with Slack installation **and user** isolation, not a fake GitHub provider.
- Universal context capability negotiation and the `ContextLoadFailed` wrapper:
  callers compose native Slack history streams and retain native history errors.
- Old memory coordinator's exact warning/alert threshold text and unbounded retry
  semantics: replaced by shared delivery's explicit retry/retention policy. Serial
  tests use the internal `serial` policy; public `queue` coalescing is tested in
  shared delivery and is not mislabeled FIFO.

## Retired SQL coverage and current boundaries

The removed `packages/postgres` source and tests are available only in Git history
at `e7894f0` / `f27947c`. No placeholder test was converted into a passing no-op,
and removal does not establish equivalent coverage of the replacement adapters.

- Historical `ConversationCoordinator.postgres.test.ts` asserted durable
  dedupe/FIFO, non-retryable advancement, two-owner exclusion with cross-thread
  concurrency, heartbeat renewal beyond the original TTL, persisted capped
  retries, and actual child-process `SIGKILL` recovery after lease expiry with
  stale SQL token rejection. These old-schema tests are intentionally retired.
- Default `packages/delivery/test/Delivery.test.ts` and `DeliveryModes.test.ts`
  exercise the shared engine in memory: dedupe, retries, ownership/concurrency,
  renewal, cancellation, stale completion and interrupted-work reconstruction.
  `Slack.dedup-retry.test.ts` retains serial/non-retryable progression. Public
  queue delivery coalesces work; these tests do not preserve legacy SQL FIFO.
- Optional `packages/delivery/test-backends/StoreContract.ts` exercises real
  Postgres/Redis conditional writes, readiness, stale-attempt fencing and
  reconstruction over fresh store Layers. It interrupts a fiber and reconstructs
  processing; it does **not** kill a process. The old killed-owner/child-process
  evidence has no current equivalent and must not be claimed as covered.
- Historical `ChannelsPostgres.test.ts` asserted old composition/table creation,
  shared `channels-subscriptions` persistence and shared SQL profile-cache expiry.
  Those contracts are retired, not migrated. Optional Slack backend
  `StoreContract.ts` covers new connection/subscription/DM-route semantics;
  `BotContract.ts` covers normalized admission, reconstructed processing,
  subscription and native posting with substituted Slack HTTP. Its cached author
  lookup is not a shared SQL profile-cache test. Default cache tests remain
  memory tests; old table/namespace compatibility is not tested or promised.
- The removed `test-backends/phase2-pending/*.ts.txt` scenarios were already
  non-executable, unverified sources referencing a removed app graph. Their
  two-runtime emulator/FIFO and non-owner cancellation scenarios are historical
  design material, not passing regression evidence or a parity claim.

Default adapter tests use command seams, not real SQL/Lua. Real backend suites
remain explicitly optional; see the delivery and Slack `test-backends/README.md`
files for their exact contracts. Neither logical reconstruction nor the retired
process-kill test establishes power-loss or replica-failover guarantees. For old
data drain/export requirements, see the root README; deleting code does not
migrate pending work or subscriptions.
