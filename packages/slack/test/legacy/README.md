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
| `ConversationCoordinator.postgres.test.ts` and support | `packages/postgres/test/`: retained optional SQL regression suite and child-owner resource.                                                                                                                                                              |
| `Channels.ha.test.ts`                                  | Its unique ownership/concurrency assertions are already retained in the SQL coordinator two-owner test; shared-engine two-runner tests cover new memory ownership. The obsolete Channels graph itself is removed.                                        |
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

No placeholder test was silently converted into a passing no-op. Optional SQL
tests are retained separately and are never prerequisites for this suite.
