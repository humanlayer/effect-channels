# Linear

`@humanlayer/channels-linear` is the issue-centric Linear provider for `@humanlayer/channels-delivery`. It supports Agent Session entry points, subscribed issue/resource events, and provider-native issue, comment, reaction, user-directory, and external link-card operations. Webhooks are durably admitted before callbacks run.

## Linear application setup

Linear has no app manifest; configure the application by hand in Linear's API settings. Create it using the app actor, so its actions appear as the app rather than a person.

- **Webhook URL:** `https://<host>/integrations/linear/webhook`
- **Scopes:** `read`, `write`, `app:mentionable`, `app:assignable`

### Scopes

| Scope             | Used for                                                                             |
| ----------------- | ------------------------------------------------------------------------------------ |
| `read`            | Reading issues, comments, attachments, users, and files.                             |
| `write`           | Comments, reactions, issue changes, attachments, file uploads, and agent activities. |
| `app:mentionable` | Letting people mention the app, which starts an Agent Session.                       |
| `app:assignable`  | Letting people assign or delegate issues to the app, which starts an Agent Session.  |

### Webhook categories

| Category             | Webhook type          | Used for                                                                                                                                                                                                                    |
| -------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Agent session events | `AgentSessionEvent`   | `created` and `prompted` start and continue agent work.                                                                                                                                                                     |
| Issues               | `Issue`               | `onIssueCreated`, plus updates and removal of subscribed issues.                                                                                                                                                            |
| Comments             | `Comment`             | Comments on subscribed issues.                                                                                                                                                                                              |
| Emoji reactions      | `Reaction`            | Reactions on subscribed issues and their comments.                                                                                                                                                                          |
| Issue attachments    | `Attachment`          | Link and file cards on subscribed issues.                                                                                                                                                                                   |
| Inbox notifications  | `AppUserNotification` | Mention and assignment notices. They run `onMentioned` and `onAssigned`, even on a subscribed issue. With Agent Session callbacks they are acknowledged without starting duplicate work, because Agent Sessions already do. |
| Permission changes   | `PermissionChange`    | Team access changes; acknowledged and logged.                                                                                                                                                                               |
| OAuth app events     | `OAuthApp`            | App revocation; acknowledged and logged.                                                                                                                                                                                    |

The app's own changes are admitted and then ignored, so they never re-enter callbacks. Other event types and actions are acknowledged and ignored.

### Credentials

```dotenv
LINEAR_WEBHOOK_SECRET=...
LINEAR_ORGANIZATION_ID=...
LINEAR_APP_USER_ID=...
LINEAR_DEVELOPER_TOKEN=...
# or, instead of the developer token:
LINEAR_CLIENT_ID=...
LINEAR_CLIENT_SECRET=...
```

- `LINEAR_WEBHOOK_SECRET` verifies webhooks. Pass it to `LinearBot.make` as `webhookSecret`.
- `LINEAR_ORGANIZATION_ID` and `LINEAR_APP_USER_ID` are the one workspace and app user this bot serves. Before its first API call, the bot checks that its token belongs to both.
- `LINEAR_DEVELOPER_TOKEN` is the application's developer token. It takes precedence when set. Replace it yourself if it is revoked.
- `LINEAR_CLIENT_ID` and `LINEAR_CLIENT_SECRET` are used only when no developer token is set. The bot then fetches and renews short-lived tokens itself.

`LinearAuth.fromEnvironment` reads these variables. Credentials are never stored in webhook admissions or mailboxes.

Unknown authenticated event types/actions are acknowledged and ignored. API credentials are resolved lazily; the first operation verifies `viewer.id` and `viewer.organization.id` before its requested query or mutation runs.

## Files

Linear keeps three representations separate:

| Representation         | Operation                                                                 | Result                                |
| ---------------------- | ------------------------------------------------------------------------- | ------------------------------------- |
| Markdown link or image | existing issue/comment Markdown writes                                    | content containing a `LinearFile` URL |
| Uploaded asset         | `issue.uploadFile({ filename, contentType, bytes })`                      | `LinearFile`                          |
| First-class issue card | `issue.uploadAttachment(...)` or `issue.createAttachment({ url, title })` | `LinearIssueAttachment`               |

`LinearIssue.files` and `LinearComment.files` are discovered from parsed Markdown links, images, reference links, autolinks, and GFM bare URLs. Only canonical `https://uploads.linear.app/<workspace>/<asset>/<file>` URLs become files; ordinary links, lookalike hosts, and other URL forms stay content.

`file.download()` streams bytes; `file.downloadBytes({ maxBytes })` fails with `LinearFileSizeLimitExceeded` from the declared size, `Content-Length`, or the running byte count. Downloads send the workspace credential only to `https://uploads.linear.app`, follow redirects manually within `LinearApiLiveOptions.filePolicy` (default three hops and a 60-second response timeout), and never forward the credential to another origin. Uploads call Linear's `fileUpload` mutation, then `PUT` the exact bytes to the signed target with only the returned headers and declared content type. The signed target URL and bearer token never appear in public values or logs. Uploads are limited to 50 MiB.

## Remote delivery

Every callback receives a `DeliveryContext` and may hand its delivery to a remote worker. The remote worker then drives the output through the delivery API; `LinearBot` sends it to Linear.

| Operation                              | Agent Session (`onAgentSessionCreated`, `onAgentSessionPrompted`)                                                                    | Issue (the other callbacks)                                                    |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `complete` / `fail` (`PresentOutcome`) | one `response`, `error`, or `elicitation` (with `select` choices for `awaitingInput` options); a short default text without Markdown | a comment when there is Markdown, otherwise nothing                            |
| `activity.set` `Working`               | ephemeral thought, replaced by the next activity                                                                                     | not supported (409)                                                            |
| `activity.set` `Idle`                  | nothing: Linear sets the session's state from its last activity                                                                      | not supported (409)                                                            |
| `messages.create`                      | lasting thought                                                                                                                      | comment                                                                        |
| `messages.update` / `delete`           | not supported (409): activities cannot change                                                                                        | edit or delete the comment                                                     |
| `links.add`                            | a labeled link on the session (`agentSessionUpdate`)                                                                                 | nothing                                                                        |
| `reactions.set`                        | on the comment that started the session, else the issue; not on the session's messages (409)                                         | on the mentioning comment, else the issue, or on a comment the delivery posted |
| `plan.put`                             | the whole Agent Plan (`agentSessionUpdate` with `plan`), replaced each time                                                          | one plan comment, edited for each later plan                                   |

`GET /deliveries/<id>` lists what the delivery supports in `supportedOperations`. The final activity replaces any ephemeral thought, so a session turn needs no separate step to clear its activity. Linear marks a session stale after 30 minutes without an activity, and any later activity revives it; on long turns, send `Working` every few minutes.

Session output is exactly-once. Each activity carries a UUID that stays the same on every attempt, and Linear refuses a second activity with an ID it has seen (`LinearApiError` reason `already_exists`), which counts as done. The automatic `Working on this…` thought before `onAgentSessionCreated` uses an ID made from the delivery ID, so a callback retry does not post it twice. Issue comments stay at-least-once.

A reaction add carries its operation's UUID as the reaction's ID; Linear answers a repeat with the reaction it already has, so a retry never reacts twice. Linear removes a reaction by its ID, so a removal deletes the reaction this delivery's last add made; with no such add, there is nothing it can find, and the removal makes no call. Portable reactions map to Linear's emoji names, which are Slack-style short codes: `thumbs_up` is `+1`, `thumbs_down` is `-1`, `laugh` is `laughing`, `hooray` is `tada`; the rest keep their names. Linear refuses a name it doesn't know. `GET /deliveries/<id>` lists where reactions can go in `reactionTargets`.

A session's plan uses Linear's Agent Plan, which Linear replaces whole on every update. Items map to steps `pending`, `inProgress`, `completed`, and `canceled`; Linear has no failed step, so a failed item is `canceled` and its text says why. The Agent Plan API is a technology preview. If Linear refuses the plan as invalid, the session shows the item in progress as an ephemeral thought instead, for the rest of that delivery. An issue delivery keeps one plan comment, as GitHub does.

Stop arrives as a `prompted` event with `signal: stop`. It marks the handed-off turn, and the remote worker sees `interruptRequested: true` in the status, stops, and calls `fail` or `complete`; that posts the final activity Linear expects. The stop prompt then runs `onAgentSessionPrompted` as the next turn, with `prompt.signal` set to `stop`. A question (`complete` with `awaitingInput`) ends the turn; the user's reply is a new delivery.

## Live API smoke

See [`scripts/README.md`](scripts/README.md). The smokes are opt-in and never print access tokens.
