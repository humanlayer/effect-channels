# Linear Next

`@humanlayer/channels-linear-next` is the issue-centric Linear provider for `@humanlayer/channels-delivery-next`. It supports Agent Session entry points, subscribed issue/resource events, and provider-native issue, comment, reaction, user-directory, and external link-card operations. Webhooks are durably admitted before callbacks run.

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

| Category             | Webhook type          | Used for                                                                                                                  |
| -------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Agent session events | `AgentSessionEvent`   | `created` and `prompted` start and continue agent work.                                                                   |
| Issues               | `Issue`               | `onIssueCreated`, plus updates and removal of subscribed issues.                                                          |
| Comments             | `Comment`             | Comments on subscribed issues.                                                                                            |
| Emoji reactions      | `Reaction`            | Reactions on subscribed issues and their comments.                                                                        |
| Issue attachments    | `Attachment`          | Link and file cards on subscribed issues.                                                                                 |
| Inbox notifications  | `AppUserNotification` | Mention and assignment notices. They are acknowledged without starting duplicate work, because Agent Sessions already do. |
| Permission changes   | `PermissionChange`    | Team access changes; acknowledged and logged.                                                                             |
| OAuth app events     | `OAuthApp`            | App revocation; acknowledged and logged.                                                                                  |

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

## Live API smoke

See [`scripts/README.md`](scripts/README.md). The smokes are opt-in and never print access tokens.
