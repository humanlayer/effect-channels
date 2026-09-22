# Linear Next

`@humanlayer/channels-linear-next` is the issue-centric Linear provider for `@humanlayer/channels-delivery-next`. The current slice supports signed `Issue.create` webhooks for one explicitly configured Linear workspace. The webhook is durably admitted before `onIssueCreated` runs; callbacks can subscribe the issue for later provider phases.

## Linear application setup

Create a Linear Application using the app actor and configure:

- webhook URL: `https://<host>/integrations/linear/webhook`
- webhook category: **Issues**
- OAuth scopes: `read`; add `write`, `app:mentionable`, and `app:assignable` when enabling later resource and notification capabilities
- client-credentials authentication for the application-owning workspace

Configure `LINEAR_WEBHOOK_SECRET`, `LINEAR_CLIENT_ID`, `LINEAR_CLIENT_SECRET`, `LINEAR_ORGANIZATION_ID`, and `LINEAR_APP_USER_ID`. The client secret and webhook secret must be treated as secrets. Access tokens are acquired lazily by the API layer; they are never placed in webhook admissions.

Phase 1 does not enable Agent Session events, Inbox Notifications, or resource mutations. Unknown authenticated event types/actions are acknowledged and ignored.
