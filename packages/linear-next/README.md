# Linear Next

`@humanlayer/channels-linear-next` is the issue-centric Linear provider for `@humanlayer/channels-delivery-next`. The current slice supports signed `Issue.create` webhooks for one explicitly configured Linear workspace. The webhook is durably admitted before `onIssueCreated` runs; callbacks can subscribe the issue for later provider phases.

## Linear application setup

Create a Linear Application using the app actor and configure:

- webhook URL: `https://<host>/integrations/linear/webhook`
- webhook category: **Issues**
- OAuth scopes: `read`; add `write`, `app:mentionable`, and `app:assignable` when enabling later resource and notification capabilities
- either an application developer token or client-credentials authentication for the application-owning workspace

Always configure `LINEAR_WEBHOOK_SECRET`, `LINEAR_ORGANIZATION_ID`, and `LINEAR_APP_USER_ID`. For API authentication, configure either `LINEAR_DEVELOPER_TOKEN` or both `LINEAR_CLIENT_ID` and `LINEAR_CLIENT_SECRET`. When both forms are present, the developer token takes precedence. Both paths lazily verify the token's `viewer` app-user and organization identities before the first provider operation. Client credentials acquire and renew short-lived access tokens automatically; developer-token replacement remains the caller's responsibility. Credentials are never placed in webhook admissions.

Phase 1 does not enable Agent Session events, Inbox Notifications, or resource mutations. Unknown authenticated event types/actions are acknowledged and ignored.
