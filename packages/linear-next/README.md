# Linear Next

`@humanlayer/channels-linear-next` is the issue-centric Linear provider for `@humanlayer/channels-delivery-next`. It supports Agent Session entry points, subscribed issue/resource events, and provider-native issue, comment, reaction, user-directory, and external link-card operations. Webhooks are durably admitted before callbacks run.

## Linear application setup

Create a Linear Application using the app actor and configure:

- webhook URL: `https://<host>/integrations/linear/webhook`
- webhook category: **Issues**
- OAuth scopes: `read`; add `write`, `app:mentionable`, and `app:assignable` when enabling later resource and notification capabilities
- either an application developer token or client-credentials authentication for the application-owning workspace

Always configure `LINEAR_WEBHOOK_SECRET`, `LINEAR_ORGANIZATION_ID`, and `LINEAR_APP_USER_ID`. For API authentication, configure either `LINEAR_DEVELOPER_TOKEN` or both `LINEAR_CLIENT_ID` and `LINEAR_CLIENT_SECRET`. When both forms are present, the developer token takes precedence. Both paths lazily verify the token's `viewer` app-user and organization identities before the first provider operation. Client credentials acquire and renew short-lived access tokens automatically; developer-token replacement remains the caller's responsibility. Credentials are never placed in webhook admissions.

Unknown authenticated event types/actions are acknowledged and ignored. API credentials are resolved lazily; the first operation verifies `viewer.id` and `viewer.organization.id` before its requested query or mutation runs.

## Live API smoke

See [`scripts/README.md`](scripts/README.md). The smoke is opt-in and never prints access tokens.
