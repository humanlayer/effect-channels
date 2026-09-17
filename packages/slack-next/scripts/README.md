# Slack API live smoke test

This is an explicit, one-off check against a real Slack workspace. It is not part of the automated test suite.

The script reads `SLACK_BOT_TOKEN` through `SlackApiLive`. It never prints the token. It performs read checks first, then creates one top-level channel message, one normal thread reply, and one streamed thread reply. It also adds and removes an `eyes` reaction.

## Before running

1. Create or choose a private Slack test channel and invite the bot.
2. Have a human post a new message in the channel. This human-created message is the test thread root.
3. Copy the channel ID and root message timestamp.
4. Confirm that the app has the required message, history, user, reaction, and agent scopes and that Slack agent features are enabled.

For a Slack message link ending in `p1234567890123456`, the timestamp is `1234567890.123456`.

## Run

From the repository root:

```bash
SLACK_SMOKE_CONFIRM=confirmed \
SLACK_SMOKE_TEAM_ID=T... \
SLACK_SMOKE_CHANNEL_ID=C... \
SLACK_SMOKE_THREAD_TS=1234567890.123456 \
bun run --cwd packages/slack-next smoke:live
```

The command explicitly loads the repository-root `.env` file, where `SLACK_BOT_TOKEN` is expected. The confirmation variable prevents accidental writes.
