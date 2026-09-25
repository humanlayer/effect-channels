# Slack API live smoke test

This is an explicit, one-off check against a real Slack workspace. It is not part of the automated test suite.

The script reads `SLACK_BOT_TOKEN` through `SlackApiLive`. It never prints the token. It performs read checks first, then creates one top-level channel message, one normal thread reply, and one streamed thread reply. It also adds and removes an `eyes` reaction.

## Before running

1. Create or choose a private Slack test channel and invite the bot.
2. Have a human post a new message in the channel. This human-created message is the test thread root.
3. Copy the channel ID and root message timestamp.
4. Confirm that the app has the required message, history, user, reaction, and agent scopes and that Slack agent features are enabled.
5. For file checks, add the `files:read` and `files:write` bot scopes under **OAuth & Permissions**, reinstall the app, and confirm `SLACK_BOT_TOKEN` is the reinstalled token.

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

## Files

Two optional variables add file checks:

- `SLACK_SMOKE_FILES=true` uploads one marked text file to the channel root with `SlackChannel.uploadFile` and one to the thread with `SlackThread.uploadFile`, including a title and initial comment. It downloads each file with `download()` and `downloadBytes()`, compares SHA-256 values with the uploaded bytes, and checks that `maxBytes` below the file size fails with `SlackFileSizeLimitExceeded`. Requires `files:write` and `files:read`.
- `SLACK_SMOKE_EXPECTED_FILE_SHA256=<hex>` downloads the first file in the thread, as returned by `conversations.replies`, and compares its SHA-256 with the expected value. Have a human attach a known file in the thread first. Requires `files:read`.

```bash
SLACK_SMOKE_CONFIRM=confirmed \
SLACK_SMOKE_TEAM_ID=T... \
SLACK_SMOKE_CHANNEL_ID=C... \
SLACK_SMOKE_THREAD_TS=1234567890.123456 \
SLACK_SMOKE_FILES=true \
SLACK_SMOKE_EXPECTED_FILE_SHA256=$(shasum -a 256 known-file.txt | cut -d' ' -f1) \
bun run --cwd packages/slack-next smoke:live
```

Output contains only file IDs, filenames, byte counts, and SHA-256 values; it never prints the token, upload URLs, or file contents. Uploaded files stay in the channel. To check scope errors, run again with a token that lacks `files:read` or `files:write`; the run fails with `SlackFileAuthorizationError` naming the missing scope.
