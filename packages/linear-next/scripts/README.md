# Linear API live smoke

Run only against a disposable development workspace:

```sh
LINEAR_CLIENT_ID=... \
LINEAR_CLIENT_SECRET=... \
LINEAR_ORGANIZATION_ID=... \
LINEAR_APP_USER_ID=... \
LINEAR_SMOKE_ISSUE_ID=... \
LINEAR_SMOKE_USER_ID=... \
bun run --cwd packages/linear-next smoke:api:live
```

The default run verifies the client-credentials token through `viewer`, fetches the issue, pages the human/app directories, lists comments and external link-card attachments, and resolves one known user. It prints IDs and counts only; it never prints or persists the generated token.

To enable destructive mutations, also set `LINEAR_SMOKE_CONFIRM_MUTATIONS=channels-live-p4-<timestamp>` plus `LINEAR_SMOKE_STATE_ID`, `LINEAR_SMOKE_PRIORITY`, `LINEAR_SMOKE_LABEL_ID`, `LINEAR_SMOKE_ASSIGNEE_ID`, and `LINEAR_SMOKE_DELEGATE_ID`. The script sequentially exercises issue status, priority, label add/remove, assignment, delegation, comment create/edit/reply, reaction add/remove, and external link-card create/update/remove. It cleans up its comments and link card but intentionally does not guess how to restore workspace-specific issue fields. Verify each visible result and confirm resulting app-authored webhooks are admitted and self-suppressed.

For the identity guard, temporarily use an incorrect `LINEAR_APP_USER_ID`; the initial `viewer` request must fail before the issue read. Use a read-only credential to verify reads succeed while mutations return a typed nonretryable `forbidden` failure.

# Linear files live smoke

Run only against a disposable issue in a development workspace. Every run uploads files, so it requires an explicit marker:

```sh
LINEAR_DEVELOPER_TOKEN=... \
LINEAR_ORGANIZATION_ID=... \
LINEAR_APP_USER_ID=... \
LINEAR_SMOKE_ISSUE_ID=... \
LINEAR_FILES_SMOKE_CONFIRM=channels-live-p5-<timestamp> \
bun run --cwd packages/linear-next smoke:files:live
```

`LINEAR_CLIENT_ID` and `LINEAR_CLIENT_SECRET` may replace `LINEAR_DEVELOPER_TOKEN`. The script uploads a marked text file and a one-pixel PNG through `LinearIssue.uploadFile`, uploads a third file through `uploadAttachment`, creates an external link card through `createAttachment`, and posts a comment containing both asset URLs plus an ordinary external link. It checks that the comment exposes exactly the two uploaded files and that a lookalike `uploads.linear.app.example.com` URL yields none. It then compares `download()` and `downloadBytes()` with the originals by SHA-256 and confirms that `maxBytes` below the file size fails with `LinearFileSizeLimitExceeded`.

Output contains only filenames, byte counts, attachment IDs, counts, and SHA-256 values. The comment and both attachment cards are removed afterward; set `LINEAR_FILES_SMOKE_KEEP=true` to keep them for manual inspection. Uploaded assets have no delete operation and remain in the workspace.
