# Added-lines diff check

**Agent action: copy this script when review-time added-line policy is useful,
then adapt its configuration.**

The script runs `no-let` and `no-explicit-function-return-type` only on lines
added since a selected base revision. It can also report configurable key-file
changes. Existing lines are intentionally outside its scope, so this check
stays separate from full-source validation.

Use changed-file alerts to direct human attention toward exceptional risk such
as migrations, public contracts, security policy, runtime configuration, or
dependency state. The goal is not another full review of every changed file.

```sh
node --import tsx src/main.ts --config diff-check.config.json --base origin/main
```

Configuration controls the base reference, OXLint config path, source
extensions, excluded path prefixes, and changed-file rules. Matchers support
individual files, directories, and extension exceptions. Neutral examples
include migrations, configuration files, and lockfiles.

Exit status is `0` when clean, `1` for violations, and `2` for invalid
configuration or execution failures. The integration test creates a temporary
Git repository and proves both shipped rules apply only to added lines.
