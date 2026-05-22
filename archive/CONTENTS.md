# Archive Contents

Branch: `archive/untracked-artifacts-2026-05-22`
Purpose: preserve local-only artifacts before server teardown.

## Files

- `worktrees.tgz`
  - Tarball of `.claude/worktrees/`
  - Contains nested agent worktree directories and their contents.

- `home-notes.tgz`
  - Tarball of `/home/zac/*.md`
  - Captures top-level research/design/strategy notes outside the repo.

- `stash-0.patch` ... `stash-30.patch`
  - Patch exports of each `git stash` entry present at archive time.
  - One file per stash index.

## Restore Notes

- Extract worktrees/home notes:
  - `tar -xzf archive/worktrees.tgz`
  - `tar -xzf archive/home-notes.tgz`

- Inspect or apply stash patches:
  - `git apply --check archive/stash-5.patch`
  - `git apply archive/stash-5.patch`

## Caveats

- Stash patch files are point-in-time exports; they are not live stash refs.
- `worktrees.tgz` is the authoritative backup for nested worktree file content.
