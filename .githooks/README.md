# Git hooks

Project-tracked git hooks. Enable once per clone:

```bash
git config core.hooksPath .githooks
```

## Hooks

- **`pre-commit`** — Runs `mvn spotless:apply` scoped to staged Java files (via
  `-DspotlessFiles`) and re-stages anything it reformatted. No-op when no
  `.java` files are staged.

## Bypass

Skip hooks for a single commit when needed:

```bash
git commit --no-verify
```
