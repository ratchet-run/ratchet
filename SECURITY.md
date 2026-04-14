# Security Policy

## Supported Versions

Ratchet is currently in alpha. Security fixes are applied on a best-effort basis to:

| Version | Supported |
| --- | --- |
| `main` | Yes |
| Latest `0.1.x` alpha line | Yes |
| Older snapshots and superseded alphas | No |

If a report affects an older snapshot, the fix will typically land on `main` and the latest active alpha line only.

## Reporting a Vulnerability

Please do not open a public GitHub issue for an undisclosed security vulnerability.

Preferred reporting paths:

1. Use GitHub's private vulnerability reporting for this repository, if it is available.
2. Otherwise, email the maintainer at `jputney@noverant.com`.

Please include:

- Affected Ratchet version or commit SHA
- Runtime and environment details
- Store backend in use
- Clear reproduction steps or proof of concept
- Expected impact
- Any relevant logs, traces, or configuration details

You should receive an acknowledgement within 3 business days. Status updates will be provided as the investigation progresses.

## Disclosure Policy

- We will confirm receipt of the report.
- We will investigate and validate the issue.
- We will work on a fix or mitigation for supported versions.
- We will coordinate disclosure timing with the reporter when practical.

Please avoid public disclosure until a fix or mitigation is available and maintainers have had a reasonable opportunity to respond.

## Non-Security Bugs

For normal defects, regressions, and feature requests, use the public issue tracker:

- Issues: <https://github.com/jcputney/ratchet/issues>
- Discussions: <https://github.com/jcputney/ratchet/discussions>
