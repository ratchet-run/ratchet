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

1. Use GitHub's [private vulnerability reporting](https://github.com/jcputney/ratchet/security/advisories/new) for this repository. Private reporting is enabled.
2. Otherwise, email the maintainer at `jputney@noverant.com`.

Please include:

- Affected Ratchet version or commit SHA
- Runtime and environment details
- Store backend in use
- Clear reproduction steps or proof of concept
- Expected impact
- Any relevant logs, traces, or configuration details

## Current Security Surface

Ratchet currently enforces these runtime boundaries:

- Job payload deserialization is guarded by `ClassPolicy` and an
  `ObjectInputFilter`; the default package allowlist is empty and the RI
  fails fast until the application provides a real policy.
- Payload validation defers class initialization while resolving job target
  classes, so policy checks happen before user-controlled static initializers
  can run.
- Archive writes route through `PayloadMasker` to reduce accidental exposure
  of secrets in operational audit data.
- Job creation captures the Jakarta Security caller principal when one is
  available and stores it with the job for audit. Authorization enforcement is
  not part of the current surface; a future `JobAuthorizationPolicy` SPI is
  tracked separately.
- MongoDB stores validate `UuidRepresentation.STANDARD` at startup to avoid
  silent UUIDv7 byte-order corruption.

## Response Timeline

| Step | Target |
| --- | --- |
| Acknowledgement of receipt | 3 business days |
| Initial assessment + severity triage | 14 calendar days |
| Patch or mitigation — Critical (CVSS 9.0+) | 90 calendar days |
| Patch or mitigation — High (CVSS 7.0–8.9) | 180 calendar days |
| Public disclosure | Coordinated with reporter; default is 90 calendar days after acknowledgement, or upon patch availability, whichever is sooner |

Status updates are provided at each transition. If a target slips, the reason and a revised target are communicated to the reporter.

## Disclosure Policy

- We confirm receipt of the report.
- We investigate and validate the issue.
- We work on a fix or mitigation for supported versions.
- We coordinate disclosure timing with the reporter, defaulting to the timeline above.

Please avoid public disclosure until a fix or mitigation is available and maintainers have had a reasonable opportunity to respond.

## Non-Security Bugs

For normal defects, regressions, and feature requests, use the public issue tracker:

- Issues: <https://github.com/jcputney/ratchet/issues>
- Discussions: <https://github.com/jcputney/ratchet/discussions>
