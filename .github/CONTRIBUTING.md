# Contributing to Ratchet

Thanks for contributing to Ratchet.

Ratchet is a CDI-based job scheduler for Jakarta EE 10/11. The project is still in alpha, so small, focused improvements are preferred over broad speculative refactors.

## Before You Start

- Use [GitHub Discussions](https://github.com/ratchet-run/ratchet/discussions) for questions, usage help, and design discussion.
- Use [GitHub Issues](https://github.com/ratchet-run/ratchet/issues) for confirmed bugs, concrete feature requests, and actionable follow-up work.
- Open an issue or discussion before starting large changes so the direction is clear before code lands.

## Development Environment

Ratchet currently expects:

- Java 17+
- Maven 3.9+
- Node.js 20+ for the documentation site
- Docker if you want to run the managed integration-test profiles locally

## Repository Layout

- `ratchet-api`: public API and SPI contracts
- `ratchet`: reference implementation
- `ratchet-store-*`: store implementations
- `ratchet-testsuite`: managed integration tests
- `ratchet-tck`: reusable Store / API / Jakarta conformance contract tests
- `website/`: Docusaurus docs site

## Local Validation

Run the smallest useful validation set for your change before opening a pull request.

### Core Build

```bash
mvn clean test -B
mvn spotless:check -B
```

### Managed Integration Tests

PostgreSQL:

```bash
mvn verify -P wildfly-managed,postgresql -B -pl :ratchet-testsuite,:ratchet-coverage -am
```

MySQL:

```bash
mvn verify -P wildfly-managed,mysql -B -pl :ratchet-testsuite,:ratchet-coverage -am
```

### Documentation Site

```bash
cd website
npm ci
npm run build
```

If your change affects public behavior, examples, configuration, startup/logging expectations, or SPIs, update the docs in the same pull request.

Doc parity is part of the public surface. Behavior changes should keep the
root docs (`README.md`, `CONTRIBUTING.md`, `SECURITY.md`), module READMEs, and the Docusaurus site consistent.

## Coding Expectations

- Keep changes scoped to the problem you are solving.
- Prefer small, reviewable pull requests over large mixed-purpose bundles.
- Match the existing style and module boundaries.
- Add or update tests when behavior changes.
- Run `mvn spotless:apply -B` if formatting fails.
- Avoid new dependencies unless they materially improve the project.
- Do not silently change public behavior without updating docs and release notes.

## Pull Requests

When opening a pull request:

- Start from `main`.
- Explain the problem and the chosen fix clearly.
- List the validation you actually ran.
- Call out behavioral changes, compatibility risks, and follow-up work.
- Update documentation when needed.

The pull request template in this repository is the expected format.

## Sign-Off (DCO)

All commits must be signed off under the [Developer Certificate of Origin 1.1](https://developercertificate.org/). The sign-off attests that you have the right to contribute the change under the project's license and is checked automatically on every pull request.

Sign off a commit with `git commit -s`, which appends a trailer like:

```
Signed-off-by: Your Name <your.email@example.com>
```

If you forget, amend the latest commit with `git commit --amend -s`, or for older commits in the branch use an interactive rebase with `git rebase --signoff <base>`.

If Ratchet later joins an Eclipse Foundation Working Group, contributors may additionally need to sign the [Eclipse Contributor Agreement (ECA)](https://www.eclipse.org/legal/eca/). DCO sign-offs in the existing history remain valid; ECA acceptance applies forward only. Until then, DCO is the only attestation required.

## Reporting Security Issues

Do not open public issues for undisclosed security vulnerabilities.

Follow the process in [SECURITY.md](./SECURITY.md).
