# Contributing to Ratchet

Thanks for contributing to Ratchet.

Ratchet is a CDI-based job scheduler for Jakarta EE. The project is still in alpha, so small, focused improvements are preferred over broad speculative refactors.

## Before You Start

- Use [GitHub Discussions](https://github.com/jcputney/ratchet/discussions) for questions, usage help, and design discussion.
- Use [GitHub Issues](https://github.com/jcputney/ratchet/issues) for confirmed bugs, concrete feature requests, and actionable follow-up work.
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
- `ratchet-tck`: reusable contract tests
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
mvn verify -P wildfly-managed,postgresql -B -pl ratchet-testsuite,ratchet-coverage -am
```

MySQL:

```bash
mvn verify -P wildfly-managed,mysql -B -pl ratchet-testsuite,ratchet-coverage -am
```

### Documentation Site

```bash
cd website
npm ci
npm run build
```

If your change affects public behavior, examples, configuration, startup/logging expectations, or SPIs, update the docs in the same pull request.

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

## Reporting Security Issues

Do not open public issues for undisclosed security vulnerabilities.

Follow the process in [SECURITY.md](./SECURITY.md).
