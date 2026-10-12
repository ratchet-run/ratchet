# Dependency license checks

## Required local policy

`check-dependency-licenses.sh` resolves compile/runtime dependencies across the
default Maven reactor and checks their effective POM license metadata against
`dependency-license-policy.json`. The inventory includes unpublished test and
load-test modules as well as published modules. Test, provided, and system scopes
are excluded, as in the existing gate. Optional compile/runtime dependencies are
included. This is a dependency-metadata check; it does not scan source files,
bundled third-party code, or every optional Maven profile.

The gate runs on code PRs, merge-queue groups, main builds, and release
verification. It needs Java 21, Maven, and Python 3.10 or newer:

```sh
mvn -B -ntp -DskipTests -Dspotbugs.skip=true -Dmaven.javadoc.skip=true install
bash infra/ci/check-dependency-licenses.sh
```

After Maven artifacts and plugins have been downloaded, verify without network
resolution:

```sh
bash infra/ci/check-dependency-licenses.sh --offline
```

There are no Eclipse or ClearlyDefined requests in either invocation. Maven
resolution still requires repository availability on a cold cache. The gate
deletes the previous aggregate inventory, forces regeneration, and rejects
unresolved reactor modules, missing or unknown license names, empty inventories,
and malformed reports. Logs and inventory are uploaded even when CI fails.

### Reviewing dependency changes

Dependencies pass when every declared license name maps through `licenseAliases`
to an allowed SPDX term. Version bumps and new artifacts using these names need
no policy entry. Effective POM licenses may be inherited from a parent POM.

When the gate fails:

1. Inspect `target/generated-sources/license/dependency-licenses.json` and the
   Maven log. Read the POM and relevant upstream LICENSE and NOTICE files.
2. Add a new name to `licenseAliases` only when its meaning is unambiguous.
   Keep generic names such as `Public Domain` unaliased.
3. For copyleft, dual-license choices, or exceptions, add an `artifacts` entry
   keyed by `groupId:artifactId`. Record declared names and URLs, an SPDX
   expression, HTTPS evidence, and any exception justification or useful note.
   `OR` permits a choice; `AND` requires both; `WITH` applies an exception.
   Multiple POM declarations do not establish an `OR` relationship by themselves.
4. Keep alias and artifact keys sorted. Run the gate and its tests, and include
   policy changes with the dependency PR.

Artifact reviews apply across versions with matching canonical license metadata:
aliased names compare by SPDX term, while unaliased names and URLs compare
exactly. Changed metadata requires a fresh evidence review. A later version
whose declared licenses are all allowed passes without the artifact review.
Evidence links are for reviewers; CI does not fetch them. Disallowed expressions
require a documented `exception`. A missing license URL can be reviewed using
other evidence; a missing license name cannot. Policy edits are human review
decisions. Do not add blanket group-ID exceptions.

Ratchet's own modules are identified by exact coordinates from this checkout's
POM tree and must retain the recorded Apache declaration. Their versions follow
the source checkout, so a release version bump does not require third-party
policy changes. Other artifacts under `run.ratchet` receive no group-wide bypass.

Run the policy, wrapper, and audit-reporting controls without Maven or services:

```sh
python3 -m unittest discover -s infra/ci -p 'test_*.py' -v
```

## Scheduled Eclipse Dash audit

`license-audit.yml` runs weekly and through **Run workflow**. It installs the
reactor, then queries Dash for external compile/runtime dependencies. Ratchet's
own group is excluded from this supplementary service lookup. The required local
gate independently checks exact reactor identities and license metadata.

The lookup has a ten-minute limit and makes one attempt. The workflow retains
the Maven log, CSV report if available, and status for 30 days. Outcomes are:

| Outcome | Workflow result | Action |
| --- | --- | --- |
| Complete, all approved | Success | Retain audit evidence. |
| Complete, review needed | Failure: review required | Inspect the flagged dependencies and update policy or dependencies as needed. |
| Timeout, setup failure, or incomplete report | Failure: audit unavailable | Inspect the service/build failure and rerun when available. No clean result was established. |

Failures are visible in Actions and the job summary and use GitHub's configured
workflow-failure notifications. Maintainers should subscribe to failed workflow
runs. This workflow is separate from `CI required`, so an external service outage
does not block unrelated PRs. A release review should include the latest completed
audit and any unresolved findings; this PR does not add a release-time service
dependency or authorize publication.

## Pull request lane selection

`affected_lanes.py` reads the reactor POMs and selects lanes whose module
closures contain changed modules. Pull requests use a reduced matrix that
covers every selected dimension value. Every other event selects the full
matrix and the full unit suite; the merge queue still skips heavy lanes.

Run locally from the repository root with a newline-separated list of changed
repository-relative paths, or select the full matrix:

```sh
python3 infra/ci/affected_lanes.py --changed-files changed-files.txt
python3 infra/ci/affected_lanes.py --full
```
