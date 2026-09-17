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

Every external `groupId:artifactId:version` must have an entry in
`dependency-license-policy.json`. Each entry records the exact declared license
names and URLs, the reviewed SPDX expression, and evidence URLs. Effective POM
licenses may be inherited from a parent POM. The original names and URLs are
preserved so changes require review. Evidence links are for reviewers; CI does
not fetch them.

For an added or upgraded dependency:

1. Run the gate and inspect
   `target/generated-sources/license/dependency-licenses.json` and the Maven log.
2. Read the version's POM (including inherited license declarations). For
   ambiguous or multiple licenses, inspect the artifact's LICENSE and NOTICE
   files or versioned upstream source. Record the evidence URL and, when relevant,
   the path inside the artifact in a `note`.
3. Add the exact version and metadata to the policy, in coordinate order. State
   the SPDX expression explicitly: `OR` permits a choice; `AND` requires both;
   `WITH` applies a specific exception. A list of POM licenses does not by itself
   establish an `OR` relationship. Remove obsolete versions once no checked
   configuration uses them.
4. Run the gate and its tests. Include the policy change with the dependency PR.

The allowlist contains the permissive and weak-copyleft license terms used by
the current graph. Unknown terms and GPL/LGPL/AGPL/SSPL fail unless the exact
dependency entry contains a documented `exception`. Existing Hibernate and
Connector/J exceptions are retained at their current versions. AOP Alliance's
public-domain declaration has a separate documented exception. An absent POM
license URL can be reviewed using other evidence; an absent license name cannot.
Do not add blanket group-ID exceptions or infer an exemption from the word
"Apache" appearing alongside another license.

Policy edits are review decisions, not automatically generated approvals. The
initial inventory records effective POM declarations for ordinary single-license
artifacts. JNA, HdrHistogram, Jakarta API, Parsson, and Yasson choices were checked
against the packaged license/notice files. Vert.x entries conservatively require
both declared licenses, since both already satisfy policy.

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
