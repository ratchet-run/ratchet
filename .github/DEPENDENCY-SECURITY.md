# Dependency security checks

OWASP Dependency-Check's runtime and tooling reports fail CI at CVSS 7. The tests
report is advisory because test dependencies never ship to users. It is still
generated, uploaded as SARIF, and checked for coverage and freshness, but its
findings do not fail the job. Splitting reports does not change dependency scopes
in published POMs.

## Scan inputs

The workflow builds the default Maven reactor, records each module's resolved
dependency tree, and stages the actual archives with their sibling Maven POMs.
`infra/ci/owasp-module-roles.json` assigns every reactor module a role. Adding a
module without classifying it fails staging.

| Report | Inputs |
| --- | --- |
| `runtime` | Published runtime modules and their compile, runtime, and provided dependencies, including optional dependencies |
| `tooling` | Published Quarkus deployment modules and TCK modules, with their non-test dependencies |
| `tests` | Nonpublished test/showcase/load applications, test-scoped dependencies from other modules and everything beneath them, and default dependency-plugin copied fixtures, including the mixed-version compatibility JARs |

Runtime and tooling enforce the CVSS 7 threshold; tests findings are advisory.
A library used in more than one role appears in each applicable report.
`provided` means that the host supplies
the dependency; it is retained with its scope recorded in the input manifest.
Spring starter and Quarkus extension dependencies are also assessed as resolved
in the default reactor. Consumer-resolved graphs are qualified separately;
Maven's transitive dependency mediation and an application's own dependency
management can select different versions.

The manifest records module origins, Maven scopes, coordinates, POM hashes,
dependency-tree and effective-POM hashes, and archive hashes. Maven's effective
models include inherited configuration and active profiles. CI rejects missing inputs, stale
staging directories, installed project archives that differ from the build,
empty reports, and staged archives missing their hashes or Maven identities in the report.
It also requires a recorded NVD database check within the past seven days.
CI pins the scanner image by digest and refreshes its bundled vulnerability data
once before all three scans. A refresh failure fails the job; subsequent reports
are diagnostic and remain subject to the freshness check. Reports are uploaded
even when a vulnerability fails the gate, with a separate SARIF category for each role.

This is a **default-reactor** scan. Independent Spring consumer reactors and the
Quarkus example, optional database/server profiles and their downloaded server
distributions, Maven build plugins and Quarkus bootstrap-resolved deployment classpaths,
container images, and website npm
dependencies require separate qualification. The workflow does not claim to
assess those environments. It does scan the published Quarkus deployment
artifacts, which are ordinary project dependencies rather than Maven plugins.

To reproduce staging from a clean build with the default Maven repository:

```sh
mvn -B -ntp -DskipTests clean install
mvn -B -ntp -DskipTests dependency:tree -DoutputType=json -DoutputFile=target/owasp-tree.json
mvn -B -ntp -DskipTests help:effective-pom -Doutput=target/owasp-effective-pom.xml
python3 infra/ci/stage_owasp.py --repository "$HOME/.m2/repository" --fetch-copied-poms
```

Use the data-refresh step and pinned scanner arguments in `workflows/owasp-depcheck.yml` against
each `target/owasp/inputs/<role>` directory. After all three scans, run:

```sh
python3 infra/ci/check_owasp_reports.py target/owasp
```

## Finding dispositions

Reviewed on 2026-09-26. These dispositions describe the selected dependencies;
they are not a claim that every affected API is reachable through Ratchet.

| Finding | Disposition |
| --- | --- |
| CVE-2026-47886, CVE-2026-59282, CVE-2026-59283 | The default Spring starter platform is Boot 4.1.1, which selects the vendor-fixed Framework 7.0.9. Boot 3.5.16 compatibility tests still select affected Framework 6.2.19; the 6.2.20 fix is enterprise-only. Compatibility results do not certify that older platform's security. |
| CVE-2026-47834 | Boot 4.1.1 selects the vendor-fixed Spring Data JPA 4.1.1. Boot 3.5.16 selects affected 3.5.13; its 3.5.14 fix is enterprise-only. Applications remaining on Boot 3 need the corresponding maintained platform fixes. |
| CVE-2026-64607, CVE-2026-54399, CVE-2026-54428, CVE-2026-71290 | Apache HTTP code is embedded inside `docker-java-transport-zerodep`. The advisory tests report keeps these findings visible. In tooling, CVE-2026-54399 and CVE-2026-54428 on the Quarkus Dev Services copy (`docker-java-transport-zerodep` 3.4.1, `httpcore5` 5.0.2) are covered by the accepted-risk exception below. Dependency-management overrides for ordinary Apache artifacts cannot replace relocated classes. A replacement transport release must contain the patched bytes and be qualified. |
| CVE-2026-88032, CVE-2026-88033 | The default reactor selects the vendor-fixed MongoDB driver family 5.11.1, including Quarkus's reactive-streams and crypt dependencies. Application BOMs can select older drivers; use the configuration below. No reachability suppression is used for encryption or GridFS code. |
| Artemis CVE-2026-49362, 49363, 49364, 57822, 57967, 67593, 75880 | The JMS test broker is updated to the vendor-fixed Artemis 2.57.0. |
| CVE-2025-67030 | Quarkus test dependencies select Plexus Utils 3.6.1, which fixes the archive extraction path traversal. |
| CVE-2026-54285 | Exact Java SDK 1.62.0 package controls correct a JavaScript product match. The affected npm package and separate Java CVEs remain visible. |
| CVE-2026-15432 | The encryption test dependency uses Tink 1.22.0. Its released chunked MAC implementation uses constant-time tag comparison. |

Spring's vendor advisories: [SpEL exponentiation](https://spring.io/security/cve-2026-47886/),
[data binding](https://spring.io/security/cve-2026-59282/),
[compiled SpEL](https://spring.io/security/cve-2026-59283/), and
[Spring Data JPA](https://spring.io/security/cve-2026-47834/).
Other vendor sources: [Apache HttpComponents security](https://hc.apache.org/security.html)
and [MongoDB security alerts](https://www.mongodb.com/resources/products/alerts).
Test dependency sources: [Artemis security](https://artemis.apache.org/components/artemis/security),
[Plexus Utils fix](https://github.com/codehaus-plexus/plexus-utils/commit/6d780b3378829318ba5c2d29547e0012d5b29642),
and [Tink timing issue](https://github.com/tink-crypto/tink-java/issues/75).

The current MongoDB qualification configuration selects driver 5.11.1. Boot and
Quarkus BOMs select older drivers unless applications explicitly override them.
Keep `bson`, `bson-record-codec`, `mongodb-driver-core`, `mongodb-driver-sync`,
`mongodb-driver-reactivestreams`, and `mongodb-crypt` aligned to 5.11.1 when present.
The [Spring guide](../website/docs/deployment/spring-boot.md#keep-the-mongodb-driver-modules-aligned)
and [Quarkus guide](../website/docs/deployment/quarkus.md#align-the-mongodb-driver-family)
show the required MongoDB BOM configuration for this baseline. These compatibility
tests do not qualify the older platform-selected drivers. Check the application's
resolved dependency tree after applying its BOMs.

One accepted-risk exception is documented below. Other suppressions must
identify the exact package/advisory mismatch and preserve an affected
component as a negative control. Being a test dependency, having an unused API,
or being part of an alpha/beta release is not evidence of a false positive.
An accepted-risk exception must name its exact package version, advisories,
owner-controlled reason, and removal condition.

## Accepted-risk exceptions

Ratchet accepts CVE-2026-54399 and CVE-2026-54428 on `httpcore5` and `httpcore5-h2`
5.0.2 relocated inside `docker-java-transport-zerodep` 3.4.1. Quarkus 3.20 LTS
Dev Services brings this transport through `quarkus-devservices-common` and
Testcontainers 1.20.6. The Quarkus BOM controls the version; Ratchet cannot override
it without leaving the LTS. This is an accepted risk, not a false positive.
Both CVEs allow memory-exhaustion DoS from a hostile HTTP peer. In this use, the
only peer is the local Docker daemon socket at development/test time.

Remove the exception when the Quarkus LTS line selects a docker-java transport
containing fixed `httpcore5` 5.4.3 or newer. The suppression matches only the two
5.0.2 packages and these two advisories. See the
[Apache advisory](https://lists.apache.org/thread/zmxh1pl2zohov5ntdh4lt85gfrlchgpy).

Raw scan reports and investigation logs belong in CI artifacts or local evidence
directories, not in the source tree.
