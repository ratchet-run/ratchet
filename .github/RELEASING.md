# Preparing and publishing a release

Releases run through the manually dispatched `Release` workflow on `main`. The workflow
uses the reactor's `-SNAPSHOT` version: `0.4.0-SNAPSHOT` produces `v0.4.0` and a follow-up
PR for `0.4.1-SNAPSHOT`. It never pushes a release commit directly to `main`.

## Before dispatch

1. Merge the intended changes and preparation PRs through the normal queue. Confirm the
   release version in every reactor POM and run `scripts/sync-version.sh` for the development
   version. Public dependency examples stay on the latest published version until release.
2. Review `.github/release-notes/<version>.md`. Include upgrade and rollback instructions,
   public API/SPI changes, and storage migrations. The workflow requires this file and uses
   it verbatim; it does not generate upgrade instructions from commit subjects.
3. Confirm CI, publishing-profile packaging, and applicable migration checks pass. For a
   storage upgrade, rehearse against a database from the previous release containing jobs,
   recurring definitions, workflows, and retained history. Fresh-schema tests alone do not
   establish upgrade safety. Record verification for each supported store.
4. Verify release credentials and signing are available to the workflow, and that the tag
   does not already exist. Dispatch only when ready to stage artifacts and create the tag.

## Workflow output

The workflow runs the full CI gate, sets release versions, builds and signs artifacts,
and stages them in Central. It checks that the expected JARs and SBOMs exist, creates a
verified release commit and tag, and opens a draft GitHub release with reviewed notes.
It also opens the next-development PR without auto-merge, because that PR updates public
examples to the release being staged.

## Complete publication

1. Inspect the validated staging bundle in the Central Portal and publish it manually.
2. Confirm the released BOM and every intended library artifact can be downloaded from
   Maven Central. Smoke-test a consumer using the published coordinates, without relying
   on locally installed reactor artifacts.
3. Publish the draft GitHub release and mark it as the latest release. Check that the
   reviewed upgrade instructions and SBOM assets are present before announcing it.
4. Merge the next-development PR, then verify public documentation references the published
   version and development references use the next snapshot.

A successful staging upload is not proof that artifacts are publicly available. If a run
stops after staging or tagging, inspect Central and GitHub before rerunning; the workflow
refuses to overwrite an existing release tag.
