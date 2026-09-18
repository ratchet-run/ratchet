"""Publish an existing release bundle using credentials that stay inside Actions."""

import base64
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)


def validate_run(run, repository):
    if (
        run["conclusion"] != "success"
        or run["status"] != "completed"
        or run["event"] != "workflow_dispatch"
        or run["head_branch"] != "main"
        or run["path"] != ".github/workflows/release.yml"
        or run["repository"]["full_name"] != repository
    ):
        raise ValueError("Source must be a successful Release workflow run on main")


def expected_artifacts(bom):
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    root = ET.fromstring(bom)
    artifacts = {
        d.findtext("m:artifactId", namespaces=ns)
        for d in root.findall("m:dependencyManagement/m:dependencies/m:dependency", ns)
        if d.findtext("m:groupId", namespaces=ns) == "run.ratchet"
    }
    if not artifacts:
        raise ValueError("Release BOM contains no Ratchet artifacts")
    return artifacts | {"ratchet-parent", "ratchet-bom", "ratchet-quarkus-parent", "ratchet-tck"}


def validate_bundle(status, deployment, version, artifacts):
    if status["deploymentId"] != deployment:
        raise ValueError("Central returned a different deployment")
    actual = set()
    for purl in status.get("purls", []):
        coordinate = urllib.parse.unquote(purl.split("?", 1)[0].split("#", 1)[0])
        match = re.fullmatch(r"pkg:maven/run\.ratchet/([^/@]+)@([^/@]+)", coordinate)
        if not match or match[2] != version:
            raise ValueError("Bundle contains coordinates outside this Ratchet release")
        actual.add(match[1])
    if actual != artifacts:
        raise ValueError(f"Bundle artifacts differ from BOM: missing={artifacts - actual}, extra={actual - artifacts}")
    if status["deploymentState"] not in {"VALIDATED", "PUBLISHING", "PUBLISHED"}:
        raise ValueError(f"Deployment is not ready: {status['deploymentState']}")


def main():
    repository = os.environ["GITHUB_REPOSITORY"]
    version = os.environ["RELEASE_VERSION"]
    run_id = os.environ["RELEASE_RUN_ID"]
    deployment = os.environ["CENTRAL_DEPLOYMENT_ID"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or not run_id.isdigit():
        raise ValueError("Expected a release version and numeric workflow run ID")
    if str(uuid.UUID(deployment)) != deployment:
        raise ValueError("Expected a canonical Central deployment UUID")
    prefix = f"repos/{repository}"
    run = json.loads(gh("api", f"{prefix}/actions/runs/{run_id}"))
    validate_run(run, repository)
    tag = f"v{version}"
    commit = json.loads(gh("api", f"{prefix}/commits/{tag}"))
    if [p["sha"] for p in commit["parents"]] != [run["head_sha"]]:
        raise ValueError("Release tag is not based on the verified workflow source")
    logs = gh("run", "view", run_id, "--repo", repository, "--log")
    if deployment not in logs:
        raise ValueError("Deployment UUID is absent from the source release run logs")
    release = json.loads(gh("api", f"{prefix}/releases/tags/{tag}"))
    notes = gh("api", f"{prefix}/contents/.github/release-notes/{version}.md?ref={tag}",
               "-H", "Accept: application/vnd.github.raw+json")
    if release["body"].strip() != notes.strip():
        raise ValueError("GitHub release notes differ from reviewed tag contents")
    bom = gh("api", f"{prefix}/contents/ratchet-bom/pom.xml?ref={tag}",
             "-H", "Accept: application/vnd.github.raw+json")
    artifacts = expected_artifacts(bom)
    jar_artifacts = artifacts - {"ratchet-parent", "ratchet-bom", "ratchet-quarkus-parent", "ratchet-tck"}
    expected_sboms = {f"{a}-{version}-cyclonedx.{fmt}" for a in jar_artifacts for fmt in ("json", "xml")}
    if {a["name"] for a in release["assets"]} != expected_sboms:
        raise ValueError("GitHub release SBOM assets do not match the release BOM")
    token = base64.b64encode(
        f"{os.environ['OSSRH_USERNAME']}:{os.environ['OSSRH_TOKEN']}".encode()
    ).decode()

    def central(path):
        request = urllib.request.Request(
            f"https://central.sonatype.com/api/v1/publisher/{path}",
            headers={"Authorization": f"Bearer {token}"}, method="POST",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            data = response.read()
            return json.loads(data) if data else None

    status_path = f"status?id={deployment}"
    status = central(status_path)
    validate_bundle(status, deployment, version, artifacts)
    print(f"Verified {len(artifacts)} artifacts for {tag} from Release run {run_id}", flush=True)
    if status["deploymentState"] == "VALIDATED":
        central(f"deployment/{deployment}")
    for _ in range(60):
        status = central(status_path)
        state = status["deploymentState"]
        print(f"Central deployment {deployment}: {state}", flush=True)
        if state == "PUBLISHED":
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
                summary.write(f"Published {tag} to Central from Release run {run_id}.\n")
            return
        if state == "FAILED":
            raise RuntimeError("Central publication failed; inspect the deployment in Central")
        time.sleep(10)
    raise TimeoutError("Publication is still pending; inspect this deployment before retrying")


if __name__ == "__main__":
    main()
