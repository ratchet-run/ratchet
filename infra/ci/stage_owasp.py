#!/usr/bin/env python3
"""Stage resolved Maven artifacts by use, preserving coordinates and provenance."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROLES = ("runtime", "tooling", "tests")
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
EXTENSIONS = {"jar": "jar", "test-jar": "jar", "war": "war", "pom": "pom"}


class InventoryError(ValueError):
    pass


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InventoryError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def effective_models(root):
    """Use Maven's actual reactor, including active profiles and inherited config."""
    document = ET.parse(root / "target/owasp-effective-pom.xml").getroot()
    projects = [document] if document.tag == f"{{{NS['m']}}}project" else list(document)
    result = {}
    for project in projects:
        build = project.findtext("m:build/m:directory", namespaces=NS)
        if not build or "${" in build or Path(build).name != "target":
            raise InventoryError(f"Unreviewed effective build directory: {build}")
        directory = Path(build).parent.resolve()
        if not directory.is_relative_to(root) or directory in result:
            raise InventoryError(f"Invalid effective reactor path: {directory}")
        result[directory] = project
    if root not in result:
        raise InventoryError("Missing root effective project")
    return result


def artifact(node):
    parts = [node[name] for name in ("groupId", "artifactId", "version")]
    classifier = node.get("classifier", "")
    for part in parts + ([classifier] if classifier else []):
        if not isinstance(part, str) or not re.fullmatch(r"[A-Za-z0-9_+.-]+", part):
            raise InventoryError(f"Unresolved/invalid Maven coordinate: {parts}")
        if part in {".", ".."}:
            raise InventoryError(f"Invalid Maven path component: {part}")
    group, name, version = parts
    if any(not segment for segment in group.split(".")):
        raise InventoryError(f"Invalid Maven group: {group}")
    kind = node["type"]
    if kind not in EXTENSIONS:
        raise InventoryError(f"Unreviewed Maven artifact type: {kind}")
    directory = Path(*group.split(".")) / name / version
    stem = f"{name}-{version}"
    suffix = f"-{classifier}" if classifier else ""
    return (
        ":".join([*parts[:2], kind, classifier, version]),
        directory / f"{stem}{suffix}.{EXTENSIONS[kind]}",
        directory / f"{stem}.pom",
    )


def copied_artifacts(directory, pom):
    """Include default dependency:copy fixtures that are outside Maven's tree.

    Maven's effective model resolves inheritance, plugin management and active
    profiles. Fail rather than guess when a future execution uses other forms.
    """
    for plugin in pom.findall("m:build/m:plugins/m:plugin", NS):
        if plugin.findtext("m:artifactId", namespaces=NS) != "maven-dependency-plugin":
            continue
        for execution in plugin.findall("m:executions/m:execution", NS):
            goals = {g.text for g in execution.findall("m:goals/m:goal", NS)}
            items = execution.findall("m:configuration/m:artifactItems/m:artifactItem", NS)
            if goals != {"copy"} or not items:
                raise InventoryError("Unreviewed default dependency-plugin execution")
            for item in items:
                def value(name, default=None):
                    text = item.findtext(f"m:{name}", namespaces=NS)
                    if text is None:
                        text = execution.findtext(f"m:configuration/m:{name}", default, NS)
                    if text is not None and "${" in text:
                        raise InventoryError(f"Unresolved dependency:copy value: {text}")
                    return text

                node = {name: value(name) for name in ("groupId", "artifactId", "version")}
                node.update(type=value("type", "jar"), classifier=value("classifier", ""))
                _, relative, _ = artifact(node)
                output_directory = value("outputDirectory")
                if not output_directory:
                    raise InventoryError("Missing dependency:copy outputDirectory")
                copied = directory / output_directory / value("destFileName", relative.name)
                if not copied.resolve().is_relative_to(directory / "target"):
                    raise InventoryError(f"External dependency:copy output: {copied}")
                yield node, copied, execution.findtext("m:id", namespaces=NS)


def stage(root, repository, policy, output):
    root, repository = root.resolve(), repository.resolve()
    reactor = effective_models(root)
    expected = {str(path.relative_to(root)) for path in reactor}
    if set(policy) != expected:
        raise InventoryError(
            f"Module roles differ from reactor: missing={sorted(expected - set(policy))}, "
            f"extra={sorted(set(policy) - expected)}"
        )
    if set(policy.values()) - {*ROLES, "metadata"}:
        raise InventoryError("Unknown module role")
    if output.exists():
        raise InventoryError(f"Refusing stale output {output}; run Maven clean first")
    expected_trees = {directory / "target/owasp-tree.json" for directory in reactor}
    actual_trees = set(root.glob("**/target/owasp-tree.json"))
    if expected_trees != actual_trees:
        raise InventoryError("Dependency trees differ from the effective reactor")
    inventory = {role: {} for role in ROLES}
    sources = {}

    def add(node, role, module, scope):
        coordinate, relative, pom = artifact(node)
        source = repository / relative
        for path in (source, repository / pom):
            if not path.is_file() or not path.resolve().is_relative_to(repository):
                raise InventoryError(f"Missing or external resolved artifact: {path}")
        record = inventory[role].setdefault(
            coordinate,
            {
                "coordinate": coordinate,
                "file": str(relative),
                "sha256": digest(source),
                "pom": str(pom),
                "pomSha256": digest(repository / pom),
                "origins": [],
            },
        )
        origin = {"module": module, "scope": scope}
        if origin not in record["origins"]:
            record["origins"].append(origin)

    for directory, model in reactor.items():
        module = str(directory.relative_to(root))
        role = policy[module]
        identity = {name: model.findtext(f"m:{name}", namespaces=NS)
                    for name in ("groupId", "artifactId", "version")}
        identity.update(type=model.findtext("m:packaging", default="jar", namespaces=NS), classifier="")
        if (identity["type"] == "pom") != (role == "metadata"):
            raise InventoryError(f"POM/module role mismatch: {module}")
        tree_file = directory / "target/owasp-tree.json"
        tree = read_json(tree_file)
        if artifact(tree)[0] != artifact(identity)[0] or tree["scope"] != "":
            raise InventoryError(f"Dependency tree belongs to another module: {module}")
        sources[module] = {
            "role": role,
            "pomSha256": digest(directory / "pom.xml"),
            "treeSha256": digest(tree_file),
        }
        # Own published archives matter too: a future shaded dependency must not
        # disappear merely because it is absent from the transitive POM graph.
        if role != "metadata":
            final_name = model.findtext("m:build/m:finalName", namespaces=NS)
            if not final_name or "${" in final_name or Path(final_name).name != final_name:
                raise InventoryError(f"Unreviewed project archive name: {module}: {final_name}")
            built = directory / "target" / f"{final_name}.{EXTENSIONS[identity['type']]}"
            _, relative, _ = artifact(tree)
            if not built.resolve().is_relative_to(directory / "target"):
                raise InventoryError(f"External project archive: {built}")
            if digest(built) != digest(repository / relative):
                raise InventoryError(f"Project archive differs from repository: {module}")
            add(tree, role, module, "project")

        def dependencies(node):
            for child in node.get("children", []):
                scope = child["scope"]
                if scope not in {"compile", "runtime", "provided", "test"}:
                    raise InventoryError(f"Unreviewed Maven scope: {module}: {scope}")
                selected = "tests" if scope == "test" or role == "metadata" else role
                add(child, selected, module, scope)
                dependencies(child)

        dependencies(tree)
        for node, copied, execution in copied_artifacts(directory, model):
            if role != "tests":
                raise InventoryError(f"Review copied-artifact role: {module}")
            _, relative, _ = artifact(node)
            if digest(copied) != digest(repository / relative):
                raise InventoryError(f"Copied artifact differs from repository: {copied}")
            add(node, "tests", module, f"plugin-copy:{execution}")
    # Validate every input before creating any output. Only successful complete
    # staging writes the manifest used by subsequent scan/report checks.
    for role, records in inventory.items():
        if not any(not item["file"].endswith(".pom") for item in records.values()):
            raise InventoryError(f"Empty scan role: {role}")
    for role, records in inventory.items():
        for record in records.values():
            for key, hash_key in (("file", "sha256"), ("pom", "pomSha256")):
                source = repository / record[key]
                destination = output / "inputs" / role / record[key]
                destination.parent.mkdir(parents=True, exist_ok=True)
                if not destination.exists():
                    shutil.copyfile(source, destination)
                if digest(destination) != record[hash_key]:
                    raise InventoryError(f"Artifact changed during staging: {source}")
    manifest = {
        "schemaVersion": 1,
        "effectivePomSha256": digest(root / "target/owasp-effective-pom.xml"),
        "modules": sources,
        "roles": {
            role: [records[key] for key in sorted(records)]
            for role, records in inventory.items()
        },
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--fetch-copied-poms", action="store_true",
                        help="Resolve missing POMs for dependency:copy fixtures using Maven")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    try:
        if args.fetch_copied_poms:
            root, repository = args.root.resolve(), args.repository.resolve()
            for directory, model in effective_models(root).items():
                for node, _, _ in copied_artifacts(directory, model):
                    _, _, pom = artifact(node)
                    if not (repository / pom).is_file():
                        coordinate = ":".join(node[key] for key in ("groupId", "artifactId", "version"))
                        subprocess.run([
                            "mvn", "-B", "-ntp", "-N", f"-Dmaven.repo.local={repository}",
                            "dependency:get", f"-Dartifact={coordinate}:pom", "-Dtransitive=false",
                        ], cwd=root, check=True)
        result = stage(
            args.root,
            args.repository,
            read_json(args.root / "infra/ci/owasp-module-roles.json"),
            args.root / "target/owasp",
        )
        print(json.dumps({role: len(rows) for role, rows in result["roles"].items()}))
    except (InventoryError, OSError, ValueError, KeyError, TypeError,
            ET.ParseError, subprocess.CalledProcessError) as error:
        print(f"OWASP staging failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
