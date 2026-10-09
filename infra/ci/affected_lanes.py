"""Pick the CI lanes and matrix rows that a pull request can affect.

A row runs when its Maven module closure (parent, dependency, plugin and import
edges under the row's -P profiles) contains a changed module. A store module
change only counts for rows that run that store: another store can only break
a row at compile time, and the always-on build job compiles the whole reactor.
Pull requests then get a reduced matrix in which every selected dimension value
appears at least once. Every other event gets the full matrix and unit suite.
"""

import argparse
import json
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

JAVA = [17, 21]
BOOT = ["3.5.16", "4.1.1"]
SERVERS = ["wildfly-managed", "wildfly-ee11-managed", "payara-managed", "payara-ee11-managed",
           "openliberty-managed", "openliberty-ee11-managed", "glassfish-managed"]
DATABASES = ["mysql", "postgresql", "mongodb", "oracle", "sqlserver"]
STORE_MODULES = {db: f"stores/ratchet-store-{db}" for db in DATABASES}
STARTER = "integrations/ratchet-spring-boot/ratchet-spring-boot-starter"
MONGO_STARTER = "integrations/ratchet-spring-boot/ratchet-spring-boot-starter-mongodb"
TESTSUITE = "testing/ratchet-testsuite"
COVERAGE = "testing/ratchet-coverage"
QUARKUS = "integrations/ratchet-quarkus/integration-tests"
SHOWCASE = "testing/ratchet-showcase"
LANES = {
    "integration": [{"server": server, "database": db} for server in SERVERS for db in DATABASES],
    "quarkus_jvm": [{"db": db, "profile": profile} for db, profile in
                    [("postgres", "db-postgres"), ("mysql", "db-mysql"), ("oracle", "db-oracle"),
                     ("sqlserver", "db-sqlserver"), ("mongodb", "db-mongo")]],
    "quarkus_native": [{"db": "mongodb", "cache": "mongo", "profile": "db-mongo,native-mongo"},
                       {"db": "sql", "cache": "sql", "profile": "db-postgres,native-sql"}],
    "spring_consumer": [{"java": java, "boot": boot, "store": store, "provider": "default"}
                        for java in JAVA for boot in BOOT for store in DATABASES] +
                       [{"java": java, "boot": boot, "store": "postgresql", "provider": "eclipselink"}
                        for java in JAVA for boot in BOOT],
    "spring_native": [{"boot": boot, "store": store} for boot in BOOT for store in DATABASES],
    "spring_runtime": [{"java": java, "boot": boot} for java in JAVA for boot in BOOT],
}


def read_pom(path):
    root = ET.parse(path).getroot()
    for element in root.iter():
        element.tag = element.tag.rsplit("}", 1)[-1]
    return root


class Graph:
    def __init__(self, repo_root):
        self.poms = {}
        self.artifacts = {}
        repo_root = Path(repo_root)

        def discover(directory):
            pom = read_pom(repo_root / directory / "pom.xml")
            if directory != ".":
                if directory in self.poms:
                    return
                artifact = pom.findtext("artifactId", "").strip()
                if "${" in artifact:
                    raise ValueError(f"Unresolved reactor artifactId in {directory}/pom.xml: {artifact}")
                self.poms[directory] = pom
                self.artifacts[artifact] = directory
            for module in pom.findall("modules/module") + pom.findall("profiles/profile/modules/module"):
                child = (repo_root / directory / module.text.strip()).resolve().relative_to(repo_root.resolve())
                discover(child.as_posix())
        discover(".")

    def edges(self, module, profiles):
        pom = self.poms[module]
        blocks = [pom]
        declared = pom.findall("profiles/profile")
        explicit_here = any(p.findtext("id") in profiles for p in declared)
        for profile in declared:
            activation = profile.find("activation")
            default = profile.findtext("activation/activeByDefault") == "true"
            other = activation is not None and any(e.tag != "activeByDefault" for e in activation)
            if profile.findtext("id") in profiles or other or (default and not explicit_here):
                blocks.append(profile)
        artifacts = set()
        paths = ["parent/artifactId", "dependencies/dependency/artifactId",
                 "build/plugins/plugin/artifactId", "build/plugins/plugin/dependencies/dependency/artifactId",
                 "build/extensions/extension/artifactId"]
        for block in blocks:
            for path in paths:
                artifacts.update(e.text.strip() for e in block.findall(path) if e.text)
            for dep in block.findall("dependencyManagement/dependencies/dependency"):
                if dep.findtext("scope") == "import":
                    artifacts.add(dep.findtext("artifactId", "").strip())
        return {self.artifacts[a] for a in artifacts if a in self.artifacts}

    def closure(self, roots, profiles=frozenset()):
        found = set()
        pending = list(roots)
        while pending:
            module = pending.pop()
            if module not in found:
                found.add(module)
                pending.extend(self.edges(module, profiles) - found)
        return found

    def downstream(self, changed, profiles=frozenset()):
        return {m for m in self.poms if self.closure([m], profiles) & changed}


def classify(graph, paths):
    changed, triggers, reasons = set(), set(), []
    code = web = False
    for path in paths:
        web |= path.startswith("website/")
        if path.endswith((".md", ".adoc")) or path.startswith("website/") or path == "LICENSE":
            continue
        code = True
        if path in {"pom.xml", ".github/workflows/ci.yml", "infra/ci/affected_lanes.py"} or path.startswith((".mvn/", ".github/actions/")):
            reasons.append(path)
        elif path.startswith("integrations/ratchet-spring-boot/consumer-tests/"):
            triggers.update({"spring_consumer", "spring_native", "spring_runtime"})
        elif path in {"infra/ci/check_runtime_consumer_reports.py", "infra/ci/runtime-consumer-expected.json"}:
            triggers.add("spring_runtime")
        else:
            matches = [m for m in graph.poms if path == m or path.startswith(m + "/")]
            if matches:
                changed.add(max(matches, key=len))
            elif path.startswith((".github/", "scripts/", "infra/", "examples/", "src/main/javadoc/", ".githooks/")) or path in {
                "qodana.yaml", "qodana.sarif.json", "owasp-suppressions.xml", "spotbugs-include.xml",
                ".gitignore", ".dockerignore", "NOTICE"}:
                continue
            else:
                reasons.append(path)
    if not paths:
        reasons.append("empty changed-files list")
    return changed, triggers, reasons, code, web


def row_model(lane, row):
    if lane == "integration":
        return [TESTSUITE], {row["server"], row["database"]}, {row["database"]}
    if lane.startswith("quarkus_"):
        profiles = set(row["profile"].split(","))
        store = {"db-postgres": "postgresql", "db-mysql": "mysql", "db-oracle": "oracle",
                 "db-sqlserver": "sqlserver", "db-mongo": "mongodb"}
        return [QUARKUS], profiles, {store[p] for p in profiles if p in store}
    if lane == "showcase":
        return [SHOWCASE], {"wildfly-managed", "postgresql"}, {"postgresql"}
    store = row.get("store", "postgresql")
    owned = {"postgresql", "mongodb"} if lane == "spring_runtime" else {store}
    return [STARTER, MONGO_STARTER, STORE_MODULES[store]], {"github"}, owned


def reduce_rows(lane, rows):
    if not rows or lane.startswith("quarkus_"):
        return rows
    if lane == "spring_consumer":
        return diagonal([r for r in rows if r["provider"] == "default"]) + diagonal(
            [r for r in rows if r["provider"] == "eclipselink"])
    return diagonal(rows)


def diagonal(rows):
    if not rows:
        return []
    values = {key: list(dict.fromkeys(r[key] for r in rows)) for key in rows[0]}
    result = [{key: items[i % len(items)] for key, items in values.items()}
              for i in range(max(map(len, values.values())))]
    return result


def select(repo_root, paths=None):
    graph = Graph(repo_root)
    full = paths is None
    changed, triggers, reasons, code, web = (set(), set(), [], True, True) if full else classify(graph, paths)
    global_change = full or bool(reasons)
    outputs = {"mode": "full" if full else "pr", "code": str(code).lower(), "web": str(web).lower()}
    for scope, profiles in [("unit", set()), ("ee11", {"jakarta-ee11-verify"})]:
        modules = graph.downstream(changed, profiles) if not global_change else set(graph.poms)
        value = "none" if not modules else "all" if modules == set(graph.poms) else "modules"
        outputs[scope + "_scope"] = value
        outputs[scope + "_modules"] = ",".join(sorted(modules)) if value == "modules" else ""

    def affected(lane, row):
        if global_change or lane in triggers or (lane == "integration" and COVERAGE in changed):
            return True
        roots, profiles, owned = row_model(lane, row)
        # Unowned stores can only break compilation here; the always-on build compiles the whole reactor.
        ignored = {module for store, module in STORE_MODULES.items() if store not in owned}
        return bool((graph.closure(roots, profiles) & changed) - ignored)

    for lane, rows in LANES.items():
        selected = [row for row in rows if affected(lane, row)]
        if not full:
            selected = reduce_rows(lane, selected)
        outputs[lane] = json.dumps(selected, separators=(",", ":"))
        outputs["has_" + lane] = str(bool(selected)).lower()
    outputs["has_showcase"] = str(affected("showcase", {})).lower()
    return outputs, changed, reasons


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true")
    mode.add_argument("--changed-files", type=Path)
    args = parser.parse_args()
    paths = None if args.full else [line.strip() for line in args.changed_files.read_text().splitlines() if line.strip()]
    try:
        outputs, changed, reasons = select(args.repo_root, paths)
    except (ValueError, OSError, ET.ParseError) as error:
        parser.exit(1, f"affected_lanes: {error}\n")
    content = "".join(f"{key}={value}\n" for key, value in outputs.items())
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            stream.write(content)
    else:
        sys.stdout.write(content)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(f"### Lane selection: {outputs['mode']}\n\n")
            if reasons or args.full:
                stream.write("Global: " + (", ".join(reasons) if reasons else "full mode") + "\n")
            else:
                stream.write("Changed modules: " + (", ".join(sorted(changed)) or "none") + "\n")
            for lane in LANES:
                rows = json.loads(outputs[lane])
                owned = set().union(*(row_model(lane, row)[2] for row in rows))
                stores = [store for store in DATABASES if store in owned]
                stream.write(f"- {lane}: {len(rows)} rows; databases/stores: {', '.join(stores) or 'none'}\n")
            stream.write(f"- showcase: {outputs['has_showcase']}\n")


if __name__ == "__main__":
    main()
