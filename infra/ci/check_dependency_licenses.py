#!/usr/bin/env python3
"""Evaluate resolved Maven license metadata against versioned, reviewed evidence."""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


class PolicyError(ValueError):
    pass


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PolicyError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def expression_allowed(expression, allowed):
    """Evaluate SPDX AND/OR/WITH; unknown license terms fail closed.

    WITH binds to a license, AND binds more tightly than OR. Parse both sides
    even when one determines the outcome, so malformed alternatives cannot pass.
    Only explicitly listed license/exception pairs are accepted as policy terms.
    """
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9.+-]*|[()]|\S", expression)
    position = 0

    def take():
        nonlocal position
        if position == len(tokens):
            raise PolicyError(f"Incomplete license expression: {expression}")
        token = tokens[position]
        position += 1
        return token

    def peek():
        return tokens[position] if position < len(tokens) else None

    def identifier():
        token = take()
        if token in {"AND", "OR", "WITH"} or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9.+-]*", token
        ):
            raise PolicyError(f"Invalid license term: {token}")
        return token

    def atom():
        if peek() == "(":
            take()
            value = either()
            if take() != ")":
                raise PolicyError(f"Unbalanced license expression: {expression}")
            return value
        term = identifier()
        if peek() == "WITH":
            take()
            term += " WITH " + identifier()
        return term in allowed

    def both():
        value = atom()
        while peek() == "AND":
            take()
            right = atom()
            value = value and right
        return value

    def either():
        value = both()
        while peek() == "OR":
            take()
            right = both()
            value = value or right
        return value

    result = either()
    if position != len(tokens):
        raise PolicyError(f"Invalid license expression: {expression}")
    return result


def reactor_coordinates(root):
    """Identify this checkout's modules, without exempting the whole group ID."""
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    coordinates = set()
    visited = set()

    def visit(directory):
        directory = directory.resolve()
        if not directory.is_relative_to(root.resolve()) or directory in visited:
            raise PolicyError(f"Invalid reactor module path: {directory}")
        visited.add(directory)
        pom = ET.parse(directory / "pom.xml").getroot()

        def value(name):
            return pom.findtext(f"m:{name}", namespaces=namespace) or pom.findtext(
                f"m:parent/m:{name}", namespaces=namespace
            )

        parts = [value("groupId"), value("artifactId"), value("version")]
        if any(not part or "${" in part for part in parts):
            raise PolicyError(f"Unresolved reactor coordinate: {directory}")
        coordinates.add(":".join(parts))
        for module in pom.findall("m:modules/m:module", namespace):
            visit(directory / module.text)

    visit(root)
    return coordinates


def normalized_licenses(licenses):
    if not isinstance(licenses, list) or not licenses:
        raise PolicyError("Missing license metadata")
    result = []
    for license_info in licenses:
        if (
            not isinstance(license_info, dict)
            or set(license_info) != {"name", "url"}
            or not all(isinstance(value, str) for value in license_info.values())
            or not license_info["name"].strip()
        ):
            raise PolicyError("Missing license name or malformed metadata")
        if license_info["name"].strip().upper() in {"UNKNOWN", "NOASSERTION", "NONE"}:
            raise PolicyError("Unknown license metadata")
        result.append((license_info["name"], license_info["url"]))
    return sorted(result)


def check(inventory, policy, reactor):
    if policy["schemaVersion"] != 1:
        raise PolicyError("Unsupported policy schema")
    dependencies = inventory["dependencies"]
    if not isinstance(dependencies, list) or not dependencies:
        raise PolicyError("Empty dependency inventory")
    allowed = set(policy["allowedLicenses"])
    reviewed = policy["dependencies"]
    reactor_licenses = normalized_licenses(policy["reactorLicenses"])
    failures = []
    seen = set()
    for dependency in dependencies:
        coordinate = dependency["coordinate"]
        try:
            if not re.fullmatch(r"[^:\s]+:[^:\s]+:[^:\s]+", coordinate):
                raise PolicyError("Invalid Maven coordinate")
            if coordinate in seen:
                raise PolicyError("Duplicate dependency")
            seen.add(coordinate)
            licenses = normalized_licenses(dependency["licenses"])
            if coordinate in reactor:
                if licenses != reactor_licenses:
                    raise PolicyError("Reactor license metadata changed")
                continue
            if coordinate not in reviewed:
                raise PolicyError("Unreviewed dependency/version; add license evidence")
            review = reviewed[coordinate]
            if licenses != normalized_licenses(review["licenses"]):
                raise PolicyError("License metadata changed; review the evidence")
            if not review["evidence"] or not all(
                isinstance(url, str) and url.startswith("https://")
                for url in review["evidence"]
            ):
                raise PolicyError("Missing review evidence")
            permitted = expression_allowed(review["expression"], allowed)
            if not permitted:
                exception = review.get("exception", "")
                if not isinstance(exception, str) or not exception.strip():
                    raise PolicyError(f"License policy rejects {review['expression']}")
        except PolicyError as error:
            failures.append(f"{coordinate}: {error}")
    if failures:
        raise PolicyError("\n".join(failures))
    return len(seen)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path(__file__).with_name("dependency-license-policy.json"),
    )
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    args = parser.parse_args()
    try:
        count = check(
            read_json(args.inventory),
            read_json(args.policy),
            reactor_coordinates(args.root),
        )
    except (
        PolicyError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        ET.ParseError,
    ) as error:
        print(f"FAIL: dependency license review required:\n{error}", file=sys.stderr)
        return 1
    print(
        f"OK: {count} dependencies match reviewed license policy (no license-service requests)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
