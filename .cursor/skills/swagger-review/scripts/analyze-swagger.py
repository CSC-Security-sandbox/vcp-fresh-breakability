#!/usr/bin/env python3
"""
analyze-swagger.py — structural analysis of a Swagger 2.0 / OAS 3.x spec.

Usage:
    python3 analyze-swagger.py <path-to-spec.yaml>

Output: JSON summary printed to stdout, then a human-readable findings table.

Dependencies: PyYAML (pip install pyyaml)
"""

import sys
import json
import re
from pathlib import Path
from collections import defaultdict

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VERB_SEGMENTS = {
    "create", "delete", "remove", "update", "edit", "modify", "get", "list",
    "fetch", "retrieve", "search", "find", "add", "set", "make", "do", "run",
    "execute", "process", "generate", "build", "check",
}

CRUD_METHODS = {"get", "post", "put", "patch", "delete"}

EXPECTED_STATUS_CODES = {
    "get":    {"200", "400", "401", "403", "404", "500"},
    "post":   {"200", "201", "202", "400", "401", "403", "409", "500"},
    "put":    {"200", "204", "400", "401", "403", "404", "500"},
    "patch":  {"200", "204", "400", "401", "403", "404", "500"},
    "delete": {"200", "204", "400", "401", "403", "404", "500"},
}


def load_spec(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def is_swagger2(spec: dict) -> bool:
    return "swagger" in spec


def get_paths(spec: dict) -> dict:
    return spec.get("paths", {})


def get_definitions(spec: dict) -> dict:
    if is_swagger2(spec):
        return spec.get("definitions", {})
    return spec.get("components", {}).get("schemas", {})


def get_security_defs(spec: dict) -> dict:
    if is_swagger2(spec):
        return spec.get("securityDefinitions", {})
    return spec.get("components", {}).get("securitySchemes", {})


def path_segments(path: str) -> list:
    return [s for s in path.strip("/").split("/") if s and not s.startswith("{")]


def has_verb_segment(path: str) -> list:
    """Return any path segments that look like verbs (not param placeholders)."""
    violations = []
    for seg in path_segments(path):
        # split camelCase and check each word
        words = re.sub(r"([A-Z])", r" \1", seg).lower().split()
        for w in words:
            if w in VERB_SEGMENTS:
                violations.append(seg)
                break
    return violations


def is_plural_noun(seg: str) -> bool:
    """Rough check: ends with 's' or is a known exception."""
    known_ok = {"health", "status", "me", "version", "infrastructure",
                "domainpeering", "tenantmigration"}
    seg_lower = seg.lower()
    if seg_lower in known_ok:
        return True
    return seg_lower.endswith("s")


# ---------------------------------------------------------------------------
# Analysis passes
# ---------------------------------------------------------------------------

def analyse_paths(paths: dict) -> dict:
    findings = []
    operation_ids = []
    missing_op_ids = []
    missing_summaries = []
    bad_naming = []
    status_code_issues = []
    method_issues = []
    schema_issues = []

    for path, path_item in paths.items():
        segments = path_segments(path)

        # Check verb in path segments
        verbs = has_verb_segment(path)
        if verbs:
            bad_naming.append({
                "path": path,
                "issue": f"Verb(s) in path segment: {verbs}",
                "severity": "critical",
                "category": "Resource Naming",
            })

        # Check first-level resource segment is plural
        non_version_segs = [s for s in segments if not re.match(r"^v\d+", s)]
        if non_version_segs:
            first = non_version_segs[0]
            if not is_plural_noun(first):
                bad_naming.append({
                    "path": path,
                    "issue": f"First resource segment '{first}' appears singular — prefer plural nouns",
                    "severity": "warning",
                    "category": "Resource Naming",
                })

        for method, operation in path_item.items():
            if method not in CRUD_METHODS and method != "options":
                continue
            if not isinstance(operation, dict):
                continue

            op_id = operation.get("operationId", "")
            if not op_id:
                missing_op_ids.append({"path": path, "method": method.upper()})
            else:
                operation_ids.append(op_id)

            if not operation.get("summary"):
                missing_summaries.append({"path": path, "method": method.upper()})

            # Validate status codes
            responses = operation.get("responses", {})
            present_codes = {str(k) for k in responses.keys()}
            expected = EXPECTED_STATUS_CODES.get(method, set())

            # GET must not have 201/202
            if method == "get" and ("201" in present_codes or "202" in present_codes):
                status_code_issues.append({
                    "path": path,
                    "method": method.upper(),
                    "issue": "GET returning 201/202 — incorrect for read operations",
                    "severity": "critical",
                    "category": "Status Codes",
                })

            # DELETE should have 204 or 200, not 201
            if method == "delete" and "201" in present_codes:
                status_code_issues.append({
                    "path": path,
                    "method": method.upper(),
                    "issue": "DELETE returning 201 Created — should be 200 or 204",
                    "severity": "critical",
                    "category": "Status Codes",
                })

            # Check for missing 500
            if "500" not in present_codes and "default" not in present_codes:
                status_code_issues.append({
                    "path": path,
                    "method": method.upper(),
                    "issue": "Missing 500 or default error response",
                    "severity": "warning",
                    "category": "Status Codes",
                })

            # Check for missing 401/403 on non-OPTIONS endpoints
            if method not in ("options", "get") or True:
                if "401" not in present_codes and "403" not in present_codes:
                    status_code_issues.append({
                        "path": path,
                        "method": method.upper(),
                        "issue": "Missing 401 and 403 responses (auth errors not documented)",
                        "severity": "warning",
                        "category": "Status Codes",
                    })

            # Check inline schemas in responses
            for code, resp in responses.items():
                if not isinstance(resp, dict):
                    continue
                if "$ref" in resp:
                    continue
                schema = resp.get("schema", {})
                if schema and "$ref" not in schema:
                    schema_type = schema.get("type", "")
                    if schema_type == "array" and "$ref" not in schema.get("items", {}):
                        schema_issues.append({
                            "path": path,
                            "method": method.upper(),
                            "code": str(code),
                            "issue": "Inline array schema in response — use $ref to a named schema",
                            "severity": "warning",
                            "category": "Schemas",
                        })

            # POST to a collection should return 201
            if method == "post":
                # heuristic: if path ends without a parameter, it's a collection
                path_clean = path.rstrip("/")
                if not path_clean.endswith("}"):
                    if "201" not in present_codes and "202" not in present_codes:
                        status_code_issues.append({
                            "path": path,
                            "method": "POST",
                            "issue": "POST to collection missing 201 Created (or 202 for async)",
                            "severity": "warning",
                            "category": "Status Codes",
                        })

    # Check operationId uniqueness
    from collections import Counter
    dup_op_ids = [op_id for op_id, count in Counter(operation_ids).items() if count > 1]

    return {
        "bad_naming": bad_naming,
        "missing_operation_ids": missing_op_ids,
        "missing_summaries": missing_summaries,
        "status_code_issues": status_code_issues,
        "schema_issues": schema_issues,
        "duplicate_operation_ids": dup_op_ids,
        "total_operations": len(operation_ids) + len(missing_op_ids),
    }


def analyse_definitions(definitions: dict) -> dict:
    issues = []
    for name, schema in definitions.items():
        if not isinstance(schema, dict):
            continue
        if not schema.get("description"):
            issues.append({
                "schema": name,
                "issue": "Schema missing top-level description",
                "severity": "suggestion",
                "category": "Documentation",
            })
        props = schema.get("properties", {})
        for prop_name, prop_schema in props.items():
            if isinstance(prop_schema, dict) and not prop_schema.get("description"):
                issues.append({
                    "schema": f"{name}.{prop_name}",
                    "issue": "Property missing description",
                    "severity": "suggestion",
                    "category": "Documentation",
                })
    return {"definition_issues": issues, "total_schemas": len(definitions)}


def analyse_security(spec: dict) -> dict:
    issues = []
    sec_defs = get_security_defs(spec)
    global_security = spec.get("security", [])
    paths = get_paths(spec)

    if not sec_defs:
        issues.append({
            "issue": "No securityDefinitions / securitySchemes found in spec",
            "severity": "critical",
            "category": "Security",
        })
        return {"security_issues": issues}

    endpoints_without_security = []
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if method not in CRUD_METHODS:
                continue
            if not isinstance(operation, dict):
                continue
            op_security = operation.get("security")
            if op_security is None and not global_security:
                endpoints_without_security.append(f"{method.upper()} {path}")

    if endpoints_without_security:
        issues.append({
            "issue": f"{len(endpoints_without_security)} endpoints have no security applied "
                     f"(and no global security default)",
            "severity": "critical",
            "category": "Security",
            "affected": endpoints_without_security[:10],
        })

    return {"security_issues": issues, "security_schemes": list(sec_defs.keys())}


def analyse_info(spec: dict) -> dict:
    issues = []
    info = spec.get("info", {})
    if not info.get("title"):
        issues.append({"issue": "Missing info.title", "severity": "warning"})
    if not info.get("description"):
        issues.append({"issue": "Missing info.description", "severity": "suggestion"})
    if not info.get("version"):
        issues.append({"issue": "Missing info.version", "severity": "warning"})

    schemes = spec.get("schemes", [])
    if "https" not in schemes:
        issues.append({
            "issue": f"HTTPS not listed in schemes (got: {schemes})",
            "severity": "warning",
            "category": "Security",
        })
    if schemes == ["http"]:
        issues.append({
            "issue": "Only HTTP listed — spec should require HTTPS",
            "severity": "critical",
            "category": "Security",
        })

    return {"info_issues": issues, "version": info.get("version", "unknown")}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def print_table(title: str, rows: list, key: str = "issue", severity_key: str = "severity"):
    if not rows:
        return
    print(f"\n{'='*70}")
    print(f"  {title} ({len(rows)} items)")
    print(f"{'='*70}")
    sev_icon = {"critical": "🔴", "warning": "🟡", "suggestion": "🟢"}
    for r in rows:
        icon = sev_icon.get(r.get(severity_key, ""), "•")
        path_info = r.get("path", r.get("schema", ""))
        method = r.get("method", "")
        loc = f"{method} {path_info}".strip() if path_info else ""
        print(f"  {icon}  [{r.get('category', '')}] {r.get(key, '')} — {loc}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze-swagger.py <path-to-spec.yaml>", file=sys.stderr)
        sys.exit(1)

    spec_path = sys.argv[1]
    if not Path(spec_path).exists():
        print(f"ERROR: File not found: {spec_path}", file=sys.stderr)
        sys.exit(1)

    spec = load_spec(spec_path)
    format_str = f"Swagger {spec.get('swagger', '')}" if is_swagger2(spec) else f"OAS {spec.get('openapi', '')}"

    print(f"\n{'='*70}")
    print(f"  Swagger Analysis: {spec_path}")
    print(f"  Format: {format_str}")
    print(f"{'='*70}")

    info_result = analyse_info(spec)
    path_result = analyse_paths(get_paths(spec))
    def_result = analyse_definitions(get_definitions(spec))
    sec_result = analyse_security(spec)

    # Summary counts
    total_ops = path_result["total_operations"]
    total_schemas = def_result["total_schemas"]
    all_issues = (
        info_result["info_issues"] +
        path_result["bad_naming"] +
        path_result["status_code_issues"] +
        path_result["schema_issues"] +
        sec_result["security_issues"]
    )
    critical = sum(1 for i in all_issues if i.get("severity") == "critical")
    warnings = sum(1 for i in all_issues if i.get("severity") == "warning")
    suggestions = sum(1 for i in all_issues if i.get("severity") == "suggestion")

    print(f"\n  Operations  : {total_ops}")
    print(f"  Schemas     : {total_schemas}")
    print(f"  🔴 Critical  : {critical}")
    print(f"  🟡 Warnings  : {warnings}")
    print(f"  🟢 Suggestions: {suggestions}")

    if info_result["info_issues"]:
        print_table("Info / Global Issues", info_result["info_issues"])

    if path_result["bad_naming"]:
        print_table("Naming Violations", path_result["bad_naming"])

    if path_result["status_code_issues"]:
        print_table("Status Code Issues", path_result["status_code_issues"])

    if path_result["schema_issues"]:
        print_table("Schema Issues", path_result["schema_issues"])

    if path_result["missing_operation_ids"]:
        print(f"\n{'='*70}")
        print(f"  Missing operationId ({len(path_result['missing_operation_ids'])} endpoints)")
        print(f"{'='*70}")
        for item in path_result["missing_operation_ids"]:
            print(f"  🟡  {item['method']} {item['path']}")

    if path_result["duplicate_operation_ids"]:
        print(f"\n{'='*70}")
        print(f"  Duplicate operationIds ({len(path_result['duplicate_operation_ids'])})")
        print(f"{'='*70}")
        for op_id in path_result["duplicate_operation_ids"]:
            print(f"  🔴  {op_id}")

    if sec_result["security_issues"]:
        print_table("Security Issues", sec_result["security_issues"])

    # JSON summary for programmatic use
    summary = {
        "spec": spec_path,
        "format": format_str,
        "version": info_result["version"],
        "total_operations": total_ops,
        "total_schemas": total_schemas,
        "counts": {"critical": critical, "warning": warnings, "suggestion": suggestions},
        "security_schemes": sec_result.get("security_schemes", []),
        "duplicate_operation_ids": path_result["duplicate_operation_ids"],
        "missing_operation_ids_count": len(path_result["missing_operation_ids"]),
        "missing_summaries_count": len(path_result["missing_summaries"]),
    }

    print(f"\n{'='*70}")
    print("  JSON Summary")
    print(f"{'='*70}")
    print(json.dumps(summary, indent=2))
    print()


if __name__ == "__main__":
    main()
