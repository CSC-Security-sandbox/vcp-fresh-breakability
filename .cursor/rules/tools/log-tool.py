#!/usr/bin/env python3
"""
Triage log helper.

Provides deterministic helpers for:
- fetching correlation-scoped raw logs from Cloud Logging
- normalizing logs into an LogBundle JSON artifact
- doing both in one command
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BUNDLE_VERSION = "2"
FETCH_ATTEMPTS = (
    ("7d", 20000),
    ("30d", 50000),
    ("30d", 200000),
)
CVS_COMPONENT_PATTERNS = (
    "cloud-volumes-service",
    "cloud-volumes-infrastructure",
    "cloud-volumes-internal",
    "cloud-volumes-service-worker",
)
CVP_COMPONENT_PATTERNS = (
    "cloud-volumes-proxy",
    "cloud-volumes-proxy-1p",
)
CVN_COMPONENT_PATTERNS = ("cloud-volumes-network",)
CORRELATION_KEYS = (
    "correlation_id",
    "correlationId",
    "x-correlation-id",
    "x_correlation_id",
    "requestCorrelationID",
)
WORKFLOW_KEYS = ("workflow_id", "workflowId", "workflowID")
JOB_KEYS = ("job_id", "jobId", "jobID")
TRACKING_KEYS = ("tracking_id", "trackingId")
REQUEST_KEYS = ("request_id", "requestId", "requestID")
PROJECT_KEYS = (
    "tenant_project_id",
    "tenantProjectId",
    "tenant_project_number",
    "tenantProjectNumber",
    "subnet_host_project",
    "subnetHostProject",
)
PAYLOAD_FRAGMENT_KEYS = {
    "resourceId",
    "resource_id",
    "resourceName",
    "resource_name",
    "volume_id",
    "volumeId",
    "pool_id",
    "poolId",
    "snapshot_id",
    "snapshotId",
    "backup_id",
    "backupId",
    "lun_name",
    "lunName",
    "creationToken",
    "creation_token",
    "operation",
    "operation_name",
    "operationName",
    "project",
    "project_id",
    "projectId",
    "project_number",
    "projectNumber",
    "tenant_project_id",
    "tenantProjectId",
    "tenant_project_number",
    "tenantProjectNumber",
    "subnet_host_project",
    "subnetHostProject",
    "network",
    "network_name",
    "networkName",
    "vpc",
    "region",
    "zone",
    "remoteRegion",
    "remote_region",
    "remoteZone",
    "remote_zone",
    "destinationLocation",
    "destination_location",
    "sourceLocation",
    "source_location",
    "destinationLocationId",
    "destination_location_id",
    "sourceLocationId",
    "source_location_id",
}
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
LONG_NUMBER_RE = re.compile(r"\b\d{5,}\b")
HEX_RE = re.compile(r"\b[0-9a-fA-F]{16,}\b")
OPERATION_RE = re.compile(r"operations/[A-Za-z0-9._:-]+")
LOCATION_PATH_RE = re.compile(r"/locations/([^/\s]+)")
FETCH_WINDOW_PADDING_SECONDS = 900
JOB_FRESHNESS = "30d"
JOB_FETCH_LIMIT = 5000
RESOURCE_FETCH_LIMIT = 8000
RESOURCE_FRESHNESS_LADDER = ("7d", "15d", "30d")
RESOURCE_LADDER_MIN_HITS = 10
MAX_JOB_IDS = 8
MAX_RESOURCE_IDS = 8
MIN_RESOURCE_ID_LEN = 4
RESOURCE_HINT_KEYS = (
    "resourceId",
    "resource_id",
    "resourceName",
    "resource_name",
    "volume_id",
    "volumeId",
    "pool_id",
    "poolId",
    "snapshot_id",
    "snapshotId",
    "backup_id",
    "backupId",
    "creationToken",
    "creation_token",
    "lun_name",
    "lunName",
)
LOCATION_HINT_KEYS = (
    "region",
    "zone",
    "remoteRegion",
    "remote_region",
    "remoteZone",
    "remote_zone",
    "destinationLocation",
    "destination_location",
    "sourceLocation",
    "source_location",
    "destinationLocationId",
    "destination_location_id",
    "sourceLocationId",
    "source_location_id",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and bundle triage logs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch raw logs only")
    add_fetch_args(fetch_parser)
    add_cross_repo_arg(fetch_parser)

    bundle_parser = subparsers.add_parser("bundle", help="Build LogBundle from raw logs")
    bundle_parser.add_argument("--log-file", required=True, help="Raw log file path")
    bundle_parser.add_argument(
        "--bundle-file",
        help="Bundle output path (default: <log-file stem>.bundle.json)",
    )
    add_cross_repo_arg(bundle_parser)

    both_parser = subparsers.add_parser(
        "fetch-and-bundle", help="Fetch raw logs and build LogBundle"
    )
    add_fetch_args(both_parser)
    add_cross_repo_arg(both_parser)
    both_parser.add_argument(
        "--bundle-file",
        help="Bundle output path (default: triagebot_logs/<correlation_id>.bundle.json)",
    )
    both_parser.add_argument(
        "--no-expand",
        action="store_true",
        help="Skip pivot/resource expansion and use correlation logs only",
    )

    return parser.parse_args()


def add_fetch_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project", required=True, help="GCP project id")
    parser.add_argument("--correlation-id", required=True, help="Correlation id")
    parser.add_argument(
        "--log-file",
        help="Raw log output path (default: triagebot_logs/<correlation_id>.json)",
    )


def add_cross_repo_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--cross-repo",
        action="store_true",
        help="Include CVS, CVP, and CVN entries in addition to VCP",
    )


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_log_file(correlation_id: str) -> Path:
    return repo_root() / "triagebot_logs" / f"{correlation_id}.json"


def default_bundle_file_from_log(log_file: Path) -> Path:
    if log_file.suffix == ".json":
        return log_file.with_name(f"{log_file.stem}.bundle.json")
    return log_file.with_name(f"{log_file.name}.bundle.json")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_entries_text(raw: str) -> list[dict[str, Any]]:
    text = raw.strip()
    if not text:
        return []
    if text.startswith("["):
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            return [parsed]
        return []

    entries: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            entries.append(item)
    return entries


def run_read_query(
    project: str,
    query: str,
    *,
    freshness: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    cmd = [
        "gcloud",
        "logging",
        "read",
        query,
        "--format=json",
        "--project",
        project,
        "--freshness",
        freshness,
        "--limit",
        str(limit),
    ]
    result = subprocess.run(
        cmd,
        cwd=repo_root(),
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        error = (result.stderr or result.stdout or "unknown fetch error").strip()
        raise RuntimeError(error)
    return parse_entries_text(result.stdout), query


def quote_filter_term(value: str) -> str:
    return json.dumps(str(value))


def combine_filter(*parts: str) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    return " AND ".join(cleaned)


def build_or_filter(terms: list[str]) -> str:
    unique_terms = list(dict.fromkeys(term for term in terms if term))
    if not unique_terms:
        return ""
    if len(unique_terms) == 1:
        return quote_filter_term(unique_terms[0])
    return "(" + " OR ".join(quote_filter_term(term) for term in unique_terms) + ")"


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_timestamp(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_time_clause(start: str, end: str) -> str:
    parts: list[str] = []
    if start:
        parts.append(f'timestamp >= "{start}"')
    if end:
        parts.append(f'timestamp <= "{end}"')
    return combine_filter(*parts)


def widened_time_window(time_window: dict[str, str]) -> tuple[str, str]:
    start_dt = parse_timestamp(time_window.get("start", ""))
    end_dt = parse_timestamp(time_window.get("end", ""))
    if start_dt is None or end_dt is None:
        return "", ""
    start_dt -= timedelta(seconds=FETCH_WINDOW_PADDING_SECONDS)
    end_dt += timedelta(seconds=FETCH_WINDOW_PADDING_SECONDS)
    return format_timestamp(start_dt), format_timestamp(end_dt)


def annotate_entry(entry: dict[str, Any], *, fetch_origin: str, fetch_label: str) -> dict[str, Any]:
    annotated = dict(entry)
    triage_meta = dict(get_path(entry, "_triage") or {})
    triage_meta["fetch_origin"] = fetch_origin
    triage_meta["fetch_label"] = fetch_label
    annotated["_triage"] = triage_meta
    return annotated


def entry_fingerprint(entry: dict[str, Any]) -> str:
    insert_id = str(entry.get("insertId", "") or "")
    log_name = str(entry.get("logName", "") or "")
    timestamp = str(entry.get("timestamp", "") or "")
    if insert_id:
        return "|".join((insert_id, log_name, timestamp))
    return "|".join((timestamp, log_name, extract_message(entry)[:500]))


def sort_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        entries,
        key=lambda entry: (
            parse_timestamp_ns(str(entry.get("timestamp", "") or "")),
            str(entry.get("insertId", "") or ""),
            str(entry.get("logName", "") or ""),
        ),
    )


def write_raw_entries(log_file: Path, entries: list[dict[str, Any]]) -> None:
    ensure_parent(log_file)
    log_file.write_text(json.dumps(sort_entries(entries), indent=2), encoding="utf-8")


def run_fetch(project: str, correlation_id: str, log_file: Path) -> dict[str, Any]:
    ensure_parent(log_file)
    write_raw_entries(log_file, [])
    last_error = ""

    for attempt_number, (freshness, limit) in enumerate(FETCH_ATTEMPTS, start=1):
        try:
            entries, _ = run_read_query(
                project,
                correlation_id,
                freshness=freshness,
                limit=limit,
            )
        except RuntimeError as exc:
            last_error = str(exc)
            if attempt_number < len(FETCH_ATTEMPTS):
                time.sleep(attempt_number)
            continue

        if entries:
            annotated = [
                annotate_entry(entry, fetch_origin="correlation", fetch_label="correlation_id")
                for entry in entries
            ]
            write_raw_entries(log_file, annotated)
            if log_file.stat().st_size > 0:
                return {
                    "attempt": attempt_number,
                    "freshness": freshness,
                    "limit": limit,
                    "log_file": str(log_file),
                    "size_bytes": log_file.stat().st_size,
                    "entries": annotated,
                    "origins": ["correlation"],
                    "fetches": 1,
                }
        if attempt_number < len(FETCH_ATTEMPTS):
            time.sleep(attempt_number)

    write_raw_entries(log_file, [])
    raise RuntimeError(
        f"fetch_status=failure correlation_id={correlation_id} log_file={log_file} error={last_error}"
    )


def load_entries(log_file: Path) -> list[dict[str, Any]]:
    return parse_entries_text(log_file.read_text(encoding="utf-8"))


def parse_timestamp_ns(value: str) -> int:
    if not value or not isinstance(value, str):
        return 0
    match = re.match(
        r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$",
        value,
    )
    if not match:
        return 0

    date_part, time_part, frac_part, tz_part = match.groups()
    if tz_part == "Z":
        tzinfo = timezone.utc
    else:
        sign = 1 if tz_part[0] == "+" else -1
        hours = int(tz_part[1:3])
        minutes = int(tz_part[4:6])
        tzinfo = timezone(sign * timedelta(hours=hours, minutes=minutes))

    dt = datetime.strptime(f"{date_part}T{time_part}", "%Y-%m-%dT%H:%M:%S").replace(
        tzinfo=tzinfo
    )
    frac_ns = int((frac_part or "0").ljust(9, "0")[:9])
    return int(dt.timestamp()) * 1_000_000_000 + frac_ns


def get_path(obj: Any, *parts: str) -> Any:
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def collect_scalars(node: Any, keys: set[str], out: dict[str, Any]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in keys and key not in out and is_scalar(value):
                out[key] = value
            collect_scalars(value, keys, out)
    elif isinstance(node, list):
        for item in node:
            collect_scalars(item, keys, out)


def is_scalar(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool)) and not isinstance(value, bool)


def first_scalar(flat: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = flat.get(key)
        if value is None:
            continue
        if isinstance(value, (str, int, float)):
            return str(value)
    return ""


def collect_payload_fragment(node: Any, out: dict[str, Any]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in PAYLOAD_FRAGMENT_KEYS and key not in out and is_simple_json(value):
                out[key] = value
            if len(out) < 16:
                collect_payload_fragment(value, out)
    elif isinstance(node, list):
        for item in node:
            if len(out) >= 16:
                break
            collect_payload_fragment(item, out)


def is_simple_json(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool, list, dict)) and len(
        json.dumps(value, default=str)
    ) <= 400


def extract_component(entry: dict[str, Any]) -> str:
    candidates = (
        get_path(entry, "resource", "labels", "container_name"),
        get_path(entry, "labels", "k8s-pod/app"),
        get_path(entry, "jsonPayload", "service", "name"),
        get_path(entry, "service", "name"),
    )
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unknown"


def classify_service(component: str) -> str:
    text = component.lower()
    if text in CVS_COMPONENT_PATTERNS:
        return "cvs"
    if text in CVP_COMPONENT_PATTERNS:
        return "cvp"
    if text in CVN_COMPONENT_PATTERNS:
        return "cvn"
    if (
        text in ("vsa-control-plane", "core-api", "worker", "google-proxy")
        or text.startswith("vcp-customer-worker")
        or text.startswith("vcp-worker")
        or text.startswith("customer-worker")
        or text.startswith("vlm-worker")
        or text.startswith("vsa-lifecycle-manager")
    ):
        return "vcp"
    return "unknown"


def extract_message(entry: dict[str, Any]) -> str:
    candidates = (
        get_path(entry, "jsonPayload", "message"),
        entry.get("textPayload"),
        get_path(entry, "protoPayload", "status", "message"),
    )
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def normalize_message_template(message: str) -> str:
    if not message:
        return ""
    text = UUID_RE.sub("<uuid>", message)
    text = OPERATION_RE.sub("operations/<id>", text)
    text = HEX_RE.sub("<hex>", text)
    text = LONG_NUMBER_RE.sub("<num>", text)
    return text[:500]


def extract_error(entry: dict[str, Any]) -> dict[str, str]:
    error_obj = get_path(entry, "jsonPayload", "error")
    if isinstance(error_obj, dict):
        return {
            "code": str(error_obj.get("code", "") or ""),
            "message": str(error_obj.get("message", "") or ""),
            "stack": str(error_obj.get("stack", "") or ""),
        }
    if isinstance(error_obj, str):
        return {
            "code": "",
            "message": error_obj,
            "stack": "",
        }

    proto_status = get_path(entry, "protoPayload", "status")
    if isinstance(proto_status, dict):
        return {
            "code": str(proto_status.get("code", "") or ""),
            "message": str(proto_status.get("message", "") or ""),
            "stack": "",
        }

    return {"code": "", "message": "", "stack": ""}


def extract_google_operations(entry: dict[str, Any]) -> list[str]:
    operations: list[str] = []
    seen: set[str] = set()

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)
        elif isinstance(node, str):
            for match in OPERATION_RE.findall(node):
                if match not in seen:
                    operations.append(match)
                    seen.add(match)

    visit(entry)
    return operations


def build_error_signature(
    source_service: str,
    component: str,
    error: dict[str, str],
    message_template: str,
) -> str:
    parts = [source_service, component, error.get("code", ""), message_template]
    return "|".join(part for part in parts if part)[:800]


def update_project_context(
    project_context: dict[str, Any],
    flat: dict[str, Any],
    source_event_index: int,
) -> None:
    project_id = first_scalar(flat, ("tenant_project_id", "tenantProjectId"))
    project_number = first_scalar(flat, ("tenant_project_number", "tenantProjectNumber"))
    subnet_host = first_scalar(flat, ("subnet_host_project", "subnetHostProject"))

    if project_id and not project_context["tenant_project_id"]:
        project_context["tenant_project_id"] = project_id
        project_context["source_event_ids"].append(source_event_index)
    if project_number and not project_context["tenant_project_number"]:
        project_context["tenant_project_number"] = project_number
        project_context["source_event_ids"].append(source_event_index)
    if subnet_host and not project_context["subnet_host_project"]:
        project_context["subnet_host_project"] = subnet_host
        project_context["source_event_ids"].append(source_event_index)


def infer_cross_service_calls(entry: dict[str, Any]) -> list[dict[str, str]]:
    source = entry["source_service"]
    message = f"{entry['component']} {entry['message']}".lower()
    calls: list[dict[str, str]] = []

    def add(target: str, boundary_type: str) -> None:
        if target == source:
            return
        if any(
            existing["callee_service"] == target and existing["boundary_type"] == boundary_type
            for existing in calls
        ):
            return
        calls.append(
            {
                "caller_service": source,
                "callee_service": target,
                "boundary_type": boundary_type,
                "evidence": entry["message"][:300],
            }
        )

    if source == "vcp":
        if "cloud-volumes-service" in message or re.search(r"\bcvs\b", message):
            add("cvs", "api-call")
        if (
            entry["component"] == "google-proxy"
            or "cloud-volumes-proxy" in message
            or "gcp api" in message
            or "proxy timeout" in message
        ):
            add("cvp", "proxy")
        if (
            "cloud-volumes-network" in message
            or "vlan attachment" in message
            or "vpc peering" in message
            or "vlnetwork" in message
            or "vxnetwork" in message
        ):
            add("cvn", "network-setup")
    elif source == "cvs":
        if "cloud-volumes-proxy" in message or "gcp operation" in message:
            add("cvp", "proxy")
        if (
            "cloud-volumes-network" in message
            or "network setup" in message
            or "vlan" in message
            or "peering" in message
            or "address range" in message
        ):
            add("cvn", "network-setup")

    return calls


def detect_terminal_status(entry: dict[str, Any]) -> str:
    message = entry["message"].lower()
    severity = entry["severity"]
    error_message = entry["error"].get("message", "").lower()
    if (
        message == "database error"
        and error_message in {"record not found", "sql: no rows in result set"}
    ):
        return ""
    if "timeout" in message or "deadline exceeded" in message or "context canceled" in message:
        return "terminal-timeout"
    if any(
        token in message
        for token in (
            "completed successfully",
            "operation complete",
            "finished successfully",
            "workflow done",
            "job done",
            "succeeded",
        )
    ):
        return "terminal-success"
    if severity in {"ERROR", "WARNING"} or any(
        token in message
        for token in (" failed", " error", "exception", "panic", "marked error")
    ):
        return "terminal-error"
    return ""


def infer_api_family(message: str) -> str:
    lowered = message.lower()
    if "servicenetworking" in lowered or "service networking" in lowered:
        return "servicenetworking"
    if any(
        token in lowered
        for token in ("compute", "router", "route", "vlan attachment", "subnetwork", "interconnect")
    ):
        return "compute"
    if "container" in lowered or "gke" in lowered:
        return "container"
    return "unknown"


def infer_scope(message: str) -> str:
    lowered = message.lower()
    if "zones/" in lowered or " zonal " in lowered:
        return "zonal"
    if "regions/" in lowered or " regional " in lowered or "region" in lowered:
        return "regional"
    if "global" in lowered:
        return "global"
    return "unknown"


def extract_locations_from_text(text: str) -> list[str]:
    if not text:
        return []
    return list(dict.fromkeys(match.group(1) for match in LOCATION_PATH_RE.finditer(text)))


def is_useful_resource_id(text: str) -> bool:
    if not text or len(text) < MIN_RESOURCE_ID_LEN:
        return False
    stripped = text.strip("0").strip("-").strip()
    return bool(stripped)


def derive_resource_hints(entries: list[dict[str, Any]]) -> list[str]:
    hints: list[str] = []
    seen_lower: set[str] = set()
    for entry in entries:
        payload_fragment = entry.get("payload_fragment", {})
        if not isinstance(payload_fragment, dict):
            continue
        for key in RESOURCE_HINT_KEYS:
            value = payload_fragment.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if not is_useful_resource_id(text):
                continue
            lowered = text.lower()
            if lowered in seen_lower:
                continue
            seen_lower.add(lowered)
            hints.append(text)
    return hints[:MAX_RESOURCE_IDS]


def derive_location_hints(entries: list[dict[str, Any]]) -> list[str]:
    locations: list[str] = []
    for entry in entries:
        payload_fragment = entry.get("payload_fragment", {})
        if isinstance(payload_fragment, dict):
            for key in LOCATION_HINT_KEYS:
                value = payload_fragment.get(key)
                if value is None:
                    continue
                text = str(value).strip()
                if text and text not in locations:
                    locations.append(text)
        for location in extract_locations_from_text(str(entry.get("message", "") or "")):
            if location not in locations:
                locations.append(location)
    return locations


def unique_related_ids(entries: list[dict[str, Any]], key: str) -> list[str]:
    values: list[str] = []
    for entry in entries:
        related_ids = entry.get("related_ids", {})
        if not isinstance(related_ids, dict):
            continue
        value = str(related_ids.get(key, "") or "").strip()
        if value and value not in values:
            values.append(value)
    return values


def derive_job_ids(normalized_entries: list[dict[str, Any]]) -> list[str]:
    """Collect job ids (folding workflow ids in as aliases since they overlap on VCP)."""
    values: list[str] = []
    seen: set[str] = set()
    for key in ("job_id", "workflow_id"):
        for value in unique_related_ids(normalized_entries, key):
            text = value.strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(text)
    return values[:MAX_JOB_IDS]


def build_job_step(bundle: dict[str, Any]) -> dict[str, Any] | None:
    """Pass 2: search by job id, scoped to the trace window ±15 min, freshness 30d."""
    normalized_entries = bundle.get("normalized_entries", [])
    if not isinstance(normalized_entries, list):
        return None

    job_ids = derive_job_ids(normalized_entries)
    filter_terms = build_or_filter(job_ids)
    if not filter_terms:
        return None

    start, end = widened_time_window(bundle.get("time_window", {}))
    time_clause = build_time_clause(start, end)
    filter_expr = combine_filter(filter_terms, time_clause) or filter_terms

    return {
        "fetch_origin": "job",
        "fetch_label": "job_id",
        "filter": filter_expr,
        "freshness": JOB_FRESHNESS,
        "limit": JOB_FETCH_LIMIT,
        "values": job_ids,
    }


def build_resource_step(bundle: dict[str, Any]) -> dict[str, Any] | None:
    """Pass 3: search by resource id, no time clause, freshness ladder 7d->15d->30d."""
    normalized_entries = bundle.get("normalized_entries", [])
    if not isinstance(normalized_entries, list):
        return None

    resource_ids = derive_resource_hints(normalized_entries)
    filter_terms = build_or_filter(resource_ids)
    if not filter_terms:
        return None

    return {
        "fetch_origin": "resource",
        "fetch_label": "resource_id",
        "filter": filter_terms,
        "freshness_ladder": list(RESOURCE_FRESHNESS_LADDER),
        "min_hits": RESOURCE_LADDER_MIN_HITS,
        "limit": RESOURCE_FETCH_LIMIT,
        "values": resource_ids,
    }


def merge_entries(*entry_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group in entry_groups:
        for entry in group:
            fingerprint = entry_fingerprint(entry)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            merged.append(entry)
    return sort_entries(merged)


def run_single_step(project: str, step: dict[str, Any]) -> list[dict[str, Any]]:
    entries, _ = run_read_query(
        project,
        step["filter"],
        freshness=step["freshness"],
        limit=step["limit"],
    )
    return [
        annotate_entry(
            entry,
            fetch_origin=step["fetch_origin"],
            fetch_label=step["fetch_label"],
        )
        for entry in entries
    ]


def run_resource_ladder(
    project: str, step: dict[str, Any]
) -> tuple[list[dict[str, Any]], str]:
    """Try each freshness in the ladder; stop once we have at least min_hits entries.

    Returns (annotated_entries, freshness_used). freshness_used is "" when nothing was
    fetched (e.g. all calls failed). This is the only resource pass we make per run.
    """
    last_entries: list[dict[str, Any]] = []
    last_freshness = ""
    for freshness in step["freshness_ladder"]:
        try:
            entries, _ = run_read_query(
                project,
                step["filter"],
                freshness=freshness,
                limit=step["limit"],
            )
        except RuntimeError:
            continue
        last_freshness = freshness
        last_entries = entries
        if len(entries) >= step["min_hits"]:
            break
    annotated = [
        annotate_entry(
            entry,
            fetch_origin=step["fetch_origin"],
            fetch_label=step["fetch_label"],
        )
        for entry in last_entries
    ]
    return annotated, last_freshness


def build_bundle_from_entries(
    raw_entries: list[dict[str, Any]],
    *,
    cross_repo: bool,
    log_file_label: str,
) -> dict[str, Any]:
    normalized_entries: list[dict[str, Any]] = []
    severity_counts = {key: 0 for key in ("ERROR", "WARNING", "INFO", "DEBUG", "DEFAULT")}
    service_breakdown: dict[str, dict[str, Any]] = {
        "vcp": {"entry_count": 0, "error_count": 0, "containers": set()},
        "cvs": {"entry_count": 0, "error_count": 0, "containers": set()},
        "cvp": {"entry_count": 0, "error_count": 0, "containers": set()},
        "cvn": {"entry_count": 0, "error_count": 0, "containers": set()},
        "unknown": {"entry_count": 0, "error_count": 0, "containers": set()},
    }
    project_context = {
        "tenant_project_id": "",
        "tenant_project_number": "",
        "subnet_host_project": "",
        "source_event_ids": [],
    }
    pending_calls: list[dict[str, Any]] = []
    pending_terminals: list[dict[str, Any]] = []
    pending_operations: dict[str, dict[str, Any]] = {}

    interesting_keys = set(
        CORRELATION_KEYS
        + WORKFLOW_KEYS
        + JOB_KEYS
        + TRACKING_KEYS
        + REQUEST_KEYS
        + PROJECT_KEYS
        + (
            "operation",
            "operation_name",
            "operationName",
            "project",
            "project_id",
            "projectId",
            "project_number",
            "projectNumber",
            "region",
            "zone",
        )
    )

    for raw_index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            continue

        flat: dict[str, Any] = {}
        collect_scalars(entry, interesting_keys, flat)
        payload_fragment: dict[str, Any] = {}
        collect_payload_fragment(entry, payload_fragment)

        timestamp = str(entry.get("timestamp", "") or "")
        timestamp_ns = parse_timestamp_ns(timestamp)
        severity = str(entry.get("severity", "DEFAULT") or "DEFAULT").upper()
        if severity not in severity_counts:
            severity = "DEFAULT"

        component = extract_component(entry)
        source_service = classify_service(component)
        if not cross_repo and source_service != "vcp":
            continue
        severity_counts[severity] += 1
        message = extract_message(entry)
        message_template = normalize_message_template(message)
        error = extract_error(entry)
        workflow_id = first_scalar(flat, WORKFLOW_KEYS)
        job_id = first_scalar(flat, JOB_KEYS)
        fetch_origin = str(get_path(entry, "_triage", "fetch_origin") or "correlation")

        normalized = {
            "_raw_index": raw_index,
            "event_id": "",
            "timestamp": timestamp,
            "timestamp_ns": timestamp_ns,
            "severity": severity,
            "component": component,
            "source_service": source_service,
            "message": message,
            "message_template": message_template,
            "fetch_origin": fetch_origin,
            "correlation_id": first_scalar(flat, CORRELATION_KEYS),
            "related_ids": {
                "workflow_id": workflow_id,
                "job_id": job_id,
                "tracking_id": first_scalar(flat, TRACKING_KEYS),
                "request_id": first_scalar(flat, REQUEST_KEYS),
            },
            "error_signature": build_error_signature(
                source_service, component, error, message_template
            ),
            "error": error,
            "payload_fragment": payload_fragment,
        }
        normalized_entries.append(normalized)

        service_breakdown[source_service]["entry_count"] += 1
        service_breakdown[source_service]["containers"].add(component)
        if severity in {"ERROR", "WARNING"}:
            service_breakdown[source_service]["error_count"] += 1

        update_project_context(project_context, flat, raw_index)

        if cross_repo:
            for call in infer_cross_service_calls(normalized):
                call["_raw_index"] = raw_index
                pending_calls.append(call)

        terminal_status = detect_terminal_status(normalized)
        if terminal_status:
            pending_terminals.append({"_raw_index": raw_index, "status": terminal_status})

        for operation_name in extract_google_operations(entry):
            existing = pending_operations.setdefault(
                operation_name,
                {
                    "operation_name": operation_name,
                    "api_family_hint": infer_api_family(message),
                    "project_hint": (
                        payload_fragment.get("subnet_host_project")
                        or payload_fragment.get("tenant_project_id")
                        or payload_fragment.get("tenant_project_number")
                        or project_context["subnet_host_project"]
                        or project_context["tenant_project_id"]
                        or project_context["tenant_project_number"]
                    ),
                    "project_hint_source": (
                        "subnet_host_project"
                        if payload_fragment.get("subnet_host_project") or project_context["subnet_host_project"]
                        else "tenant_project_id"
                        if payload_fragment.get("tenant_project_id") or project_context["tenant_project_id"]
                        else "tenant_project_number"
                        if payload_fragment.get("tenant_project_number")
                        or project_context["tenant_project_number"]
                        else "unknown"
                    ),
                    "scope_hint": infer_scope(message),
                    "region": str(payload_fragment.get("region", "") or ""),
                    "zone": str(payload_fragment.get("zone", "") or ""),
                    "target_link_hint": "",
                    "source_event_indices": [],
                },
            )
            existing["source_event_indices"].append(raw_index)
            if existing["api_family_hint"] == "unknown":
                existing["api_family_hint"] = infer_api_family(message)
            if existing["scope_hint"] == "unknown":
                existing["scope_hint"] = infer_scope(message)

    normalized_entries.sort(key=lambda item: (item["timestamp_ns"], item["_raw_index"]))
    index_to_event_id: dict[int, str] = {}
    for index, entry in enumerate(normalized_entries, start=1):
        event_id = f"evt-{index:06d}"
        entry["event_id"] = event_id
        index_to_event_id[entry["_raw_index"]] = event_id
        del entry["_raw_index"]

    error_inventory = [
        {
            "event_id": entry["event_id"],
            "timestamp": entry["timestamp"],
            "severity": entry["severity"],
            "source_service": entry["source_service"],
            "component": entry["component"],
            "message": entry["message"],
            "error_signature": entry["error_signature"],
        }
        for entry in normalized_entries
        if entry["severity"] in {"ERROR", "WARNING"}
    ]

    cross_service_calls: list[dict[str, Any]] = []
    boundary_candidates: list[dict[str, Any]] = []
    for idx, call in enumerate(pending_calls, start=1):
        boundary_id = f"bnd-{idx:04d}"
        source_event_id = index_to_event_id.get(call["_raw_index"], "")
        cross_service_calls.append(
            {
                "boundary_id": boundary_id,
                "caller_service": call["caller_service"],
                "callee_service": call["callee_service"],
                "boundary_type": call["boundary_type"],
                "source_event_id": source_event_id,
                "evidence": call["evidence"],
            }
        )

        target_event_id = ""
        source_entry = next(
            (entry for entry in normalized_entries if entry["event_id"] == source_event_id), None
        )
        if source_entry is not None:
            for candidate in normalized_entries:
                if candidate["timestamp_ns"] < source_entry["timestamp_ns"]:
                    continue
                if candidate["source_service"] != call["callee_service"]:
                    continue
                if candidate["timestamp_ns"] - source_entry["timestamp_ns"] > 300_000_000_000:
                    break
                target_event_id = candidate["event_id"]
                break
        if target_event_id:
            boundary_candidates.append(
                {
                    "boundary_id": boundary_id,
                    "caller_service": call["caller_service"],
                    "callee_service": call["callee_service"],
                    "boundary_type": call["boundary_type"],
                    "source_event_id": source_event_id,
                    "target_event_id": target_event_id,
                    "evidence": call["evidence"],
                }
            )

    terminal_events = []
    for item in pending_terminals:
        entry = next(
            (candidate for candidate in normalized_entries if candidate["event_id"] == index_to_event_id.get(item["_raw_index"], "")),
            None,
        )
        if entry is None:
            continue
        terminal_events.append(
            {
                "event_id": entry["event_id"],
                "timestamp": entry["timestamp"],
                "source_service": entry["source_service"],
                "status": item["status"],
                "reason": entry["message"][:300],
            }
        )

    recovered_error_signatures: list[str] = []
    last_error_by_service = {key: "" for key in ("vcp", "cvs", "cvp", "cvn", "unknown")}
    for entry in normalized_entries:
        if entry["severity"] in {"ERROR", "WARNING"}:
            last_error_by_service[entry["source_service"]] = entry["event_id"]

    google_operation_hints = []
    for operation in pending_operations.values():
        google_operation_hints.append(
            {
                "operation_name": operation["operation_name"],
                "api_family_hint": operation["api_family_hint"],
                "project_hint": operation["project_hint"] or "",
                "project_hint_source": operation["project_hint_source"],
                "scope_hint": operation["scope_hint"],
                "region": operation["region"],
                "zone": operation["zone"],
                "target_link_hint": operation["target_link_hint"],
                "source_event_ids": [
                    index_to_event_id[index]
                    for index in operation["source_event_indices"]
                    if index in index_to_event_id
                ],
            }
        )

    project_context["source_event_ids"] = [
        index_to_event_id[index]
        for index in project_context["source_event_ids"]
        if index in index_to_event_id
    ]
    project_context["source_event_ids"] = list(dict.fromkeys(project_context["source_event_ids"]))

    fetch_origin_counts: dict[str, int] = {}
    for entry in normalized_entries:
        fetch_origin = str(entry.get("fetch_origin", "correlation") or "correlation")
        fetch_origin_counts[fetch_origin] = fetch_origin_counts.get(fetch_origin, 0) + 1

    derived_pivots = {
        "job_ids": derive_job_ids(normalized_entries),
        "resource_ids": derive_resource_hints(normalized_entries),
        "request_ids": unique_related_ids(normalized_entries, "request_id"),
        "tracking_ids": unique_related_ids(normalized_entries, "tracking_id"),
        "location_hints": derive_location_hints(normalized_entries),
        "google_operations": [
            item["operation_name"]
            for item in google_operation_hints
            if item.get("operation_name")
        ],
    }

    services = [
        service
        for service, details in service_breakdown.items()
        if details["entry_count"] > 0
    ]
    services.sort()
    for details in service_breakdown.values():
        details["containers"] = sorted(details["containers"])

    time_window = {
        "start": normalized_entries[0]["timestamp"] if normalized_entries else "",
        "end": normalized_entries[-1]["timestamp"] if normalized_entries else "",
    }

    bundle = {
        "bundle_version": BUNDLE_VERSION,
        "cross_repo": cross_repo,
        "log_file": log_file_label,
        "entry_count": len(normalized_entries),
        "time_window": time_window,
        "severity_counts": severity_counts,
        "fetch_summary": {
            "origins": sorted(fetch_origin_counts.keys()),
            "origin_counts": fetch_origin_counts,
            "resource_lookback": "",
            "expansion_attempts": [],
        },
        "derived_pivots": derived_pivots,
        "project_context": project_context,
        "services": services,
        "service_breakdown": service_breakdown,
        "normalized_entries": normalized_entries,
        "error_inventory": error_inventory,
        "cross_service_calls": cross_service_calls,
        "google_operation_hints": google_operation_hints,
        "boundary_candidates": boundary_candidates,
        "terminal_events": terminal_events,
        "recovered_error_signatures": recovered_error_signatures,
        "last_error_by_service": last_error_by_service,
    }

    return bundle


def relative_log_label(log_file: Path) -> str:
    return str(log_file.relative_to(repo_root())) if log_file.is_relative_to(repo_root()) else str(log_file)


def build_bundle(log_file: Path, bundle_file: Path, cross_repo: bool) -> dict[str, Any]:
    bundle = build_bundle_from_entries(
        load_entries(log_file),
        cross_repo=cross_repo,
        log_file_label=relative_log_label(log_file),
    )
    ensure_parent(bundle_file)
    bundle_file.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    return bundle


def fetch_and_bundle_expanded(
    project: str,
    correlation_id: str,
    log_file: Path,
    bundle_file: Path,
    *,
    cross_repo: bool,
    expand: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Three-pass progressive fetch.

    Pass 1: correlation_id (existing run_fetch with built-in retries).
    Pass 2: job_id values discovered in pass 1, scoped to widened trace window.
    Pass 3: resource ids discovered in passes 1+2; freshness ladder 7d->15d->30d,
            widening when fewer than RESOURCE_LADDER_MIN_HITS lines come back.
    """
    correlation_result = run_fetch(project, correlation_id, log_file)
    merged_entries = list(correlation_result.get("entries", []))
    fetch_steps = 1
    origins: set[str] = {"correlation"}
    resource_lookback = ""
    expansion_attempts: list[dict[str, Any]] = []

    if expand and merged_entries:
        bundle_after_pass1 = build_bundle_from_entries(
            merged_entries,
            cross_repo=cross_repo,
            log_file_label=relative_log_label(log_file),
        )
        job_step = build_job_step(bundle_after_pass1)
        if job_step is None:
            expansion_attempts.append(
                {"origin": "job", "status": "skipped", "reason": "no job ids in pass 1 bundle"}
            )
        else:
            fetch_steps += 1
            try:
                job_entries = run_single_step(project, job_step)
                expansion_attempts.append(
                    {
                        "origin": "job",
                        "status": "ok",
                        "fetched": len(job_entries),
                        "values_used": job_step.get("values", []),
                        "freshness": job_step["freshness"],
                    }
                )
                if job_entries:
                    merged_entries = merge_entries(merged_entries, job_entries)
                    origins.add("job")
            except RuntimeError as exc:
                expansion_attempts.append(
                    {
                        "origin": "job",
                        "status": "error",
                        "error": str(exc)[:500],
                        "values_used": job_step.get("values", []),
                        "freshness": job_step["freshness"],
                    }
                )

        bundle_after_pass2 = build_bundle_from_entries(
            merged_entries,
            cross_repo=cross_repo,
            log_file_label=relative_log_label(log_file),
        )
        resource_step = build_resource_step(bundle_after_pass2)
        if resource_step is None:
            expansion_attempts.append(
                {
                    "origin": "resource",
                    "status": "skipped",
                    "reason": "no usable resource ids after pass 2",
                }
            )
        else:
            fetch_steps += 1
            try:
                resource_entries, resource_lookback = run_resource_ladder(project, resource_step)
                expansion_attempts.append(
                    {
                        "origin": "resource",
                        "status": "ok" if resource_lookback else "error",
                        "fetched": len(resource_entries),
                        "values_used": resource_step.get("values", []),
                        "freshness_used": resource_lookback,
                        "freshness_ladder": resource_step["freshness_ladder"],
                        **(
                            {}
                            if resource_lookback
                            else {"error": "all freshness levels failed"}
                        ),
                    }
                )
                if resource_entries:
                    merged_entries = merge_entries(merged_entries, resource_entries)
                    origins.add("resource")
            except RuntimeError as exc:
                expansion_attempts.append(
                    {
                        "origin": "resource",
                        "status": "error",
                        "error": str(exc)[:500],
                        "values_used": resource_step.get("values", []),
                        "freshness_ladder": resource_step["freshness_ladder"],
                    }
                )

    write_raw_entries(log_file, merged_entries)
    bundle = build_bundle(log_file, bundle_file, cross_repo)
    fetch_summary = bundle.setdefault("fetch_summary", {})
    if resource_lookback:
        fetch_summary["resource_lookback"] = resource_lookback
    fetch_summary["expansion_attempts"] = expansion_attempts
    ensure_parent(bundle_file)
    bundle_file.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    failed_expansions = [a for a in expansion_attempts if a.get("status") == "error"]
    fetch_result = {
        "attempt": correlation_result["attempt"],
        "freshness": correlation_result["freshness"],
        "limit": correlation_result["limit"],
        "log_file": str(log_file),
        "size_bytes": log_file.stat().st_size,
        "entries": merged_entries,
        "origins": sorted(origins),
        "fetches": fetch_steps,
        "resource_lookback": resource_lookback,
        "expansion_attempts": expansion_attempts,
        "failed_expansions": [a["origin"] for a in failed_expansions],
    }
    return fetch_result, bundle


def print_fetch_summary(result: dict[str, Any]) -> None:
    origins = ",".join(result.get("origins", []))
    failed = ",".join(result.get("failed_expansions", []))
    print(
        "fetch_status=success "
        f"log_file={result['log_file']} "
        f"attempt={result['attempt']} "
        f"freshness={result['freshness']} "
        f"limit={result['limit']} "
        f"size_bytes={result['size_bytes']} "
        f"fetches={result.get('fetches', 1)} "
        f"origins={origins or 'correlation'} "
        f"resource_lookback={result.get('resource_lookback') or 'none'} "
        f"failed_expansions={failed or 'none'}"
    )


def print_bundle_summary(bundle: dict[str, Any], bundle_file: Path) -> None:
    window = f"{bundle['time_window']['start']}..{bundle['time_window']['end']}"
    services = ",".join(bundle["services"])
    fetch_summary = bundle.get("fetch_summary", {}) or {}
    origins = ",".join(fetch_summary.get("origins", []))
    resource_lookback = fetch_summary.get("resource_lookback") or "none"
    failed = ",".join(
        attempt["origin"]
        for attempt in fetch_summary.get("expansion_attempts", [])
        if attempt.get("status") == "error"
    )
    print(
        "fetch_status=success "
        f"cross_repo={'true' if bundle['cross_repo'] else 'false'} "
        f"entries={bundle['entry_count']} "
        f"window={window} "
        f"services={services or 'none'} "
        f"origins={origins or 'correlation'} "
        f"resource_lookback={resource_lookback} "
        f"failed_expansions={failed or 'none'} "
        f"bundle_file={bundle_file}"
    )


def main() -> int:
    args = parse_args()

    try:
        if args.command == "fetch":
            log_file = Path(args.log_file) if args.log_file else default_log_file(args.correlation_id)
            result = run_fetch(args.project, args.correlation_id, log_file)
            print_fetch_summary(result)
            return 0

        if args.command == "bundle":
            log_file = Path(args.log_file)
            bundle_file = Path(args.bundle_file) if args.bundle_file else default_bundle_file_from_log(log_file)
            bundle = build_bundle(log_file, bundle_file, args.cross_repo)
            print_bundle_summary(bundle, bundle_file)
            return 0

        if args.command == "fetch-and-bundle":
            log_file = Path(args.log_file) if args.log_file else default_log_file(args.correlation_id)
            bundle_file = (
                Path(args.bundle_file)
                if args.bundle_file
                else default_bundle_file_from_log(log_file)
            )
            fetch_result, bundle = fetch_and_bundle_expanded(
                args.project,
                args.correlation_id,
                log_file,
                bundle_file,
                cross_repo=args.cross_repo,
                expand=not args.no_expand,
            )
            print_fetch_summary(fetch_result)
            print_bundle_summary(bundle, bundle_file)
            return 0

    except Exception as exc:  # pylint: disable=broad-except
        print(str(exc), file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
