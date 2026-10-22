"""Validate shard metadata and select read replicas deterministically."""
from __future__ import annotations
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Iterable

class QueryValidationError(ValueError):
    pass

class ShardRegistryError(ValueError):
    pass

def validate_identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise QueryValidationError("Invalid identifier")
    return value

_USED_FOR_RANK = {"GR-Node3": 0, "GR-Node2": 1}
_OTHER_RANK = 4
_PAYROLL_MYSQLDBS = frozenset({"payroll", "payroll_master"})
_PAYROLL_NAME_PREFIX = "bigdata"

def _require_str(entry: dict, field: str, where: str, *, allow_empty: bool = False) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise ShardRegistryError(f"{where}: '{field}' must be a non-empty string")
    return value


def _validate_entry(index: int, raw: Any) -> dict[str, Any]:
    """Validate one ``mysql_shards`` entry and project the routing fields.

    Any structural problem poisons the whole snapshot (fail closed at load
    time): a silently-skipped entry could strand a tenant's range unnoticed.
    ``clientstart``/``clientend`` may be null (row retained but never matched);
    a non-empty clientstart REQUIRES a non-empty clientend so the inclusive
    range compare can never raise or half-match. Empty strings normalise to
    None for consistent range validation.
    """
    where = f"mysql_shards[{index}]"
    if not isinstance(raw, dict):
        raise ShardRegistryError(f"{where}: entry must be an object")
    catalog = _require_str(raw, "catalog", where)
    host = _require_str(raw, "host", where)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host):
        raise ShardRegistryError(f"{where}: invalid host")
    database = _require_str(raw, "database", where)
    env_type = _require_str(raw, "env_type", where)
    try:
        validate_identifier(catalog)
        validate_identifier(database)
        validate_identifier(env_type.lower())
    except QueryValidationError as exc:
        raise ShardRegistryError(f"{where}: invalid routing identifier") from exc
    if env_type != env_type.upper():
        raise ShardRegistryError(f"{where}: 'env_type' must be uppercase")
    port = raw.get("port")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ShardRegistryError(f"{where}: 'port' must be an integer in 1..65535")
    clientstart = raw.get("clientstart")
    clientend = raw.get("clientend")
    for field, value in (("clientstart", clientstart), ("clientend", clientend)):
        if value is not None and not isinstance(value, str):
            raise ShardRegistryError(f"{where}: '{field}' must be a string or null")
    if not clientstart:
        clientstart = None
    if not clientend:
        clientend = None
    if clientstart is not None and clientend is None:
        raise ShardRegistryError(
            f"{where}: 'clientend' is required when 'clientstart' is set"
        )
    if clientstart is not None and clientstart > clientend:
        raise ShardRegistryError(f"{where}: reversed client range")
    for value in (clientstart, clientend):
        if value is not None and (not value.isprintable() or value != value.strip()):
            raise ShardRegistryError(f"{where}: invalid client range bound")
    return {
        "catalog": catalog,
        "host": host,
        "port": port,
        "database": database,
        "env_type": env_type,
        "clientstart": clientstart,
        "clientend": clientend,
    }


def validate_shard_registry(payload: Any) -> list[dict[str, Any]]:
    """Validate the shared refresh/runtime contract before a snapshot is used."""
    if not isinstance(payload, dict) or not isinstance(payload.get("mysql_shards"), list):
        raise ShardRegistryError("shard registry must contain a 'mysql_shards' list")
    entries = [_validate_entry(i, raw) for i, raw in enumerate(payload["mysql_shards"])]
    names = [entry["catalog"] for entry in entries]
    if len(names) != len(set(names)):
        raise ShardRegistryError("shard registry contains duplicate catalog names")
    return entries



def _clean(value: Any) -> str | None:
    """Normalise registry strings: strip, and map empty to None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_payroll_row(row: dict[str, Any]) -> bool:
    """True for normalised rows of the partitioned payroll store (out of scope)."""
    return (row.get("database") in _PAYROLL_MYSQLDBS) or (
        (row.get("name") or "").lower().startswith(_PAYROLL_NAME_PREFIX)
    )


def _normalise_env_types(env_types: Iterable[str] | None) -> list[str] | None:
    """Dedupe operator --env-type filters (case-insensitive); None = keep all envs.

    Matching is case-insensitive but the recorded/returned values keep the
    operator's original (stripped) casing for audit metadata. An empty result
    (no values, or only blanks) normalises to None, i.e. no env filter.
    """
    if env_types is None:
        return None
    seen: set[str] = set()
    out: list[str] = []
    for env in env_types:
        text = str(env).strip()
        key = text.casefold()
        if key and key not in seen:
            seen.add(key)
            out.append(text)
    return out or None


def _normalise_client_codes(clients: Iterable[str] | None) -> list[str] | None:
    """Validate/dedupe operator --client codes; None = keep all shard groups.

    A code must survive the registry range format the rest of the pipeline
    treats as valid: a non-empty printable string (the same shape as the
    clientstart/clientend values it is range-matched against). Anything else is
    a hard ValueError so main() renders a clean `error:` exit instead of a
    silently empty snapshot. Dedupe preserves first-seen order. An empty input
    normalises to None (no client filter).
    """
    if clients is None:
        return None
    seen: set[str] = set()
    out: list[str] = []
    for code in clients:
        text = code.strip() if isinstance(code, str) else ""
        if not text or not text.isprintable():
            raise ValueError(
                f"invalid --client code {code!r}: must be a non-empty printable "
                "string matching the registry clientstart/clientend format"
            )
        if text not in seen:
            seen.add(text)
            out.append(text)
    return out or None


def _normalise_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Project a raw registry row to snapshot fields; None if unusable.

    A row is unusable when it carries no resolvable host (mysqlhost, falling
    back to mysqlip), no env_type (null/blank -- grouping and catalog naming
    key off it, so an empty env would only surface later as a crash or an
    unmatchable entry), or its mysqlport cannot be coerced to an int -- such a
    row could never back a Trino catalog, so it is skipped (and counted)
    rather than poisoning the snapshot.
    """
    if not isinstance(raw, dict):
        raise ValueError("registry rows must be objects")
    host = _clean(raw.get("mysqlhost")) or _clean(raw.get("mysqlip"))
    if host is None:
        return None
    env_type = _clean(raw.get("env_type"))
    if env_type is None:
        return None
    try:
        port = int(str(raw.get("mysqlport")).strip())
    except (TypeError, ValueError):
        return None
    if not 1 <= port <= 65535:
        return None
    return {
        "env_type": env_type.upper(),
        "database": _clean(raw.get("mysqldb")),
        "name": _clean(raw.get("name")),
        "host": host,
        "port": port,
        "clientstart": _clean(raw.get("clientstart")),
        "clientend": _clean(raw.get("clientend")),
        "used_for": _clean(raw.get("used_for")),
    }


def _node_rank(row: dict[str, Any]) -> tuple[int, str, int]:
    """Read-preference sort key: rank, then host, then port (deterministic)."""
    return (_USED_FOR_RANK.get(row.get("used_for") or "", _OTHER_RANK), row["host"], row["port"])


def _pick_read_node(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Choose Node3, then Node2, in every environment; never use other roles."""
    candidates = [row for row in rows if row.get("used_for") in _USED_FOR_RANK]
    if not candidates:
        return None
    return min(candidates, key=_node_rank)


def _group_key(row: dict[str, Any]) -> tuple[str, str, str | None, str | None]:
    return (row["env_type"], row["database"], row["clientstart"], row["clientend"])


def select_shard_entries(
    rows: list[dict[str, Any]],
    mysqldbs: Iterable[str],
    name: str,
    *,
    env_types: Iterable[str] | None = None,
    clients: Iterable[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Filter, dedupe and select read nodes from normalised registry rows.

    Pure: returns (selected entries WITHOUT catalog names, report). The report
    carries counts and the ineligible-shard labels for the caller to print.

    Filter order is deliberate: payroll exclusion and the mysqldb/name row
    filter run first, then the optional --env-type row filter (case-insensitive)
    within those already-selected rows, then the optional --client group filter
    keeps only shard groups whose clientstart <= code <= clientend range
    contains at least one given code (inclusive lexicographic, the registry
    routing semantics). Both run BEFORE replica
    dedupe and catalog naming so a narrowed run still produces deterministic
    range-derived catalog names independent of the selected subset.
    """
    wanted_dbs = set(mysqldbs)
    if name != "online" or wanted_dbs != {"application"}:
        raise ValueError("Only online catalogs in the application schema are supported")
    env_filter = _normalise_env_types(env_types)
    client_filter = _normalise_client_codes(clients)
    wanted_envs = {env.casefold() for env in env_filter} if env_filter else None
    report: dict[str, Any] = {
        "input_rows": len(rows),
        "payroll_excluded": 0,
        "unusable_rows": 0,
        "env_type_excluded_rows": 0,
        "client_excluded_groups": 0,
        "filtered_rows": 0,
        "shard_groups": 0,
        "shards_without_eligible_replica": [],
        "per_env_db": {},
    }
    usable: list[dict[str, Any]] = []
    for raw in rows:
        row = _normalise_row(raw)
        if row is None:
            report["unusable_rows"] += 1
            continue
        usable.append(row)
    groups: dict[tuple, list[dict[str, Any]]] = {}
    for row in usable:
        if _is_payroll_row(row):
            report["payroll_excluded"] += 1
            continue
        if row["database"] not in wanted_dbs or row["name"] != name:
            continue
        if wanted_envs is not None and row["env_type"].casefold() not in wanted_envs:
            report["env_type_excluded_rows"] += 1
            continue
        report["filtered_rows"] += 1
        groups.setdefault(_group_key(row), []).append(row)
    if client_filter:
        kept: dict[tuple, list[dict[str, Any]]] = {}
        for key, members in groups.items():
            _, _, clientstart, clientend = key
            # A group with an unbounded/half-bounded range can never positively
            # contain a code (and half-bounded rows are rejected at runtime
            # load), so it never survives a client filter.
            if (
                clientstart is not None
                and clientend is not None
                and any(clientstart <= code <= clientend for code in client_filter)
            ):
                kept[key] = members
        report["client_excluded_groups"] = len(groups) - len(kept)
        groups = kept
    selected: list[dict[str, Any]] = []
    for key, members in groups.items():
        chosen = _pick_read_node(members)
        if chosen is None:
            env_type, database, clientstart, clientend = key
            report["shards_without_eligible_replica"].append(f"{env_type}/{database} {clientstart}-{clientend}")
            continue
        selected.append(
            {
                "host": chosen["host"],
                "port": chosen["port"],
                "database": chosen["database"],
                "env_type": chosen["env_type"],
                "clientstart": chosen["clientstart"],
                "clientend": chosen["clientend"],
            }
        )
        env_db = (chosen["env_type"], chosen["database"])
        report["per_env_db"][env_db] = report["per_env_db"].get(env_db, 0) + 1
    report["shard_groups"] = len(selected)
    return selected, report


def assign_catalog_names(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Name each range independently of filtering, ordering and replica choice.

    A versioned hash of (environment, database, start, end) identifies a range.
    Host/port are deliberately excluded so replica failover keeps the name.
    Range changes create new names, preventing stale catalogs from silently
    pointing newly assigned ranges at a different source.
    """
    named = []
    for entry in entries:
        env = entry["env_type"].upper()
        identity = ["v1", env, entry["database"], entry["clientstart"], entry["clientend"]]
        digest = hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()[:16]
        catalog = f"mysql_shard_{env.lower()}_{digest}"
        try:
            validate_identifier(catalog)
        except QueryValidationError as exc:
            raise ValueError(f"registry env_type {env!r} produces an invalid catalog name") from exc
        named.append({**entry, "env_type": env, "catalog": catalog})
    return sorted(named, key=lambda e: (e["env_type"], e["database"], e["clientstart"] or "", e["clientend"] or ""))


def build_snapshot(
    rows: list[dict[str, Any]],
    *,
    mysqldbs: Iterable[str],
    name: str,
    source: str,
    env_types: Iterable[str] | None = None,
    clients: Iterable[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Full pure pipeline: rows -> snapshot dict (+ report), ready to serialise.

    The resolved filter selection is recorded under "filters" for auditability
    (which mysqldb/name and any env_type/client narrowing produced this
    snapshot); consumers read only "mysql_shards"/"clickhouse", so the block is
    metadata only and never changes routing behavior.
    """
    mysqldbs = list(mysqldbs)
    selected, report = select_shard_entries(rows, mysqldbs, name, env_types=env_types, clients=clients)
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source,
        "filters": {
            "mysqldb": list(dict.fromkeys(mysqldbs)),
            "name": name,
            "env_type": _normalise_env_types(env_types),
            "client": _normalise_client_codes(clients),
        },
        "mysql_shards": assign_catalog_names(selected),
    }
    try:
        validate_shard_registry(snapshot)
    except ShardRegistryError as exc:
        raise ValueError(f"invalid shard snapshot: {exc}") from exc
    return snapshot, report
