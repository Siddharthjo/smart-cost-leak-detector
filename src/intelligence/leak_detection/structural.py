import pandas as pd
import logging
from typing import List, Dict, Set, Optional

from src.intelligence.leak_detection.rule_based import get_service_category

logger = logging.getLogger(__name__)

# ===================== SERVICE KEYWORDS =====================

COMPUTE_KEYWORDS = {
    "aws":   ["ec2"],
    "azure": ["virtual machines"],
    "gcp":   ["compute engine"],
}

STORAGE_KEYWORDS = {
    "aws":   ["ebs", "s3"],
    "azure": ["disk", "storage"],
    "gcp":   ["persistent disk", "cloud storage"],
}

SNAPSHOT_KEYWORDS = {
    "aws":   ["snapshot"],
    "azure": ["snapshot", "backup"],
    "gcp":   ["snapshot"],
}


# ===================== HELPERS =====================

def _norm_provider(provider: str) -> str:
    return provider.lower() if provider else ""


def is_compute(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in COMPUTE_KEYWORDS.get(_norm_provider(provider), [])
    )


def is_storage(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in STORAGE_KEYWORDS.get(_norm_provider(provider), [])
    )


def is_snapshot(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in SNAPSHOT_KEYWORDS.get(_norm_provider(provider), [])
    )


_BLOCK_STORAGE_KEYWORDS = {
    "aws":   ["ebs", "volume"],
    "azure": ["disk"],
    "gcp":   ["persistent disk"],
}


def is_block_storage(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in _BLOCK_STORAGE_KEYWORDS.get(_norm_provider(provider), [])
    )


# ===================== ORPHANED STORAGE =====================

def detect_orphaned_storage(normalized_df: pd.DataFrame) -> List[Dict]:
    """
    Detects storage resources billing in date+region windows
    where NO compute was active for the same provider.

    FIX from v1: the original logic compared resource_id values directly
    (EBS vol-xxx vs EC2 i-xxx — never the same string), producing near-zero
    true positives. The corrected approach uses (provider, date, region)
    co-occurrence: if a storage resource was active in a date+region slot
    where zero compute ran, it is orphaned.
    """
    leaks: List[Dict] = []

    # Slots where compute was running: {(provider, str(date), region)}
    compute_slots: Set[tuple] = set()

    # Storage resources and their active slots
    storage_resources: Dict[str, Dict] = {}

    for _, row in normalized_df.iterrows():
        provider    = row.get("provider")
        service     = row.get("service")
        resource_id = row.get("resource_id")
        date        = row.get("date")
        region      = row.get("region", "unknown") or "unknown"

        if not resource_id:
            continue

        slot = (provider, str(date), region)

        if is_compute(service, provider):
            compute_slots.add(slot)

        elif is_block_storage(service, provider):
            if resource_id not in storage_resources:
                storage_resources[resource_id] = {
                    "provider":    provider,
                    "service":     service,
                    "resource_id": resource_id,
                    "slots":       set(),
                }
            storage_resources[resource_id]["slots"].add(slot)

    for rid, info in storage_resources.items():
        # Orphaned = none of its active slots had any compute in same provider+region
        overlap = info["slots"] & compute_slots
        if not overlap:
            leaks.append({
                "leak_type":   "ORPHANED_STORAGE",
                "provider":    info["provider"],
                "service":     info["service"],
                "resource_id": rid,
                "reason": (
                    "Storage resource active in regions/dates where no "
                    "compute was running — likely detached or abandoned"
                ),
            })

    return leaks


# ===================== IDLE DATABASE =====================

IDLE_DB_MIN_DAYS           = 7
IDLE_DB_USAGE_RATIO_THRESHOLD = 0.2
IDLE_DB_MIN_DAILY_COST     = 10.0


def detect_idle_databases(
    lifespan_data: list,
    usage_ratio_data: list,
    daily_cost_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
) -> List[Dict]:
    leaks: List[Dict] = []

    usage_lookup = {
        (u["provider"], u["service"], u["resource_id"]): u["usage_to_cost_ratio"]
        for u in usage_ratio_data
    }

    avg_daily_cost = (
        normalized_df
        .groupby(["provider", "service", "resource_id"])["cost"]
        .mean()
        .to_dict()
    )

    for r in lifespan_data:
        provider    = r["provider"]
        service     = r["service"]
        resource_id = r["resource_id"]
        days_active = r["days_active"]

        if get_service_category(service) != "database":
            continue
        if days_active < IDLE_DB_MIN_DAYS:
            continue

        usage_ratio = usage_lookup.get((provider, service, resource_id))
        if usage_ratio is None or usage_ratio > IDLE_DB_USAGE_RATIO_THRESHOLD:
            continue

        daily_cost = avg_daily_cost.get((provider, service, resource_id), 0)
        if daily_cost < IDLE_DB_MIN_DAILY_COST:
            continue

        leaks.append({
            "leak_type":   "IDLE_DATABASE",
            "provider":    provider,
            "service":     service,
            "resource_id": resource_id,
            "reason": (
                f"Database active {days_active} days with minimal usage "
                f"(ratio {usage_ratio:.4f})"
            ),
            "estimated_monthly_waste": round(daily_cost * 30, 2),
        })

    return leaks


# ===================== SNAPSHOT / BACKUP SPRAWL =====================

def detect_snapshot_sprawl(normalized_df: pd.DataFrame) -> List[Dict]:
    """
    Snapshots generating cost with no active parent resource.
    """
    leaks: List[Dict] = []

    active_resources: Set[str] = set()
    snapshot_resources: Dict[str, Dict] = {}

    for _, row in normalized_df.iterrows():
        provider    = row.get("provider")
        service     = row.get("service")
        resource_id = row.get("resource_id")

        if not resource_id:
            continue

        category = get_service_category(service)
        if category in {"compute", "database"}:
            active_resources.add(resource_id)

        if is_snapshot(service, provider):
            snapshot_resources.setdefault(resource_id, {
                "provider":    provider,
                "service":     service,
                "resource_id": resource_id,
            })

    for rid, info in snapshot_resources.items():
        if rid not in active_resources:
            leaks.append({
                "leak_type":   "SNAPSHOT_SPRAWL",
                "provider":    info["provider"],
                "service":     info["service"],
                "resource_id": rid,
                "reason": (
                    "Snapshot or backup generating cost with no "
                    "active parent resource"
                ),
            })

    return leaks


# ===================== UNTAGGED RESOURCES =====================

def detect_untagged_resources(
    normalized_df: pd.DataFrame,
    daily_cost_df: Optional[pd.DataFrame] = None,
    top_n: int = 20,
) -> List[Dict]:
    """
    Detect resources with no ownership metadata.

    FIX from v1: previously emitted one leak per resource with no cap,
    producing thousands of LOW-signal findings for large datasets.
    Now capped to `top_n` by total cost — focuses on the untagged
    resources that actually matter financially.
    """
    candidates: List[Dict] = []
    seen: Set[str] = set()

    # Build cost lookup per resource_id
    resource_cost: Dict[str, float] = {}
    if "resource_id" in normalized_df.columns and "cost" in normalized_df.columns:
        rc = (
            normalized_df.dropna(subset=["resource_id"])
            .groupby("resource_id")["cost"]
            .sum()
        )
        resource_cost = rc.to_dict()

    # Build cost lookup per (provider, service) — fallback for blank resource IDs
    service_cost: Dict[tuple, float] = {}
    if "service" in normalized_df.columns and "cost" in normalized_df.columns:
        sc = (
            normalized_df.dropna(subset=["service"])
            .groupby(["provider", "service"])["cost"]
            .sum()
        )
        service_cost = sc.to_dict()

    for _, row in normalized_df.iterrows():
        resource_id = row.get("resource_id")
        if not resource_id or resource_id in seen:
            continue
        seen.add(resource_id)

        provider = row.get("provider")
        service  = row.get("service")

        resource_total = resource_cost.get(resource_id, 0.0)
        service_total  = service_cost.get((provider, service), 0.0)
        if resource_total < 0.01 and service_total < 0.01:
            continue

        ownership_found = False
        for col in row.index:
            if any(k in col.lower() for k in ["owner", "project", "environment"]):
                val = row.get(col)
                if val and str(val).lower() not in {"", "unknown", "none", "nan"}:
                    ownership_found = True
                    break

        if not ownership_found:
            candidates.append({
                "leak_type":   "UNTAGGED_RESOURCE",
                "provider":    provider,
                "service":     service,
                "resource_id": resource_id,
                "reason": (
                    "Resource has no ownership tags "
                    "(owner / project / environment missing)"
                ),
                "_cost": resource_cost.get(resource_id, 0.0),
            })

    # Cap to top_n by cost — high-cost untagged resources are the priority
    candidates.sort(key=lambda x: -x["_cost"])
    top = candidates[:top_n]

    if len(candidates) > top_n:
        logger.info(
            f"Untagged resources: {len(candidates)} found, "
            f"capped to top {top_n} by cost"
        )

    for c in top:
        c.pop("_cost", None)

    return top
