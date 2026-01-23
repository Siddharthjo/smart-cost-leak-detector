from src.intelligence.leak_detection.rule_based import get_service_category
from typing import List, Dict, Set

# ---------------- SERVICE KEYWORDS ----------------

COMPUTE_KEYWORDS = {
    "aws": ["ec2"],
    "azure": ["virtual machines"],
    "gcp": ["compute engine"],
}

STORAGE_KEYWORDS = {
    "aws": ["ebs", "s3"],
    "azure": ["disk", "storage"],
    "gcp": ["persistent disk", "cloud storage"],
}

SNAPSHOT_KEYWORDS = {
    "aws": ["snapshot"],
    "azure": ["snapshot", "backup"],
    "gcp": ["snapshot"],
}

# ---------------- HELPERS ----------------

def _normalize_provider(provider: str) -> str:
    return provider.lower() if provider else ""


def is_compute(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in COMPUTE_KEYWORDS.get(_normalize_provider(provider), [])
    )


def is_storage(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in STORAGE_KEYWORDS.get(_normalize_provider(provider), [])
    )


def is_snapshot(service: str, provider: str) -> bool:
    if not service or not provider:
        return False
    return any(
        k in service.lower()
        for k in SNAPSHOT_KEYWORDS.get(_normalize_provider(provider), [])
    )

# ---------------- ORPHANED STORAGE ----------------

def detect_orphaned_storage(normalized_df) -> List[Dict]:
    leaks: List[Dict] = []

    compute_seen: Set[str] = set()
    storage_seen: Dict[str, Dict] = {}

    for _, row in normalized_df.iterrows():
        provider = row.get("provider")
        service = row.get("service")
        resource_id = row.get("resource_id")

        if not resource_id:
            continue

        if is_compute(service, provider):
            compute_seen.add(resource_id)

        elif is_storage(service, provider):
            storage_seen.setdefault(
                resource_id,
                {
                    "provider": provider,
                    "service": service,
                    "resource_id": resource_id,
                }
            )

    for rid, info in storage_seen.items():
        if rid not in compute_seen:
            leaks.append({
                "leak_type": "ORPHANED_STORAGE",
                "provider": info["provider"],
                "service": info["service"],
                "resource_id": rid,
                "reason": "Storage resource generating cost with no attached compute",
            })

    return leaks

# ---------------- IDLE DATABASE ----------------

IDLE_DB_MIN_DAYS = 7
IDLE_DB_USAGE_RATIO_THRESHOLD = 0.2
IDLE_DB_MIN_DAILY_COST = 10.0


def detect_idle_databases(
    lifespan_data,
    usage_ratio_data,
    daily_cost_df,
    normalized_df
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
        provider = r["provider"]
        service = r["service"]
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
            "leak_type": "IDLE_DATABASE",
            "provider": provider,
            "service": service,
            "resource_id": resource_id,
            "reason": (
                f"Database active {days_active} days with minimal usage "
                f"(usage-to-cost ratio {usage_ratio:.2f})"
            ),
            "estimated_monthly_waste": round(daily_cost * 30, 2)
        })

    return leaks

# ---------------- SNAPSHOT / BACKUP SPRAWL ----------------

SNAPSHOT_MIN_DAYS = 7
SNAPSHOT_MIN_DAILY_COST = 0.5


def detect_snapshot_sprawl(normalized_df):
    """
    Detect orphaned snapshots / backups:
    Snapshots generating cost with no active parent resource.
    """

    leaks = []

    active_resources = set()
    snapshot_resources = {}

    for _, row in normalized_df.iterrows():
        provider = row.get("provider")
        service = row.get("service")
        resource_id = row.get("resource_id")

        if not resource_id:
            continue

        category = get_service_category(service)
        if category in {"compute", "database"}:
            active_resources.add(resource_id)

        if is_snapshot(service, provider):
            snapshot_resources.setdefault(
                resource_id,
                {
                    "provider": provider,
                    "service": service,
                    "resource_id": resource_id,
                }
            )

    for rid, info in snapshot_resources.items():
        if rid not in active_resources:
            leaks.append({
                "leak_type": "SNAPSHOT_SPRAWL",
                "provider": info["provider"],
                "service": info["service"],
                "resource_id": rid,
                "reason": "Snapshot or backup generating cost with no active parent resource",
            })

    return leaks

# ---------------- UNTAGGED RESOURCES ----------------

def detect_untagged_resources(normalized_df) -> List[Dict]:
    """
    Detect resources with no ownership metadata.
    Emits ONE leak per resource.
    """

    leaks: List[Dict] = []
    seen: Set[str] = set()

    for _, row in normalized_df.iterrows():
        resource_id = row.get("resource_id")
        if not resource_id or resource_id in seen:
            continue

        seen.add(resource_id)

        provider = row.get("provider")
        service = row.get("service")

        ownership_found = False
        for col in row.index:
            if any(k in col.lower() for k in ["owner", "project", "environment"]):
                val = row.get(col)
                if val and str(val).lower() not in {"", "unknown", "none", "nan"}:
                    ownership_found = True
                    break

        if not ownership_found:
            leaks.append({
                "leak_type": "UNTAGGED_RESOURCE",
                "provider": provider,
                "service": service,
                "resource_id": resource_id,
                "reason": "Resource has no ownership tags (owner / project / environment missing)",
            })

    return leaks