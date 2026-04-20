"""Tests for src/intelligence/leak_detection/structural.py"""

from datetime import date, timedelta

import pandas as pd
import pytest

from src.intelligence.leak_detection.structural import (
    detect_idle_databases,
    detect_orphaned_storage,
    detect_snapshot_sprawl,
    detect_untagged_resources,
)


# ===================== detect_orphaned_storage =====================

class TestDetectOrphanedStorage:
    def _make_df(self, rows):
        return pd.DataFrame(rows)

    def test_detects_storage_with_no_compute(self):
        rows = [
            {"date": date(2024, 3, 1), "provider": "AWS", "service": "ebs",
             "resource_id": "vol-001", "region": "us-east-1"},
        ]
        leaks = detect_orphaned_storage(self._make_df(rows))
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "ORPHANED_STORAGE"
        assert leaks[0]["resource_id"] == "vol-001"

    def test_no_leak_when_compute_present_same_region_and_date(self):
        rows = [
            {"date": date(2024, 3, 1), "provider": "AWS", "service": "ec2",
             "resource_id": "i-001", "region": "us-east-1"},
            {"date": date(2024, 3, 1), "provider": "AWS", "service": "ebs",
             "resource_id": "vol-001", "region": "us-east-1"},
        ]
        leaks = detect_orphaned_storage(self._make_df(rows))
        assert leaks == []

    def test_leak_when_compute_in_different_region(self):
        rows = [
            {"date": date(2024, 3, 1), "provider": "AWS", "service": "ec2",
             "resource_id": "i-001", "region": "us-west-2"},
            {"date": date(2024, 3, 1), "provider": "AWS", "service": "ebs",
             "resource_id": "vol-001", "region": "us-east-1"},
        ]
        leaks = detect_orphaned_storage(self._make_df(rows))
        assert len(leaks) == 1

    def test_skips_rows_without_resource_id(self):
        rows = [
            {"date": date(2024, 3, 1), "provider": "AWS", "service": "ebs",
             "resource_id": None, "region": "us-east-1"},
        ]
        leaks = detect_orphaned_storage(self._make_df(rows))
        assert leaks == []

    def test_empty_df(self):
        df = pd.DataFrame(columns=["date", "provider", "service", "resource_id", "region"])
        assert detect_orphaned_storage(df) == []


# ===================== detect_idle_databases =====================

class TestDetectIdleDatabases:
    def _make_norm_df(self, provider="AWS", service="rds", resource_id="db-001",
                      cost_per_day=30.0, n_days=10):
        end = date(2024, 3, 31)
        rows = [{"date": end - timedelta(days=i), "provider": provider,
                 "service": service, "resource_id": resource_id,
                 "cost": cost_per_day, "usage": 0.01}
                for i in range(n_days)]
        return pd.DataFrame(rows)

    def _make_daily_df(self, provider="AWS", service="rds", daily_cost=30.0, n_days=10):
        end = date(2024, 3, 31)
        rows = [{"date": end - timedelta(days=i), "provider": provider,
                 "service": service, "daily_cost": daily_cost}
                for i in range(n_days)]
        return pd.DataFrame(rows)

    def test_detects_idle_database(self):
        norm_df  = self._make_norm_df()
        daily_df = self._make_daily_df()
        lifespan = [{"provider": "AWS", "service": "rds",
                     "resource_id": "db-001", "days_active": 10}]
        usage    = [{"provider": "AWS", "service": "rds",
                     "resource_id": "db-001", "usage_to_cost_ratio": 0.01}]

        leaks = detect_idle_databases(lifespan, usage, daily_df, norm_df)
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "IDLE_DATABASE"
        assert leaks[0]["estimated_monthly_waste"] == pytest.approx(30.0 * 30, abs=1)

    def test_no_leak_for_active_db(self):
        norm_df  = self._make_norm_df()
        daily_df = self._make_daily_df()
        lifespan = [{"provider": "AWS", "service": "rds",
                     "resource_id": "db-001", "days_active": 10}]
        usage    = [{"provider": "AWS", "service": "rds",
                     "resource_id": "db-001", "usage_to_cost_ratio": 5.0}]  # above threshold

        leaks = detect_idle_databases(lifespan, usage, daily_df, norm_df)
        assert leaks == []

    def test_no_leak_for_non_database_service(self):
        norm_df  = self._make_norm_df(service="ec2")
        daily_df = self._make_daily_df(service="ec2")
        lifespan = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-001", "days_active": 10}]
        usage    = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-001", "usage_to_cost_ratio": 0.01}]

        leaks = detect_idle_databases(lifespan, usage, daily_df, norm_df)
        assert leaks == []

    def test_no_leak_for_new_db(self):
        norm_df  = self._make_norm_df(n_days=3)
        daily_df = self._make_daily_df(n_days=3)
        lifespan = [{"provider": "AWS", "service": "rds",
                     "resource_id": "db-001", "days_active": 3}]  # below 7-day minimum
        usage    = [{"provider": "AWS", "service": "rds",
                     "resource_id": "db-001", "usage_to_cost_ratio": 0.01}]

        leaks = detect_idle_databases(lifespan, usage, daily_df, norm_df)
        assert leaks == []


# ===================== detect_snapshot_sprawl =====================

class TestDetectSnapshotSprawl:
    def test_detects_snapshot_with_no_parent(self):
        rows = [
            {"provider": "AWS", "service": "snapshot", "resource_id": "snap-001"},
        ]
        leaks = detect_snapshot_sprawl(pd.DataFrame(rows))
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "SNAPSHOT_SPRAWL"

    def test_no_leak_when_parent_resource_exists(self):
        # snapshot resource_id matches an active compute/database resource_id
        rows = [
            {"provider": "AWS", "service": "ec2",      "resource_id": "snap-001"},
            {"provider": "AWS", "service": "snapshot",  "resource_id": "snap-001"},
        ]
        leaks = detect_snapshot_sprawl(pd.DataFrame(rows))
        assert leaks == []

    def test_skips_rows_without_resource_id(self):
        rows = [
            {"provider": "AWS", "service": "snapshot", "resource_id": None},
        ]
        leaks = detect_snapshot_sprawl(pd.DataFrame(rows))
        assert leaks == []

    def test_empty_df(self):
        df = pd.DataFrame(columns=["provider", "service", "resource_id"])
        assert detect_snapshot_sprawl(df) == []


# ===================== detect_untagged_resources =====================

class TestDetectUntaggedResources:
    def _make_df(self, resource_ids, owner_tag=None):
        rows = [{"provider": "AWS", "service": "ec2", "resource_id": rid,
                 "cost": 100.0, "date": date(2024, 3, 1)}
                for rid in resource_ids]
        df = pd.DataFrame(rows)
        if owner_tag is not None:
            df["owner"] = owner_tag
        return df

    def test_detects_resources_without_tags(self):
        df = self._make_df(["i-001", "i-002"])
        leaks = detect_untagged_resources(df)
        assert len(leaks) == 2
        assert all(l["leak_type"] == "UNTAGGED_RESOURCE" for l in leaks)

    def test_no_leak_when_owner_tag_present(self):
        df = self._make_df(["i-001"], owner_tag="team-a")
        leaks = detect_untagged_resources(df)
        assert leaks == []

    def test_no_leak_for_unknown_owner_value(self):
        # "unknown" / "none" / "" values are treated as missing
        df = self._make_df(["i-001"], owner_tag="unknown")
        leaks = detect_untagged_resources(df)
        assert len(leaks) == 1

    def test_top_n_cap(self):
        df = self._make_df([f"i-{i:03d}" for i in range(50)])
        leaks = detect_untagged_resources(df, top_n=10)
        assert len(leaks) == 10

    def test_top_n_sorted_by_cost(self):
        rows = [{"provider": "AWS", "service": "ec2", "resource_id": f"i-{i:03d}",
                 "cost": float(i), "date": date(2024, 3, 1)}
                for i in range(1, 21)]
        df = pd.DataFrame(rows)
        leaks = detect_untagged_resources(df, top_n=5)
        # Highest-cost resources should be surfaced first
        costs = [next(r["cost"] for _, r in df.iterrows() if r["resource_id"] == l["resource_id"])
                 for l in leaks]
        assert costs == sorted(costs, reverse=True)

    def test_deduplication_per_resource(self):
        rows = [
            {"provider": "AWS", "service": "ec2", "resource_id": "i-001",
             "cost": 10.0, "date": date(2024, 3, 1)},
            {"provider": "AWS", "service": "ec2", "resource_id": "i-001",
             "cost": 20.0, "date": date(2024, 3, 2)},
        ]
        df = pd.DataFrame(rows)
        leaks = detect_untagged_resources(df)
        assert len(leaks) == 1  # same resource_id — deduplicated

    def test_empty_df(self):
        df = pd.DataFrame(columns=["provider", "service", "resource_id", "cost"])
        assert detect_untagged_resources(df) == []
