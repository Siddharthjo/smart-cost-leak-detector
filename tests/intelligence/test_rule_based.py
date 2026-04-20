"""Tests for src/intelligence/leak_detection/rule_based.py"""

from datetime import date, timedelta

import pandas as pd
import pytest

from src.intelligence.leak_detection.rule_based import (
    ZOMBIE_MIN_DAYS,
    detect_always_on_high_cost,
    detect_idle_resources,
    detect_runaway_costs,
    detect_zombie_resources,
    get_service_category,
)


# ===================== get_service_category =====================

class TestGetServiceCategory:
    def test_ec2_is_compute(self):
        assert get_service_category("ec2") == "compute"

    def test_virtual_machines_is_compute(self):
        assert get_service_category("Virtual Machines") == "compute"

    def test_compute_engine_is_compute(self):
        assert get_service_category("Compute Engine") == "compute"

    def test_s3_is_storage(self):
        assert get_service_category("s3") == "storage"

    def test_cloud_storage_is_storage(self):
        assert get_service_category("Cloud Storage") == "storage"

    def test_rds_is_database(self):
        assert get_service_category("rds") == "database"

    def test_lambda_is_serverless(self):
        assert get_service_category("lambda") == "serverless"

    def test_unknown_service_is_other(self):
        assert get_service_category("kinesis") == "other"

    def test_empty_string(self):
        assert get_service_category("") == "other"

    def test_none(self):
        assert get_service_category(None) == "other"


# ===================== detect_zombie_resources =====================

class TestDetectZombieResources:
    def _make_lifespan(self, days_active, provider="AWS", service="ec2", resource_id="i-001"):
        return [{"provider": provider, "service": service,
                 "resource_id": resource_id, "days_active": days_active}]

    def _make_usage(self, ratio, provider="AWS", service="ec2", resource_id="i-001"):
        return [{"provider": provider, "service": service,
                 "resource_id": resource_id, "usage_to_cost_ratio": ratio}]

    def test_detects_long_running_low_usage(self):
        lifespan = self._make_lifespan(days_active=20)
        usage    = self._make_usage(ratio=0.001)
        leaks, zombie_ids = detect_zombie_resources(lifespan, usage)
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "ZOMBIE_RESOURCE"
        assert leaks[0]["resource_id"] == "i-001"
        assert "i-001" in zombie_ids

    def test_no_leak_for_young_resource(self):
        lifespan = self._make_lifespan(days_active=ZOMBIE_MIN_DAYS - 1)
        usage    = self._make_usage(ratio=0.001)
        leaks, _ = detect_zombie_resources(lifespan, usage)
        assert leaks == []

    def test_no_leak_for_high_usage_ratio(self):
        # High usage-to-cost ratio means the resource is actively used
        lifespan = self._make_lifespan(days_active=20)
        usage    = self._make_usage(ratio=50.0)
        leaks, _ = detect_zombie_resources(lifespan, usage)
        assert leaks == []

    def test_no_leak_when_usage_missing(self):
        lifespan = self._make_lifespan(days_active=20)
        leaks, _ = detect_zombie_resources(lifespan, [])
        assert leaks == []

    def test_empty_inputs(self):
        leaks, zombie_ids = detect_zombie_resources([], [])
        assert leaks == []
        assert zombie_ids == set()

    def test_multiple_resources_independent(self):
        lifespan = [
            {"provider": "AWS", "service": "ec2", "resource_id": "i-001", "days_active": 20},
            {"provider": "AWS", "service": "ec2", "resource_id": "i-002", "days_active": 20},
        ]
        usage = [
            {"provider": "AWS", "service": "ec2", "resource_id": "i-001", "usage_to_cost_ratio": 0.001},
            {"provider": "AWS", "service": "ec2", "resource_id": "i-002", "usage_to_cost_ratio": 100.0},
        ]
        leaks, zombie_ids = detect_zombie_resources(lifespan, usage)
        assert len(leaks) == 1
        assert "i-001" in zombie_ids
        assert "i-002" not in zombie_ids


# ===================== detect_idle_resources =====================

class TestDetectIdleResources:
    def _make_daily_cost_df(self, provider="AWS", service="ec2", daily_cost=10.0, n_days=5):
        end = date(2024, 3, 31)
        rows = [{"date": end - timedelta(days=i), "provider": provider,
                 "service": service, "daily_cost": daily_cost}
                for i in range(n_days)]
        return pd.DataFrame(rows)

    def test_detects_idle_compute(self):
        lifespan = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-idle", "days_active": 5}]
        usage    = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-idle", "usage_to_cost_ratio": 1.0}]
        daily_df = self._make_daily_cost_df()

        leaks = detect_idle_resources(lifespan, usage, daily_df, excluded_resource_ids=set())
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "IDLE_RESOURCE"

    def test_excludes_zombie_ids(self):
        lifespan = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-zombie", "days_active": 5}]
        usage    = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-zombie", "usage_to_cost_ratio": 1.0}]
        daily_df = self._make_daily_cost_df()

        leaks = detect_idle_resources(lifespan, usage, daily_df, excluded_resource_ids={"i-zombie"})
        assert leaks == []

    def test_skips_non_compute_services(self):
        lifespan = [{"provider": "AWS", "service": "s3",
                     "resource_id": "bucket-1", "days_active": 5}]
        usage    = [{"provider": "AWS", "service": "s3",
                     "resource_id": "bucket-1", "usage_to_cost_ratio": 1.0}]
        daily_df = self._make_daily_cost_df(service="s3")

        leaks = detect_idle_resources(lifespan, usage, daily_df, excluded_resource_ids=set())
        assert leaks == []

    def test_skips_low_cost_services(self):
        lifespan = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-cheap", "days_active": 5}]
        usage    = [{"provider": "AWS", "service": "ec2",
                     "resource_id": "i-cheap", "usage_to_cost_ratio": 1.0}]
        daily_df = self._make_daily_cost_df(daily_cost=0.001)  # below min daily cost

        leaks = detect_idle_resources(lifespan, usage, daily_df, excluded_resource_ids=set())
        assert leaks == []


# ===================== detect_runaway_costs =====================

class TestDetectRunawayCosts:
    def _make_daily_df(self, costs: list, provider="AWS", service="ec2"):
        end = date(2024, 3, 31)
        rows = [{"date": end - timedelta(days=len(costs) - 1 - i),
                 "provider": provider, "service": service, "daily_cost": c}
                for i, c in enumerate(costs)]
        return pd.DataFrame(rows)

    def test_detects_cost_growth_via_percent(self):
        # 100% → 1000% growth — well above 30% threshold, no z_score column
        costs = [2.0, 4.0, 8.0, 16.0, 100.0]
        df = self._make_daily_df(costs)
        leaks = detect_runaway_costs(df, usage_ratio_data=[])
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "RUNAWAY_COST"
        assert leaks[0]["service"] == "ec2"

    def test_no_leak_for_stable_costs(self):
        costs = [10.0, 10.0, 10.0, 10.0, 10.0]
        df = self._make_daily_df(costs)
        leaks = detect_runaway_costs(df, usage_ratio_data=[])
        assert leaks == []

    def test_no_leak_for_low_cost_service(self):
        # Below RUNAWAY_MIN_DAILY_COST average
        costs = [0.01, 0.02, 0.05, 0.10, 1.00]
        df = self._make_daily_df(costs)
        leaks = detect_runaway_costs(df, usage_ratio_data=[])
        assert leaks == []

    def test_no_leak_for_high_usage_ratio(self):
        # A service with legitimate high usage is excluded
        costs = [2.0, 5.0, 10.0, 50.0, 200.0]
        df = self._make_daily_df(costs)
        usage = [{"provider": "AWS", "service": "ec2", "resource_id": "",
                  "usage_to_cost_ratio": 100.0}]
        leaks = detect_runaway_costs(df, usage_ratio_data=usage)
        assert leaks == []

    def test_detects_via_zscore(self):
        """When z_score column is present and spike is high, use statistical path."""
        costs = [10.0] * 10 + [200.0]  # big spike at end
        df = self._make_daily_df(costs)
        # Add z_score column manually (simulating compute_cost_zscore output)
        df["rolling_mean"] = 10.0
        df["rolling_std"]  = 1.0
        df["z_score"] = (df["daily_cost"] - df["rolling_mean"]) / (df["rolling_std"] + 0.01)
        leaks = detect_runaway_costs(df, usage_ratio_data=[])
        assert any(l["leak_type"] == "RUNAWAY_COST" for l in leaks)

    def test_not_enough_days_skipped(self):
        costs = [10.0, 20.0]  # below RUNAWAY_MIN_DAYS (3)
        df = self._make_daily_df(costs)
        leaks = detect_runaway_costs(df, usage_ratio_data=[])
        assert leaks == []

    def test_empty_df(self):
        df = pd.DataFrame(columns=["date", "provider", "service", "daily_cost"])
        leaks = detect_runaway_costs(df, usage_ratio_data=[])
        assert leaks == []


# ===================== detect_always_on_high_cost =====================

class TestDetectAlwaysOnHighCost:
    def _make_dfs(self, service="ec2", daily_cost=75.0, n_days=20, add_owner=False):
        end = date(2024, 3, 31)
        rows_daily, rows_norm = [], []
        for i in range(n_days):
            d = end - timedelta(days=n_days - 1 - i)
            rows_daily.append({"date": d, "provider": "AWS", "service": service,
                                "daily_cost": daily_cost})
            row = {"date": d, "provider": "AWS", "service": service,
                   "cost": daily_cost, "resource_id": "res-001"}
            if add_owner:
                row["owner"] = "team-a"
            rows_norm.append(row)
        return pd.DataFrame(rows_daily), pd.DataFrame(rows_norm)

    def test_detects_expensive_unowned_compute(self):
        daily_df, norm_df = self._make_dfs(service="ec2", daily_cost=75.0)
        leaks = detect_always_on_high_cost(daily_df, norm_df)
        assert len(leaks) == 1
        assert leaks[0]["leak_type"] == "ALWAYS_ON_HIGH_COST"

    def test_no_leak_when_owned(self):
        daily_df, norm_df = self._make_dfs(service="ec2", daily_cost=75.0, add_owner=True)
        leaks = detect_always_on_high_cost(daily_df, norm_df)
        assert leaks == []

    def test_no_leak_for_cheap_service(self):
        daily_df, norm_df = self._make_dfs(service="ec2", daily_cost=5.0)
        leaks = detect_always_on_high_cost(daily_df, norm_df)
        assert leaks == []

    def test_no_leak_for_non_compute(self):
        # s3 is not in the compute/database category
        daily_df, norm_df = self._make_dfs(service="s3", daily_cost=100.0)
        leaks = detect_always_on_high_cost(daily_df, norm_df)
        assert leaks == []

    def test_no_leak_for_intermittent_service(self):
        # Only present 5 out of 20 days — below 90% presence ratio
        end = date(2024, 3, 31)
        rows_daily = [{"date": end - timedelta(days=i), "provider": "AWS",
                       "service": "ec2", "daily_cost": 100.0}
                      for i in range(5)]
        all_dates = [{"date": end - timedelta(days=i), "provider": "AWS",
                      "service": "ec2", "daily_cost": 0.0}
                     for i in range(20)]
        daily_df = pd.DataFrame(all_dates)
        norm_df  = pd.DataFrame(rows_daily)
        leaks = detect_always_on_high_cost(daily_df, norm_df)
        # Average cost will be low due to zero-cost days
        assert leaks == []
