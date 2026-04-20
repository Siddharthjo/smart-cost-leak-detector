import pandas as pd
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# Minimum dollar threshold to avoid noisy micro-leaks
RI_MIN_WASTE_USD = 10.0


def detect_reserved_instance_waste(raw_df: pd.DataFrame) -> List[Dict]:
    """
    Detect underutilized Reserved Instances and Savings Plans.

    Uses raw (pre-normalized) AWS CUR data because the normalized
    schema drops the item_type and reservation columns needed here.

    Detects two patterns:
    1. SavingsPlanNegation — unused savings plan commitment billed anyway
    2. reservation_unused_recurring_fee — RI hours purchased but not consumed

    Both are directly quantifiable waste — unlike heuristic detectors,
    these numbers come straight from the billing line items.
    """
    leaks: List[Dict] = []

    if raw_df is None or raw_df.empty:
        return leaks

    cols = set(raw_df.columns)

    # ---- SAVINGS PLAN WASTE ----
    if "line_item_line_item_type" in cols and "line_item_unblended_cost" in cols:
        sp_rows = raw_df[
            raw_df["line_item_line_item_type"].isin([
                "SavingsPlanNegation",
                "SavingsPlanRecurringFee",
            ])
        ]
        if not sp_rows.empty:
            total = pd.to_numeric(
                sp_rows["line_item_unblended_cost"], errors="coerce"
            ).sum()
            if total >= RI_MIN_WASTE_USD:
                leaks.append({
                    "leak_type": "RI_SAVINGS_PLAN_WASTE",
                    "provider":  "AWS",
                    "service":   "Savings Plans",
                    "resource_id": None,
                    "reason": (
                        f"Unused savings plan commitment costing "
                        f"${total:,.2f} in this billing period"
                    ),
                })
                logger.info(f"Savings Plan waste detected: ${total:,.2f}")

    # ---- UNUSED RESERVED INSTANCES ----
    has_qty  = "reservation_unused_quantity" in cols
    has_fee  = "reservation_unused_recurring_fee" in cols

    if has_qty and has_fee:
        unused = raw_df[
            pd.to_numeric(raw_df["reservation_unused_quantity"], errors="coerce").fillna(0) > 0
        ].copy()

        if not unused.empty:
            unused_fee = pd.to_numeric(
                unused["reservation_unused_recurring_fee"], errors="coerce"
            ).sum()

            if unused_fee >= RI_MIN_WASTE_USD:
                # Try to break down by service
                if "product_servicecode" in cols:
                    by_service = (
                        unused.groupby("product_servicecode")
                        .apply(lambda g: pd.to_numeric(
                            g["reservation_unused_recurring_fee"], errors="coerce"
                        ).sum())
                        .sort_values(ascending=False)
                    )
                    top = by_service.head(3)
                    detail = ", ".join(
                        f"{svc}: ${cost:,.2f}"
                        for svc, cost in top.items()
                    )
                    reason = (
                        f"Unused reserved capacity costing ${unused_fee:,.2f} "
                        f"({detail})"
                    )
                else:
                    reason = (
                        f"Unused reserved instance capacity costing "
                        f"${unused_fee:,.2f} in this billing period"
                    )

                leaks.append({
                    "leak_type": "RI_UNUSED_RESERVATION",
                    "provider":  "AWS",
                    "service":   "Reserved Instances",
                    "resource_id": None,
                    "reason": reason,
                })
                logger.info(f"Unused RI detected: ${unused_fee:,.2f}")

    return leaks
