"""
Report Writer
Saves structured JSON and human-readable Markdown reports.
Implements delta detection: tags each leak as NEW or EXISTING
by comparing against the previous run's leak IDs.
"""

import json
import os
import logging
from datetime import datetime, date
from typing import List, Dict

logger = logging.getLogger(__name__)

REPORTS_DIR = "data/outputs/reports"
DELTA_FILE  = os.path.join(REPORTS_DIR, "latest_leak_ids.json")


# ===================== HELPERS =====================

class _DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


def _ensure_dir():
    os.makedirs(REPORTS_DIR, exist_ok=True)


def _leak_id(leak: Dict) -> str:
    return (
        f"{leak.get('leak_type')}::{leak.get('provider')}"
        f"::{leak.get('service')}::{leak.get('resource_id')}"
    )


def _load_previous_ids() -> set:
    if not os.path.exists(DELTA_FILE):
        return set()
    try:
        with open(DELTA_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def _save_current_ids(leaks: List[Dict]):
    with open(DELTA_FILE, "w") as f:
        json.dump([_leak_id(l) for l in leaks], f)


def _apply_delta(leaks: List[Dict]) -> List[Dict]:
    previous = _load_previous_ids()
    tagged = []
    for leak in leaks:
        status = "EXISTING" if _leak_id(leak) in previous else "NEW"
        tagged.append({**leak, "status": status})
    _save_current_ids(leaks)
    return tagged


# ===================== JSON REPORT =====================

def save_json_report(
    leaks: List[Dict],
    forecasts: List[Dict],
    stats: Dict,
) -> str:
    _ensure_dir()
    tagged = _apply_delta(leaks)

    total_monthly  = sum(l.get("estimated_monthly_waste", 0) for l in tagged)
    total_annual   = round(total_monthly * 12, 2)
    new_count      = sum(1 for l in tagged if l.get("status") == "NEW")

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "total_leaks":                        len(tagged),
            "new_leaks":                          new_count,
            "high":   sum(1 for l in tagged if l.get("severity") == "HIGH"),
            "medium": sum(1 for l in tagged if l.get("severity") == "MEDIUM"),
            "low":    sum(1 for l in tagged if l.get("severity") == "LOW"),
            "estimated_total_monthly_waste_usd":  round(total_monthly, 2),
            "estimated_total_annual_waste_usd":   total_annual,
        },
        "leaks":          tagged,
        "forecasts":      forecasts,
        "pipeline_stats": stats,
    }

    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORTS_DIR, f"report_{ts}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2, cls=_DateEncoder)

    logger.info(f"JSON report saved → {path}")
    return path


# ===================== MARKDOWN REPORT =====================

def save_markdown_report(
    leaks: List[Dict],
    forecasts: List[Dict],
) -> str:
    _ensure_dir()

    total_monthly = sum(l.get("estimated_monthly_waste", 0) for l in leaks)
    new_leaks     = [l for l in leaks if l.get("status") == "NEW"]
    high_leaks    = [l for l in leaks if l.get("severity") == "HIGH"]

    lines = [
        "# Cloud Cost Leak Report",
        f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "---",
        "",
        "## Executive Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total leaks detected | **{len(leaks)}** |",
        f"| New since last run | **{len(new_leaks)}** |",
        f"| High severity | {sum(1 for l in leaks if l.get('severity') == 'HIGH')} |",
        f"| Medium severity | {sum(1 for l in leaks if l.get('severity') == 'MEDIUM')} |",
        f"| Estimated monthly waste | **${total_monthly:,.2f}** |",
        f"| Estimated annual waste | **${total_monthly * 12:,.2f}** |",
        "",
    ]

    # 30-day forecast section
    if forecasts:
        lines += [
            "## 30-Day Cost Forecast",
            "",
            "| Provider | Service | Projected Monthly | Last 30d Actual | Trend |",
            "|----------|---------|-------------------|-----------------|-------|",
        ]
        for fc in forecasts[:10]:
            trend_str = (
                f"+{fc['trend_pct']}% ⚠️"
                if fc["trend_pct"] > 15
                else f"{fc['trend_pct']}%"
            )
            lines.append(
                f"| {fc['provider']} | {fc['service']} "
                f"| ${fc['projected_monthly_cost']:,.2f} "
                f"| ${fc['last_30d_actual']:,.2f} "
                f"| {trend_str} |"
            )
        lines.append("")

    # High severity detail
    if high_leaks:
        lines += ["## High Severity Findings", ""]
        for leak in high_leaks[:15]:
            status_badge = "🆕 NEW" if leak.get("status") == "NEW" else "🔁 Existing"
            lines += [
                f"### {leak.get('leak_type')} — {leak.get('service')} {status_badge}",
                "",
                f"| Field | Value |",
                f"|-------|-------|",
                f"| Resource | `{leak.get('resource_id') or 'Service-level'}` |",
                f"| Provider | {leak.get('provider')} |",
                f"| Reason | {leak.get('reason')} |",
                f"| Confidence | {leak.get('confidence', 'N/A')} |",
                f"| Est. monthly waste | ${leak.get('estimated_monthly_waste', 0):,.2f} |",
                f"| Est. annual waste | ${leak.get('estimated_annual_waste', 0):,.2f} |",
                "",
            ]
            rec = leak.get("llm_recommendation")
            if rec:
                lines += [
                    "**AI Recommendation**",
                    "",
                    f"- **Root cause:** {rec.get('root_cause', 'N/A')}",
                    f"- **Fix:** `{rec.get('fix_command', 'N/A')}`",
                    f"- **Time to fix:** {rec.get('estimated_remediation_minutes', '?')} minutes",
                    f"- **Risk:** {rec.get('risk_level', '?')} — {rec.get('risk_note', '')}",
                    f"- **Priority note:** {rec.get('priority_reason', '')}",
                    "",
                ]
            else:
                lines += [f"**Recommended action:** {leak.get('recommended_action')}", ""]

    # Full table
    lines += [
        "---",
        "",
        "## All Detected Leaks",
        "",
        "| # | Severity | Type | Service | Resource | Monthly Waste | Confidence | Status |",
        "|---|----------|------|---------|----------|---------------|------------|--------|",
    ]
    for i, leak in enumerate(leaks, 1):
        lines.append(
            f"| {i} "
            f"| {leak.get('severity')} "
            f"| {leak.get('leak_type')} "
            f"| {leak.get('service')} "
            f"| {(leak.get('resource_id') or 'N/A')[:30]} "
            f"| ${leak.get('estimated_monthly_waste', 0):,.2f} "
            f"| {leak.get('confidence', 'N/A')} "
            f"| {leak.get('status', 'N/A')} |"
        )

    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORTS_DIR, f"report_{ts}.md")
    with open(path, "w") as f:
        f.write("\n".join(lines))

    logger.info(f"Markdown report saved → {path}")
    return path
