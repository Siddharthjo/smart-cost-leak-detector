"""
LLM Recommendation Engine
Uses Claude API to enrich cost leak findings with:
  - Root cause explanation
  - Exact fix command (AWS CLI / console)
  - Remediation risk assessment
  - Time to fix estimate
"""

import os
import json
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Only enrich these severities to control API cost
ENRICH_SEVERITIES = {"HIGH", "MEDIUM"}

# Prompt template — structured JSON output enforced
_PROMPT = """\
You are a senior AWS FinOps engineer reviewing a cloud cost leak finding.

Leak details:
{leak_json}

Respond with a JSON object ONLY — no preamble, no markdown fences.
Use exactly these keys:
{{
  "root_cause": "<one sentence: why does this leak exist>",
  "fix_command": "<exact AWS CLI command or console action to remediate>",
  "estimated_remediation_minutes": <integer>,
  "risk_level": "<LOW|MEDIUM|HIGH>",
  "risk_note": "<one sentence: what could go wrong when fixing this>",
  "priority_reason": "<one sentence: why act now or why it can wait>"
}}
"""


def _build_prompt(leak: Dict) -> str:
    summary = {
        "leak_type":              leak.get("leak_type"),
        "provider":               leak.get("provider"),
        "service":                leak.get("service"),
        "resource_id":            leak.get("resource_id"),
        "reason":                 leak.get("reason"),
        "severity":               leak.get("severity"),
        "estimated_monthly_waste": leak.get("estimated_monthly_waste"),
        "confidence":             leak.get("confidence"),
    }
    return _PROMPT.format(leak_json=json.dumps(summary, indent=2))


def _call_claude(leak: Dict, client) -> Dict:
    """Single API call for one leak. Returns parsed recommendation dict."""
    prompt = _build_prompt(leak)
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)

    except json.JSONDecodeError as e:
        logger.warning(
            f"LLM returned non-JSON for {leak.get('leak_type')} / "
            f"{leak.get('service')}: {e}"
        )
        return {}
    except Exception as e:
        logger.warning(
            f"LLM call failed for {leak.get('leak_type')} / "
            f"{leak.get('service')}: {e}"
        )
        return {}


def enrich_leaks_with_llm(
    scored_leaks: List[Dict],
    api_key: Optional[str] = None,
    max_leaks: int = 10,
) -> List[Dict]:
    """
    Enrich top `max_leaks` HIGH/MEDIUM severity leaks with Claude recommendations.

    Leaks below MEDIUM severity are returned unchanged — they're not worth
    the API cost to analyze individually.

    Args:
        scored_leaks: Output of score_leaks()
        api_key:      Anthropic API key. Falls back to ANTHROPIC_API_KEY env var.
        max_leaks:    Max number of leaks to enrich (default 10).

    Returns:
        Same list with `llm_recommendation` dict added to enriched leaks.
    """
    try:
        import anthropic
    except ImportError:
        logger.warning(
            "anthropic package not installed — skipping LLM enrichment. "
            "Run: pip install anthropic"
        )
        return scored_leaks

    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        logger.warning(
            "ANTHROPIC_API_KEY not set — skipping LLM enrichment. "
            "Pass --api-key or set the env var."
        )
        return scored_leaks

    client = anthropic.Anthropic(api_key=key)

    # Select leaks to enrich: HIGH first, then MEDIUM, capped at max_leaks
    priority = [
        l for l in scored_leaks
        if l.get("severity") in ENRICH_SEVERITIES
    ][:max_leaks]

    enrich_keys = {
        (
            l.get("leak_type"),
            l.get("provider"),
            l.get("service"),
            l.get("resource_id"),
        )
        for l in priority
    }

    enriched: List[Dict] = []
    for leak in scored_leaks:
        key_tuple = (
            leak.get("leak_type"),
            leak.get("provider"),
            leak.get("service"),
            leak.get("resource_id"),
        )
        if key_tuple in enrich_keys:
            logger.info(
                f"LLM enriching: {leak.get('leak_type')} / "
                f"{leak.get('service')} [{leak.get('severity')}]"
            )
            rec = _call_claude(leak, client)
            enriched.append({**leak, "llm_recommendation": rec})
        else:
            enriched.append(leak)

    return enriched
