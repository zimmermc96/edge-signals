#!/usr/bin/env python3
"""Signal Accuracy Tracker

Logs signals from all scanners (weather, crypto, economics), checks settlement
outcomes, and calculates running accuracy statistics.

Usage:
    python3 accuracy_tracker.py log          # log current signals from all scanners
    python3 accuracy_tracker.py check DATE   # check settlements for DATE (YYYY-MM-DD)
    python3 accuracy_tracker.py report       # show accuracy report
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Paths ──────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "practice_data"
SIGNAL_LOG = DATA_DIR / "signal_log.jsonl"
RESULTS_LOG = DATA_DIR / "results_log.jsonl"

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# CLI station codes used by IEM for weather settlement
CLI_STATION_MAP = {
    "NYC": "KNYC",
    "Chicago": "KMDW",
    "Miami": "KMIA",
    "Austin": "KAUS",
    "Denver": "KDEN",
}


# ── Signal Logging ─────────────────────────────────────────────────────────

def log_signals(signals: list[dict]) -> list[dict]:
    """Append signals to the JSONL log with timestamps and unique IDs.

    Each signal gets:
      - signal_id: unique UUID
      - logged_at: ISO 8601 timestamp (UTC)
      - date: YYYY-MM-DD date the signal applies to

    Returns the enriched signal list.
    """
    now = datetime.now(timezone.utc)
    enriched = []

    for sig in signals:
        entry = {
            "signal_id": str(uuid.uuid4()),
            "logged_at": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            **sig,
        }
        enriched.append(entry)

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(SIGNAL_LOG, "a") as f:
            for entry in enriched:
                f.write(json.dumps(entry) + "\n")
    except OSError:
        # Ephemeral filesystem (e.g., Render free tier) — signals still returned in memory
        pass

    return enriched


def _read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file, returning empty list if missing or corrupt."""
    entries = []
    if not path.exists():
        return entries
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _append_jsonl(path: Path, entries: list[dict]):
    """Append entries to a JSONL file."""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")
    except OSError:
        pass


# ── Weather Settlement ─────────────────────────────────────────────────────

def _fetch_cli_actuals(date_str: str) -> dict:
    """Fetch CLI (Climatological) actual high temps from IEM for a date.

    Uses https://mesonet.agron.iastate.edu/geojson/cli.py?dt=YYYY-MM-DD
    Returns dict mapping station name (e.g. "New York") to actual high temp (F).
    """
    url = f"https://mesonet.agron.iastate.edu/geojson/cli.py?dt={date_str}"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, json.JSONDecodeError):
        return {}

    actuals = {}
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        station = props.get("station", "")
        high = props.get("high")
        if station and high is not None:
            actuals[station] = high
    return actuals


def _check_weather_signal(signal: dict, cli_actuals: dict) -> str:
    """Check if a weather signal was correct against CLI actuals.

    Matches the signal's city to a CLI station, then checks if the actual
    high fell in the predicted bracket.

    Returns "correct", "incorrect", or "pending".
    """
    city = signal.get("city", "")
    label = signal.get("label", "")
    ticker = signal.get("ticker", "")

    # Map city name to CLI station code
    station_code = CLI_STATION_MAP.get(city)
    if not station_code:
        return "pending"

    # Find the actual high for this station
    actual_high = cli_actuals.get(station_code)
    if actual_high is None:
        return "pending"

    # Parse the bracket from the label
    # Formats: "Below 65°F", "Above 70°F", "65-70°F"
    try:
        if label.startswith("Below "):
            cap = float(label.replace("Below ", "").replace("°F", ""))
            # Signal said BUY YES on "below cap" means predicting temp < cap
            # Signal said BUY NO on "below cap" means predicting temp >= cap
            if signal.get("signal") == "BUY YES":
                return "correct" if actual_high < cap else "incorrect"
            else:
                return "correct" if actual_high >= cap else "incorrect"

        elif label.startswith("Above "):
            floor = float(label.replace("Above ", "").replace("°F", ""))
            if signal.get("signal") == "BUY YES":
                return "correct" if actual_high > floor else "incorrect"
            else:
                return "correct" if actual_high <= floor else "incorrect"

        elif "-" in label and "°F" in label:
            parts = label.replace("°F", "").split("-")
            floor = float(parts[0])
            cap = float(parts[1])
            if signal.get("signal") == "BUY YES":
                return "correct" if floor <= actual_high <= cap else "incorrect"
            else:
                return "correct" if actual_high < floor or actual_high > cap else "incorrect"

    except (ValueError, IndexError):
        pass

    return "pending"


# ── Crypto Settlement ──────────────────────────────────────────────────────

def _fetch_settled_crypto_markets(date_str: str) -> dict:
    """Fetch settled Kalshi crypto markets for a date.

    Looks for settled markets in the KXBTCD and KXETHD series that closed
    on or after the given date. Returns dict mapping ticker -> result ("yes"/"no").
    """
    settled = {}
    for series in ["KXBTCD", "KXETHD"]:
        try:
            cursor = None
            while True:
                params = {
                    "series_ticker": series,
                    "status": "settled",
                    "limit": 200,
                }
                if cursor:
                    params["cursor"] = cursor
                resp = requests.get(
                    f"{KALSHI_BASE}/markets", params=params, timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("markets", [])
                for m in batch:
                    close_time = m.get("close_time", "")
                    if date_str in close_time:
                        result = m.get("result", "")
                        if result:
                            settled[m["ticker"]] = result
                cursor = data.get("cursor")
                if not cursor or not batch:
                    break
                time.sleep(0.05)
        except requests.RequestException:
            continue
    return settled


def _check_crypto_signal(signal: dict, settled_markets: dict) -> str:
    """Check if a crypto signal was correct against settled market results.

    Returns "correct", "incorrect", or "pending".
    """
    ticker = signal.get("ticker", "")
    if ticker not in settled_markets:
        return "pending"

    market_result = settled_markets[ticker]  # "yes" or "no"
    signal_action = signal.get("signal", "")

    if signal_action == "BUY YES":
        return "correct" if market_result == "yes" else "incorrect"
    elif signal_action == "BUY NO":
        return "correct" if market_result == "no" else "incorrect"

    return "pending"


# ── Economics Settlement ───────────────────────────────────────────────────

def _fetch_settled_econ_markets(date_str: str) -> dict:
    """Fetch settled Kalshi economics markets for a date.

    Checks CPI, GDP, and Fed rate series. Returns dict mapping ticker -> result.
    """
    settled = {}
    for series in ["KXCPI", "KXGDP", "KXFED", "KXPCECORE"]:
        try:
            cursor = None
            while True:
                params = {
                    "series_ticker": series,
                    "status": "settled",
                    "limit": 200,
                }
                if cursor:
                    params["cursor"] = cursor
                resp = requests.get(
                    f"{KALSHI_BASE}/markets", params=params, timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("markets", [])
                for m in batch:
                    close_time = m.get("close_time", "")
                    # Econ markets: match if close_time falls on the target date
                    if date_str in close_time:
                        result = m.get("result", "")
                        if result:
                            settled[m["ticker"]] = result
                cursor = data.get("cursor")
                if not cursor or not batch:
                    break
                time.sleep(0.05)
        except requests.RequestException:
            continue
    return settled


def _check_econ_signal(signal: dict, settled_markets: dict) -> str:
    """Check if an economics signal was correct.

    Many econ signals are INFO-only (edge=0), so they are always "pending"
    unless they had a directional signal.
    """
    if signal.get("signal") == "INFO":
        return "pending"

    ticker = signal.get("ticker", "")
    if ticker not in settled_markets:
        return "pending"

    market_result = settled_markets[ticker]
    signal_action = signal.get("signal", "")

    if signal_action == "BUY YES":
        return "correct" if market_result == "yes" else "incorrect"
    elif signal_action == "BUY NO":
        return "correct" if market_result == "no" else "incorrect"

    return "pending"


# ── Settlement Checker ─────────────────────────────────────────────────────

def check_settled(date: str) -> list[dict]:
    """Check which signals from a given date have settled and record results.

    Args:
        date: YYYY-MM-DD date string to check

    Returns:
        List of result dicts with outcome field added.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Read all signals for this date
    all_signals = _read_jsonl(SIGNAL_LOG)
    date_signals = [s for s in all_signals if s.get("date") == date]

    if not date_signals:
        print(f"  No signals found for {date}.")
        return []

    # Load existing results to avoid re-checking already-settled signals
    existing_results = _read_jsonl(RESULTS_LOG)
    already_checked = {r["signal_id"] for r in existing_results
                       if r.get("outcome") in ("correct", "incorrect")}

    # Only check signals that haven't been finalized
    to_check = [s for s in date_signals if s.get("signal_id") not in already_checked]

    if not to_check:
        print(f"  All signals for {date} already have final results.")
        return existing_results

    # Fetch settlement data (only for categories that have pending signals)
    categories_needed = {s.get("category") for s in to_check}

    cli_actuals = {}
    if "weather" in categories_needed:
        print(f"  Fetching CLI actuals for {date}...")
        cli_actuals = _fetch_cli_actuals(date)
        if cli_actuals:
            print(f"  Found CLI data for {len(cli_actuals)} stations.")
        else:
            print(f"  No CLI data available yet for {date}.")

    settled_crypto = {}
    if "crypto" in categories_needed:
        print(f"  Fetching settled crypto markets for {date}...")
        settled_crypto = _fetch_settled_crypto_markets(date)
        print(f"  Found {len(settled_crypto)} settled crypto markets.")

    settled_econ = {}
    if categories_needed & {"fed_rate", "cpi", "gdp", "economics"}:
        print(f"  Fetching settled econ markets for {date}...")
        settled_econ = _fetch_settled_econ_markets(date)
        print(f"  Found {len(settled_econ)} settled econ markets.")

    # Check each signal
    results = []
    for sig in to_check:
        category = sig.get("category", "")

        if category == "weather":
            outcome = _check_weather_signal(sig, cli_actuals)
        elif category == "crypto":
            outcome = _check_crypto_signal(sig, settled_crypto)
        elif category in ("fed_rate", "cpi", "gdp", "economics"):
            outcome = _check_econ_signal(sig, settled_econ)
        else:
            outcome = "pending"

        result = {
            **sig,
            "outcome": outcome,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        # For weather, include actual high if available
        if category == "weather" and cli_actuals:
            station_code = CLI_STATION_MAP.get(sig.get("city", ""))
            if station_code and station_code in cli_actuals:
                result["actual_value"] = cli_actuals[station_code]

        results.append(result)

    # Save results (append new, don't duplicate)
    _append_jsonl(RESULTS_LOG, results)

    # Print summary
    correct = sum(1 for r in results if r["outcome"] == "correct")
    incorrect = sum(1 for r in results if r["outcome"] == "incorrect")
    pending = sum(1 for r in results if r["outcome"] == "pending")

    print(f"\n  Results for {date}:")
    print(f"    Checked:   {len(results)} signals")
    print(f"    Correct:   {correct}")
    print(f"    Incorrect: {incorrect}")
    print(f"    Pending:   {pending}")

    for r in results:
        icon = {"correct": "+", "incorrect": "-", "pending": "?"}[r["outcome"]]
        edge_str = f"{r.get('edge', 0):+.1f}%" if r.get("edge") else ""
        actual_str = f" (actual: {r['actual_value']})" if "actual_value" in r else ""
        print(f"    [{icon}] {r.get('category','?'):>8} | {r.get('label',''):<25} "
              f"| {r.get('signal',''):>8} {edge_str}{actual_str}")

    return results


# ── Accuracy Report ────────────────────────────────────────────────────────

def get_accuracy_report() -> dict:
    """Calculate running accuracy statistics from the results log.

    Returns a dict with:
      - overall: total, correct, incorrect, pending, accuracy_pct
      - by_category: same breakdown per category
      - edge_analysis: avg edge on correct vs incorrect signals
      - recent_days: last 7 days of daily accuracy
      - generated_at: timestamp
    """
    results = _read_jsonl(RESULTS_LOG)

    if not results:
        return {
            "overall": {
                "total": 0, "correct": 0, "incorrect": 0,
                "pending": 0, "accuracy_pct": None,
            },
            "by_category": {},
            "edge_analysis": {},
            "recent_days": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # De-duplicate: if a signal was checked multiple times, use the latest
    # (a signal might go from "pending" to "correct" on re-check)
    latest_by_id = {}
    for r in results:
        sid = r.get("signal_id", "")
        if not sid:
            continue
        existing = latest_by_id.get(sid)
        if existing is None:
            latest_by_id[sid] = r
        else:
            # Prefer non-pending over pending; then latest checked_at
            if existing["outcome"] == "pending" and r["outcome"] != "pending":
                latest_by_id[sid] = r
            elif r.get("checked_at", "") > existing.get("checked_at", ""):
                latest_by_id[sid] = r

    unique_results = list(latest_by_id.values())

    def _calc_stats(entries: list[dict]) -> dict:
        total = len(entries)
        correct = sum(1 for e in entries if e["outcome"] == "correct")
        incorrect = sum(1 for e in entries if e["outcome"] == "incorrect")
        pending = sum(1 for e in entries if e["outcome"] == "pending")
        decided = correct + incorrect
        accuracy_pct = round(correct / decided * 100, 1) if decided > 0 else None
        return {
            "total": total,
            "correct": correct,
            "incorrect": incorrect,
            "pending": pending,
            "accuracy_pct": accuracy_pct,
        }

    # Overall stats
    overall = _calc_stats(unique_results)

    # By category
    categories = {}
    for r in unique_results:
        cat = r.get("category", "unknown")
        categories.setdefault(cat, []).append(r)

    by_category = {}
    for cat, entries in sorted(categories.items()):
        by_category[cat] = _calc_stats(entries)

    # Edge analysis: average edge on correct vs incorrect signals
    correct_edges = [r["edge"] for r in unique_results
                     if r["outcome"] == "correct" and isinstance(r.get("edge"), (int, float))
                     and r["edge"] != 0]
    incorrect_edges = [r["edge"] for r in unique_results
                       if r["outcome"] == "incorrect" and isinstance(r.get("edge"), (int, float))
                       and r["edge"] != 0]

    edge_analysis = {
        "avg_edge_correct": round(sum(correct_edges) / len(correct_edges), 1) if correct_edges else None,
        "avg_edge_incorrect": round(sum(incorrect_edges) / len(incorrect_edges), 1) if incorrect_edges else None,
        "avg_abs_edge_correct": round(sum(abs(e) for e in correct_edges) / len(correct_edges), 1) if correct_edges else None,
        "avg_abs_edge_incorrect": round(sum(abs(e) for e in incorrect_edges) / len(incorrect_edges), 1) if incorrect_edges else None,
        "correct_signal_count": len(correct_edges),
        "incorrect_signal_count": len(incorrect_edges),
    }

    # Recent days: last 7 days with data
    days = {}
    for r in unique_results:
        d = r.get("date", "unknown")
        days.setdefault(d, []).append(r)

    recent_days = []
    for d in sorted(days.keys(), reverse=True)[:7]:
        day_stats = _calc_stats(days[d])
        day_stats["date"] = d
        recent_days.append(day_stats)

    return {
        "overall": overall,
        "by_category": by_category,
        "edge_analysis": edge_analysis,
        "recent_days": recent_days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ── CLI ────────────────────────────────────────────────────────────────────

def _print_report(report: dict):
    """Pretty-print the accuracy report."""
    print(f"\n{'='*74}")
    print(f"  SIGNAL ACCURACY REPORT")
    print(f"  Generated: {report['generated_at'][:19]} UTC")
    print(f"{'='*74}")

    ov = report["overall"]
    if ov["total"] == 0:
        print("\n  No signals tracked yet. Run 'log' to start tracking.\n")
        return

    print(f"\n  OVERALL")
    print(f"    Total signals:  {ov['total']}")
    print(f"    Correct:        {ov['correct']}")
    print(f"    Incorrect:      {ov['incorrect']}")
    print(f"    Pending:        {ov['pending']}")
    if ov["accuracy_pct"] is not None:
        print(f"    Accuracy:       {ov['accuracy_pct']}%")
    else:
        print(f"    Accuracy:       N/A (no settled signals)")

    print(f"\n  BY CATEGORY")
    for cat, stats in report["by_category"].items():
        acc = f"{stats['accuracy_pct']}%" if stats["accuracy_pct"] is not None else "N/A"
        print(f"    {cat:<12} {stats['correct']}/{stats['correct']+stats['incorrect']:>3} correct "
              f"({acc})  |  {stats['pending']} pending  |  {stats['total']} total")

    ea = report["edge_analysis"]
    print(f"\n  EDGE ANALYSIS")
    if ea["avg_edge_correct"] is not None:
        print(f"    Avg edge on CORRECT signals:    {ea['avg_edge_correct']:+.1f}%  "
              f"(abs: {ea['avg_abs_edge_correct']:.1f}%, n={ea['correct_signal_count']})")
    else:
        print(f"    Avg edge on CORRECT signals:    N/A")
    if ea["avg_edge_incorrect"] is not None:
        print(f"    Avg edge on INCORRECT signals:  {ea['avg_edge_incorrect']:+.1f}%  "
              f"(abs: {ea['avg_abs_edge_incorrect']:.1f}%, n={ea['incorrect_signal_count']})")
    else:
        print(f"    Avg edge on INCORRECT signals:  N/A")

    if report["recent_days"]:
        print(f"\n  RECENT DAYS")
        for day in report["recent_days"]:
            acc = f"{day['accuracy_pct']}%" if day["accuracy_pct"] is not None else "pending"
            print(f"    {day['date']}  {day['correct']}/{day['correct']+day['incorrect']} "
                  f"({acc})  [{day['total']} signals]")

    print()


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 accuracy_tracker.py [log|check DATE|report]")
        print()
        print("  log          Log current signals from all scanners")
        print("  check DATE   Check settlements for DATE (YYYY-MM-DD)")
        print("  report       Show accuracy report")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "log":
        # Import scanner engine and log all signals
        try:
            from app.scanner_engine import scan_weather, scan_crypto, scan_economics
        except ImportError:
            print("  Error: Could not import scanner_engine. "
                  "Run from project root.\n")
            sys.exit(1)

        print(f"\n{'='*74}")
        print(f"  LOGGING SIGNALS")
        print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'='*74}\n")

        all_signals = []

        print("  Scanning weather...")
        try:
            weather = scan_weather()
            all_signals.extend(weather)
            print(f"    {len(weather)} weather signals")
        except Exception as e:
            print(f"    Weather scan error: {e}")

        print("  Scanning crypto...")
        try:
            crypto = scan_crypto()
            all_signals.extend(crypto)
            print(f"    {len(crypto)} crypto signals")
        except Exception as e:
            print(f"    Crypto scan error: {e}")

        print("  Scanning economics...")
        try:
            econ = scan_economics()
            # Only log econ signals with a directional signal (not INFO)
            directional_econ = [s for s in econ if s.get("signal") != "INFO"]
            all_signals.extend(directional_econ)
            print(f"    {len(directional_econ)} econ signals "
                  f"({len(econ) - len(directional_econ)} INFO-only skipped)")
        except Exception as e:
            print(f"    Econ scan error: {e}")

        if all_signals:
            logged = log_signals(all_signals)
            print(f"\n  Logged {len(logged)} signals to {SIGNAL_LOG}\n")
        else:
            print(f"\n  No actionable signals found.\n")

    elif cmd == "check":
        if len(sys.argv) < 3:
            # Default to yesterday
            date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"  No date specified, checking yesterday: {date}")
        else:
            date = sys.argv[2]

        print(f"\n{'='*74}")
        print(f"  CHECKING SETTLEMENTS FOR {date}")
        print(f"{'='*74}\n")

        check_settled(date)
        print()

    elif cmd == "report":
        report = get_accuracy_report()
        _print_report(report)

    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python3 accuracy_tracker.py [log|check DATE|report]")
        sys.exit(1)


if __name__ == "__main__":
    main()
