#!/usr/bin/env python3
"""Economic Data Arbitrage Scanner

Compares Kalshi economic prediction markets to professional nowcasts
(Cleveland Fed, Atlanta Fed GDPNow, CME FedWatch) to find mispricings.

Usage:
    python3 econ_scanner.py           # scan all economic markets
    python3 econ_scanner.py --fed     # Fed rate markets only
    python3 econ_scanner.py --cpi     # CPI markets only
    python3 econ_scanner.py --gdp     # GDP markets only

Requires: FRED API key (free: https://fred.stlouisfed.org/docs/api/api_key.html)
    export FRED_API_KEY="your-key-here"
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, timezone
from math import erf, sqrt

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
FRED_BASE = "https://api.stlouisfed.org/fred"

MIN_EDGE_PCT = 8


# ── FRED API ────────────────────────────────────────────────────────────────

def get_fred_series(series_id, limit=10):
    """Fetch latest values from a FRED series."""
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        return None

    resp = requests.get(f"{FRED_BASE}/series/observations", params={
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }, timeout=15)
    resp.raise_for_status()
    return resp.json().get("observations", [])


def get_gdpnow():
    """Get latest GDPNow estimate from FRED."""
    obs = get_fred_series("GDPNOW", limit=5)
    if obs:
        for o in obs:
            if o["value"] != ".":
                return float(o["value"]), o["date"]
    return None, None


def get_fed_funds_rate():
    """Get current Fed funds target rate."""
    obs = get_fred_series("DFEDTARU", limit=5)
    if obs:
        for o in obs:
            if o["value"] != ".":
                return float(o["value"]), o["date"]
    return None, None


def get_latest_cpi():
    """Get latest CPI data."""
    obs = get_fred_series("CPIAUCSL", limit=5)
    if obs:
        values = [(o["date"], float(o["value"])) for o in obs if o["value"] != "."]
        if len(values) >= 2:
            latest = values[0]
            prev = values[1]
            mom_change = (latest[1] - prev[1]) / prev[1] * 100
            return {
                "date": latest[0],
                "value": latest[1],
                "mom_pct": mom_change,
            }
    return None


def get_unemployment():
    """Get latest unemployment rate."""
    obs = get_fred_series("UNRATE", limit=3)
    if obs:
        for o in obs:
            if o["value"] != ".":
                return float(o["value"]), o["date"]
    return None, None


# ── Kalshi Data ─────────────────────────────────────────────────────────────

def get_kalshi_markets(series_ticker):
    """Fetch all open markets for a series."""
    all_markets = []
    cursor = None
    while True:
        params = {"series_ticker": series_ticker, "status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
        data = resp.json()
        batch = data.get("markets", [])
        all_markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
        time.sleep(0.05)
    return all_markets


# ── Fed Rate Scanner ────────────────────────────────────────────────────────

def scan_fed_markets():
    """Compare Kalshi Fed rate markets to current rate and FRED data."""
    print(f"\n  {'='*70}")
    print(f"  FED RATE MARKETS (KXFED)")
    print(f"  {'='*70}")

    current_rate, rate_date = get_fed_funds_rate()
    if current_rate:
        print(f"\n  Current Fed funds target (upper): {current_rate}% (as of {rate_date})")

    markets = get_kalshi_markets("KXFED")
    if not markets:
        print("  No Fed rate markets found.")
        return []

    # Group by event (each FOMC meeting)
    events = {}
    for m in markets:
        et = m.get("event_ticker", "")
        events.setdefault(et, []).append(m)

    edges = []
    for event_ticker in sorted(events.keys()):
        event_markets = events[event_ticker]
        event_markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))

        print(f"\n  Event: {event_ticker}")
        print(f"  Close: {event_markets[0].get('close_time', '')[:10]}")
        print(f"  Rules: {(event_markets[0].get('rules_primary') or '')[:150]}")
        print(f"  {'Outcome':<30} {'YES ask':>8} {'YES bid':>8} {'Vol':>10}")
        print(f"  {'-'*60}")

        for m in event_markets:
            title = m.get("yes_sub_title") or m.get("ticker")
            yes_ask = m.get("yes_ask_dollars", "?")
            yes_bid = m.get("yes_bid_dollars", "?")
            vol = int(float(m.get("volume_fp") or 0))
            print(f"  {title:<30} {yes_ask:>8} {yes_bid:>8} {vol:>10,}")

    return edges


# ── CPI Scanner ─────────────────────────────────────────────────────────────

def scan_cpi_markets():
    """Compare Kalshi CPI markets to latest CPI data and nowcast."""
    print(f"\n  {'='*70}")
    print(f"  CPI / INFLATION MARKETS (KXCPI)")
    print(f"  {'='*70}")

    cpi_data = get_latest_cpi()
    if cpi_data:
        print(f"\n  Latest CPI: {cpi_data['value']:.1f} ({cpi_data['date']})")
        print(f"  Month-over-month change: {cpi_data['mom_pct']:.2f}%")

    markets = get_kalshi_markets("KXCPI")
    if not markets:
        print("  No CPI markets found.")
        return []

    # Group by event
    events = {}
    for m in markets:
        et = m.get("event_ticker", "")
        events.setdefault(et, []).append(m)

    edges = []
    for event_ticker in sorted(events.keys()):
        event_markets = events[event_ticker]
        event_markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))

        print(f"\n  Event: {event_ticker}")
        print(f"  Close: {event_markets[0].get('close_time', '')[:10]}")
        rules = (event_markets[0].get('rules_primary') or '')[:200]
        print(f"  Rules: {rules}")
        print(f"  {'Bracket':<25} {'YES ask':>8} {'YES bid':>8} {'Vol':>10}")
        print(f"  {'-'*55}")

        for m in event_markets:
            st = m.get("strike_type", "")
            floor = m.get("floor_strike")
            cap = m.get("cap_strike")
            title = m.get("yes_sub_title") or ""

            if st == "less" and cap is not None:
                label = f"Below {cap}%"
            elif st == "greater" and floor is not None:
                label = f"Above {floor}%"
            elif st == "between" and floor is not None and cap is not None:
                label = f"{floor}% to {cap}%"
            else:
                label = title[:25]

            yes_ask = m.get("yes_ask_dollars", "?")
            yes_bid = m.get("yes_bid_dollars", "?")
            vol = int(float(m.get("volume_fp") or 0))
            print(f"  {label:<25} {yes_ask:>8} {yes_bid:>8} {vol:>10,}")

    return edges


# ── GDP Scanner ─────────────────────────────────────────────────────────────

def scan_gdp_markets():
    """Compare Kalshi GDP markets to GDPNow."""
    print(f"\n  {'='*70}")
    print(f"  GDP MARKETS (KXGDP)")
    print(f"  {'='*70}")

    gdpnow, gdp_date = get_gdpnow()
    if gdpnow:
        print(f"\n  GDPNow estimate: {gdpnow:+.1f}% (updated {gdp_date})")
    else:
        print(f"\n  GDPNow: unavailable (set FRED_API_KEY)")

    markets = get_kalshi_markets("KXGDP")
    if not markets:
        print("  No GDP markets found.")
        return []

    # Group by event
    events = {}
    for m in markets:
        et = m.get("event_ticker", "")
        events.setdefault(et, []).append(m)

    edges = []
    for event_ticker in sorted(events.keys()):
        event_markets = events[event_ticker]
        event_markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))

        print(f"\n  Event: {event_ticker}")
        print(f"  Close: {event_markets[0].get('close_time', '')[:10]}")
        rules = (event_markets[0].get('rules_primary') or '')[:200]
        print(f"  Rules: {rules}")
        print(f"  {'Bracket':<25} {'YES ask':>8} {'Model%':>8} {'Edge':>7}")
        print(f"  {'-'*55}")

        for m in event_markets:
            st = m.get("strike_type", "")
            floor = m.get("floor_strike")
            cap = m.get("cap_strike")

            if st == "less" and cap is not None:
                label = f"Below {cap}%"
            elif st == "greater" and floor is not None:
                label = f"Above {floor}%"
            elif st == "between" and floor is not None and cap is not None:
                label = f"{floor}% to {cap}%"
            else:
                label = m.get("yes_sub_title", "")[:25]

            yes_ask = m.get("yes_ask_dollars", "?")
            yes_bid = m.get("yes_bid_dollars", "?")

            # Calculate model probability if we have GDPNow
            model_pct = ""
            edge = ""
            if gdpnow and yes_ask != "?":
                spread = 0.8  # GDPNow typical ±0.8% error near release
                floor_val = float(floor) if floor is not None else -999
                cap_val = float(cap) if cap is not None else 999

                def ncdf(x, mu, s):
                    return 0.5 * (1.0 + erf((x - mu) / (s * sqrt(2.0))))

                p_below_cap = ncdf(cap_val, gdpnow, spread) if cap_val < 900 else 1.0
                p_below_floor = ncdf(floor_val, gdpnow, spread) if floor_val > -900 else 0.0
                prob = p_below_cap - p_below_floor
                model_pct = f"{prob:.0%}"

                mkt_prob = float(yes_ask)
                e = (prob - mkt_prob) * 100
                edge = f"{e:+.1f}%"

                if abs(e) > MIN_EDGE_PCT:
                    signal = "BUY YES" if e > 0 else "BUY NO"
                    edges.append({
                        "ticker": m["ticker"],
                        "label": label,
                        "signal": signal,
                        "model_prob": prob,
                        "market_prob": mkt_prob,
                        "edge": e,
                    })

            print(f"  {label:<25} {yes_ask:>8} {model_pct:>8} {edge:>7}")

    return edges


# ── PCE Scanner ─────────────────────────────────────────────────────────────

def scan_pce_markets():
    """Scan Core PCE markets."""
    print(f"\n  {'='*70}")
    print(f"  CORE PCE MARKETS (KXPCECORE)")
    print(f"  {'='*70}")

    markets = get_kalshi_markets("KXPCECORE")
    if not markets:
        print("  No Core PCE markets found.")
        return []

    events = {}
    for m in markets:
        et = m.get("event_ticker", "")
        events.setdefault(et, []).append(m)

    for event_ticker in sorted(events.keys()):
        event_markets = events[event_ticker]
        event_markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))

        print(f"\n  Event: {event_ticker}")
        print(f"  Close: {event_markets[0].get('close_time', '')[:10]}")
        print(f"  {'Bracket':<25} {'YES ask':>8} {'Vol':>10}")
        print(f"  {'-'*45}")

        for m in event_markets:
            title = m.get("yes_sub_title") or m.get("ticker")
            yes_ask = m.get("yes_ask_dollars", "?")
            vol = int(float(m.get("volume_fp") or 0))
            print(f"  {title:<25} {yes_ask:>8} {vol:>10,}")

    return []


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)
    print(f"\n{'#'*74}")
    print(f"  ECONOMIC DATA SCANNER — Nowcasts vs Kalshi Markets")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'#'*74}")

    fred_key = os.environ.get("FRED_API_KEY")
    if not fred_key:
        print(f"\n  ⚠ FRED_API_KEY not set. Get one free at:")
        print(f"    https://fred.stlouisfed.org/docs/api/api_key.html")
        print(f"  Running without nowcast data (market prices only).\n")

    mode = sys.argv[1] if len(sys.argv) > 1 else "--all"
    all_edges = []

    if mode in ("--all", "--fed"):
        all_edges.extend(scan_fed_markets())
    if mode in ("--all", "--cpi"):
        all_edges.extend(scan_cpi_markets())
    if mode in ("--all", "--gdp"):
        all_edges.extend(scan_gdp_markets())
    if mode in ("--all", "--pce"):
        all_edges.extend(scan_pce_markets())

    # Summary
    if all_edges:
        print(f"\n{'='*74}")
        print(f"  EDGES FOUND")
        print(f"{'='*74}\n")
        all_edges.sort(key=lambda x: abs(x["edge"]), reverse=True)
        for e in all_edges[:10]:
            print(f"  {e['signal']} — {e['label']}")
            print(f"    Model: {e['model_prob']:.0%} | Market: {e['market_prob']:.0%} | Edge: {e['edge']:+.1f}%")
            print(f"    Ticker: {e['ticker']}")
            print()

    print(f"\n  Mode: PRACTICE — no trades placed.\n")


if __name__ == "__main__":
    main()
