#!/usr/bin/env python3
"""Weather Market Model v2 — Bias-Corrected Multi-Model Ensemble

Pulls GFS ensemble + HRRR + NBM from Open-Meteo, applies station bias
corrections, and produces bracket probabilities for each city.
Compares to Kalshi market prices to identify real edges.

No trading — practice/analysis only.

Usage:
    python3 model_v2.py              # today's analysis
    python3 model_v2.py --backtest   # score against known CLI actuals
"""

import json
import sys
import time
import requests
from datetime import datetime, timezone, timedelta
from math import erf, sqrt

from station_bias import BIAS_TABLE, correct_gfs_forecast

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Station coordinates (exact settlement stations)
STATIONS = {
    "KXHIGHNY":  {"city": "NYC",     "lat": 40.7812, "lon": -73.9665, "tz": "America/New_York"},
    "KXHIGHCHI": {"city": "Chicago",  "lat": 41.7868, "lon": -87.7522, "tz": "America/Chicago"},
    "KXHIGHMIA": {"city": "Miami",    "lat": 25.7959, "lon": -80.2870, "tz": "America/New_York"},
    "KXHIGHAUS": {"city": "Austin",   "lat": 30.1945, "lon": -97.6699, "tz": "America/Chicago"},
    "KXHIGHDEN": {"city": "Denver",   "lat": 39.8561, "lon": -104.6737, "tz": "America/Denver"},
}


def normal_cdf(x, mu, sigma):
    return 0.5 * (1.0 + erf((x - mu) / (sigma * sqrt(2.0))))


def bracket_prob(temp, spread, floor, cap):
    """Probability that actual temp falls between floor and cap."""
    p_below_cap = normal_cdf(cap, temp, spread) if cap < 900 else 1.0
    p_below_floor = normal_cdf(floor, temp, spread) if floor > -900 else 0.0
    return max(0, p_below_cap - p_below_floor)


# ── Data Fetching ───────────────────────────────────────────────────────────

def fetch_gfs_ensemble(lat, lon, tz, days=2):
    """Fetch GFS 31-member ensemble from Open-Meteo."""
    resp = requests.get("https://ensemble-api.open-meteo.com/v1/ensemble", params={
        "latitude": lat, "longitude": lon,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": tz,
        "forecast_days": days,
        "models": "gfs_seamless",
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_hrrr_nbm(lat, lon, tz, days=2):
    """Fetch HRRR and NBM forecasts from Open-Meteo."""
    results = {}
    for model in ["gfs_hrrr", "ncep_nbm_conus"]:
        try:
            resp = requests.get("https://api.open-meteo.com/v1/forecast", params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max",
                "hourly": "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone": tz,
                "forecast_days": days,
                "models": model,
            }, timeout=30)
            resp.raise_for_status()
            results[model] = resp.json()
        except Exception as e:
            print(f"    Warning: {model} fetch failed: {e}")
    return results


def fetch_kalshi_brackets(series, date_code):
    """Fetch Kalshi market brackets for a series/date."""
    params = {"series_ticker": series, "status": "open", "limit": 200}
    resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
    markets = resp.json().get("markets", [])
    # Filter to target date
    today_markets = [m for m in markets if date_code in m.get("event_ticker", "")]
    # Sort by strike
    today_markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))
    return today_markets


def fetch_current_station_temp(station_id):
    """Get current max temp from ASOS station."""
    try:
        resp = requests.get(
            f"https://mesonet.agron.iastate.edu/json/current.py?station={station_id[1:]}&network={get_network(station_id)}",
            timeout=15,
        )
        data = resp.json()
        return data.get("last_ob", {}).get("max_dayairtemp[F]")
    except Exception:
        return None


def get_network(station_id):
    networks = {"KNYC": "NY_ASOS", "KMDW": "IL_ASOS", "KMIA": "FL_ASOS",
                "KAUS": "TX_ASOS", "KDEN": "CO_ASOS"}
    return networks.get(station_id, "")


def fetch_cli_actual(date_str):
    """Fetch CLI actuals from IEM for a specific date."""
    try:
        resp = requests.get(
            f"https://mesonet.agron.iastate.edu/geojson/cli.py?dt={date_str}",
            timeout=15,
        )
        data = resp.json()
        results = {}
        station_map = {"KNYC": "NYC", "KMDW": "MDW", "KMIA": "MIA",
                       "KAUS": "AUS", "KDEN": "DEN"}
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            station = props.get("station", "")
            for kalshi_id, cli_id in station_map.items():
                if station.endswith(cli_id) or station == cli_id:
                    results[kalshi_id] = props.get("high")
        return results
    except Exception:
        return {}


# ── Analysis ────────────────────────────────────────────────────────────────

def analyze_city(series, info, date_code, target_date):
    """Full analysis for one city."""
    city = info["city"]
    lat, lon, tz = info["lat"], info["lon"], info["tz"]
    bias_info = BIAS_TABLE.get(series, {})
    station_id = bias_info.get("station", "").split("(")[-1].rstrip(")").strip()

    print(f"\n  {'='*70}")
    print(f"  {city} ({series})")
    print(f"  {'='*70}")

    # 1. GFS Ensemble
    try:
        ens_data = fetch_gfs_ensemble(lat, lon, tz)
        dates = ens_data["daily"]["time"]
        day_idx = dates.index(target_date) if target_date in dates else 0

        members = []
        for key in ens_data["daily"]:
            if key.startswith("temperature_2m_max") and "member" in key:
                val = ens_data["daily"][key][day_idx]
                if val is not None:
                    members.append(val)

        ens_mean = sum(members) / len(members) if members else None
        ens_min = min(members) if members else None
        ens_max = max(members) if members else None
        ens_spread = (sum((m - ens_mean)**2 for m in members) / len(members))**0.5 if members else 3.0

        print(f"  GFS Ensemble ({len(members)} members):")
        print(f"    Raw mean: {ens_mean:.1f}°F  range: {ens_min:.0f}-{ens_max:.0f}°F  spread: {ens_spread:.1f}°F")
    except Exception as e:
        print(f"  GFS Ensemble: Error — {e}")
        ens_mean = None
        ens_spread = 3.0

    # 2. HRRR + NBM
    hrrr_max = None
    nbm_max = None
    try:
        model_data = fetch_hrrr_nbm(lat, lon, tz)
        for model_name, data in model_data.items():
            if "daily" in data and "temperature_2m_max" in data["daily"]:
                dates = data["daily"]["time"]
                day_idx = dates.index(target_date) if target_date in dates else 0
                val = data["daily"]["temperature_2m_max"][day_idx]
                if model_name == "gfs_hrrr":
                    hrrr_max = val
                elif model_name == "ncep_nbm_conus":
                    nbm_max = val

        if hrrr_max:
            print(f"  HRRR: {hrrr_max:.1f}°F")
        if nbm_max:
            print(f"  NBM:  {nbm_max:.1f}°F")
    except Exception as e:
        print(f"  HRRR/NBM: Error — {e}")

    # 3. Bias correction
    if ens_mean is not None:
        correction = correct_gfs_forecast(series, ens_mean)
        corrected = correction["corrected"]
        adj_spread = max(ens_spread, correction["confidence_1sigma"])

        print(f"\n  Bias Correction ({correction['method']}):")
        print(f"    Raw GFS: {ens_mean:.1f}°F → Corrected: {corrected:.1f}°F "
              f"(bias: {correction['raw_bias']:+.1f}°F)")
        print(f"    Adjusted spread: ±{adj_spread:.1f}°F "
              f"(68% CI: {corrected - adj_spread:.0f}-{corrected + adj_spread:.0f}°F)")
    else:
        corrected = nbm_max or hrrr_max or 75
        adj_spread = 3.0

    # 4. Multi-model blend
    model_temps = [t for t in [corrected, hrrr_max, nbm_max] if t is not None]
    if len(model_temps) > 1:
        blend = sum(model_temps) / len(model_temps)
        print(f"\n  Multi-model blend: {blend:.1f}°F (avg of {len(model_temps)} models)")
    else:
        blend = corrected

    # 5. Current station observation
    station_full = bias_info.get("station", "").split("(")[-1].split(")")[0] if bias_info else ""
    # Use the station ID from our STATIONS-like mapping
    station_ids = {"KXHIGHNY": "KNYC", "KXHIGHCHI": "KMDW", "KXHIGHMIA": "KMIA",
                   "KXHIGHAUS": "KAUS", "KXHIGHDEN": "KDEN"}
    sid = station_ids.get(series, "")
    current_max = fetch_current_station_temp(sid)
    if current_max:
        print(f"  Current station max: {current_max}°F")
        # If current max already exceeds our forecast, adjust
        if current_max > blend:
            print(f"    ⚠ Station already above forecast! Adjusting blend to {current_max}°F")
            blend = max(blend, float(current_max))

    # 6. Kalshi brackets
    markets = fetch_kalshi_brackets(series, date_code)
    if not markets:
        print(f"  No Kalshi markets found for {date_code}")
        return

    print(f"\n  {'Bracket':<15} {'Model%':>7} {'Market%':>8} {'Edge':>7} {'Signal':>8}")
    print(f"  {'-'*55}")

    edges = []
    for m in markets:
        st = m.get("strike_type")
        floor = float(m.get("floor_strike") or -999)
        cap = float(m.get("cap_strike") or 999)
        yes_ask = float(m.get("yes_ask_dollars") or 0)
        no_ask = float(m.get("no_ask_dollars") or 0)
        ticker = m.get("ticker", "")

        if st == "less":
            label = f"< {cap:.0f}°F"
        elif st == "greater":
            label = f"> {floor:.0f}°F"
        elif st == "between":
            label = f"{floor:.0f}-{cap:.0f}°F"
        else:
            continue

        # Model probability
        model_prob = bracket_prob(blend, adj_spread, floor, cap)
        market_prob = yes_ask

        # Edge calculation
        if market_prob > 0.02 and market_prob < 0.98:
            edge_yes = (model_prob - market_prob) * 100
            edge_no = ((1 - model_prob) - no_ask) * 100 if no_ask else 0

            if abs(edge_yes) > abs(edge_no):
                edge = edge_yes
                signal = "BUY YES" if edge > 15 else ("SELL" if edge < -15 else "—")
            else:
                edge = -edge_no
                signal = "BUY NO" if edge_no > 15 else "—"
        else:
            edge = 0
            signal = "—"

        print(f"  {label:<15} {model_prob:>6.0%} {market_prob:>7.0%} {edge:>+6.1f}% {signal:>8}")

        if abs(edge) > 15 and "BUY" in signal:
            edges.append({
                "ticker": ticker,
                "label": label,
                "signal": signal,
                "model_prob": model_prob,
                "market_prob": market_prob,
                "edge": edge,
                "price": yes_ask if "YES" in signal else no_ask,
            })

    return edges


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)
    target_date = now.strftime("%Y-%m-%d")
    date_code = now.strftime("%y") + now.strftime("%b").upper() + now.strftime("%d")

    print(f"\n{'#'*74}")
    print(f"  WEATHER MODEL v2 — BIAS-CORRECTED MULTI-MODEL ENSEMBLE")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Target date: {target_date} ({date_code})")
    print(f"  Mode: PRACTICE — no trades")
    print(f"{'#'*74}")

    all_edges = []

    for series, info in STATIONS.items():
        try:
            edges = analyze_city(series, info, date_code, target_date)
            if edges:
                all_edges.extend(edges)
            time.sleep(0.5)
        except Exception as e:
            print(f"\n  {info['city']}: Error — {e}")

    # Summary
    print(f"\n{'='*74}")
    print(f"  SUMMARY")
    print(f"{'='*74}")

    if all_edges:
        all_edges.sort(key=lambda x: abs(x["edge"]), reverse=True)
        print(f"\n  Potential edges found ({len(all_edges)}):\n")
        for e in all_edges:
            print(f"    {e['signal']} {e['label']} @ {int(e['price']*100)}¢")
            print(f"    Model: {e['model_prob']:.0%} | Market: {e['market_prob']:.0%} | Edge: {e['edge']:+.1f}%")
            print(f"    Ticker: {e['ticker']}")
            print()
    else:
        print("\n  No significant edges found. Markets look fairly priced.\n")

    print(f"  REMINDER: This is practice mode. Track results before betting.\n")


if __name__ == "__main__":
    main()
