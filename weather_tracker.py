#!/usr/bin/env python3
"""Weather Market Practice Tracker

Logs NWS forecasts, Kalshi market prices, and actual CLI outcomes
to build a dataset for finding real edges. No trading — just learning.

Run twice daily:
  1. Morning (8-9 AM ET): captures forecasts + market prices
  2. Next morning: captures yesterday's CLI actuals and scores predictions

Usage:
  python3 weather_tracker.py forecast    # log today's forecasts + market prices
  python3 weather_tracker.py results     # log yesterday's CLI actuals + score accuracy
  python3 weather_tracker.py report      # show accuracy report across all tracked days
"""

import csv
import json
import sys
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent / "practice_data"
FORECAST_LOG = DATA_DIR / "forecasts.csv"
RESULTS_LOG = DATA_DIR / "results.csv"

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
NWS_BASE = "https://api.weather.gov"
NWS_HEADERS = {"User-Agent": "KalshiWeatherTracker/1.0 (practice@example.com)"}

# Exact settlement stations
STATIONS = {
    "KXHIGHNY": {
        "city": "NYC",
        "station_id": "KNYC",
        "lat": 40.7812, "lon": -73.9665,
        "cli_code": "NYC",
        "type": "high_temp",
    },
    "KXHIGHCHI": {
        "city": "Chicago",
        "station_id": "KMDW",
        "lat": 41.7868, "lon": -87.7522,
        "cli_code": "MDW",
        "type": "high_temp",
    },
    "KXHIGHMIA": {
        "city": "Miami",
        "station_id": "KMIA",
        "lat": 25.7959, "lon": -80.2870,
        "cli_code": "MIA",
        "type": "high_temp",
    },
    "KXHIGHAUS": {
        "city": "Austin",
        "station_id": "KAUS",
        "lat": 30.1945, "lon": -97.6699,
        "cli_code": "AUS",
        "type": "high_temp",
    },
    "KXHIGHDEN": {
        "city": "Denver",
        "station_id": "KDEN",
        "lat": 39.8561, "lon": -104.6737,
        "cli_code": "DEN",
        "type": "high_temp",
    },
    "KXRAINNYC": {
        "city": "NYC",
        "station_id": "KNYC",
        "lat": 40.7812, "lon": -73.9665,
        "cli_code": "NYC",
        "type": "rain",
    },
}


# ── NWS Data ────────────────────────────────────────────────────────────────

def get_nws_daily_forecast(lat, lon):
    """Get the daily forecast high temp from NWS."""
    grid = requests.get(
        f"{NWS_BASE}/points/{lat},{lon}",
        headers=NWS_HEADERS, timeout=15
    ).json()["properties"]
    url = f"{NWS_BASE}/gridpoints/{grid['gridId']}/{grid['gridX']},{grid['gridY']}/forecast"
    resp = requests.get(url, headers=NWS_HEADERS, timeout=15)
    return resp.json()["properties"]["periods"]


def get_nws_hourly_forecast(lat, lon):
    """Get hourly forecast from NWS."""
    grid = requests.get(
        f"{NWS_BASE}/points/{lat},{lon}",
        headers=NWS_HEADERS, timeout=15
    ).json()["properties"]
    url = f"{NWS_BASE}/gridpoints/{grid['gridId']}/{grid['gridX']},{grid['gridY']}/forecast/hourly"
    resp = requests.get(url, headers=NWS_HEADERS, timeout=15)
    return resp.json()["properties"]["periods"]


def get_station_observations(station_id, limit=50):
    """Get recent observations from a specific ASOS station."""
    resp = requests.get(
        f"{NWS_BASE}/stations/{station_id}/observations?limit={limit}",
        headers=NWS_HEADERS, timeout=15
    )
    return resp.json().get("features", [])


def get_hourly_peak(lat, lon, target_date):
    """Get the peak temperature from hourly forecast for a specific date."""
    periods = get_nws_hourly_forecast(lat, lon)
    peak = None
    peak_hour = None
    precip_max = 0
    for p in periods:
        if target_date in p["startTime"]:
            temp = p["temperature"]
            precip = p.get("probabilityOfPrecipitation", {}).get("value") or 0
            precip_max = max(precip_max, precip)
            if peak is None or temp > peak:
                peak = temp
                peak_hour = p["startTime"][11:16]
    return peak, peak_hour, precip_max


def get_current_station_max(station_id, target_date):
    """Get the max temperature observed so far today at a station."""
    obs = get_station_observations(station_id)
    max_c = None
    max_f = None
    for f in obs:
        p = f["properties"]
        ts = p.get("timestamp", "")
        if target_date not in ts:
            continue
        tc = p.get("temperature", {}).get("value")
        if tc is not None:
            if max_c is None or tc > max_c:
                max_c = tc
                max_f = tc * 9 / 5 + 32
    return max_c, max_f


# ── Kalshi Data ─────────────────────────────────────────────────────────────

def get_kalshi_markets(series_ticker, target_date_code):
    """Fetch Kalshi markets for a series, filtered to a specific date."""
    params = {"series_ticker": series_ticker, "status": "open", "limit": 200}
    resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
    markets = resp.json().get("markets", [])
    # Filter to target date
    return [m for m in markets if target_date_code in m.get("event_ticker", "")]


def format_date_code(dt):
    """Convert date to Kalshi event ticker format: 26MAR31"""
    return dt.strftime("%y") + dt.strftime("%b").upper() + dt.strftime("%d")


# ── Forecast Logging ────────────────────────────────────────────────────────

def log_forecasts():
    """Log today's NWS forecasts and Kalshi market prices."""
    DATA_DIR.mkdir(exist_ok=True)

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    date_code = format_date_code(now)

    print(f"\n{'='*80}")
    print(f"  WEATHER TRACKER — FORECAST LOG")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*80}\n")

    rows = []

    for series, info in STATIONS.items():
        city = info["city"]
        station_id = info["station_id"]

        try:
            if info["type"] == "high_temp":
                # Get NWS daily forecast
                daily = get_nws_daily_forecast(info["lat"], info["lon"])
                daily_high = None
                for p in daily:
                    if p.get("isDaytime") and today in p.get("startTime", ""):
                        daily_high = p["temperature"]
                        break

                # Get hourly peak
                hourly_peak, peak_hour, precip_max = get_hourly_peak(
                    info["lat"], info["lon"], today
                )

                # Get current station observation
                max_c, max_f = get_current_station_max(station_id, today)

                # Get Kalshi markets
                markets = get_kalshi_markets(series, date_code)
                market_data = {}
                for m in markets:
                    st = m.get("strike_type")
                    floor = m.get("floor_strike")
                    cap = m.get("cap_strike")
                    if st == "less":
                        label = f"below_{cap}"
                    elif st == "greater":
                        label = f"above_{floor}"
                    elif st == "between":
                        label = f"{floor}_{cap}"
                    else:
                        continue
                    market_data[label] = {
                        "ticker": m["ticker"],
                        "yes_ask": m.get("yes_ask_dollars"),
                        "yes_bid": m.get("yes_bid_dollars"),
                        "no_ask": m.get("no_ask_dollars"),
                    }

                # Determine which bracket the forecast falls in
                forecast_bracket = None
                for m in markets:
                    st = m.get("strike_type")
                    floor = float(m.get("floor_strike") or 0)
                    cap = float(m.get("cap_strike") or 999)
                    if hourly_peak is not None:
                        if st == "less" and hourly_peak < cap:
                            forecast_bracket = m["ticker"]
                        elif st == "greater" and hourly_peak > floor:
                            forecast_bracket = m["ticker"]
                        elif st == "between" and floor <= hourly_peak <= cap:
                            forecast_bracket = m["ticker"]

                row = {
                    "date": today,
                    "log_time": now.isoformat(),
                    "series": series,
                    "city": city,
                    "station": station_id,
                    "type": "high_temp",
                    "nws_daily_high": daily_high,
                    "nws_hourly_peak": hourly_peak,
                    "nws_peak_hour": peak_hour,
                    "nws_precip_max": precip_max,
                    "current_max_c": f"{max_c:.1f}" if max_c else "",
                    "current_max_f": f"{max_f:.1f}" if max_f else "",
                    "market_prices": json.dumps(market_data),
                    "forecast_bracket": forecast_bracket,
                }
                rows.append(row)

                print(f"  {city} ({station_id}):")
                print(f"    NWS daily high: {daily_high}°F")
                print(f"    NWS hourly peak: {hourly_peak}°F at {peak_hour}")
                print(f"    Current station max: {max_f:.0f}°F ({max_c:.1f}°C)" if max_f else "    No obs yet")
                print(f"    Max precip chance: {precip_max}%")
                print(f"    Forecast bracket: {forecast_bracket}")
                print(f"    Brackets:")
                for label, md in sorted(market_data.items()):
                    print(f"      {label:<15} YES:{md['yes_ask']} bid:{md['yes_bid']}")
                print()

            elif info["type"] == "rain":
                markets = get_kalshi_markets(series, date_code)
                if markets:
                    m = markets[0]
                    row = {
                        "date": today,
                        "log_time": now.isoformat(),
                        "series": series,
                        "city": city,
                        "station": station_id,
                        "type": "rain",
                        "nws_daily_high": "",
                        "nws_hourly_peak": "",
                        "nws_peak_hour": "",
                        "nws_precip_max": "",
                        "current_max_c": "",
                        "current_max_f": "",
                        "market_prices": json.dumps({
                            "rain_yes": {
                                "ticker": m["ticker"],
                                "yes_ask": m.get("yes_ask_dollars"),
                                "yes_bid": m.get("yes_bid_dollars"),
                            }
                        }),
                        "forecast_bracket": "",
                    }
                    rows.append(row)

            time.sleep(0.3)  # be polite to NWS

        except Exception as e:
            print(f"  {city}: Error — {e}\n")

    # Write to CSV
    file_exists = FORECAST_LOG.exists()
    with open(FORECAST_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"  Logged {len(rows)} forecasts to {FORECAST_LOG}\n")


# ── Results Logging ─────────────────────────────────────────────────────────

def log_results():
    """Check yesterday's CLI reports and score our forecasts."""
    DATA_DIR.mkdir(exist_ok=True)

    now = datetime.now(timezone.utc)
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n{'='*80}")
    print(f"  WEATHER TRACKER — RESULTS FOR {yesterday}")
    print(f"{'='*80}\n")

    # Read yesterday's forecasts
    if not FORECAST_LOG.exists():
        print("  No forecast data yet. Run 'forecast' first.\n")
        return

    forecasts = []
    with open(FORECAST_LOG, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["date"] == yesterday:
                forecasts.append(row)

    if not forecasts:
        print(f"  No forecasts found for {yesterday}.\n")
        return

    rows = []
    for fc in forecasts:
        if fc["type"] != "high_temp":
            continue

        series = fc["series"]
        info = STATIONS.get(series)
        if not info:
            continue

        # Get the actual CLI high from station observations
        # Use the max observation from yesterday
        max_c, max_f = get_current_station_max(info["station_id"], yesterday)

        # Determine which bracket the actual fell in
        market_data = json.loads(fc.get("market_prices", "{}"))
        actual_bracket = None

        # Check settled markets
        date_code_yest = format_date_code(datetime.fromisoformat(yesterday))
        try:
            params = {"series_ticker": series, "limit": 200}
            resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
            all_markets = resp.json().get("markets", [])
            for m in all_markets:
                if date_code_yest in m.get("event_ticker", ""):
                    if m.get("result") == "yes":
                        actual_bracket = m["ticker"]
                        break
        except Exception:
            pass

        hourly_peak = fc.get("nws_hourly_peak", "")
        daily_high = fc.get("nws_daily_high", "")
        forecast_bracket = fc.get("forecast_bracket", "")

        # Calculate errors
        hourly_error = ""
        daily_error = ""
        if max_f and hourly_peak:
            hourly_error = f"{max_f - float(hourly_peak):.1f}"
        if max_f and daily_high:
            daily_error = f"{max_f - float(daily_high):.1f}"

        bracket_correct = ""
        if forecast_bracket and actual_bracket:
            bracket_correct = "YES" if forecast_bracket == actual_bracket else "NO"

        row = {
            "date": yesterday,
            "city": fc["city"],
            "station": fc["station"],
            "nws_daily_high": daily_high,
            "nws_hourly_peak": hourly_peak,
            "actual_max_c": f"{max_c:.1f}" if max_c else "",
            "actual_max_f": f"{max_f:.1f}" if max_f else "",
            "daily_error_f": daily_error,
            "hourly_error_f": hourly_error,
            "forecast_bracket": forecast_bracket,
            "actual_bracket": actual_bracket or "",
            "bracket_correct": bracket_correct,
            "market_prices": fc.get("market_prices", ""),
        }
        rows.append(row)

        print(f"  {fc['city']} ({fc['station']}):")
        print(f"    NWS daily forecast: {daily_high}°F")
        print(f"    NWS hourly peak:    {hourly_peak}°F")
        print(f"    Actual CLI high:    {max_f:.0f}°F ({max_c:.1f}°C)" if max_f else "    Actual: unknown")
        print(f"    Daily error:        {daily_error}°F")
        print(f"    Hourly error:       {hourly_error}°F")
        print(f"    Forecast bracket:   {forecast_bracket}")
        print(f"    Actual bracket:     {actual_bracket or 'unknown'}")
        print(f"    Bracket correct:    {bracket_correct}")
        print()

    if rows:
        file_exists = RESULTS_LOG.exists()
        with open(RESULTS_LOG, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
        print(f"  Logged {len(rows)} results to {RESULTS_LOG}\n")


# ── Accuracy Report ─────────────────────────────────────────────────────────

def show_report():
    """Show accuracy report across all tracked days."""
    if not RESULTS_LOG.exists():
        print("  No results data yet. Run 'results' first.\n")
        return

    print(f"\n{'='*80}")
    print(f"  WEATHER TRACKER — ACCURACY REPORT")
    print(f"{'='*80}\n")

    with open(RESULTS_LOG, "r") as f:
        reader = csv.DictReader(f)
        results = list(reader)

    if not results:
        print("  No results recorded yet.\n")
        return

    # Stats by city
    cities = {}
    for r in results:
        city = r["city"]
        if city not in cities:
            cities[city] = {
                "daily_errors": [],
                "hourly_errors": [],
                "bracket_correct": 0,
                "bracket_total": 0,
            }

        if r.get("daily_error_f"):
            cities[city]["daily_errors"].append(float(r["daily_error_f"]))
        if r.get("hourly_error_f"):
            cities[city]["hourly_errors"].append(float(r["hourly_error_f"]))
        if r.get("bracket_correct"):
            cities[city]["bracket_total"] += 1
            if r["bracket_correct"] == "YES":
                cities[city]["bracket_correct"] += 1

    for city, stats in sorted(cities.items()):
        print(f"  {city}:")

        if stats["daily_errors"]:
            errors = stats["daily_errors"]
            avg = sum(errors) / len(errors)
            abs_avg = sum(abs(e) for e in errors) / len(errors)
            print(f"    Daily forecast bias:  {avg:+.1f}°F avg (positive = station runs hot)")
            print(f"    Daily forecast MAE:   {abs_avg:.1f}°F")

        if stats["hourly_errors"]:
            errors = stats["hourly_errors"]
            avg = sum(errors) / len(errors)
            abs_avg = sum(abs(e) for e in errors) / len(errors)
            print(f"    Hourly forecast bias: {avg:+.1f}°F avg")
            print(f"    Hourly forecast MAE:  {abs_avg:.1f}°F")

        if stats["bracket_total"] > 0:
            pct = stats["bracket_correct"] / stats["bracket_total"] * 100
            print(f"    Bracket accuracy:     {stats['bracket_correct']}/{stats['bracket_total']} ({pct:.0f}%)")

        print(f"    Days tracked:         {len(stats['daily_errors'])}")
        print()

    total_correct = sum(s["bracket_correct"] for s in cities.values())
    total_tracked = sum(s["bracket_total"] for s in cities.values())
    if total_tracked:
        print(f"  OVERALL BRACKET ACCURACY: {total_correct}/{total_tracked} ({total_correct/total_tracked*100:.0f}%)")
    print(f"  TOTAL DAYS TRACKED: {len(set(r['date'] for r in results))}")
    print()


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 weather_tracker.py [forecast|results|report]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "forecast":
        log_forecasts()
    elif cmd == "results":
        log_results()
    elif cmd == "report":
        show_report()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python3 weather_tracker.py [forecast|results|report]")
        sys.exit(1)


if __name__ == "__main__":
    main()
