#!/usr/bin/env python3
"""Kalshi Weather Trading Assistant

Fetches active weather markets from Kalshi, compares odds against
NWS (weather.gov) forecast data, and recommends trades where there's
a meaningful edge. Supports an approval workflow for placing trades.
"""

import os
import sys
import csv
import json
import time
import base64
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ── Config ──────────────────────────────────────────────────────────────────

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
NWS_BASE = "https://api.weather.gov"
NWS_HEADERS = {"User-Agent": "KalshiWeatherBot/1.0 (contact@example.com)"}

# Minimum edge (%) to flag a recommendation
MIN_EDGE_PCT = 10

# Max contracts per trade
DEFAULT_CONTRACTS = 10

# Trade log file
TRADE_LOG = Path(__file__).parent / "trade_log.csv"

# Weather series we track
# Coordinates match the EXACT NWS settlement stations used by Kalshi:
#   NYC = Central Park (KNYC), Chicago = Midway (KMDW), Miami = MIA Airport (KMIA)
#   Austin = Bergstrom Airport (KAUS), Denver = Denver Intl (KDEN)
# spread = typical NWS forecast error (°F std dev) for next-day high temps
# Coastal/stable climates (Miami) have lower spread; continental (Denver, Chicago) higher
# cli_site = NWS CLI product issuer code for fetching the actual settlement report
WEATHER_SERIES = {
    "KXHIGHNY":  {"city": "New York (Central Park)",  "lat": 40.7812, "lon": -73.9665, "type": "high_temp", "spread": 3.0, "cli_site": "NYC"},
    "KXHIGHCHI": {"city": "Chicago (Midway)",          "lat": 41.7868, "lon": -87.7522, "type": "high_temp", "spread": 3.5, "cli_site": "MDW"},
    "KXHIGHMIA": {"city": "Miami (MIA Airport)",       "lat": 25.7959, "lon": -80.2870, "type": "high_temp", "spread": 2.0, "cli_site": "MIA"},
    "KXHIGHAUS": {"city": "Austin (Bergstrom)",        "lat": 30.1945, "lon": -97.6699, "type": "high_temp", "spread": 3.0, "cli_site": "AUS"},
    "KXHIGHDEN": {"city": "Denver",                    "lat": 39.8561, "lon": -104.6737, "type": "high_temp", "spread": 4.0, "cli_site": "DEN"},
    "KXRAINNYC": {"city": "New York (Central Park)",   "lat": 40.7812, "lon": -73.9665, "type": "rain",      "cli_site": "NYC"},
}


# ── Kalshi Auth ─────────────────────────────────────────────────────────────

class KalshiClient:
    def __init__(self, api_key_id: str, private_key_path: str):
        self.api_key_id = api_key_id
        self.base = KALSHI_BASE
        with open(private_key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, timestamp_ms: int, method: str, path: str) -> str:
        message = f"{timestamp_ms}{method}{path}".encode()
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _headers(self, method: str, path: str) -> dict:
        # Signature must use full path including /trade-api/v2 prefix
        full_path = f"/trade-api/v2{path}"
        ts = int(time.time() * 1000)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts),
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, full_path),
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: dict = None) -> dict:
        url = f"{self.base}{path}"
        resp = requests.get(url, headers=self._headers("GET", path), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, body: dict) -> dict:
        url = f"{self.base}{path}"
        resp = requests.post(url, headers=self._headers("POST", path), json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()


# ── Weather.gov Forecasts ───────────────────────────────────────────────────

def get_nws_grid(lat: float, lon: float) -> dict:
    """Get NWS grid office and coordinates for a lat/lon."""
    resp = requests.get(
        f"{NWS_BASE}/points/{lat},{lon}",
        headers=NWS_HEADERS, timeout=15,
    )
    resp.raise_for_status()
    props = resp.json()["properties"]
    return {
        "office": props["gridId"],
        "grid_x": props["gridX"],
        "grid_y": props["gridY"],
    }


def get_nws_forecast(lat: float, lon: float) -> dict:
    """Fetch daily forecast periods from NWS."""
    grid = get_nws_grid(lat, lon)
    url = f"{NWS_BASE}/gridpoints/{grid['office']}/{grid['grid_x']},{grid['grid_y']}/forecast"
    resp = requests.get(url, headers=NWS_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()["properties"]["periods"]


def get_nws_gridpoint_data(lat: float, lon: float) -> dict:
    """Fetch raw gridpoint data (quantitative forecasts) from NWS."""
    grid = get_nws_grid(lat, lon)
    url = f"{NWS_BASE}/gridpoints/{grid['office']}/{grid['grid_x']},{grid['grid_y']}"
    resp = requests.get(url, headers=NWS_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()["properties"]


def c_to_f(celsius: float) -> float:
    return celsius * 9.0 / 5.0 + 32.0


def parse_nws_max_temps(gridpoint_data: dict, target_date: str) -> list[float]:
    """Extract max temperature forecasts (in °F) for target_date (YYYY-MM-DD).

    The gridpoint data may contain multiple maxTemperature entries for the same
    date — short-duration entries (PT1H) are overnight carryovers from the
    previous day. We want the daytime entry (typically PT12H or PT13H) which
    represents the actual forecast high.
    """
    temps = []
    for entry in gridpoint_data.get("maxTemperature", {}).get("values", []):
        valid_time = entry["validTime"]  # e.g. "2026-03-31T14:00:00+00:00/PT13H"
        if target_date not in valid_time or entry["value"] is None:
            continue
        # Skip short-duration entries (PT1H, PT2H) — these are overnight carryovers
        # The real daytime forecast has duration >= PT6H
        duration = valid_time.split("/")[-1] if "/" in valid_time else ""
        if duration in ("PT1H", "PT2H", "PT3H"):
            continue
        temps.append(c_to_f(entry["value"]))
    return temps


def parse_nws_precip_prob(gridpoint_data: dict, target_date: str) -> list[float]:
    """Extract precipitation probability values for target_date."""
    probs = []
    for entry in gridpoint_data.get("probabilityOfPrecipitation", {}).get("values", []):
        valid_time = entry["validTime"]
        if target_date in valid_time and entry["value"] is not None:
            probs.append(entry["value"])
    return probs


# ── Market Analysis ─────────────────────────────────────────────────────────

def fetch_weather_markets(client: KalshiClient) -> list[dict]:
    """Fetch all open weather markets."""
    all_markets = []
    for series_ticker in WEATHER_SERIES:
        cursor = None
        while True:
            params = {"series_ticker": series_ticker, "status": "open", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            data = client.get("/markets", params)
            batch = data.get("markets", [])
            all_markets.extend(batch)
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
            time.sleep(0.05)
    return all_markets


def fetch_weather_markets_public() -> list[dict]:
    """Fetch weather markets without authentication (public endpoint)."""
    all_markets = []
    for series_ticker in WEATHER_SERIES:
        cursor = None
        while True:
            params = {"series_ticker": series_ticker, "status": "open", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("markets", [])
            all_markets.extend(batch)
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
            time.sleep(0.05)
    return all_markets


def group_markets_by_event(markets: list[dict]) -> dict[str, list[dict]]:
    """Group markets by their event_ticker."""
    events = {}
    for m in markets:
        et = m.get("event_ticker", "")
        events.setdefault(et, []).append(m)
    return events


def estimate_temp_probability(forecast_temp_f: float, floor: float, cap: float,
                               strike_type: str, spread: float = 3.0) -> float:
    """Estimate probability that actual temp falls in a bracket.

    Uses a simple normal distribution approximation. NWS forecasts have
    roughly ±3°F typical error for next-day high temps.
    """
    from math import erf, sqrt

    def normal_cdf(x, mu, sigma):
        return 0.5 * (1.0 + erf((x - mu) / (sigma * sqrt(2.0))))

    mu = forecast_temp_f
    sigma = spread

    if strike_type == "less":
        # below cap
        return normal_cdf(cap, mu, sigma)
    elif strike_type == "greater":
        # above floor
        return 1.0 - normal_cdf(floor, mu, sigma)
    elif strike_type == "between":
        return normal_cdf(cap, mu, sigma) - normal_cdf(floor, mu, sigma)
    else:
        return 0.5  # unknown


def analyze_temperature_event(event_markets: list[dict], forecast_temp_f: float,
                              spread: float = 3.0) -> list[dict]:
    """Analyze a temperature event's markets against NWS forecast."""
    recommendations = []

    for m in event_markets:
        ticker = m["ticker"]
        yes_ask = m.get("yes_ask_dollars")
        no_ask = m.get("no_ask_dollars")

        if not yes_ask:
            continue

        market_yes_price = float(yes_ask)
        if market_yes_price <= 0.01 or market_yes_price >= 0.99:
            continue  # skip illiquid extremes

        market_implied_prob = market_yes_price  # $0.65 = 65% implied

        floor_strike = m.get("floor_strike")
        cap_strike = m.get("cap_strike")
        strike_type = m.get("strike_type", "")

        # Parse strikes
        floor_f = float(floor_strike) if floor_strike is not None else None
        cap_f = float(cap_strike) if cap_strike is not None else None

        forecast_prob = estimate_temp_probability(
            forecast_temp_f,
            floor_f if floor_f is not None else -999,
            cap_f if cap_f is not None else 999,
            strike_type,
            spread=spread,
        )

        # Calculate edge
        edge_yes = (forecast_prob - market_implied_prob) * 100  # as percentage points
        edge_no = ((1 - forecast_prob) - float(no_ask or "1")) * 100 if no_ask else 0

        # Build label
        if strike_type == "less" and cap_f is not None:
            label = f"below {cap_f:.0f}°F"
        elif strike_type == "greater" and floor_f is not None:
            label = f"above {floor_f:.0f}°F"
        elif strike_type == "between" and floor_f is not None and cap_f is not None:
            label = f"{floor_f:.0f}-{cap_f:.0f}°F"
        else:
            label = ticker

        rec = {
            "ticker": ticker,
            "event_ticker": m.get("event_ticker", ""),
            "label": label,
            "forecast_temp_f": forecast_temp_f,
            "market_yes_price": market_yes_price,
            "market_no_price": float(no_ask) if no_ask else None,
            "forecast_prob": forecast_prob,
            "edge_yes": edge_yes,
            "edge_no": edge_no,
            "strike_type": strike_type,
        }

        # Flag if meaningful edge exists
        if edge_yes >= MIN_EDGE_PCT:
            rec["action"] = "BUY YES"
            rec["edge"] = edge_yes
            rec["price"] = market_yes_price
            recommendations.append(rec)
        elif edge_no >= MIN_EDGE_PCT:
            rec["action"] = "BUY NO"
            rec["edge"] = edge_no
            rec["price"] = float(no_ask)
            recommendations.append(rec)

    return recommendations


def analyze_rain_event(event_markets: list[dict], precip_probs: list[float]) -> list[dict]:
    """Analyze a rain event against NWS precipitation probability."""
    if not precip_probs:
        return []

    # Use max precip probability during the day as the "will it rain" estimate
    max_precip_prob = max(precip_probs) / 100.0  # convert to 0-1

    recommendations = []
    for m in event_markets:
        ticker = m["ticker"]
        yes_ask = m.get("yes_ask_dollars")
        no_ask = m.get("no_ask_dollars")

        if not yes_ask:
            continue

        market_yes_price = float(yes_ask)
        if market_yes_price <= 0.01 or market_yes_price >= 0.99:
            continue

        edge_yes = (max_precip_prob - market_yes_price) * 100
        edge_no = ((1 - max_precip_prob) - float(no_ask or "1")) * 100 if no_ask else 0

        rec = {
            "ticker": ticker,
            "event_ticker": m.get("event_ticker", ""),
            "label": "Rain in NYC",
            "forecast_prob": max_precip_prob,
            "market_yes_price": market_yes_price,
            "market_no_price": float(no_ask) if no_ask else None,
            "edge_yes": edge_yes,
            "edge_no": edge_no,
        }

        if edge_yes >= MIN_EDGE_PCT:
            rec["action"] = "BUY YES"
            rec["edge"] = edge_yes
            rec["price"] = market_yes_price
            recommendations.append(rec)
        elif edge_no >= MIN_EDGE_PCT:
            rec["action"] = "BUY NO"
            rec["edge"] = edge_no
            rec["price"] = float(no_ask)
            recommendations.append(rec)

    return recommendations


# ── Trade Execution ─────────────────────────────────────────────────────────

def place_trade(client: KalshiClient, ticker: str, action: str,
                side: str, count: int, price_dollars: str) -> dict:
    """Place a trade on Kalshi."""
    body = {
        "ticker": ticker,
        "action": action,
        "side": side,
        "count": count,
        "type": "limit",
    }
    if side == "yes":
        body["yes_price_dollars"] = price_dollars
    else:
        body["no_price_dollars"] = price_dollars
    return client.post("/portfolio/orders", body)


def log_trade(rec: dict, approved: bool, order_result: dict = None):
    """Append trade to CSV log."""
    file_exists = TRADE_LOG.exists()
    with open(TRADE_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "ticker", "event", "label", "action", "price",
                "forecast_prob", "edge", "approved", "order_id", "status",
            ])
        writer.writerow([
            datetime.now(timezone.utc).isoformat(),
            rec.get("ticker"),
            rec.get("event_ticker"),
            rec.get("label"),
            rec.get("action"),
            rec.get("price"),
            f"{rec.get('forecast_prob', 0):.2%}",
            f"{rec.get('edge', 0):.1f}%",
            approved,
            order_result.get("order", {}).get("order_id") if order_result else "",
            order_result.get("order", {}).get("status") if order_result else "skipped",
        ])


# ── Main ────────────────────────────────────────────────────────────────────

def run_analysis(client: KalshiClient = None):
    """Run the full analysis pipeline. Works with or without auth."""
    print(f"\n{'='*80}")
    print(f"  KALSHI WEATHER TRADING ASSISTANT")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*80}\n")

    # Fetch markets
    print("  Fetching weather markets...")
    if client:
        markets = fetch_weather_markets(client)
    else:
        markets = fetch_weather_markets_public()
    print(f"  Found {len(markets)} open weather contracts.\n")

    if not markets:
        print("  No open weather markets found.")
        return []

    # Filter out markets closing within the next hour (stale prices)
    cutoff = datetime.now(timezone.utc) + timedelta(hours=1)
    markets = [
        m for m in markets
        if m.get("close_time") and datetime.fromisoformat(m["close_time"]) > cutoff
    ]
    print(f"  {len(markets)} contracts still open for trading.\n")

    events = group_markets_by_event(markets)
    all_recs = []

    # Target today and tomorrow for forecast matching
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    target_dates = [today, tomorrow]

    # Fetch forecasts and analyze each event
    nws_cache = {}  # cache gridpoint data by (lat, lon)
    for event_ticker, event_markets in events.items():
        # Figure out which series this belongs to
        series = None
        for s, info in WEATHER_SERIES.items():
            if event_ticker.startswith(s):
                series = s
                break
        if not series:
            continue

        info = WEATHER_SERIES[series]
        cache_key = (info["lat"], info["lon"])

        # Fetch NWS data (cached)
        if cache_key not in nws_cache:
            try:
                print(f"  Fetching NWS forecast for {info['city']}...")
                nws_cache[cache_key] = {
                    "gridpoint": get_nws_gridpoint_data(info["lat"], info["lon"]),
                    "forecast": get_nws_forecast(info["lat"], info["lon"]),
                }
                time.sleep(0.2)  # be polite to NWS
            except Exception as e:
                print(f"  Warning: Could not fetch NWS data for {info['city']}: {e}")
                continue

        nws = nws_cache[cache_key]

        # Extract the event date from the ticker (e.g. KXHIGHNY-26APR01 -> 2026-04-01)
        event_date = None
        for td in target_dates:
            # Match date in event_ticker format: YY + MON + DD
            if td in event_ticker:
                event_date = td
                break
        if not event_date:
            # Try to match by parsing the ticker suffix
            event_date = today  # default to today

        city_spread = info.get("spread", 3.0)

        if info["type"] == "high_temp":
            # Get forecast high temp for the event date
            temps = parse_nws_max_temps(nws["gridpoint"], event_date)
            if not temps:
                # Try from daily forecast periods
                for period in nws["forecast"]:
                    if period.get("isDaytime") and event_date in period.get("startTime", ""):
                        temps = [float(period["temperature"])]
                        break
            if not temps:
                # Try tomorrow if today didn't match
                for alt_date in target_dates:
                    temps = parse_nws_max_temps(nws["gridpoint"], alt_date)
                    if temps:
                        event_date = alt_date
                        break
                if not temps:
                    for period in nws["forecast"]:
                        if period.get("isDaytime"):
                            temps = [float(period["temperature"])]
                            event_date = period.get("startTime", "")[:10]
                            break
            if not temps:
                print(f"  No temp forecast found for {info['city']}")
                continue

            forecast_temp = temps[0]
            print(f"  {info['city']} forecast high: {forecast_temp:.0f}°F ({event_ticker}, {event_date}, ±{city_spread}°F)")

            recs = analyze_temperature_event(event_markets, forecast_temp, spread=city_spread)
            all_recs.extend(recs)

        elif info["type"] == "rain":
            probs = parse_nws_precip_prob(nws["gridpoint"], event_date)
            if not probs:
                for period in nws["forecast"]:
                    if event_date in period.get("startTime", ""):
                        pval = period.get("probabilityOfPrecipitation", {}).get("value")
                        if pval is not None:
                            probs.append(pval)
            if not probs:
                print(f"  No precip forecast found for {info['city']} on {target_date}")
                continue

            max_prob = max(probs)
            print(f"  {info['city']} rain probability: {max_prob:.0f}% ({event_ticker})")

            recs = analyze_rain_event(event_markets, probs)
            all_recs.extend(recs)

    # Sort by edge descending
    all_recs.sort(key=lambda r: r.get("edge", 0), reverse=True)
    return all_recs


def print_recommendations(recs: list[dict]):
    """Print recommendations."""
    if not recs:
        print("\n  No recommendations today — market prices look fair.\n")
        return

    print(f"\n{'─'*80}")
    print(f"  TOP RECOMMENDATIONS (min edge: {MIN_EDGE_PCT}%)")
    print(f"{'─'*80}\n")

    for i, rec in enumerate(recs[:5], 1):
        action = rec["action"]
        label = rec["label"]
        price_cents = int(rec["price"] * 100)
        forecast_pct = rec["forecast_prob"] * 100
        edge = rec["edge"]
        ticker = rec["ticker"]

        side_word = "YES" if "YES" in action else "NO"
        print(f"  [{i}] SUGGESTED: {action} on {label} @ {price_cents}¢")
        print(f"      Weather.gov says {forecast_pct:.0f}% chance | Edge: +{edge:.1f}%")
        print(f"      Ticker: {ticker}")
        print()


def approval_workflow(recs: list[dict], client: KalshiClient = None):
    """Interactive approval workflow for top recommendations."""
    top_recs = recs[:5]
    if not top_recs:
        return

    if not client:
        print("  [Trade execution disabled — no API key configured]\n")
        return

    print(f"{'─'*80}")
    print("  APPROVAL WORKFLOW")
    print(f"{'─'*80}\n")

    # Check balance
    try:
        balance = client.get("/portfolio/balance")
        bal_dollars = balance.get("balance", 0) / 100
        print(f"  Account balance: ${bal_dollars:.2f}\n")
    except Exception as e:
        print(f"  Could not fetch balance: {e}\n")

    for i, rec in enumerate(top_recs, 1):
        action = rec["action"]
        label = rec["label"]
        price_cents = int(rec["price"] * 100)
        edge = rec["edge"]

        side = "yes" if "YES" in action else "no"
        cost = rec["price"] * DEFAULT_CONTRACTS

        print(f"  [{i}] {action} on {label} @ {price_cents}¢")
        print(f"      {DEFAULT_CONTRACTS} contracts = ${cost:.2f} max cost")
        answer = input(f"      Execute? (yes/no): ").strip().lower()

        if answer in ("yes", "y"):
            try:
                result = place_trade(
                    client,
                    ticker=rec["ticker"],
                    action="buy",
                    side=side,
                    count=DEFAULT_CONTRACTS,
                    price_dollars=f"{rec['price']:.4f}",
                )
                order_id = result.get("order", {}).get("order_id", "unknown")
                status = result.get("order", {}).get("status", "unknown")
                print(f"      ✓ Order placed! ID: {order_id} | Status: {status}\n")
                log_trade(rec, approved=True, order_result=result)
            except Exception as e:
                print(f"      ✗ Order failed: {e}\n")
                log_trade(rec, approved=True)
        else:
            print(f"      – Skipped.\n")
            log_trade(rec, approved=False)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Kalshi Weather Trading Assistant")
    parser.add_argument("--no-trade", action="store_true",
                        help="Analysis only, skip approval workflow")
    args = parser.parse_args()

    # Load Kalshi credentials from environment
    api_key_id = os.environ.get("KALSHI_API_KEY_ID")
    private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")

    client = None
    if api_key_id and private_key_path:
        try:
            client = KalshiClient(api_key_id, private_key_path)
            print("  Authenticated with Kalshi API.")
        except Exception as e:
            print(f"  Warning: Could not load Kalshi credentials: {e}")
            print("  Running in read-only mode.\n")
    else:
        print("  No Kalshi credentials found. Running in read-only mode.")
        print("  Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH to enable trading.\n")

    recs = run_analysis(client)
    print_recommendations(recs)

    if args.no_trade:
        return

    if not sys.stdin.isatty():
        print("  [Non-interactive mode — skipping approval workflow]\n")
        return

    approval_workflow(recs, client)


if __name__ == "__main__":
    main()
