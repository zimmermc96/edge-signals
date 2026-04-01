"""Core scanner engine — runs all scanners and returns structured results."""

import time
import requests
from datetime import datetime, timezone, timedelta
from math import erf, sqrt, log, exp
import json
import os

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
DERIBIT_BASE = "https://www.deribit.com/api/v2"
FRED_BASE = "https://api.stlouisfed.org/fred"
NWS_HEADERS = {"User-Agent": "EdgeSignals/1.0"}

# Cache to avoid hammering APIs
_cache = {}
CACHE_TTL = 300  # 5 minutes


def _cached_get(url, params=None, ttl=CACHE_TTL):
    key = f"{url}:{json.dumps(params or {}, sort_keys=True)}"
    now = time.time()
    if key in _cache and now - _cache[key]["ts"] < ttl:
        return _cache[key]["data"]
    resp = requests.get(url, params=params, headers=NWS_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = {"data": data, "ts": now}
    return data


# ── Weather Signals ─────────────────────────────────────────────────────────

WEATHER_STATIONS = {
    "KXHIGHNY":  {"city": "NYC", "lat": 40.7812, "lon": -73.9665, "tz": "America/New_York", "bias": -1.56, "spread": 2.94},
    "KXHIGHCHI": {"city": "Chicago", "lat": 41.7868, "lon": -87.7522, "tz": "America/Chicago", "bias": -1.97, "spread": 2.05},
    "KXHIGHMIA": {"city": "Miami", "lat": 25.7959, "lon": -80.2870, "tz": "America/New_York", "bias": -1.71, "spread": 1.80},
    "KXHIGHAUS": {"city": "Austin", "lat": 30.1945, "lon": -97.6699, "tz": "America/Chicago", "bias": -2.80, "spread": 1.64},
    "KXHIGHDEN": {"city": "Denver", "lat": 39.8561, "lon": -104.6737, "tz": "America/Denver", "bias": 0.66, "spread": 3.14},
}


def normal_cdf(x, mu, sigma):
    return 0.5 * (1.0 + erf((x - mu) / (sigma * sqrt(2.0))))


def scan_weather():
    """Run weather scanner, return list of signal dicts."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    date_code = now.strftime("%y") + now.strftime("%b").upper() + now.strftime("%d")

    signals = []

    for series, info in WEATHER_STATIONS.items():
        try:
            # GFS ensemble
            ens_data = _cached_get("https://ensemble-api.open-meteo.com/v1/ensemble", {
                "latitude": info["lat"], "longitude": info["lon"],
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": info["tz"],
                "forecast_days": 2,
                "models": "gfs_seamless",
            })
            dates = ens_data["daily"]["time"]
            day_idx = dates.index(today) if today in dates else 0

            members = [ens_data["daily"][k][day_idx]
                       for k in ens_data["daily"]
                       if "member" in k and ens_data["daily"][k][day_idx] is not None]

            if not members:
                continue

            raw_mean = sum(members) / len(members)
            corrected = raw_mean - info["bias"]
            spread = info["spread"]

            # Kalshi markets
            resp = _cached_get(f"{KALSHI_BASE}/markets", {
                "series_ticker": series, "status": "open", "limit": 200
            })
            markets = [m for m in resp.get("markets", []) if date_code in m.get("event_ticker", "")]
            markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))

            for m in markets:
                st = m.get("strike_type")
                floor = float(m.get("floor_strike") or -999)
                cap = float(m.get("cap_strike") or 999)
                yes_ask = float(m.get("yes_ask_dollars") or 0)

                if yes_ask <= 0.02 or yes_ask >= 0.98:
                    continue

                p_cap = normal_cdf(cap, corrected, spread) if cap < 900 else 1.0
                p_floor = normal_cdf(floor, corrected, spread) if floor > -900 else 0.0
                model_prob = p_cap - p_floor
                edge = (model_prob - yes_ask) * 100

                if st == "less":
                    label = f"Below {cap:.0f}°F"
                elif st == "greater":
                    label = f"Above {floor:.0f}°F"
                elif st == "between":
                    label = f"{floor:.0f}-{cap:.0f}°F"
                else:
                    continue

                if abs(edge) > 10:
                    signals.append({
                        "category": "weather",
                        "city": info["city"],
                        "label": label,
                        "ticker": m["ticker"],
                        "model_prob": round(model_prob, 3),
                        "market_prob": round(yes_ask, 3),
                        "edge": round(edge, 1),
                        "signal": "BUY YES" if edge > 10 else "BUY NO",
                        "forecast": round(corrected, 1),
                        "spread": round(spread, 1),
                        "confidence": "high" if info["spread"] < 2.0 else "medium" if info["spread"] < 3.0 else "low",
                    })

        except Exception:
            continue

    signals.sort(key=lambda s: abs(s["edge"]), reverse=True)
    return signals


# ── Crypto Signals ──────────────────────────────────────────────────────────

def scan_crypto():
    """Run crypto arb scanner, return list of signal dicts."""
    from scipy.stats import norm as scipy_norm
    signals = []

    for currency, series_prefix in [("BTC", "KXBTC"), ("ETH", "KXETH")]:
        try:
            # Spot price
            spot_data = _cached_get(f"{DERIBIT_BASE}/public/get_index_price",
                                    {"index_name": f"{currency.lower()}_usd"})
            spot = spot_data["result"]["index_price"]

            # Deribit options
            opts_data = _cached_get(f"{DERIBIT_BASE}/public/get_book_summary_by_currency",
                                    {"currency": currency, "kind": "option"})
            options = opts_data["result"]

            # Build probability curve from calls
            import re
            now = datetime.now(timezone.utc)
            curve = {}
            months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                       "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

            for opt in options:
                name = opt.get("instrument_name", "")
                parts = name.split("-")
                if len(parts) != 4 or parts[3] != "C":
                    continue
                match = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", parts[1])
                if not match:
                    continue

                mark_iv = opt.get("mark_iv")
                if not mark_iv or mark_iv <= 0:
                    continue

                day = int(match.group(1))
                month = months.get(match.group(2), 1)
                year = int("20" + match.group(3))
                expiry = datetime(year, month, day, 8, 0, tzinfo=timezone.utc)
                strike = float(parts[2])
                iv = mark_iv / 100.0
                tte = (expiry - now).total_seconds() / (365.25 * 24 * 3600)
                if tte <= 0:
                    continue

                d2 = (log(spot / strike) + (-0.5 * iv**2) * tte) / (iv * sqrt(tte))
                prob = scipy_norm.cdf(d2)
                expiry_str = expiry.strftime("%Y-%m-%d")
                curve[(expiry_str, strike)] = prob

            # Kalshi threshold markets
            for suffix in ["D"]:
                kalshi_data = _cached_get(f"{KALSHI_BASE}/markets", {
                    "series_ticker": f"{series_prefix}{suffix}", "status": "open", "limit": 200
                })
                for m in kalshi_data.get("markets", []):
                    if m.get("strike_type") != "greater":
                        continue
                    threshold = float(m.get("floor_strike") or 0)
                    yes_ask = float(m.get("yes_ask_dollars") or 0)
                    if yes_ask <= 0.02 or yes_ask >= 0.98:
                        continue

                    close_time = m.get("close_time", "")[:10]

                    # Find closest Deribit match
                    best_prob = None
                    best_dist = float("inf")
                    for (exp, strike), prob in curve.items():
                        try:
                            gap = abs((datetime.strptime(exp, "%Y-%m-%d") -
                                       datetime.strptime(close_time, "%Y-%m-%d")).days)
                        except ValueError:
                            continue
                        if gap > 3:
                            continue
                        dist = abs(strike - threshold)
                        if dist < best_dist and dist < threshold * 0.03:
                            best_dist = dist
                            best_prob = prob

                    if best_prob is None:
                        continue

                    edge = (best_prob - yes_ask) * 100
                    if abs(edge) > 8:
                        signals.append({
                            "category": "crypto",
                            "asset": currency,
                            "label": f"{currency} > ${threshold:,.0f}",
                            "ticker": m["ticker"],
                            "model_prob": round(best_prob, 3),
                            "market_prob": round(yes_ask, 3),
                            "edge": round(edge, 1),
                            "signal": "BUY YES" if edge > 8 else "BUY NO",
                            "spot": round(spot, 2),
                            "confidence": "high" if abs(edge) > 20 else "medium",
                        })

        except Exception:
            continue

    signals.sort(key=lambda s: abs(s["edge"]), reverse=True)
    return signals[:15]


# ── Economic Signals ────────────────────────────────────────────────────────

def scan_economics():
    """Run economics scanner, return list of signal dicts."""
    signals = []
    fred_key = os.environ.get("FRED_API_KEY")

    # Fed rate markets
    try:
        fed_data = _cached_get(f"{KALSHI_BASE}/markets", {
            "series_ticker": "KXFED", "status": "open", "limit": 200
        })
        markets = fed_data.get("markets", [])

        # Get current rate from FRED
        current_rate = None
        if fred_key:
            rate_data = _cached_get(f"{FRED_BASE}/series/observations", {
                "series_id": "DFEDTARU", "api_key": fred_key,
                "file_type": "json", "sort_order": "desc", "limit": 5
            })
            for o in rate_data.get("observations", []):
                if o["value"] != ".":
                    current_rate = float(o["value"])
                    break

        # Group by event and find nearest FOMC
        events = {}
        for m in markets:
            et = m.get("event_ticker", "")
            events.setdefault(et, []).append(m)

        for event_ticker in sorted(events.keys())[:2]:  # next 2 meetings
            event_markets = events[event_ticker]
            for m in event_markets:
                yes_ask = float(m.get("yes_ask_dollars") or 0)
                title = m.get("yes_sub_title", "")
                vol = float(m.get("volume_fp") or 0)
                if vol < 100:
                    continue

                signals.append({
                    "category": "fed_rate",
                    "label": f"Fed {title}",
                    "ticker": m["ticker"],
                    "event": event_ticker,
                    "market_prob": round(yes_ask, 3),
                    "current_rate": current_rate,
                    "volume": int(vol),
                    "close_date": m.get("close_time", "")[:10],
                    "confidence": "high",
                    "signal": "INFO",
                    "edge": 0,
                })

    except Exception:
        pass

    # CPI markets
    try:
        cpi_data = _cached_get(f"{KALSHI_BASE}/markets", {
            "series_ticker": "KXCPI", "status": "open", "limit": 200
        })
        cpi_markets = cpi_data.get("markets", [])

        events = {}
        for m in cpi_markets:
            et = m.get("event_ticker", "")
            events.setdefault(et, []).append(m)

        for event_ticker in sorted(events.keys())[:1]:  # next CPI release
            event_markets = events[event_ticker]
            event_markets.sort(key=lambda m: float(m.get("floor_strike") or m.get("cap_strike") or 0))
            for m in event_markets:
                yes_ask = float(m.get("yes_ask_dollars") or 0)
                st = m.get("strike_type")
                floor = m.get("floor_strike")
                cap = m.get("cap_strike")
                vol = float(m.get("volume_fp") or 0)

                if st == "greater" and floor is not None:
                    label = f"CPI MoM > {floor}%"
                else:
                    continue

                signals.append({
                    "category": "cpi",
                    "label": label,
                    "ticker": m["ticker"],
                    "event": event_ticker,
                    "market_prob": round(yes_ask, 3),
                    "volume": int(vol),
                    "close_date": m.get("close_time", "")[:10],
                    "confidence": "medium",
                    "signal": "INFO",
                    "edge": 0,
                })

    except Exception:
        pass

    return signals


# ── Master Scanner ──────────────────────────────────────────────────────────

def scan_all():
    """Run all scanners and return combined results."""
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "weather": scan_weather(),
        "crypto": scan_crypto(),
        "economics": scan_economics(),
    }

    # Top edges across all categories
    all_edges = []
    for cat in ["weather", "crypto"]:
        for s in results[cat]:
            if abs(s.get("edge", 0)) > 8:
                all_edges.append(s)
    all_edges.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    results["top_edges"] = all_edges[:10]

    return results
