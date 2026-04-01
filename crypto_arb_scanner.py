#!/usr/bin/env python3
"""Crypto Arbitrage Scanner — Kalshi vs Deribit Options

Compares Kalshi crypto market prices to Deribit options-implied probabilities
to find mispricings. Uses delta from Deribit options as the probability benchmark.

Usage:
    python3 crypto_arb_scanner.py              # scan for edges
    python3 crypto_arb_scanner.py --practice   # log without trading
"""

import json
import sys
import time
import requests
from datetime import datetime, timezone, timedelta
from math import log, sqrt, exp
from scipy.stats import norm

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
DERIBIT_BASE = "https://www.deribit.com/api/v2"

MIN_EDGE_PCT = 8  # minimum edge to flag


# ── Deribit Data ────────────────────────────────────────────────────────────

def get_btc_index_price():
    """Get current BTC spot price from Deribit."""
    resp = requests.get(f"{DERIBIT_BASE}/public/get_index_price",
                        params={"index_name": "btc_usd"}, timeout=15)
    resp.raise_for_status()
    return resp.json()["result"]["index_price"]


def get_eth_index_price():
    """Get current ETH spot price from Deribit."""
    resp = requests.get(f"{DERIBIT_BASE}/public/get_index_price",
                        params={"index_name": "eth_usd"}, timeout=15)
    resp.raise_for_status()
    return resp.json()["result"]["index_price"]


def get_deribit_options(currency="BTC"):
    """Get all active options with mark prices and greeks."""
    resp = requests.get(f"{DERIBIT_BASE}/public/get_book_summary_by_currency",
                        params={"currency": currency, "kind": "option"}, timeout=30)
    resp.raise_for_status()
    return resp.json()["result"]


def get_option_greeks(instrument_name):
    """Get detailed greeks for a specific option."""
    resp = requests.get(f"{DERIBIT_BASE}/public/get_order_book",
                        params={"instrument_name": instrument_name}, timeout=15)
    resp.raise_for_status()
    result = resp.json()["result"]
    return result.get("greeks", {}), result


def implied_prob_above(spot, strike, time_to_expiry_years, iv, r=0.0):
    """Calculate P(price > strike at expiry) using Black-Scholes.

    This is the risk-neutral probability, which is what options price.
    """
    if time_to_expiry_years <= 0 or iv <= 0:
        return 1.0 if spot > strike else 0.0

    d2 = (log(spot / strike) + (r - 0.5 * iv**2) * time_to_expiry_years) / \
         (iv * sqrt(time_to_expiry_years))
    return norm.cdf(d2)


def parse_deribit_instrument(name):
    """Parse instrument name like BTC-24APR26-80000-C into components."""
    parts = name.split("-")
    if len(parts) != 4:
        return None
    currency = parts[0]
    date_str = parts[1]  # e.g. 24APR26 or 3APR26 (single digit day)
    strike = float(parts[2])
    option_type = parts[3]  # C or P

    # Parse date — day can be 1 or 2 digits
    import re
    match = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", date_str)
    if not match:
        return None
    day = int(match.group(1))
    month_str = match.group(2)
    year = int("20" + match.group(3))
    months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
              "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
    month = months.get(month_str, 1)
    expiry = datetime(year, month, day, 8, 0, tzinfo=timezone.utc)  # 8 AM UTC expiry

    return {
        "currency": currency,
        "expiry": expiry,
        "strike": strike,
        "type": option_type,
        "name": name,
    }


# ── Kalshi Data ─────────────────────────────────────────────────────────────

def get_kalshi_threshold_markets(series_prefix, status="open"):
    """Get all threshold (above/below) markets for a crypto series."""
    all_markets = []
    for suffix in ["D"]:  # D = daily thresholds
        series = f"{series_prefix}{suffix}"
        cursor = None
        while True:
            params = {"series_ticker": series, "status": status, "limit": 200}
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

    # Also get year-end markets
    for series in [f"{series_prefix}Y"]:
        cursor = None
        while True:
            params = {"series_ticker": series, "status": status, "limit": 200}
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


def parse_kalshi_strike(market):
    """Extract the threshold price from a Kalshi market."""
    strike_type = market.get("strike_type", "")
    floor = market.get("floor_strike")
    cap = market.get("cap_strike")

    if strike_type == "greater" and floor is not None:
        return float(floor), "above"
    elif strike_type == "less" and cap is not None:
        return float(cap), "below"
    elif strike_type == "between" and floor is not None and cap is not None:
        return (float(floor), float(cap)), "between"
    return None, None


# ── Scanner ─────────────────────────────────────────────────────────────────

def build_deribit_probability_curve(options_data, spot, currency="BTC"):
    """Build a probability curve from Deribit options.

    Returns dict mapping (expiry_date, strike) -> implied_probability_above
    """
    now = datetime.now(timezone.utc)
    curve = {}

    for opt in options_data:
        name = opt.get("instrument_name", "")
        parsed = parse_deribit_instrument(name)
        if not parsed or parsed["type"] != "C":  # only use calls
            continue

        mark_iv = opt.get("mark_iv")
        if not mark_iv or mark_iv <= 0:
            continue

        iv = mark_iv / 100.0  # convert from percentage
        tte = (parsed["expiry"] - now).total_seconds() / (365.25 * 24 * 3600)
        if tte <= 0:
            continue

        prob = implied_prob_above(spot, parsed["strike"], tte, iv)
        expiry_str = parsed["expiry"].strftime("%Y-%m-%d")
        curve[(expiry_str, parsed["strike"])] = {
            "prob": prob,
            "iv": iv,
            "tte_days": tte * 365.25,
            "instrument": name,
            "mark_price_btc": opt.get("mark_price", 0),
        }

    return curve


def find_closest_deribit_iv(curve, target_strike, target_expiry=None,
                            max_expiry_gap_days=30):
    """Find the closest Deribit IV for a given strike price.

    Returns the raw IV and metadata (NOT a pre-computed probability) so the
    caller can recompute the probability using Kalshi's actual time-to-expiry.

    Prefers the nearest expiry first, then the nearest strike within that expiry.
    Falls back to interpolating IV between the two nearest strikes if the exact
    strike isn't available.
    """
    # Group options by expiry, filter by date proximity
    by_expiry = {}  # expiry_str -> [(strike, data), ...]
    for (expiry, strike), data in curve.items():
        if target_expiry:
            try:
                exp_date = datetime.strptime(expiry, "%Y-%m-%d")
                tgt_date = datetime.strptime(target_expiry, "%Y-%m-%d")
                gap = abs((exp_date - tgt_date).days)
                if gap > max_expiry_gap_days:
                    continue
            except ValueError:
                continue
        by_expiry.setdefault(expiry, []).append((strike, data))

    if not by_expiry:
        return None

    # Pick the nearest expiry to the target
    if target_expiry:
        tgt_date = datetime.strptime(target_expiry, "%Y-%m-%d")
        best_expiry = min(by_expiry.keys(),
                          key=lambda e: abs((datetime.strptime(e, "%Y-%m-%d") - tgt_date).days))
    else:
        best_expiry = min(by_expiry.keys())

    strikes_data = sorted(by_expiry[best_expiry], key=lambda x: x[0])
    strikes = [s for s, _ in strikes_data]
    ivs = [d["iv"] for _, d in strikes_data]

    # Exact match
    for strike, data in strikes_data:
        if abs(strike - target_strike) < target_strike * 0.001:
            return {**data, "strike": strike, "expiry": best_expiry,
                    "iv_source": "exact_strike"}

    # Interpolate between the two nearest bracketing strikes
    below = [(s, d) for s, d in strikes_data if s < target_strike]
    above = [(s, d) for s, d in strikes_data if s > target_strike]

    if below and above:
        s_lo, d_lo = below[-1]
        s_hi, d_hi = above[0]
        # Linear interpolation in strike space
        w = (target_strike - s_lo) / (s_hi - s_lo)
        interp_iv = d_lo["iv"] * (1 - w) + d_hi["iv"] * w
        ref_data = d_lo if w < 0.5 else d_hi
        return {**ref_data, "iv": interp_iv,
                "strike": target_strike, "expiry": best_expiry,
                "iv_source": f"interp_{s_lo:.0f}_{s_hi:.0f}"}

    # Fallback: nearest strike (extrapolation — less reliable)
    nearest_strike, nearest_data = min(strikes_data,
                                       key=lambda x: abs(x[0] - target_strike))
    return {**nearest_data, "strike": nearest_strike, "expiry": best_expiry,
            "iv_source": "nearest_strike"}


def scan_crypto(currency="BTC", series_prefix="KXBTC"):
    """Scan for arbitrage between Kalshi and Deribit for a crypto asset."""
    print(f"\n  Fetching {currency} data...")

    # Get spot price
    if currency == "BTC":
        spot = get_btc_index_price()
    else:
        spot = get_eth_index_price()
    print(f"  {currency} spot: ${spot:,.2f}")

    # Get Deribit options
    options = get_deribit_options(currency)
    print(f"  Deribit: {len(options)} active options")

    # Build probability curve
    curve = build_deribit_probability_curve(options, spot, currency)
    print(f"  Probability curve: {len(curve)} call strikes mapped")

    # Get Kalshi markets
    kalshi_markets = get_kalshi_threshold_markets(series_prefix)
    print(f"  Kalshi: {len(kalshi_markets)} markets")

    if not kalshi_markets:
        print(f"  No Kalshi threshold markets found for {series_prefix}")
        return []

    # Compare
    edges = []
    print(f"\n  {'Strike':>10} {'Kalshi%':>8} {'Deribit%':>9} {'Edge':>7} {'Type':>6} {'Signal':>10} {'Ticker'}")
    print(f"  {'-'*75}")

    for m in kalshi_markets:
        threshold, direction = parse_kalshi_strike(m)
        if threshold is None or direction == "between":
            continue

        yes_ask = float(m.get("yes_ask_dollars") or 0)
        no_ask = float(m.get("no_ask_dollars") or 0)
        if yes_ask <= 0.01 or yes_ask >= 0.99:
            continue

        vol = float(m.get("volume_fp") or 0)
        ticker = m.get("ticker", "")

        # Parse Kalshi close time and compute Kalshi-specific time-to-expiry
        close_time = m.get("close_time", "")
        kalshi_expiry = close_time[:10] if close_time else None

        # Calculate Kalshi TTE in years from now until close_time
        now = datetime.now(timezone.utc)
        if close_time:
            try:
                kalshi_close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            except ValueError:
                kalshi_close_dt = now + timedelta(hours=24)  # fallback
        else:
            kalshi_close_dt = now + timedelta(hours=24)

        kalshi_tte = max((kalshi_close_dt - now).total_seconds() / (365.25 * 24 * 3600), 0.0001)

        # Find nearest Deribit IV (NOT pre-computed prob)
        deribit = find_closest_deribit_iv(curve, threshold, target_expiry=kalshi_expiry)

        if not deribit:
            continue

        # Skip if strike mismatch > 3% (no close option available)
        if abs(deribit["strike"] - threshold) > threshold * 0.03:
            continue

        # Recompute probability using Deribit's IV but KALSHI's time-to-expiry
        deribit_iv = deribit["iv"]
        deribit_prob = implied_prob_above(spot, threshold, kalshi_tte, deribit_iv)
        if direction == "below":
            deribit_prob = 1 - deribit_prob

        kalshi_prob = yes_ask
        kalshi_tte_hours = kalshi_tte * 365.25 * 24
        deribit_tte_days = deribit.get("tte_days", 0)

        edge = (deribit_prob - kalshi_prob) * 100

        signal = ""
        if edge > MIN_EDGE_PCT:
            signal = "BUY YES"
        elif edge < -MIN_EDGE_PCT:
            signal = "BUY NO"

        if signal or abs(edge) > 5:
            print(f"  ${threshold:>9,.0f} {kalshi_prob:>7.0%} {deribit_prob:>8.0%} "
                  f"{edge:>+6.1f}% {direction:>6} {signal:>10} {ticker}"
                  f"  [Kalshi {kalshi_tte_hours:.1f}h | Deribit {deribit_tte_days:.1f}d | IV src: {deribit.get('iv_source','')}]")

            if signal:
                edges.append({
                    "ticker": ticker,
                    "threshold": threshold,
                    "direction": direction,
                    "kalshi_prob": kalshi_prob,
                    "deribit_prob": deribit_prob,
                    "edge": edge,
                    "signal": signal,
                    "price": yes_ask if "YES" in signal else no_ask,
                    "deribit_iv": deribit_iv,
                    "deribit_instrument": deribit["instrument"],
                    "iv_source": deribit.get("iv_source", ""),
                    "kalshi_tte_hours": kalshi_tte_hours,
                    "deribit_tte_days": deribit_tte_days,
                    "volume": vol,
                })

    return edges


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)

    print(f"\n{'#'*74}")
    print(f"  CRYPTO ARBITRAGE SCANNER — Kalshi vs Deribit Options")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Min edge threshold: {MIN_EDGE_PCT}%")
    print(f"{'#'*74}")

    all_edges = []

    # Scan BTC
    try:
        btc_edges = scan_crypto("BTC", "KXBTC")
        all_edges.extend(btc_edges)
    except Exception as e:
        print(f"  BTC scan error: {e}")

    time.sleep(1)

    # Scan ETH
    try:
        eth_edges = scan_crypto("ETH", "KXETH")
        all_edges.extend(eth_edges)
    except Exception as e:
        print(f"  ETH scan error: {e}")

    # Summary
    print(f"\n{'='*74}")
    print(f"  ARBITRAGE OPPORTUNITIES")
    print(f"{'='*74}")

    if all_edges:
        all_edges.sort(key=lambda x: abs(x["edge"]), reverse=True)
        for e in all_edges[:10]:
            price_cents = int(e["price"] * 100)
            print(f"\n  {e['signal']} @ {price_cents}¢ — {e['direction']} ${e['threshold']:,.0f}")
            print(f"    Ticker: {e['ticker']}")
            print(f"    Kalshi: {e['kalshi_prob']:.0%} | Deribit: {e['deribit_prob']:.0%} | Edge: {e['edge']:+.1f}%")
            print(f"    Deribit IV: {e['deribit_iv']:.0%} | Ref: {e['deribit_instrument']} | IV src: {e.get('iv_source','')}")
            print(f"    Kalshi TTE: {e.get('kalshi_tte_hours',0):.1f}h | Deribit TTE: {e.get('deribit_tte_days',0):.1f}d | Vol: {int(e['volume']):,}")
    else:
        print("\n  No significant arb opportunities found.")
        print("  Markets appear efficiently priced relative to options.")

    print(f"\n  Mode: PRACTICE — no trades placed.\n")


if __name__ == "__main__":
    main()
