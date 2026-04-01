#!/usr/bin/env python3
"""Kalshi Market Research Tool

Fetches top markets by volume from Kalshi's public API,
displays yes/no prices, and optionally flags markets where
odds may disagree with recent news (requires ANTHROPIC_API_KEY).
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
TOP_N = 20  # number of top markets to display


def fetch_all_open_markets():
    """Paginate through all open markets."""
    markets = []
    cursor = None
    while True:
        params = {"limit": 1000, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("markets", [])
        markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
        time.sleep(0.1)  # respect rate limits
    return markets


def top_markets_by_volume(markets, n=TOP_N):
    """Sort markets by 24h volume descending, return top n."""
    for m in markets:
        m["_vol_24h"] = float(m.get("volume_24h_fp") or "0")
        m["_vol_total"] = float(m.get("volume_fp") or "0")
    markets.sort(key=lambda m: m["_vol_24h"], reverse=True)
    return markets[:n]


def format_price(dollars_str):
    """Convert price string to cents display (e.g. '0.65' -> '65¢')."""
    if not dollars_str:
        return "—"
    try:
        cents = round(float(dollars_str) * 100)
        return f"{cents}¢"
    except (ValueError, TypeError):
        return "—"


def print_market_table(markets):
    """Print a formatted table of markets."""
    print(f"\n{'='*90}")
    print(f"  KALSHI TOP {len(markets)} MARKETS BY 24H VOLUME")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*90}\n")

    for i, m in enumerate(markets, 1):
        ticker = m.get("ticker", "???")
        title = m.get("yes_sub_title") or ticker
        event_ticker = m.get("event_ticker", "")

        yes_bid = format_price(m.get("yes_bid_dollars"))
        yes_ask = format_price(m.get("yes_ask_dollars"))
        no_bid = format_price(m.get("no_bid_dollars"))
        no_ask = format_price(m.get("no_ask_dollars"))
        last = format_price(m.get("last_price_dollars"))
        vol_24h = int(m["_vol_24h"])
        vol_total = int(m["_vol_total"])

        print(f"  {i:>2}. {title}")
        print(f"      Ticker: {ticker}  |  Event: {event_ticker}")
        print(f"      YES {yes_bid}/{yes_ask}  |  NO {no_bid}/{no_ask}  |  Last: {last}")
        print(f"      Vol 24h: {vol_24h:,}  |  Vol total: {vol_total:,}")
        print()


def analyze_with_claude(markets):
    """Use Claude to flag markets where odds may disagree with recent news."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [Skipping news analysis — set ANTHROPIC_API_KEY to enable]\n")
        return

    try:
        import anthropic
    except ImportError:
        print("  [Skipping news analysis — run: pip install anthropic]\n")
        return

    print(f"{'='*90}")
    print("  NEWS DISAGREEMENT ANALYSIS (via Claude)")
    print(f"{'='*90}\n")

    market_summaries = []
    for m in markets:
        title = m.get("yes_sub_title") or m.get("ticker", "???")
        yes_bid = m.get("yes_bid_dollars", "0")
        last = m.get("last_price_dollars", "0")
        vol_24h = int(m["_vol_24h"])
        close_time = m.get("close_time", "unknown")
        rules = m.get("rules_primary", "")[:200]

        market_summaries.append(
            f"- {title}\n"
            f"  Yes price: ${yes_bid} | Last: ${last} | 24h vol: {vol_24h:,}\n"
            f"  Closes: {close_time}\n"
            f"  Rules: {rules}"
        )

    prompt = f"""You are a prediction market analyst. Today is {datetime.now(timezone.utc).strftime('%Y-%m-%d')}.

Below are the top Kalshi prediction markets by 24-hour volume with their current YES prices
(a YES price of $0.70 means the market implies a 70% probability).

{chr(10).join(market_summaries)}

For each market, based on your knowledge of recent events and news:
1. Briefly state what the current price implies
2. Flag any markets where the implied probability seems notably too high or too low
   given recent developments — these are potential opportunities
3. Rate your confidence in each flag (low/medium/high)

Focus only on markets where you see a genuine disagreement. If the price looks reasonable,
say so briefly and move on. Be concise."""

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    print(response.content[0].text)
    print()


def main():
    print("  Fetching open markets from Kalshi...")
    try:
        markets = fetch_all_open_markets()
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching markets: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Found {len(markets):,} open markets.")

    top = top_markets_by_volume(markets)
    if not top:
        print("  No open markets found.")
        sys.exit(0)

    print_market_table(top)
    analyze_with_claude(top)


if __name__ == "__main__":
    main()
