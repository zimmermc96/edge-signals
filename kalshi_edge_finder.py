#!/usr/bin/env python3
"""Kalshi Edge Finder

Scans all high-volume Kalshi markets, uses Claude to identify potential
mispricings, and presents trade recommendations with an approval workflow.
"""

import os
import sys
import csv
import time
import base64
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
TRADE_LOG = Path(__file__).parent / "trade_log.csv"
DEFAULT_CONTRACTS = 10

# Skip sports — prices are efficient and games are in-progress
SKIP_SERIES_PREFIXES = (
    "KXNBA", "KXMLB", "KXNFL", "KXNHL", "KXMLS", "KXNCAA",
    "KXPGA", "KXUFC", "KXF1", "KXSOCCER", "KXTENNIS",
)


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


# ── Market Fetching ─────────────────────────────────────────────────────────

def fetch_open_markets() -> list[dict]:
    """Fetch all open markets from public API."""
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
        time.sleep(0.1)
    return markets


def filter_and_rank(markets: list[dict], top_n: int = 50) -> list[dict]:
    """Filter out sports, illiquid markets, and rank by 24h volume."""
    filtered = []
    cutoff = datetime.now(timezone.utc) + timedelta(hours=2)

    for m in markets:
        ticker = m.get("ticker", "")
        event_ticker = m.get("event_ticker", "")

        # Skip sports
        if any(event_ticker.startswith(p) for p in SKIP_SERIES_PREFIXES):
            continue

        # Skip markets closing soon
        close_time = m.get("close_time")
        if close_time:
            try:
                if datetime.fromisoformat(close_time) < cutoff:
                    continue
            except ValueError:
                pass

        # Need real prices
        yes_ask = m.get("yes_ask_dollars")
        if not yes_ask:
            continue
        price = float(yes_ask)
        if price <= 0.03 or price >= 0.97:
            continue  # skip extremes

        m["_vol_24h"] = float(m.get("volume_24h_fp") or "0")
        m["_vol_total"] = float(m.get("volume_fp") or "0")

        # Need some volume
        if m["_vol_24h"] < 1000:
            continue

        filtered.append(m)

    filtered.sort(key=lambda x: x["_vol_24h"], reverse=True)
    return filtered[:top_n]


def format_market_for_analysis(m: dict) -> str:
    """Format a market for Claude analysis."""
    ticker = m.get("ticker", "???")
    title = m.get("yes_sub_title") or ticker
    event_ticker = m.get("event_ticker", "")
    yes_ask = float(m.get("yes_ask_dollars", "0"))
    no_ask = float(m.get("no_ask_dollars", "0"))
    last = float(m.get("last_price_dollars", "0"))
    vol_24h = int(m["_vol_24h"])
    close_time = m.get("close_time", "unknown")
    rules = (m.get("rules_primary") or "")[:300]

    return (
        f"- **{title}** (ticker: {ticker})\n"
        f"  Event: {event_ticker}\n"
        f"  YES ask: {yes_ask:.2f} ({yes_ask*100:.0f}% implied) | "
        f"NO ask: {no_ask:.2f} | Last: {last:.2f}\n"
        f"  24h volume: {vol_24h:,} contracts | Closes: {close_time}\n"
        f"  Rules: {rules}\n"
    )


# ── Claude Analysis ─────────────────────────────────────────────────────────

def analyze_markets_with_claude(markets: list[dict]) -> list[dict]:
    """Use Claude to identify mispricings."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  Set ANTHROPIC_API_KEY to enable AI mispricing analysis.\n")
        return []

    try:
        import anthropic
    except ImportError:
        print("  Run: pip install anthropic\n")
        return []

    print(f"  Analyzing {len(markets)} markets with Claude...\n")

    market_text = "\n".join(format_market_for_analysis(m) for m in markets)

    prompt = f"""You are an expert prediction market analyst. Today is {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.

Below are the top non-sports Kalshi prediction markets by 24-hour volume. Each shows the current YES ask price (which equals the market's implied probability).

{market_text}

Your job: identify the TOP 5 markets where the implied probability seems MOST WRONG given what you know about current events, data, and base rates.

For each pick, respond in EXACTLY this JSON format (no other text):
```json
[
  {{
    "ticker": "EXACT_TICKER",
    "title": "short description",
    "side": "yes" or "no",
    "market_price": 0.XX,
    "your_estimate": 0.XX,
    "edge_pct": XX.X,
    "confidence": "low" or "medium" or "high",
    "reasoning": "1-2 sentence explanation of why the market is mispriced"
  }}
]
```

Rules:
- Only flag markets where you see >= 10 percentage points of edge
- "edge_pct" = abs(your_estimate - market_price) * 100
- "side" = the side you'd BUY (yes if underpriced, no if overpriced)
- Be honest about confidence — "high" only if based on hard data, not vibes
- If the market price looks roughly fair, don't include it
- Use the EXACT ticker from the data above"""

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text

    # Extract JSON from response
    try:
        # Find JSON array in response
        start = text.index("[")
        end = text.rindex("]") + 1
        picks = json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError) as e:
        print(f"  Could not parse Claude's response: {e}")
        print(f"  Raw response:\n{text}\n")
        return []

    return picks


# ── Trade Execution ─────────────────────────────────────────────────────────

def place_trade(client: KalshiClient, ticker: str, side: str,
                count: int, price_dollars: float) -> dict:
    """Place a limit buy order."""
    body = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "count": count,
        "type": "limit",
    }
    if side == "yes":
        body["yes_price_dollars"] = f"{price_dollars:.4f}"
    else:
        body["no_price_dollars"] = f"{1 - price_dollars:.4f}"
    return client.post("/portfolio/orders", body)


def log_trade(pick: dict, approved: bool, order_result: dict = None):
    """Append trade to CSV log."""
    file_exists = TRADE_LOG.exists()
    with open(TRADE_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "ticker", "title", "side", "market_price",
                "estimated_prob", "edge_pct", "confidence", "reasoning",
                "approved", "order_id", "status",
            ])
        writer.writerow([
            datetime.now(timezone.utc).isoformat(),
            pick.get("ticker"),
            pick.get("title"),
            pick.get("side"),
            pick.get("market_price"),
            pick.get("your_estimate"),
            pick.get("edge_pct"),
            pick.get("confidence"),
            pick.get("reasoning"),
            approved,
            order_result.get("order", {}).get("order_id") if order_result else "",
            order_result.get("order", {}).get("status") if order_result else "skipped",
        ])


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Kalshi Edge Finder")
    parser.add_argument("--no-trade", action="store_true",
                        help="Analysis only, skip approval workflow")
    parser.add_argument("--top", type=int, default=40,
                        help="Number of markets to analyze (default: 40)")
    args = parser.parse_args()

    print(f"\n{'='*80}")
    print(f"  KALSHI EDGE FINDER")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*80}\n")

    # Load Kalshi credentials
    api_key_id = os.environ.get("KALSHI_API_KEY_ID")
    private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    kalshi = None
    if api_key_id and private_key_path:
        try:
            kalshi = KalshiClient(api_key_id, private_key_path)
            balance = kalshi.get("/portfolio/balance")
            bal_dollars = balance.get("balance", 0) / 100
            print(f"  Kalshi account balance: ${bal_dollars:.2f}")
        except Exception as e:
            print(f"  Kalshi auth warning: {e}")

    # Fetch and filter markets
    print("  Fetching all open markets...")
    all_markets = fetch_open_markets()
    print(f"  Found {len(all_markets):,} total open markets.")

    top = filter_and_rank(all_markets, top_n=args.top)
    print(f"  {len(top)} non-sports markets with volume > 1K after filtering.\n")

    if not top:
        print("  No qualifying markets found.")
        return

    # Show what we're analyzing
    print(f"  Top markets by 24h volume:")
    for i, m in enumerate(top[:10], 1):
        title = m.get("yes_sub_title") or m.get("ticker")
        yes_ask = float(m.get("yes_ask_dollars", "0"))
        vol = int(m["_vol_24h"])
        print(f"    {i:>2}. {title[:50]:<50} YES {yes_ask*100:.0f}¢  vol:{vol:>10,}")
    if len(top) > 10:
        print(f"    ... and {len(top) - 10} more\n")
    else:
        print()

    # Claude analysis
    picks = analyze_markets_with_claude(top)

    if not picks:
        print("  No mispricings identified.\n")
        return

    # Display recommendations
    print(f"{'─'*80}")
    print(f"  MISPRICING RECOMMENDATIONS")
    print(f"{'─'*80}\n")

    for i, pick in enumerate(picks, 1):
        side = pick.get("side", "?").upper()
        title = pick.get("title", "?")
        ticker = pick.get("ticker", "?")
        mkt_price = pick.get("market_price", 0)
        estimate = pick.get("your_estimate", 0)
        edge = pick.get("edge_pct", 0)
        confidence = pick.get("confidence", "?")
        reasoning = pick.get("reasoning", "")

        price_cents = int(mkt_price * 100)
        est_cents = int(estimate * 100)

        print(f"  [{i}] BUY {side} on {title} @ {price_cents}¢")
        print(f"      Ticker: {ticker}")
        print(f"      Market: {price_cents}¢ | Estimate: {est_cents}¢ | Edge: +{edge:.1f}% | Confidence: {confidence}")
        print(f"      Why: {reasoning}")
        print()

    # Approval workflow
    if args.no_trade:
        return

    if not kalshi:
        print("  [Trade execution disabled — no Kalshi credentials]\n")
        return

    if not sys.stdin.isatty():
        print("  [Non-interactive mode — skipping approval workflow]\n")
        return

    print(f"{'─'*80}")
    print(f"  APPROVAL WORKFLOW")
    print(f"{'─'*80}\n")

    for i, pick in enumerate(picks, 1):
        side = pick.get("side", "yes")
        title = pick.get("title", "?")
        ticker = pick.get("ticker", "?")
        mkt_price = pick.get("market_price", 0)
        edge = pick.get("edge_pct", 0)
        confidence = pick.get("confidence", "?")

        if side == "yes":
            cost_per = mkt_price
        else:
            cost_per = 1 - mkt_price
        total_cost = cost_per * DEFAULT_CONTRACTS

        print(f"  [{i}] BUY {side.upper()} on {title} @ {int(mkt_price*100)}¢")
        print(f"      {DEFAULT_CONTRACTS} contracts = ${total_cost:.2f} max cost | Edge: +{edge:.1f}% ({confidence})")
        answer = input(f"      Execute? (yes/no): ").strip().lower()

        if answer in ("yes", "y"):
            try:
                result = place_trade(kalshi, ticker, side, DEFAULT_CONTRACTS, mkt_price)
                order_id = result.get("order", {}).get("order_id", "unknown")
                status = result.get("order", {}).get("status", "unknown")
                print(f"      >> Order placed! ID: {order_id} | Status: {status}\n")
                log_trade(pick, approved=True, order_result=result)
            except Exception as e:
                print(f"      >> Order failed: {e}\n")
                log_trade(pick, approved=True)
        else:
            print(f"      -- Skipped.\n")
            log_trade(pick, approved=False)

    print(f"  Trades logged to {TRADE_LOG}\n")


if __name__ == "__main__":
    main()
