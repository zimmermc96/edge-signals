# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Kalshi prediction market research and weather trading tools.

## Commands

```bash
pip3 install -r requirements.txt        # install deps
python3 kalshi_scanner.py               # top markets by volume + optional AI analysis
python3 kalshi_weather_bot.py           # weather trading assistant
```

Environment variables:
- `ANTHROPIC_API_KEY` — enables Claude-powered news analysis in kalshi_scanner.py
- `KALSHI_API_KEY_ID` — Kalshi API key UUID (enables trading in weather bot)
- `KALSHI_PRIVATE_KEY_PATH` — path to RSA private key file from Kalshi

## Architecture

- **kalshi_scanner.py** — fetches all open markets, sorts by 24h volume, prints top 20 with yes/no prices, optionally uses Claude to flag news/odds disagreements.
- **kalshi_weather_bot.py** — weather-specific trading assistant:
  - Fetches weather markets (temp highs for NYC/Chicago/Miami/Austin/Denver + NYC rain)
  - Pulls NWS forecasts from weather.gov (free, no key)
  - Compares Kalshi odds vs forecast probability using normal distribution model (±3°F spread)
  - Flags markets with edge >= 10%, filters out illiquid (1¢/99¢) and near-close markets
  - Interactive approval workflow: approve/reject each trade, executes via Kalshi API
  - Logs all trades to `trade_log.csv`

## Kalshi API

- Base URL: `https://api.elections.kalshi.com/trade-api/v2`
- Market data endpoints are **public** (no auth needed): `/markets`, `/markets/{ticker}`, `/markets/{ticker}/orderbook`
- Trading endpoints require RSA-PSS signature auth (3 headers: KEY, TIMESTAMP, SIGNATURE)
- No server-side sort — must paginate all markets and sort client-side
- Rate limit: 20 reads/sec (basic tier)
- Weather series tickers: `KXHIGHNY`, `KXHIGHCHI`, `KXHIGHMIA`, `KXHIGHAUS`, `KXHIGHDEN`, `KXRAINNYC`
- Weather markets are multi-outcome (temp brackets) or binary (rain yes/no)
- Settlement source: NWS Climatological Report
