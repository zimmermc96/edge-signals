#!/usr/bin/env python3
"""Cleveland Fed Inflation Nowcast Scraper

Fetches real-time inflation nowcasts from the Cleveland Fed's JSON API.
The Cleveland Fed updates these every business day around 10:00 AM ET.

Data source: https://www.clevelandfed.org/indicators-and-data/inflation-nowcasting

Three JSON endpoints provide nowcast data at different horizons:
  - Monthly:   month-over-month percent change
  - Quarterly: quarterly annualized percent change
  - Yearly:    year-over-year percent change

Each endpoint returns 4 nowcast series + 4 "actual" series:
  - CPI Inflation (headline)
  - Core CPI Inflation (ex food & energy)
  - PCE Inflation (headline)
  - Core PCE Inflation (ex food & energy)

Usage:
    python3 cleveland_fed_nowcast.py                  # print latest nowcasts
    python3 cleveland_fed_nowcast.py --history         # show daily evolution this month
    python3 cleveland_fed_nowcast.py --json            # output as JSON

As a module:
    from cleveland_fed_nowcast import get_nowcasts
    data = get_nowcasts()
    print(data["monthly"]["cpi"])  # e.g. 0.25 (MoM % change)
"""

import json
import ssl
import sys
import urllib.request
from datetime import datetime, timezone

BASE_URL = "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting"

ENDPOINTS = {
    "monthly": f"{BASE_URL}/nowcast_month.json?sc_lang=en",
    "quarterly": f"{BASE_URL}/nowcast_quarter.json?sc_lang=en",
    "yearly": f"{BASE_URL}/nowcast_year.json?sc_lang=en",
}

# Series names as they appear in the JSON -> our clean keys
SERIES_MAP = {
    "CPI Inflation": "cpi",
    "Core CPI Inflation": "core_cpi",
    "PCE Inflation": "pce",
    "Core PCE Inflation": "core_pce",
    "Actual CPI Inflation": "actual_cpi",
    "Actual Core CPI Inflation": "actual_core_cpi",
    "Actual PCE Inflation": "actual_pce",
    "Actual Core PCE Inflation": "actual_core_pce",
}

# Reusable SSL context (Cleveland Fed cert chain may not verify in all envs)
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def _fetch_json(url: str) -> list:
    """Fetch and parse JSON from a Cleveland Fed endpoint."""
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, context=_SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read())


def _parse_chart(chart_obj: dict) -> dict:
    """Parse a single chart object from the Cleveland Fed JSON.

    Returns dict with:
      - comment: date string from the chart metadata
      - subcaption: e.g. "2026-3" or "2026:Q1"
      - categories: list of date labels (business days)
      - series: dict mapping clean key -> list of (date_label, value) tuples
      - latest: dict mapping clean key -> latest non-empty float value
    """
    meta = chart_obj.get("chart", {})
    comment = meta.get("_comment", "")
    subcaption = meta.get("subcaption", "")
    yaxis = meta.get("yaxisname", "")

    # Extract category labels (the x-axis dates)
    categories = []
    for cat in chart_obj.get("categories", [{}])[0].get("category", []):
        label = cat.get("label", "")
        if cat.get("vline") == "true":
            continue  # skip vertical line markers
        categories.append(label)

    # Parse each dataset series
    series = {}
    latest = {}
    for ds in chart_obj.get("dataset", []):
        raw_name = ds.get("seriesname", "")
        key = SERIES_MAP.get(raw_name, raw_name)

        points = []
        for i, d in enumerate(ds.get("data", [])):
            val_str = d.get("value", "")
            if val_str:
                val = float(val_str)
                label = categories[i] if i < len(categories) else f"idx_{i}"
                points.append((label, val))

        series[key] = points
        if points:
            latest[key] = points[-1][1]

    return {
        "comment": comment,
        "subcaption": subcaption,
        "yaxis": yaxis,
        "categories": categories,
        "series": series,
        "latest": latest,
    }


def get_nowcasts(horizons=None) -> dict:
    """Fetch latest Cleveland Fed inflation nowcasts.

    Args:
        horizons: list of horizons to fetch, subset of
                  ["monthly", "quarterly", "yearly"]. Default: all three.

    Returns:
        Dict with structure:
        {
            "fetched_at": "2026-03-31T15:00:00Z",
            "monthly": {
                "as_of": "2026-03-31 00:00",
                "period": "2026-3",
                "unit": "Month-over-month percent change",
                "cpi": 0.844,
                "core_cpi": 0.204,
                "pce": 0.606,
                "core_pce": 0.233,
                "history": {  # daily evolution of nowcasts this month
                    "cpi": [("03/02", 0.252), ("03/03", 0.261), ...],
                    ...
                }
            },
            "quarterly": { ... },
            "yearly": { ... },
        }
    """
    if horizons is None:
        horizons = ["monthly", "quarterly", "yearly"]

    result = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    for horizon in horizons:
        url = ENDPOINTS.get(horizon)
        if not url:
            continue

        data = _fetch_json(url)
        if not data:
            continue

        # The JSON is an array of chart objects, one per month/quarter.
        # The last one is the most recent (current period).
        chart = _parse_chart(data[-1])

        result[horizon] = {
            "as_of": chart["comment"],
            "period": chart["subcaption"],
            "unit": chart["yaxis"],
            **chart["latest"],
            "history": {
                k: v for k, v in chart["series"].items()
                if not k.startswith("actual_")
            },
        }

    return result


def get_cpi_nowcast_mom() -> dict:
    """Convenience: get just the monthly CPI/Core CPI nowcast.

    Returns:
        {
            "as_of": "2026-03-31 00:00",
            "period": "2026-3",
            "cpi_mom": 0.844,       # headline CPI MoM %
            "core_cpi_mom": 0.204,  # core CPI MoM %
            "pce_mom": 0.606,       # headline PCE MoM %
            "core_pce_mom": 0.233,  # core PCE MoM %
        }
    """
    data = get_nowcasts(horizons=["monthly"])
    m = data.get("monthly", {})
    return {
        "as_of": m.get("as_of"),
        "period": m.get("period"),
        "cpi_mom": m.get("cpi"),
        "core_cpi_mom": m.get("core_cpi"),
        "pce_mom": m.get("pce"),
        "core_pce_mom": m.get("core_pce"),
    }


def print_nowcasts(data: dict, show_history: bool = False):
    """Pretty-print nowcast data to stdout."""
    print(f"\n{'='*65}")
    print(f"  CLEVELAND FED INFLATION NOWCAST")
    print(f"  Fetched: {data['fetched_at']}")
    print(f"{'='*65}")

    labels = {
        "cpi": "CPI (Headline)",
        "core_cpi": "Core CPI",
        "pce": "PCE (Headline)",
        "core_pce": "Core PCE",
    }

    horizon_names = {
        "monthly": "MONTH-OVER-MONTH",
        "quarterly": "QUARTERLY (ANNUALIZED)",
        "yearly": "YEAR-OVER-YEAR",
    }

    for horizon in ("monthly", "quarterly", "yearly"):
        h = data.get(horizon)
        if not h:
            continue

        print(f"\n  --- {horizon_names[horizon]} ---")
        print(f"  Period: {h['period']}  |  As of: {h['as_of']}")
        print(f"  Unit: {h['unit']}")
        print()
        print(f"  {'Measure':<20} {'Nowcast':>10}")
        print(f"  {'-'*32}")

        for key, label in labels.items():
            val = h.get(key)
            if val is not None:
                print(f"  {label:<20} {val:>10.4f}%")
            else:
                print(f"  {label:<20} {'N/A':>10}")

        # Show actuals if available
        for key in ("actual_cpi", "actual_core_cpi", "actual_pce", "actual_core_pce"):
            val = h.get(key)
            if val is not None:
                clean = key.replace("actual_", "")
                label = f"Actual {labels.get(clean, clean)}"
                print(f"  {label:<20} {val:>10.4f}%")

        if show_history and horizon == "monthly":
            history = h.get("history", {})
            print(f"\n  Daily Nowcast Evolution (this month):")
            for key, label in labels.items():
                pts = history.get(key, [])
                if pts:
                    print(f"\n  {label}:")
                    for date_label, val in pts:
                        print(f"    {date_label}: {val:.4f}%")

    print()


# ── Leading Indicators Reference ────────────────────────────────────────────
# These data sources publish BEFORE official CPI and can improve predictions:
#
# 1. GAS PRICES (largest swing factor for headline CPI)
#    - AAA Gas Prices: https://gasprices.aaa.com/
#    - EIA Weekly Retail: https://www.eia.gov/petroleum/gasdiesel/
#    - FRED series: GASREGW (weekly regular gas)
#
# 2. USED CAR PRICES (volatile CPI component)
#    - Manheim Index: https://www.coxautoinc.com/market-insights/manheim-used-vehicle-value-index/
#      Released ~5th business day of month. Leads CPI used cars by 1-2 months.
#    - FRED series: CUSR0000SETA02 (CPI used cars & trucks)
#
# 3. SHELTER / RENT (largest CPI weight, ~36%)
#    - Zillow ZORI: https://www.zillow.com/research/data/
#      Monthly. Leads CPI rent by ~12 months due to lease turnover lag.
#    - Apartment List Rent Index: https://www.apartmentlist.com/research/national-rent-data
#
# 4. OTHER
#    - NY Fed Underlying Inflation Gauge: FRED UIG series
#    - MIT Billion Prices Project (discontinued but was useful)
#    - Truflation: https://truflation.com/ (daily on-chain CPI estimate)


def main():
    show_history = "--history" in sys.argv
    as_json = "--json" in sys.argv

    try:
        data = get_nowcasts()
    except Exception as e:
        print(f"Error fetching nowcasts: {e}", file=sys.stderr)
        sys.exit(1)

    if as_json:
        # Remove history lists for clean JSON (they have tuples)
        output = {}
        for k, v in data.items():
            if isinstance(v, dict) and "history" in v:
                v = {kk: vv for kk, vv in v.items() if kk != "history"}
            output[k] = v
        print(json.dumps(output, indent=2))
    else:
        print_nowcasts(data, show_history=show_history)


if __name__ == "__main__":
    main()
