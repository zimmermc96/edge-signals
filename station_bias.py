#!/usr/bin/env python3
"""Station Bias Model: GFS (Open-Meteo) vs CLI Actuals

Built from March 2026 data (30 days). Provides bias correction factors
for adjusting GFS model forecasts to better predict CLI settlement values
on Kalshi weather markets.

Usage:
    from station_bias import correct_gfs_forecast, BIAS_TABLE

    corrected = correct_gfs_forecast("KXHIGHNY", gfs_value=62.0)
    # Returns dict with corrected temp, confidence band, method used
"""

# ── Bias correction table ────────────────────────────────────────────────────
# Derived from 30-day comparison: Open-Meteo GFS archive vs CLI actuals
# March 1-30, 2026
#
# avg_bias:    mean(GFS - CLI).  Negative = GFS runs cold vs CLI.
# std_bias:    standard deviation of daily bias (1-sigma uncertainty)
# mae:         mean absolute error
# direction:   fraction of days GFS ran cold (< 0)
# linear:      (slope, intercept) for temperature-dependent correction
#              bias = slope * GFS + intercept  =>  corrected = GFS * (1-slope) - intercept
#              Only populated when |correlation| > 0.30
# consistency: "high" if std < 2.0, "medium" if < 3.0, "low" otherwise

BIAS_TABLE = {
    "KXHIGHNY": {
        "station": "NYC (Central Park)",
        "avg_bias": -1.56,
        "median_bias": -2.00,
        "std_bias": 2.94,
        "mae": 2.68,
        "pct_cold": 80,
        "consistency": "medium",
        "linear": None,  # r = -0.06, no temp-dependent pattern
        "notes": "GFS consistently undershoots CLI by ~1.6F. Flat correction adequate.",
    },
    "KXHIGHCHI": {
        "station": "Chicago (Midway)",
        "avg_bias": -1.97,
        "median_bias": -1.85,
        "std_bias": 2.05,
        "mae": 2.16,
        "pct_cold": 83,
        "consistency": "medium",
        "linear": (-0.0746, 1.97),  # r = -0.61, strong temp dependence
        "notes": "Strong temp-dependent cold bias. GFS misses warm days badly "
                 "(up to -7.9F on Mar 21 when CLI=77). Use linear correction.",
    },
    "KXHIGHMIA": {
        "station": "Miami (MIA Airport)",
        "avg_bias": -1.71,
        "median_bias": -1.60,
        "std_bias": 1.80,
        "mae": 1.98,
        "pct_cold": 87,
        "consistency": "high",
        "linear": (0.0462, -5.41),  # r = -0.36, moderate
        "notes": "Most consistent station. GFS almost always undershoots by 1-2F. "
                 "Tight confidence band. High-confidence correction.",
    },
    "KXHIGHAUS": {
        "station": "Austin (Bergstrom)",
        "avg_bias": -2.80,
        "median_bias": -3.15,
        "std_bias": 1.64,
        "mae": 3.01,
        "pct_cold": 93,
        "consistency": "high",
        "linear": (-0.0954, 4.89),  # r = -0.62, strong temp dependence
        "notes": "Largest and most reliable cold bias. GFS undershoots CLI by ~3F "
                 "on average, worse on hot days. 93% of days GFS was cold. "
                 "Highest-conviction correction of all stations.",
    },
    "KXHIGHDEN": {
        "station": "Denver (DEN)",
        "avg_bias": +0.66,
        "median_bias": -0.20,
        "std_bias": 3.14,
        "mae": 2.38,
        "pct_cold": 53,
        "consistency": "low",
        "linear": None,  # r = -0.26, too noisy
        "notes": "Only station where GFS runs slightly warm on average, but median "
                 "is near zero and direction is split 47/53. High variance makes "
                 "correction unreliable. Treat GFS as unbiased here; widen spread.",
    },
}


def correct_gfs_forecast(series_ticker: str, gfs_value: float) -> dict:
    """Apply bias correction to a raw GFS forecast value.

    Args:
        series_ticker: Kalshi series ticker (e.g. "KXHIGHNY")
        gfs_value: Raw GFS forecast temperature in Fahrenheit

    Returns:
        dict with keys:
            corrected: bias-corrected temperature (float)
            method: "linear" or "flat" correction method used
            confidence_1sigma: +/- range for 68% confidence
            confidence_2sigma: +/- range for 95% confidence
            raw_bias: the bias value subtracted
    """
    if series_ticker not in BIAS_TABLE:
        return {
            "corrected": gfs_value,
            "method": "none",
            "confidence_1sigma": 3.0,
            "confidence_2sigma": 6.0,
            "raw_bias": 0.0,
        }

    entry = BIAS_TABLE[series_ticker]

    if entry["linear"] is not None:
        slope, intercept = entry["linear"]
        estimated_bias = slope * gfs_value + intercept
        corrected = gfs_value - estimated_bias
        method = "linear"
    else:
        estimated_bias = entry["avg_bias"]
        corrected = gfs_value - entry["avg_bias"]
        method = "flat"

    return {
        "corrected": round(corrected, 1),
        "method": method,
        "confidence_1sigma": round(entry["std_bias"], 1),
        "confidence_2sigma": round(entry["std_bias"] * 2, 1),
        "raw_bias": round(estimated_bias, 2),
    }


def get_adjusted_spread(series_ticker: str, base_spread: float) -> float:
    """Return a spread value adjusted for station bias reliability.

    For stations with high-consistency bias correction, we can tighten
    the spread (more confident in our corrected forecast).
    For low-consistency stations, widen the spread.

    Args:
        series_ticker: Kalshi series ticker
        base_spread: the base NWS spread from WEATHER_SERIES config

    Returns:
        Adjusted spread value
    """
    if series_ticker not in BIAS_TABLE:
        return base_spread

    entry = BIAS_TABLE[series_ticker]
    if entry["consistency"] == "high":
        return base_spread * 0.85  # tighten -- we're more confident after correction
    elif entry["consistency"] == "low":
        return base_spread * 1.15  # widen -- correction is unreliable
    else:
        return base_spread  # medium consistency, keep as-is


# ── Quick summary printout ───────────────────────────────────────────────────

if __name__ == "__main__":
    print("Station Bias Correction Factors (March 2026, 30 days)")
    print("=" * 75)
    print(f"{'Ticker':<12} {'Station':<25} {'Avg Bias':>9} {'Std':>6} {'MAE':>6} {'Consist':>10}")
    print("-" * 75)
    for ticker, d in BIAS_TABLE.items():
        print(
            f"{ticker:<12} {d['station']:<25} {d['avg_bias']:>+9.2f} "
            f"{d['std_bias']:>6.2f} {d['mae']:>6.2f} {d['consistency']:>10}"
        )

    print("\nExample corrections:")
    for ticker in BIAS_TABLE:
        # Simulate a 70F GFS forecast
        result = correct_gfs_forecast(ticker, 70.0)
        print(
            f"  {ticker}: GFS=70.0F -> Corrected={result['corrected']}F "
            f"({result['method']}, bias={result['raw_bias']:+.2f}F, "
            f"+/-{result['confidence_1sigma']}F)"
        )
