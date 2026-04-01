#!/usr/bin/env python3
"""
EdgeSignals Email Alert System

Scans prediction markets for high-edge signals and emails alerts to subscribers.

Usage:
    python3 alert_sender.py              # Send alerts for signals with edge > 15%
    python3 alert_sender.py --digest     # Send daily digest of ALL signals
    python3 alert_sender.py --dry-run    # Print email instead of sending
    python3 alert_sender.py --digest --dry-run

Environment variables:
    SMTP_HOST      - SMTP server hostname (default: smtp.gmail.com)
    SMTP_PORT      - SMTP server port (default: 587)
    SMTP_USER      - SMTP username / login
    SMTP_PASSWORD  - SMTP password or app-specific password
    SMTP_FROM      - From address (defaults to SMTP_USER)
"""

import argparse
import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
SUBSCRIBERS_PATH = BASE_DIR / "practice_data" / "subscribers.json"
ALERT_LOG_PATH = BASE_DIR / "practice_data" / "alert_log.jsonl"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("alert_sender")

# ---------------------------------------------------------------------------
# Subscriber helpers
# ---------------------------------------------------------------------------


def load_subscribers() -> list[dict]:
    """Load subscriber list from JSON file."""
    if not SUBSCRIBERS_PATH.exists():
        log.warning("Subscribers file not found at %s — creating sample file.", SUBSCRIBERS_PATH)
        SUBSCRIBERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        sample = [
            {"email": "demo@example.com", "plan": "pro", "api_key": "demo-key-001"},
        ]
        SUBSCRIBERS_PATH.write_text(json.dumps(sample, indent=2))
        return sample

    with open(SUBSCRIBERS_PATH) as f:
        subscribers = json.load(f)
    log.info("Loaded %d subscriber(s) from %s", len(subscribers), SUBSCRIBERS_PATH)
    return subscribers


# ---------------------------------------------------------------------------
# Scanner integration
# ---------------------------------------------------------------------------


def fetch_signals() -> dict:
    """Run all scanners and return the raw results dict."""
    # Import here so module-level import errors don't break --help
    sys.path.insert(0, str(BASE_DIR))
    from app.scanner_engine import scan_all
    log.info("Running scanners...")
    results = scan_all()
    log.info(
        "Scan complete — weather: %d, crypto: %d, economics: %d",
        len(results.get("weather", [])),
        len(results.get("crypto", [])),
        len(results.get("economics", [])),
    )
    return results


def filter_signals(results: dict, min_edge: float = 15.0) -> list[dict]:
    """Return signals whose absolute edge exceeds *min_edge* percent."""
    filtered = []
    for category in ("weather", "crypto", "economics"):
        for sig in results.get(category, []):
            if abs(sig.get("edge", 0)) >= min_edge:
                filtered.append(sig)
    filtered.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    return filtered


def all_signals(results: dict) -> list[dict]:
    """Return every signal from every scanner (for digest mode)."""
    combined = []
    for category in ("weather", "crypto", "economics"):
        combined.extend(results.get(category, []))
    combined.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    return combined


# ---------------------------------------------------------------------------
# Email formatting
# ---------------------------------------------------------------------------

EDGE_COLORS = {
    "high": "#16a34a",   # green
    "medium": "#ca8a04",  # amber
    "low": "#6b7280",    # gray
}


def _signal_row(sig: dict) -> str:
    """Build one <tr> for the signals table."""
    edge = sig.get("edge", 0)
    edge_color = "#16a34a" if edge > 0 else "#dc2626"
    confidence = sig.get("confidence", "medium")
    conf_color = EDGE_COLORS.get(confidence, "#6b7280")
    return f"""<tr>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;font-family:monospace;">
            {sig.get('ticker', 'N/A')}
        </td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:{edge_color};font-weight:600;">
            {edge:+.1f}%
        </td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;">
            {sig.get('signal', 'INFO')}
        </td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;">
            <span style="color:{conf_color};font-weight:600;">{confidence.upper()}</span>
        </td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;">
            {sig.get('label', '')}
        </td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;">
            {sig.get('category', '')}
        </td>
    </tr>"""


def build_email_html(signals: list[dict], digest: bool = False) -> str:
    """Build a professional HTML email body."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    mode = "Daily Digest" if digest else "Alert"

    rows = "\n".join(_signal_row(s) for s in signals)

    html = f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:700px;margin:20px auto;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1e3a5f,#2563eb);padding:24px 32px;color:#ffffff;">
      <h1 style="margin:0;font-size:22px;font-weight:700;">EdgeSignals {mode}</h1>
      <p style="margin:6px 0 0;font-size:13px;opacity:0.85;">{now_str} &mdash; {len(signals)} signal{'s' if len(signals) != 1 else ''} detected</p>
    </div>

    <!-- Signals table -->
    <div style="padding:24px 32px;">
      <table style="width:100%;border-collapse:collapse;font-size:14px;">
        <thead>
          <tr style="background:#f1f5f9;">
            <th style="padding:10px 12px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;">Ticker</th>
            <th style="padding:10px 12px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;">Edge</th>
            <th style="padding:10px 12px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;">Signal</th>
            <th style="padding:10px 12px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;">Confidence</th>
            <th style="padding:10px 12px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;">Description</th>
            <th style="padding:10px 12px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;">Category</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </div>

    <!-- Footer -->
    <div style="padding:16px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;font-size:12px;color:#6b7280;">
      <p style="margin:0;">
        You are receiving this because you are subscribed to EdgeSignals alerts.<br>
        To unsubscribe, reply to this email or contact
        <a href="mailto:unsubscribe@edgesignals.io?subject=Unsubscribe" style="color:#2563eb;">unsubscribe@edgesignals.io</a>.
      </p>
      <p style="margin:8px 0 0;font-size:11px;color:#9ca3af;">
        This is not financial advice. Past signals do not guarantee future performance.
      </p>
    </div>

  </div>
</body>
</html>"""
    return html


def build_subject(signals: list[dict], digest: bool = False) -> str:
    prefix = "EdgeSignals Digest" if digest else "EdgeSignals"
    return f"{prefix}: {len(signals)} new signal{'s' if len(signals) != 1 else ''} detected"


# ---------------------------------------------------------------------------
# SMTP sending
# ---------------------------------------------------------------------------


def get_smtp_config() -> dict:
    return {
        "host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
        "port": int(os.environ.get("SMTP_PORT", "587")),
        "user": os.environ.get("SMTP_USER", ""),
        "password": os.environ.get("SMTP_PASSWORD", ""),
        "from_addr": os.environ.get("SMTP_FROM") or os.environ.get("SMTP_USER", "alerts@edgesignals.io"),
    }


def send_email(to_addr: str, subject: str, html_body: str, smtp_cfg: dict) -> bool:
    """Send a single HTML email via SMTP. Returns True on success."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_cfg["from_addr"]
    msg["To"] = to_addr

    # Plain-text fallback
    plain = "This email requires an HTML-capable mail client. View it in your browser."
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(smtp_cfg["host"], smtp_cfg["port"], timeout=30) as server:
            server.ehlo()
            if smtp_cfg["port"] != 25:
                server.starttls()
                server.ehlo()
            if smtp_cfg["user"] and smtp_cfg["password"]:
                server.login(smtp_cfg["user"], smtp_cfg["password"])
            server.sendmail(smtp_cfg["from_addr"], [to_addr], msg.as_string())
        log.info("Sent email to %s", to_addr)
        return True
    except Exception as exc:
        log.error("Failed to send to %s: %s", to_addr, exc)
        return False


# ---------------------------------------------------------------------------
# Alert log
# ---------------------------------------------------------------------------


def log_alert(subscriber: dict, signals: list[dict], success: bool, digest: bool, dry_run: bool):
    """Append one JSON line to the alert log."""
    ALERT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "email": subscriber["email"],
        "plan": subscriber.get("plan", "free"),
        "num_signals": len(signals),
        "top_edge": max((abs(s.get("edge", 0)) for s in signals), default=0),
        "digest": digest,
        "dry_run": dry_run,
        "success": success,
    }
    with open(ALERT_LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------


def run(dry_run: bool = False, digest: bool = False):
    subscribers = load_subscribers()
    if not subscribers:
        log.warning("No subscribers — nothing to do.")
        return

    results = fetch_signals()

    if digest:
        signals = all_signals(results)
    else:
        signals = filter_signals(results, min_edge=15.0)

    if not signals:
        log.info("No signals meet the threshold — no emails to send.")
        return

    subject = build_subject(signals, digest=digest)
    html_body = build_email_html(signals, digest=digest)
    smtp_cfg = get_smtp_config()

    log.info(
        "Preparing to %s '%s' (%d signals) to %d subscriber(s)",
        "print" if dry_run else "send",
        subject,
        len(signals),
        len(subscribers),
    )

    if dry_run:
        print("\n" + "=" * 70)
        print(f"SUBJECT: {subject}")
        print(f"TO: {', '.join(s['email'] for s in subscribers)}")
        print("=" * 70)
        print(html_body)
        print("=" * 70 + "\n")
        for sub in subscribers:
            log_alert(sub, signals, success=True, digest=digest, dry_run=True)
        log.info("Dry run complete — email printed above.")
        return

    # Actually send
    if not smtp_cfg["user"] or not smtp_cfg["password"]:
        log.error(
            "SMTP_USER and SMTP_PASSWORD must be set to send emails. "
            "Use --dry-run to preview without sending."
        )
        sys.exit(1)

    for sub in subscribers:
        ok = send_email(sub["email"], subject, html_body, smtp_cfg)
        log_alert(sub, signals, success=ok, digest=digest, dry_run=False)

    log.info("Done. Alert log written to %s", ALERT_LOG_PATH)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="EdgeSignals — email alert system for prediction market signals",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the email to stdout instead of sending it",
    )
    parser.add_argument(
        "--digest",
        action="store_true",
        help="Send a daily digest of ALL signals (ignores edge threshold)",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run, digest=args.digest)
