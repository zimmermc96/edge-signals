"""EdgeSignals — Prediction Market Signal Platform

FastAPI app serving a dashboard and API for prediction market signals.
"""

import os
import json
import time
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import stripe
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
import jinja2

# ── Stripe Configuration ──────────────────────────────────────────────────
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

PLAN_PRICE_MAP = {
    "pro": {
        "name": "Pro",
        "amount": 2900,      # $29.00 in cents
        "interval": "month",
    },
    "algo": {
        "name": "Algo",
        "amount": 9900,      # $99.00 in cents
        "interval": "month",
    },
}

SUBSCRIBERS_FILE = Path(__file__).parent.parent / "practice_data" / "subscribers.json"


def _load_subscribers() -> dict:
    try:
        if SUBSCRIBERS_FILE.exists():
            return json.loads(SUBSCRIBERS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _save_subscribers(data: dict):
    try:
        SUBSCRIBERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SUBSCRIBERS_FILE.write_text(json.dumps(data, indent=2))
    except OSError:
        # Ephemeral filesystem (e.g., Render free tier) — log but don't crash
        import logging
        logging.warning("Could not write subscribers file (ephemeral filesystem?)")

app = FastAPI(title="EdgeSignals", version="0.1.0")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Use jinja2 directly to avoid Starlette template cache bug on Python 3.14
_jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(BASE_DIR / "templates")),
    autoescape=True,
)


def render(template_name: str, context: dict) -> HTMLResponse:
    tmpl = _jinja_env.get_template(template_name)
    html = tmpl.render(**context)
    return HTMLResponse(html)


# ── Background Scan Cache ──────────────────────────────────────────────────

SCAN_CACHE_TTL = 300  # 5 minutes

_scan_cache = {
    "dashboard": None,   # cached result for weather + economics (fast scanners)
    "crypto": None,      # cached result for crypto scanner (slow)
    "full": None,        # cached result for scan_all (API)
}
_scan_timestamps = {
    "dashboard": 0.0,
    "crypto": 0.0,
    "full": 0.0,
}
_scan_lock = threading.Lock()
_refresh_in_progress = {
    "dashboard": False,
    "crypto": False,
    "full": False,
}


def _build_dashboard_data(weather, economics, crypto=None):
    """Build dashboard-shaped data dict from individual scan results."""
    data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "weather": weather or [],
        "crypto": crypto or [],
        "economics": economics or [],
    }
    all_edges = []
    for cat in ["weather", "crypto"]:
        for s in data[cat]:
            if abs(s.get("edge", 0)) > 8:
                all_edges.append(s)
    all_edges.sort(key=lambda s: abs(s.get("edge", 0)), reverse=True)
    data["top_edges"] = all_edges[:10]
    return data


def _run_dashboard_scan():
    """Run weather + economics only (skips slow crypto)."""
    from app.scanner_engine import scan_weather, scan_economics
    weather = scan_weather()
    economics = scan_economics()
    data = _build_dashboard_data(weather, economics)
    with _scan_lock:
        _scan_cache["dashboard"] = data
        _scan_timestamps["dashboard"] = time.time()
        _refresh_in_progress["dashboard"] = False
    return data


def _run_crypto_scan():
    """Run crypto scanner only."""
    from app.scanner_engine import scan_crypto
    crypto = scan_crypto()
    with _scan_lock:
        _scan_cache["crypto"] = crypto
        _scan_timestamps["crypto"] = time.time()
        _refresh_in_progress["crypto"] = False
    return crypto


def _run_full_scan():
    """Run all scanners (for API endpoint)."""
    from app.scanner_engine import scan_all
    data = scan_all()
    with _scan_lock:
        _scan_cache["full"] = data
        _scan_timestamps["full"] = time.time()
        _refresh_in_progress["full"] = False
    return data


def _get_cached_or_refresh(cache_key, scan_func):
    """Return cached result if fresh, else serve stale + trigger background refresh."""
    now = time.time()
    with _scan_lock:
        cached = _scan_cache[cache_key]
        age = now - _scan_timestamps[cache_key]
        refreshing = _refresh_in_progress[cache_key]

    # Cache is fresh
    if cached is not None and age < SCAN_CACHE_TTL:
        return cached

    # Cache is stale but exists — serve it and refresh in background
    if cached is not None and not refreshing:
        with _scan_lock:
            _refresh_in_progress[cache_key] = True
        t = threading.Thread(target=scan_func, daemon=True)
        t.start()
        return cached

    # No cache at all — must block and scan now
    if cached is None:
        return scan_func()

    # Refresh already in progress, serve stale
    return cached


# ── API Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/signals")
async def get_signals():
    """Get all current signals across all categories."""
    return _get_cached_or_refresh("full", _run_full_scan)


@app.get("/api/signals/weather")
async def get_weather_signals():
    from app.scanner_engine import scan_weather
    return {"signals": scan_weather()}


@app.get("/api/signals/crypto")
async def get_crypto_signals():
    """Get crypto signals (uses cache if available)."""
    crypto = _get_cached_or_refresh("crypto", _run_crypto_scan)
    return {"signals": crypto}


@app.get("/api/signals/economics")
async def get_econ_signals():
    from app.scanner_engine import scan_economics
    return {"signals": scan_economics()}


@app.post("/api/refresh")
async def force_refresh():
    """Force a fresh scan of all scanners. Returns immediately, scan runs in background."""
    triggered = []
    for key, func in [("dashboard", _run_dashboard_scan), ("crypto", _run_crypto_scan), ("full", _run_full_scan)]:
        with _scan_lock:
            if not _refresh_in_progress[key]:
                _refresh_in_progress[key] = True
                t = threading.Thread(target=func, daemon=True)
                t.start()
                triggered.append(key)
    return {"status": "ok", "triggered": triggered}


# ── Web Dashboard ───────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Serve weather + economics from cache (fast); crypto loaded separately
    data = _get_cached_or_refresh("dashboard", _run_dashboard_scan)

    # Merge in crypto from its own cache if available (non-blocking)
    with _scan_lock:
        crypto_cached = _scan_cache["crypto"]
        crypto_age = time.time() - _scan_timestamps["crypto"]

    crypto_available = crypto_cached is not None and crypto_age < SCAN_CACHE_TTL * 2
    if crypto_available:
        # Rebuild data with crypto included for top_edges
        data = _build_dashboard_data(data["weather"], data["economics"], crypto_cached)

    return render("dashboard.html", {
        "request": request,
        "data": data,
        "crypto_available": crypto_available,
        "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    })


@app.get("/track-record", response_class=HTMLResponse)
async def track_record(request: Request):
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from accuracy_tracker import get_accuracy_report
    report = get_accuracy_report()
    return render("track_record.html", {
        "request": request,
        "report": report,
    })


@app.get("/pricing", response_class=HTMLResponse)
async def pricing(request: Request):
    return render("pricing.html", {
        "request": request,
        "stripe_publishable_key": STRIPE_PUBLISHABLE_KEY,
    })


# ── Stripe Payment Endpoints ──────────────────────────────────────────────

@app.post("/api/create-checkout")
async def create_checkout(request: Request):
    """Create a Stripe Checkout session for Pro or Algo plan."""
    form = await request.form()
    plan = form.get("plan", "").lower()

    if plan not in PLAN_PRICE_MAP:
        raise HTTPException(status_code=400, detail=f"Invalid plan: {plan}")

    plan_info = PLAN_PRICE_MAP[plan]
    base_url = str(request.base_url).rstrip("/")

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"EdgeSignals {plan_info['name']}",
                        "description": f"EdgeSignals {plan_info['name']} — monthly subscription",
                    },
                    "unit_amount": plan_info["amount"],
                    "recurring": {"interval": plan_info["interval"]},
                },
                "quantity": 1,
            }],
            metadata={"plan": plan},
            success_url=f"{base_url}/api/checkout-success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{base_url}/pricing",
        )
    except stripe.StripeError as e:
        raise HTTPException(status_code=502, detail=str(e))

    return RedirectResponse(url=session.url, status_code=303)


@app.get("/api/checkout-success", response_class=HTMLResponse)
async def checkout_success(request: Request, session_id: str = ""):
    """Handle redirect after successful Stripe Checkout."""
    if not session_id:
        return RedirectResponse(url="/pricing")

    try:
        session = stripe.checkout.Session.retrieve(session_id)
    except stripe.StripeError:
        return RedirectResponse(url="/pricing")

    email = session.customer_details.email if session.customer_details else "unknown"
    plan = session.metadata.get("plan", "unknown")

    # Store subscriber with a generated API key
    subs = _load_subscribers()
    if email not in subs:
        subs[email] = {
            "plan": plan,
            "api_key": str(uuid.uuid4()),
            "stripe_customer_id": session.customer or "",
            "stripe_subscription_id": session.subscription or "",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    else:
        # Upgrade existing subscriber
        subs[email]["plan"] = plan
        subs[email]["stripe_subscription_id"] = session.subscription or ""
    _save_subscribers(subs)

    api_key = subs[email]["api_key"]

    return render("checkout_success.html", {
        "request": request,
        "email": email,
        "plan": plan,
        "api_key": api_key,
    })


@app.post("/api/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events for subscription lifecycle."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except (ValueError, stripe.SignatureVerificationError):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    event_type = event["type"]
    data_object = event["data"]["object"]

    if event_type == "checkout.session.completed":
        email = data_object.get("customer_details", {}).get("email", "")
        plan = data_object.get("metadata", {}).get("plan", "unknown")
        if email:
            subs = _load_subscribers()
            if email not in subs:
                subs[email] = {
                    "plan": plan,
                    "api_key": str(uuid.uuid4()),
                    "stripe_customer_id": data_object.get("customer", ""),
                    "stripe_subscription_id": data_object.get("subscription", ""),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            _save_subscribers(subs)

    elif event_type in (
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        # If subscription canceled or changed, update the record
        customer_id = data_object.get("customer", "")
        status = data_object.get("status", "")
        subs = _load_subscribers()
        for email, info in subs.items():
            if info.get("stripe_customer_id") == customer_id:
                if status in ("canceled", "unpaid", "past_due"):
                    info["plan"] = "free"
                info["subscription_status"] = status
                break
        _save_subscribers(subs)

    return JSONResponse({"status": "ok"})
