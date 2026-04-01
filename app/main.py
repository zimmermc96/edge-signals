"""EdgeSignals — Prediction Market Signal Platform

FastAPI app serving a dashboard and API for prediction market signals.
"""

import os
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import jinja2

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


# ── API Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/signals")
async def get_signals():
    """Get all current signals across all categories."""
    from app.scanner_engine import scan_all
    return scan_all()


@app.get("/api/signals/weather")
async def get_weather_signals():
    from app.scanner_engine import scan_weather
    return {"signals": scan_weather()}


@app.get("/api/signals/crypto")
async def get_crypto_signals():
    from app.scanner_engine import scan_crypto
    return {"signals": scan_crypto()}


@app.get("/api/signals/economics")
async def get_econ_signals():
    from app.scanner_engine import scan_economics
    return {"signals": scan_economics()}


# ── Web Dashboard ───────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    from app.scanner_engine import scan_all
    data = scan_all()
    return render("dashboard.html", {
        "request": request,
        "data": data,
        "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    })


@app.get("/pricing", response_class=HTMLResponse)
async def pricing(request: Request):
    return render("pricing.html", {"request": request})
