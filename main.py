from __future__ import annotations

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import asyncio
import hashlib
import hmac
import logging
import os
import threading
import time

import anyio
import requests
from mcp.server.mcpserver import MCPServer
from starlette.responses import JSONResponse
from starlette.routing import Route

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("claudeworkemail")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    on_startup()
    # The MCP transport needs its session manager running for the app's lifetime.
    async with mcp_server.session_manager.run():
        yield


app = FastAPI(lifespan=lifespan)


def _clean_env(name: str) -> str | None:
    """Read an env var, stripping stray whitespace and wrapping quotes."""
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip().strip('"').strip("'").strip()
    return v or None


CLAY_WEBHOOK_URL = _clean_env("CLAY_WEBHOOK_URL")
CLAY_AUTH_TOKEN = _clean_env("CLAY_AUTH_TOKEN")
# Clay table webhook sources check this header for the source's auth token.
CLAY_AUTH_HEADER = _clean_env("CLAY_AUTH_HEADER") or "x-clay-webhook-auth"
# A lookup still pending after this long becomes "timeout".
PENDING_TTL_SECONDS = int(_clean_env("PENDING_TTL_SECONDS") or 1800)
# Shared secret for the MCP endpoint. Unset or too short = endpoint disabled.
MCP_SECRET = _clean_env("MCP_SECRET")
MCP_SECRET_MIN_LENGTH = 32
# How long one find_email tool call waits for Clay's callback before returning "pending".
MCP_WAIT_SECONDS = int(_clean_env("MCP_WAIT_SECONDS") or 50)

# In-memory store: lookup_key -> {status, work_email, submitted_at, ...}
# Wiped on every restart/redeploy.
results_store: dict[str, dict] = {}
store_lock = threading.Lock()

# Outcome of the most recent push to Clay, for /health.
last_clay_push: dict = {"status_code": None, "ok": None, "at": None, "error": None}

TERMINAL = {"completed", "not_found", "failed", "timeout"}


def _iso(ts: float | None) -> str | None:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)) if ts else None


def _redact(text: str) -> str:
    """Strip secrets from anything we log or return."""
    if not text:
        return ""
    for secret in (CLAY_AUTH_TOKEN, CLAY_WEBHOOK_URL):
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text


def make_lookup_key(full_name: str, company_domain: str) -> str:
    """Generate a consistent lookup key from name + domain."""
    raw = f"{full_name.strip().lower()}|{company_domain.strip().lower()}"
    return hashlib.md5(raw.encode()).hexdigest()


def expire_stale() -> None:
    """Turn any lookup pending longer than the TTL into 'timeout'."""
    cutoff = time.time() - PENDING_TTL_SECONDS
    with store_lock:
        for entry in results_store.values():
            if entry.get("status") == "pending" and entry.get("submitted_at", 0) < cutoff:
                entry["status"] = "timeout"
                entry["reason"] = "no_callback_within_ttl"
                entry["completed_at"] = time.time()


def _load_backlog_seed() -> None:
    """Load lookups to pre-mark as terminal from the BACKLOG_SEED variable.

    Format: comma-separated entries "lookup_key|status|reason|submitted_at"
    (submitted_at = epoch seconds, optional). Used to keep pre-fix stuck
    lookups answering "failed"/"timeout" instead of 404 after a redeploy
    wipes the in-memory store. Never re-runs them.
    """
    raw = os.getenv("BACKLOG_SEED")
    if not raw:
        return
    n = 0
    with store_lock:
        for item in raw.split(","):
            parts = item.strip().split("|")
            if len(parts) < 3:
                continue
            key, status, reason = parts[0], parts[1], parts[2]
            if not key or status not in ("failed", "timeout") or key in results_store:
                continue
            try:
                submitted_at = float(parts[3]) if len(parts) > 3 and parts[3] else None
            except ValueError:
                submitted_at = None
            results_store[key] = {
                "status": status,
                "reason": reason,
                "work_email": None,
                "full_name": None,
                "company_domain": None,
                "submitted_at": submitted_at,
                "completed_at": time.time(),
                "seeded": True,
            }
            n += 1
    log.info("Loaded %d backlog lookups from BACKLOG_SEED", n)


def on_startup():
    _load_backlog_seed()
    if not CLAY_WEBHOOK_URL:
        log.error("CLAY_WEBHOOK_URL is not set")
    if not CLAY_AUTH_TOKEN:
        log.warning("CLAY_AUTH_TOKEN is not set; Clay will reject pushes if the source requires auth")
    if not _mcp_enabled():
        log.warning("MCP_SECRET is unset or shorter than %d chars; /mcp is disabled", MCP_SECRET_MIN_LENGTH)


@app.get("/health")
def health():
    expire_stale()
    with store_lock:
        counts: dict[str, int] = {}
        for r in results_store.values():
            counts[r["status"]] = counts.get(r["status"], 0) + 1
    degraded = last_clay_push["ok"] is False
    return {
        "ok": not degraded,
        "status": "degraded" if degraded else "ok",
        "pending": counts.get("pending", 0),
        "counts": counts,
        "last_clay_push": {
            "status_code": last_clay_push["status_code"],
            "ok": last_clay_push["ok"],
            "at": _iso(last_clay_push["at"]),
            "error": last_clay_push["error"],
        },
        "config": {
            "webhook_url_set": bool(CLAY_WEBHOOK_URL),
            "auth_token_set": bool(CLAY_AUTH_TOKEN),
            "auth_header": CLAY_AUTH_HEADER,
            "pending_ttl_seconds": PENDING_TTL_SECONDS,
            "mcp_enabled": _mcp_enabled(),
        },
    }


def _record_push(status_code: int | None, ok: bool, error: str | None) -> None:
    last_clay_push.update(status_code=status_code, ok=ok, at=time.time(), error=error)


def _fail(lookup_key: str, reason: str, status_code: int | None, excerpt: str) -> None:
    with store_lock:
        e = results_store[lookup_key]
        e["status"] = "failed"
        e["reason"] = reason
        e["clay_status_code"] = status_code
        e["error"] = excerpt
        e["completed_at"] = time.time()


# ── Step 1: Skill sends contact info here ──────────────────────────
@app.post("/find-email")
def find_email(payload: dict):
    """
    Accepts: { "full_name", "company", "company_domain", "linkedin_url" }
    Forwards to Clay webhook, stores a pending result.
    Returns the lookup_key for polling.
    """
    if not CLAY_WEBHOOK_URL:
        raise HTTPException(status_code=500, detail="CLAY_WEBHOOK_URL not set")

    full_name = payload.get("full_name", "")
    company_domain = payload.get("company_domain", "")

    if not full_name or not company_domain:
        raise HTTPException(status_code=400, detail="full_name and company_domain are required")

    expire_stale()
    lookup_key = make_lookup_key(full_name, company_domain)
    return submit_lookup(payload, lookup_key)


def submit_lookup(payload: dict, lookup_key: str) -> dict:
    """Store a pending lookup and push it to Clay. Shared by POST /find-email and the MCP tool."""
    with store_lock:
        results_store[lookup_key] = {
            "status": "pending",
            "work_email": None,
            "full_name": payload.get("full_name", ""),
            "company": payload.get("company", ""),
            "company_domain": payload.get("company_domain", ""),
            "linkedin_url": payload.get("linkedin_url", ""),
            "submitted_at": time.time(),
        }

    headers = {"Content-Type": "application/json"}
    if CLAY_AUTH_TOKEN:
        headers[CLAY_AUTH_HEADER] = CLAY_AUTH_TOKEN

    # Include lookup_key so Clay can send it back in the callback
    clay_payload = {**payload, "lookup_key": lookup_key}

    try:
        r = requests.post(CLAY_WEBHOOK_URL, json=clay_payload, headers=headers, timeout=30)
    except requests.Timeout:
        _record_push(None, False, "timeout")
        _fail(lookup_key, "clay_timeout", None, "Clay webhook timed out")
        log.warning("Clay push timed out for %s", lookup_key)
        raise HTTPException(status_code=504, detail="Clay webhook timed out")
    except requests.RequestException as exc:
        msg = _redact(str(exc))[:300]
        _record_push(None, False, msg)
        _fail(lookup_key, "clay_unreachable", None, msg)
        log.warning("Clay push failed for %s: %s", lookup_key, msg)
        raise HTTPException(status_code=502, detail="Clay webhook unreachable")

    if not 200 <= r.status_code < 300:
        excerpt = _redact(r.text or "")[:300]
        _record_push(r.status_code, False, excerpt or f"HTTP {r.status_code}")
        _fail(lookup_key, f"clay_http_{r.status_code}", r.status_code, excerpt)
        log.warning("Clay push returned %s for %s; body: %r", r.status_code, lookup_key, excerpt)
        return {
            "status": "failed",
            "lookup_key": lookup_key,
            "clay_status_code": r.status_code,
            "error": excerpt,
            "message": f"Clay rejected the push (HTTP {r.status_code}); lookup marked failed",
        }

    _record_push(r.status_code, True, None)
    return {
        "status": "pending",
        "lookup_key": lookup_key,
        "clay_status_code": r.status_code,
        "message": f"Poll GET /result/{lookup_key} for the email",
    }


# ── Step 2: Clay sends enriched result back here ───────────────────
@app.post("/callback")
def callback(payload: dict):
    """
    Clay's HTTP API enrichment POSTs here after finding the work email.
    Accepts: { "lookup_key", "work_email", "full_name", "company_domain" }
    """
    lookup_key = payload.get("lookup_key")
    work_email = payload.get("work_email", "")

    # If no lookup_key provided, try to reconstruct it
    if not lookup_key:
        full_name = payload.get("full_name", "")
        company_domain = payload.get("company_domain", "")
        if full_name and company_domain:
            lookup_key = make_lookup_key(full_name, company_domain)
        else:
            raise HTTPException(status_code=400, detail="lookup_key or (full_name + company_domain) required")

    with store_lock:
        if lookup_key in results_store:
            e = results_store[lookup_key]
            e["status"] = "completed" if work_email else "not_found"
            e["work_email"] = work_email
            e["completed_at"] = time.time()
            e.pop("reason", None)
            # Optional extras, if the Clay HTTP action is configured to send them.
            for extra in ("confidence", "source"):
                if payload.get(extra) not in (None, ""):
                    e[extra] = payload[extra]
        else:
            # Store even if we didn't initiate — allows manual Clay runs
            results_store[lookup_key] = {
                "status": "completed" if work_email else "not_found",
                "work_email": work_email,
                "full_name": payload.get("full_name", ""),
                "company_domain": payload.get("company_domain", ""),
                "completed_at": time.time(),
            }

    return {"status": "received", "lookup_key": lookup_key}


# ── Step 3: Skill polls here to get the result ─────────────────────
@app.get("/result/{lookup_key}")
def get_result(lookup_key: str):
    """
    Returns the current state of an email lookup.
    Skill polls this until status is terminal:
    'completed' | 'not_found' | 'failed' | 'timeout'.
    """
    expire_stale()
    with store_lock:
        if lookup_key not in results_store:
            raise HTTPException(status_code=404, detail="No result found for this key")
        entry = dict(results_store[lookup_key])

    response = {
        "status": entry["status"],
        "work_email": entry.get("work_email"),
        "full_name": entry.get("full_name"),
        "company_domain": entry.get("company_domain"),
    }

    if entry["status"] == "pending":
        elapsed = int(time.time() - entry.get("submitted_at", time.time()))
        response["elapsed_seconds"] = elapsed
        response["message"] = "Clay enrichment still running. Poll again in 10-15 seconds."
    elif entry["status"] in ("failed", "timeout"):
        response["reason"] = entry.get("reason")
        if entry.get("clay_status_code") is not None:
            response["clay_status_code"] = entry["clay_status_code"]
        if entry.get("error"):
            response["error"] = entry["error"]

    return response


@app.get("/results")
def list_results():
    """Debug endpoint — lists all stored results. Remove in production."""
    expire_stale()
    with store_lock:
        return {
            "count": len(results_store),
            "results": {k: {**v, "submitted_at": None, "completed_at": None} for k, v in results_store.items()},
        }


# ── MCP server (Streamable HTTP) ───────────────────────────────────
# Lets Claude call the lookup as a custom connector. Reachable at
#   /mcp/<MCP_SECRET>                          (secret in the URL path), or
#   /mcp  with  Authorization: Bearer <MCP_SECRET>
mcp_server = MCPServer(
    name="find-work-email",
    instructions=(
        "Finds a person's verified work email through Clay enrichment. "
        "Each new lookup spends enrichment credits, so call it only for a specific named person."
    ),
)

FIND_EMAIL_DESCRIPTION = """Find a specific person's work email address via Clay enrichment.

Use this when you have a named person at a known organization and need their business email
(for example a decision-maker at an allocator with no email on file). Do not use it to
discover who works at a company, for personal emails, or for bulk guessing: every new lookup
spends paid enrichment credits. Check existing records for an email first.

Inputs: full_name and company are required. company_domain (e.g. "calpers.ca.gov", no
scheme or path) makes the match far more reliable and should be passed whenever known;
linkedin_url helps further.

The lookup is asynchronous and usually finishes within a minute. Result `status` is one of:
- completed: `email` holds the address.
- not_found: Clay found no email. Do not retry.
- pending: still running. Call this tool again with the SAME arguments to keep waiting;
  that does not start a new lookup or spend more credits.
- failed / timeout: see `reason`. Calling again starts a fresh lookup.

`source` names the data provider. `confidence` is the provider's score when it supplies one,
otherwise null; a null confidence means unscored, not low."""


def _tool_result(lookup_key: str, entry: dict) -> dict:
    status = entry["status"]
    out = {
        "status": status,
        "email": entry.get("work_email") or None,
        "confidence": entry.get("confidence"),
        "source": entry.get("source") or "clay_work_email_enrichment",
        "lookup_key": lookup_key,
    }
    if status == "pending":
        out["message"] = "Still running. Call find_email again with the same arguments to keep waiting."
    elif status in ("failed", "timeout"):
        out["reason"] = entry.get("reason")
    return out


@mcp_server.tool(name="find_email", title="Find work email", description=FIND_EMAIL_DESCRIPTION)
async def find_email_tool(
    full_name: str,
    company: str,
    company_domain: str | None = None,
    linkedin_url: str | None = None,
) -> dict:
    full_name = (full_name or "").strip()
    company = (company or "").strip()
    company_domain = (company_domain or "").strip().lower()
    linkedin_url = (linkedin_url or "").strip()
    if not full_name or not company:
        raise ValueError("full_name and company are required")
    if not CLAY_WEBHOOK_URL:
        raise RuntimeError("Lookup service is not configured (CLAY_WEBHOOK_URL not set)")

    expire_stale()
    # Same key as POST /find-email when a domain is given; falls back to the company name.
    lookup_key = make_lookup_key(full_name, company_domain or company)

    with store_lock:
        existing = dict(results_store.get(lookup_key) or {})
    # Reuse a running or answered lookup rather than paying for it twice.
    if existing.get("status") not in ("pending", "completed", "not_found"):
        payload = {
            "full_name": full_name,
            "company": company,
            "company_domain": company_domain,
            "linkedin_url": linkedin_url,
        }
        try:
            pushed = await anyio.to_thread.run_sync(submit_lookup, payload, lookup_key)
        except HTTPException as exc:
            return {
                "status": "failed",
                "email": None,
                "confidence": None,
                "source": "clay_work_email_enrichment",
                "lookup_key": lookup_key,
                "reason": str(exc.detail),
            }
        if pushed.get("status") == "failed":
            with store_lock:
                return _tool_result(lookup_key, dict(results_store[lookup_key]))

    deadline = time.monotonic() + MCP_WAIT_SECONDS
    while True:
        with store_lock:
            entry = dict(results_store.get(lookup_key) or {"status": "failed", "reason": "lost_on_restart"})
        if entry["status"] != "pending" or time.monotonic() >= deadline:
            return _tool_result(lookup_key, entry)
        await asyncio.sleep(1)


def _mcp_enabled() -> bool:
    return bool(MCP_SECRET) and len(MCP_SECRET) >= MCP_SECRET_MIN_LENGTH


class _RedactMCPSecret(logging.Filter):
    """Keep the URL-path secret out of uvicorn's access log."""

    def filter(self, record: logging.LogRecord) -> bool:
        if MCP_SECRET and isinstance(record.args, tuple):
            record.args = tuple(
                a.replace(MCP_SECRET, "[REDACTED]") if isinstance(a, str) else a for a in record.args
            )
        return True


logging.getLogger("uvicorn.access").addFilter(_RedactMCPSecret())


def _secret_ok(candidate: str) -> bool:
    return hmac.compare_digest(candidate.encode(), (MCP_SECRET or "").encode())


_mcp_asgi = mcp_server.streamable_http_app(
    streamable_http_path="/",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",  # public service behind Railway's proxy; access is gated by the secret below
)


class MCPGate:
    """Only lets a request reach the MCP app if it carries MCP_SECRET."""

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return
        if not _mcp_enabled():
            await JSONResponse({"detail": "Not Found"}, status_code=404)(scope, receive, send)
            return

        path_secret = scope.get("path_params", {}).get("secret")
        if path_secret is not None:
            if not _secret_ok(path_secret):
                await JSONResponse({"detail": "Not Found"}, status_code=404)(scope, receive, send)
                return
        else:
            auth = dict(scope["headers"]).get(b"authorization", b"").decode("latin-1")
            scheme, _, token = auth.partition(" ")
            if scheme.lower() != "bearer" or not _secret_ok(token.strip()):
                await JSONResponse({"detail": "Unauthorized"}, status_code=401)(scope, receive, send)
                return

        inner = dict(scope, path="/", raw_path=b"/", root_path="")
        await _mcp_asgi(inner, receive, send)


_mcp_gate = MCPGate()
app.router.routes.append(Route("/mcp", endpoint=_mcp_gate))
app.router.routes.append(Route("/mcp/{secret}", endpoint=_mcp_gate))
