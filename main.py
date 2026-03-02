from fastapi import FastAPI, HTTPException
import os
import requests
import hashlib
import time

app = FastAPI()

CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL")
CLAY_AUTH_TOKEN = os.getenv("CLAY_AUTH_TOKEN")  # optional

# In-memory store: lookup_key -> {status, work_email, submitted_at, ...}
# For production, swap this for Redis or a DB
results_store = {}


def make_lookup_key(full_name: str, company_domain: str) -> str:
    """Generate a consistent lookup key from name + domain."""
    raw = f"{full_name.strip().lower()}|{company_domain.strip().lower()}"
    return hashlib.md5(raw.encode()).hexdigest()


@app.get("/health")
def health():
    return {"ok": True, "pending": len([r for r in results_store.values() if r["status"] == "pending"])}


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

    lookup_key = make_lookup_key(full_name, company_domain)

    # Store as pending
    results_store[lookup_key] = {
        "status": "pending",
        "work_email": None,
        "full_name": full_name,
        "company": payload.get("company", ""),
        "company_domain": company_domain,
        "linkedin_url": payload.get("linkedin_url", ""),
        "submitted_at": time.time(),
    }

    # Forward to Clay webhook
    headers = {"Content-Type": "application/json"}
    if CLAY_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {CLAY_AUTH_TOKEN}"

    # Include lookup_key so Clay can send it back in the callback
    clay_payload = {**payload, "lookup_key": lookup_key}

    try:
        r = requests.post(CLAY_WEBHOOK_URL, json=clay_payload, headers=headers, timeout=30)
    except requests.Timeout:
        results_store[lookup_key]["status"] = "error"
        raise HTTPException(status_code=504, detail="Clay webhook timed out")

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

    if lookup_key in results_store:
        results_store[lookup_key]["status"] = "completed" if work_email else "not_found"
        results_store[lookup_key]["work_email"] = work_email
        results_store[lookup_key]["completed_at"] = time.time()
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
    Skill polls this until status is 'completed' or 'not_found'.
    """
    if lookup_key not in results_store:
        raise HTTPException(status_code=404, detail="No result found for this key")

    entry = results_store[lookup_key]
    response = {
        "status": entry["status"],
        "work_email": entry.get("work_email"),
        "full_name": entry.get("full_name"),
        "company_domain": entry.get("company_domain"),
    }

    # Include wait time if still pending
    if entry["status"] == "pending":
        elapsed = int(time.time() - entry.get("submitted_at", time.time()))
        response["elapsed_seconds"] = elapsed
        response["message"] = "Clay enrichment still running. Poll again in 10-15 seconds."

    return response


# ── Cleanup: remove stale entries older than 1 hour ─────────────────
@app.on_event("startup")
def cleanup_schedule():
    """Note: For production, use a background task or Redis TTL instead."""
    pass


@app.get("/results")
def list_results():
    """Debug endpoint — lists all stored results. Remove in production."""
    return {
        "count": len(results_store),
        "results": {k: {**v, "submitted_at": None, "completed_at": None} for k, v in results_store.items()},
    }
