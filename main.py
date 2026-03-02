from fastapi import FastAPI, HTTPException
import os
import requests

app = FastAPI()

CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL")
CLAY_AUTH_TOKEN = os.getenv("CLAY_AUTH_TOKEN")  # optional

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/find-email")
def find_email(payload: dict):
    if not CLAY_WEBHOOK_URL:
        raise HTTPException(status_code=500, detail="CLAY_WEBHOOK_URL not set")

    headers = {"Content-Type": "application/json"}
    # Only include if you set a token in Clay's webhook settings
    if CLAY_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {CLAY_AUTH_TOKEN}"

    r = requests.post(CLAY_WEBHOOK_URL, json=payload, headers=headers, timeout=30)
    return {"status": "sent_to_clay", "clay_status_code": r.status_code}