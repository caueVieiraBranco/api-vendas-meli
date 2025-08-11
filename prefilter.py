
import os, time
import requests
from fastapi import FastAPI, Request, Response, Header

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REFRESH_TOKEN = os.environ["ML_REFRESH_TOKEN"]
N8N_WEBHOOK_URL = os.environ["N8N_WEBHOOK_URL"]  # ex: https://.../webhook/meli/venda
FORWARD_SECRET = os.environ.get("FORWARD_SECRET", "change-me")

app = FastAPI()

_access_token = None
_token_exp = 0

def get_access_token():
    global _access_token, _token_exp
    # reuse token if still valid
    if _access_token and time.time() < _token_exp - 60:
        return _access_token
    r = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "client_id": ML_CLIENT_ID,
            "client_secret": ML_CLIENT_SECRET,
            "refresh_token": ML_REFRESH_TOKEN,
        },
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    _access_token = data["access_token"]
    _token_exp = time.time() + int(data.get("expires_in", 600))
    return _access_token

def is_paid(order_json: dict) -> bool:
    status = (order_json.get("status") or "").lower()
    if status not in ("paid", "confirmed"):
        return False
    payments = order_json.get("payments") or []
    return any((p.get("status") or "").lower() == "approved" for p in payments)

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/meli/venda")
async def meli_venda(request: Request):
    try:
        payload = await request.json()
    except Exception:
        # Always 200 to avoid ML retries if our side had a parse error
        return Response(status_code=200)

    topic = (payload.get("topic") or "").lower()
    resource = payload.get("resource") or ""
    if topic != "orders_v2" or "/orders/" not in resource:
        return Response(status_code=200)

    # Extract order_id
    try:
        order_id = [s for s in resource.split("/") if s][-1]
    except Exception:
        return Response(status_code=200)

    # Check real status in ML API
    try:
        token = get_access_token()
        r = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}?include=payments",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        r.raise_for_status()
        order = r.json()
    except Exception:
        # Do not forward to n8n on error; still 200 to ML
        return Response(status_code=200)

    if not is_paid(order):
        # Not a valid sale -> do not forward
        return Response(status_code=200)

    # Forward to n8n only now (this will count as 1 execution there)
    try:
        requests.post(
            N8N_WEBHOOK_URL,
            json=payload,
            headers={"X-Forwarded-Secret": FORWARD_SECRET},
            timeout=15,
        )
    except Exception:
        # We still return 200 to Mercado Livre; avoid their retries
        pass

    return Response(status_code=200)
