import os, re, time, hmac, hashlib, asyncio, logging
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
import httpx
import aiosqlite
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# =============================================================================
# Config via env
# =============================================================================
SERVICE_NAME = os.getenv("SERVICE_NAME", "meli-webhook")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.2.0-state-gate")

ALLOWED_TOPICS = [t.strip() for t in os.getenv("ALLOWED_TOPICS", "orders_v2").split(",") if t.strip()]
N8N_SALES_WEBHOOK_URL = os.getenv("N8N_SALES_WEBHOOK_URL", "").strip()
DISABLE_SEMANTIC_BLOCKS = os.getenv("DISABLE_SEMANTIC_BLOCKS", "0") == "1"
ALLOW_TRIGGER_WHEN_FULFILLED = os.getenv("ALLOW_TRIGGER_WHEN_FULFILLED", "0") == "1"

ML_CLIENT_ID = os.getenv("ML_CLIENT_ID", "").strip()
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET", "").strip()
ML_REFRESH_TOKEN = os.getenv("ML_REFRESH_TOKEN", "").strip()

DB_PATH = os.getenv("DB_PATH", "notif.db")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").encode("utf-8") if os.getenv("WEBHOOK_SECRET") else None

FORWARD_TIMEOUT = float(os.getenv("FORWARD_TIMEOUT", "15.0"))
FORWARD_MAX_RETRIES = int(os.getenv("FORWARD_MAX_RETRIES", "3"))
FORWARD_BACKOFF_BASE = float(os.getenv("FORWARD_BACKOFF_BASE", "0.8"))
LATE_DUPLICATE_SECONDS = int(os.getenv("LATE_DUPLICATE_SECONDS", str(24 * 3600)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI(title=SERVICE_NAME, version=SERVICE_VERSION)

_http: httpx.AsyncClient | None = None
_db: aiosqlite.Connection | None = None

# =============================================================================
# Helpers
# =============================================================================
def _now_utc_ts() -> int:
    return int(time.time())

def parse_iso_to_ts(iso_str: str | None) -> int | None:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None

def verify_hmac_sha256(secret: bytes, raw_body: bytes, sig_header: str) -> bool:
    try:
        algo, hexsig = sig_header.split("=", 1)
        if algo.lower() != "sha256":
            return False
        mac = hmac.new(secret, msg=raw_body, digestmod=hashlib.sha256)
        return hmac.compare_digest(mac.hexdigest(), hexsig.strip())
    except Exception:
        return False

def extract_order_id(resource_path: str) -> Optional[str]:
    m = re.search(r"/orders/(\d+)", resource_path or "")
    return m.group(1) if m else None

# =============================================================================
# DB
# =============================================================================
async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deliveries (
            topic TEXT NOT NULL,
            resource TEXT NOT NULL,
            sent TEXT NOT NULL,
            seen_at INTEGER NOT NULL,
            UNIQUE(topic, resource, sent)
        );
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_orders (
            order_id TEXT PRIMARY KEY,
            processed_at INTEGER NOT NULL
        );
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS order_state (
            order_id TEXT PRIMARY KEY,
            forwarded_status TEXT,
            first_forwarded_at INTEGER
        );
    """)
    await conn.commit()
    logging.info("📦 SQLite iniciado (%s)", DB_PATH)

async def mark_delivery(topic: str, resource: str, sent: str) -> bool:
    try:
        await _db.execute(
            "INSERT INTO deliveries(topic, resource, sent, seen_at) VALUES(?,?,?,?)",
            (topic, resource, sent or "", _now_utc_ts())
        )
        await _db.commit()
        return True
    except Exception:
        return False

async def claim_order(order_id: str) -> bool:
    try:
        await _db.execute(
            "INSERT INTO processed_orders(order_id, processed_at) VALUES(?,?)",
            (order_id, _now_utc_ts())
        )
        await _db.commit()
        return True
    except Exception:
        return False

async def finalize_order(order_id: str) -> None:
    try:
        await _db.execute("DELETE FROM processed_orders WHERE order_id = ?", (order_id,))
        await _db.commit()
    except Exception:
        pass

async def get_order_state(order_id: str):
    cur = await _db.execute(
        "SELECT forwarded_status, first_forwarded_at FROM order_state WHERE order_id = ?",
        (order_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    return row if row else (None, None)

async def upsert_order_state(order_id: str, status_key: str):
    await _db.execute("""
        INSERT INTO order_state(order_id, forwarded_status, first_forwarded_at)
        VALUES(?,?,?)
        ON CONFLICT(order_id) DO UPDATE SET forwarded_status=excluded.forwarded_status
    """, (order_id, status_key, _now_utc_ts()))
    await _db.commit()

# =============================================================================
# OAuth
# =============================================================================
class TokenCache:
    def __init__(self):
        self._token = None
        self._exp_ts = 0

    async def get_token(self):
        if self._token and _now_utc_ts() < self._exp_ts - 30:
            return self._token
        await self.refresh()
        return self._token

    async def refresh(self):
        data = {
            "grant_type": "refresh_token",
            "client_id": ML_CLIENT_ID,
            "client_secret": ML_CLIENT_SECRET,
            "refresh_token": ML_REFRESH_TOKEN,
        }
        resp = await _http.post(
            "https://api.mercadolibre.com/oauth/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        js = resp.json()
        self._token = js.get("access_token")
        self._exp_ts = _now_utc_ts() + int(js.get("expires_in", 1800))

token_cache = TokenCache()

# =============================================================================
# N8N forwarding
# =============================================================================
async def post_to_n8n(payload: Dict[str, Any], idempotency_key: Optional[str] = None):
    if not N8N_SALES_WEBHOOK_URL:
        logging.warning("⚠️ N8N URL não configurada")
        return {"n8n_status": 204}

    headers = {"Content-Type": "application/json"}
    if idempotency_key:
        headers["X-Idempotency-Key"] = idempotency_key

    resp = await _http.post(N8N_SALES_WEBHOOK_URL, json=payload, headers=headers)
    return {"n8n_status": resp.status_code, "body": resp.text}

# =============================================================================
# Decision helpers
# =============================================================================
def derive_status_key(order):
    if order.get("status") == "paid":
        return "paid"
    if order.get("status") == "confirmed":
        if any(p.get("status") == "approved" for p in order.get("payments", [])):
            return "confirmed_approved"
    return None

def should_block_semantic(order):
    if order.get("fulfilled") is True:
        return "fulfilled"
    if "delivered" in set(order.get("tags") or []):
        return "delivered"
    return None

# =============================================================================
# Lifecycle
# =============================================================================
@app.on_event("startup")
async def startup():
    global _http, _db
    _http = httpx.AsyncClient()
    _db = await aiosqlite.connect(DB_PATH)
    await init_db(_db)
    logging.info("🚀 %s %s iniciado | allowed_topics=%s", SERVICE_NAME, SERVICE_VERSION, ALLOWED_TOPICS)

@app.on_event("shutdown")
async def shutdown():
    await _http.aclose()
    await _db.close()

# =============================================================================
# Endpoints
# =============================================================================
@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.post("/meli/webhook")
async def meli_webhook(request: Request, x_hub_signature_256: Optional[str] = Header(None)):
    raw = await request.body()
    payload = await request.json()

    topic = payload.get("topic")
    resource = payload.get("resource")
    sent_iso = payload.get("sent")

    logging.info("📥 Webhook recebido | topic=%s | resource=%s | sent=%s", topic, resource, sent_iso)

    if topic not in ALLOWED_TOPICS:
        logging.info("🚫 Topic ignorado | topic=%s | allowed=%s", topic, ALLOWED_TOPICS)
        return {"ok": True, "ignored": "topic_not_allowed"}

    order_id = extract_order_id(resource)
    logging.info("🔎 Extração order_id | resource=%s | order_id=%s", resource, order_id)

    if not order_id:
        return {"ok": True, "ignored": "no_order_id"}

    first = await mark_delivery(topic, resource, sent_iso)
    logging.info("🧱 Dedup | first_delivery=%s | order_id=%s", first, order_id)
    if not first:
        return {"ok": True, "ignored": "duplicate"}

    claimed = await claim_order(order_id)
    logging.info("🔐 Claim pedido | order_id=%s | claimed=%s", order_id, claimed)
    if not claimed:
        return {"ok": True, "ignored": "already_processing"}

    try:
        token = await token_cache.get_token()
        resp = await _http.get(
            f"https://api.mercadolibre.com/orders/{order_id}?include=payments",
            headers={"Authorization": f"Bearer {token}"}
        )
        order = resp.json()
        logging.info("📦 Order carregada | order_id=%s | status=%s", order_id, order.get("status"))

        status_key = derive_status_key(order)
        prev_status, _ = await get_order_state(order_id)
        logging.info("🧠 Decisão estado | order_id=%s | status_key=%s | prev_status=%s",
                     order_id, status_key, prev_status)

        block = should_block_semantic(order)
        if block:
            logging.info("⛔ Bloqueio semântico | order_id=%s | reason=%s", order_id, block)
            return {"ok": True, "ignored": block}

        if status_key and status_key != prev_status:
            logging.info("➡️ Encaminhando para n8n | order_id=%s | status_key=%s", order_id, status_key)

            body = {
                "topic": topic,
                "resource": resource,
                "sent": sent_iso,
                "sent_unix": sent_unix,
                "order_id": order_id,
                "decision": "forwarded",
                "forwarded_status": status_key,
                "order_status": order.get("status"),
                "paid_amount": order.get("paid_amount"),
                "total_amount": order.get("total_amount"),
                "tags": order.get("tags"),
                "internal_tags": order.get("internal_tags"),
                # "order": order,  # opcional, manter comentado
            }
        
            forward_result = await post_to_n8n(
                body,
                idempotency_key=f"order-{order_id}-{status_key}"
            )
        
            logging.info(
                "✅ Retorno n8n | order_id=%s | result=%s",
                order_id, forward_result
            )
        
            await upsert_order_state(order_id, status_key)
        
            return {
                "ok": True,
                "forwarded": status_key,
                "order_id": order_id,
                **forward_result
            }


        logging.info("⏭️ Nenhuma transição relevante | order_id=%s", order_id)
        return {"ok": True, "ignored": "no_transition"}

    finally:
        await finalize_order(order_id)
