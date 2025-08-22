import os, re, time, hmac, hashlib, asyncio, logging
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Request, Header, HTTPException
from pydantic import BaseModel
import httpx
import aiosqlite
from datetime import datetime, timezone

try:
    # Python 3.9+ zoneinfo
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# -----------------------------------------------------------------------------
# Config via env
# -----------------------------------------------------------------------------
N8N_WEBHOOK_URL = os.getenv("N8N_SALES_WEBHOOK_URL")  # ex: https://.../webhook/meli/vendas
ALLOWED_TOPICS = set(os.getenv("ALLOWED_TOPICS", "orders_v2").split(","))
FORWARD_TIMEOUT = float(os.getenv("FORWARD_TIMEOUT", "15.0"))
FORWARD_MAX_RETRIES = int(os.getenv("FORWARD_MAX_RETRIES", "3"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # opcional (assinatura HMAC)
CLIENT_ID = os.getenv("ML_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("ML_REFRESH_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "notif.db")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------------------------------------------------------
# FastAPI app + health endpoints
# -----------------------------------------------------------------------------
app = FastAPI(title="ML Webhook (Render) – vendas pagas only")

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/")
async def root():
    return {"ok": True, "service": "meli-webhook", "version": "1.1.0-dedupe-atomic"}

@app.head("/")
async def head_root():
    return {}

# -----------------------------------------------------------------------------
# Token cache (refresh com refresh_token)
# -----------------------------------------------------------------------------
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"

class TokenCache:
    def __init__(self):
        self._access = None
        self._exp = 0

    async def get(self) -> str:
        now = time.time()
        if self._access and now < (self._exp - 30):
            return self._access
        if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
            raise RuntimeError("Env ML_CLIENT_ID/ML_CLIENT_SECRET/ML_REFRESH_TOKEN não configurados")
        data = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
        }
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(ML_TOKEN_URL, data=data, headers={"Content-Type":"application/x-www-form-urlencoded"})
            r.raise_for_status()
            js = r.json()
        self._access = js["access_token"]
        self._exp = now + int(js.get("expires_in", 300))
        logging.info("🔁 Access token renovado.")
        return self._access

TOKENS = TokenCache()

# -----------------------------------------------------------------------------
# SQLite idempotência (entregas + pedidos)
# -----------------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS deliveries (
            topic TEXT NOT NULL,
            resource TEXT NOT NULL,
            sent TEXT,
            seen_at INTEGER NOT NULL,
            UNIQUE(topic, resource, sent)
        );""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS processed_orders (
            order_id TEXT PRIMARY KEY,
            processed_at INTEGER NOT NULL
        );""")
        await db.commit()
    logging.info("📦 SQLite pronto em %s", DB_PATH)

@app.on_event("startup")
async def on_startup():
    await init_db()

# Dedupa entregas: agora deduplica por (topic, resource), independente de 'sent'
async def already_seen_delivery(topic: str, resource: str, sent: Optional[str]) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM deliveries WHERE topic = ? AND resource = ? LIMIT 1",
            (topic, resource)
        ) as cur:
            row = await cur.fetchone()
            if row:
                return True
        await db.execute(
            "INSERT INTO deliveries(topic, resource, sent, seen_at) VALUES(?,?,?,?)",
            (topic, resource, sent or "", int(time.time()))
        )
        await db.commit()
        return False

# Claim atômico do pedido: só o primeiro request segue
async def claim_order(order_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO processed_orders(order_id, processed_at) VALUES(?,?)",
                (order_id, int(time.time()))
            )
            await db.commit()
            return True
        except Exception:
            return False  # já existe

# Só para telemetria (mantém updated_at)
async def finalize_order(order_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE processed_orders SET processed_at = ? WHERE order_id = ?",
            (int(time.time()), order_id)
        )
        await db.commit()

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def verify_signature_if_any(raw: bytes, signature: Optional[str]) -> bool:
    if not WEBHOOK_SECRET:
        return True
    if not signature:
        return False
    calc = hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    if signature.startswith("sha256="):
        signature = signature.split("=",1)[1]
    return hmac.compare_digest(calc, signature)

def extract_order_id(resource: str) -> Optional[str]:
    # /orders/{id}
    m = re.search(r"/orders/(\d+)", resource or "")
    return m.group(1) if m else None

def now_brt_iso():
    # America/Sao_Paulo ISO
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

# -----------------------------------------------------------------------------
# Modelos
# -----------------------------------------------------------------------------
class MLNotification(BaseModel):
    resource: str
    topic: str
    user_id: Optional[int] = None
    application_id: Optional[int] = None
    attempts: Optional[int] = None
    sent: Optional[str] = None
    received: Optional[str] = None

# -----------------------------------------------------------------------------
# Regra de venda válida (espelhando seu n8n)
# -----------------------------------------------------------------------------
def is_valid_sale(order: Dict[str, Any]) -> bool:
    """
    Regra:
      - order.status == "paid"  -> válido
      - OU (order.status == "confirmed" e existe payment.status == "approved")
    """
    status = (order.get("status") or "").lower()
    if status == "paid":
        return True
    if status == "confirmed":
        payments: List[Dict[str, Any]] = order.get("payments") or []
        for p in payments:
            if (p.get("status") or "").lower() == "approved":
                return True
    return False

# -----------------------------------------------------------------------------
# Fetch order com include=payments
# -----------------------------------------------------------------------------
async def fetch_order(order_id: str) -> Dict[str, Any]:
    token = await TOKENS.get()
    url = f"https://api.mercadolibre.com/orders/{order_id}"
    params = {"include": "payments"}  # traz pagamentos embutidos
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params, headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        return r.json()

# -----------------------------------------------------------------------------
# Forward ao n8n com retry exponencial
# -----------------------------------------------------------------------------
async def post_to_n8n(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not N8N_WEBHOOK_URL:
        return {"forwarded": False, "error": "N8N_SALES_WEBHOOK_URL not set"}
    delay = 0.8
    last_err = None
    async with httpx.AsyncClient(timeout=FORWARD_TIMEOUT) as client:
        for attempt in range(1, FORWARD_MAX_RETRIES + 1):
            try:
                resp = await client.post(
                    N8N_WEBHOOK_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                return {"forwarded": True, "n8n_status": resp.status_code, "n8n_body": resp.text}
            except httpx.HTTPError as e:
                last_err = str(e)
                await asyncio.sleep(delay)
                delay *= 2
    return {"forwarded": False, "error": last_err or "unknown"}

# -----------------------------------------------------------------------------
# Handler principal
# -----------------------------------------------------------------------------
@app.post("/meli/webhook")
async def meli_webhook(
    request: Request,
    x_hub_signature_256: Optional[str] = Header(default=None),
):
    raw = await request.body()
    if not verify_signature_if_any(raw, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="invalid signature")

    # Tenta JSON
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON")

    # Normaliza notificação
    try:
        notif = MLNotification(**data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"bad payload: {e}")

    # Dedup de entrega (topic+resource), independente de 'sent'
    if await already_seen_delivery(notif.topic, notif.resource, notif.sent):
        return {"status": "ok", "dedup_delivery": True}

    # Filtra por topic
    if notif.topic not in ALLOWED_TOPICS:
        return {"status": "ignored", "reason": "topic_not_allowed", "topic": notif.topic}

    # Extrai order_id
    order_id = extract_order_id(notif.resource)
    if not order_id:
        return {"status": "ignored", "reason": "no_order_id", "resource": notif.resource}

    # Claim atômico: só o primeiro request para este order_id segue
    got_claim = await claim_order(order_id)
    if not got_claim:
        return {"status": "ok", "dedup_order": True, "order_id": order_id}

    # Busca a ordem com pagamentos
    try:
        order = await fetch_order(order_id)
    except httpx.HTTPStatusError as e:
        logging.warning(f"fetch_order error {e.response.status_code} for order {order_id}")
        # Respondemos 200 para evitar reentrega do ML
        return {"status": "fetch_error", "code": e.response.status_code, "order_id": order_id}

    # ----------------- BLOQUEIOS SEMÂNTICOS -----------------
    tags_lower = {str(t).lower() for t in (order.get("tags") or [])}
    internal_tags_lower = {str(t).lower() for t in (order.get("internal_tags") or [])}
    fulfilled_true = order.get("fulfilled") is True  # True/False/None

    if fulfilled_true:
        return {"status": "ignored_order", "order_id": order_id, "reason": "fulfilled_true"}

    if "delivered" in tags_lower:
        return {"status": "ignored_order", "order_id": order_id, "reason": "tag_delivered"}

    if "invoice_authorized" in internal_tags_lower:
        return {"status": "ignored_order", "order_id": order_id, "reason": "internal_tag_invoice_authorized"}
    # --------------------------------------------------------

    # Regra “venda paga”
    if not is_valid_sale(order):
        return {
            "status": "ignored_order",
            "order_id": order_id,
            "order_status": order.get("status"),
            "reason": "not_paid_or_not_approved",
        }

    # Payload para o n8n
    enriched = {
        "topic": notif.topic,
        "resource": notif.resource,
        "order_id": order_id,
        "sent": notif.sent or now_brt_iso(),
        "sent_unix": int(time.time()),
        "order_status": order.get("status"),
        "seller_id": (order.get("seller") or {}).get("id"),
        "buyer_id": (order.get("buyer") or {}).get("id"),
        "total_amount": order.get("total_amount"),
        "paid_amount": order.get("paid_amount"),
        "tags": list(tags_lower),
        "internal_tags": list(internal_tags_lower),
        "fulfilled": fulfilled_true,
        # "order": order,  # descomente para enviar o pedido completo
    }

    # Encaminha ao n8n somente quando for venda válida e passar nos filtros
    forward_result = await post_to_n8n(enriched)

    # Finaliza (telemetria)
    await finalize_order(order_id)

    return {"status": "processed", "order_id": order_id, **forward_result}



