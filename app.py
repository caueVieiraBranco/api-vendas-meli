import os, re, time, hmac, hashlib, asyncio, logging
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
import httpx
import aiosqlite
from datetime import datetime, timezone

try:
    # Python 3.9+ zoneinfo
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

# Mercado Livre OAuth (refresh)
ML_CLIENT_ID = os.getenv("ML_CLIENT_ID", "").strip()
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET", "").strip()
ML_REFRESH_TOKEN = os.getenv("ML_REFRESH_TOKEN", "").strip()

# Persistência local (efêmera no Render Free)
DB_PATH = os.getenv("DB_PATH", "notif.db")

# Segurança opcional (HMAC do corpo do webhook do ML)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").encode("utf-8") if os.getenv("WEBHOOK_SECRET") else None

# Post para n8n
FORWARD_TIMEOUT = float(os.getenv("FORWARD_TIMEOUT", "15.0"))
FORWARD_MAX_RETRIES = int(os.getenv("FORWARD_MAX_RETRIES", "3"))
FORWARD_BACKOFF_BASE = float(os.getenv("FORWARD_BACKOFF_BASE", "0.8"))

# Supressão temporal de reentregas muito tardias (opcional)
LATE_DUPLICATE_SECONDS = int(os.getenv("LATE_DUPLICATE_SECONDS", str(24 * 3600)))  # 24h

# Logging simples
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# =============================================================================
# App
# =============================================================================
app = FastAPI(title=SERVICE_NAME, version=SERVICE_VERSION)

# globais do processo
_http: httpx.AsyncClient | None = None
_db: aiosqlite.Connection | None = None

# =============================================================================
# Helpers gerais
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
    """
    Espera header no formato: "sha256=<HEX>"
    """
    try:
        algo, hexsig = sig_header.split("=", 1)
        if algo.lower() != "sha256":
            return False
        mac = hmac.new(secret, msg=raw_body, digestmod=hashlib.sha256)
        expected = mac.hexdigest()
        # compare constant-time
        return hmac.compare_digest(expected, hexsig.strip())
    except Exception:
        return False

def extract_order_id(resource_path: str) -> Optional[str]:
    """
    Ex.: "/orders/200001234567890" -> "200001234567890"
    """
    m = re.search(r"/orders/(\d+)", resource_path or "")
    return m.group(1) if m else None

# =============================================================================
# DB (SQLite, efêmero)
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
        forwarded_status TEXT,          -- "paid" ou "confirmed_approved"
        first_forwarded_at INTEGER      -- epoch seconds
    );
    """)
    await conn.commit()
    logging.info("📦 SQLite pronto em %s", DB_PATH)

async def mark_delivery(topic: str, resource: str, sent: str) -> bool:
    """
    Retorna True se inseriu (1ª vez), False se já existia (duplicado).
    """
    assert _db is not None
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
    """
    Claim atômico para evitar corrida (em-processo). Remove no finalize_order().
    True = conseguiu claim; False = já havia claim ativo.
    """
    assert _db is not None
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
    assert _db is not None
    try:
        await _db.execute("DELETE FROM processed_orders WHERE order_id = ?", (order_id,))
        await _db.commit()
    except Exception:
        pass

async def get_order_state(order_id: str) -> Tuple[Optional[str], Optional[int]]:
    assert _db is not None
    cur = await _db.execute(
        "SELECT forwarded_status, first_forwarded_at FROM order_state WHERE order_id = ?",
        (order_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    if row:
        return row[0], row[1]
    return None, None

async def upsert_order_state(order_id: str, status_key: str, ts: Optional[int] = None) -> None:
    assert _db is not None
    ts = ts or _now_utc_ts()
    await _db.execute("""
        INSERT INTO order_state(order_id, forwarded_status, first_forwarded_at)
        VALUES(?,?,?)
        ON CONFLICT(order_id) DO UPDATE SET
            forwarded_status = excluded.forwarded_status
    """, (order_id, status_key, ts))
    await _db.commit()

# =============================================================================
# ML OAuth Token cache
# =============================================================================
class TokenCache:
    def __init__(self):
        self._token: Optional[str] = None
        self._exp_ts: int = 0

    async def get_token(self) -> str:
        now = _now_utc_ts()
        if self._token and now < (self._exp_ts - 30):
            return self._token
        await self.refresh()
        return self._token or ""

    async def refresh(self) -> None:
        if not (ML_CLIENT_ID and ML_CLIENT_SECRET and ML_REFRESH_TOKEN):
            raise RuntimeError("ML OAuth envs ausentes (ML_CLIENT_ID/SECRET/REFRESH_TOKEN).")
        assert _http is not None
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
            timeout=FORWARD_TIMEOUT,
        )
        if resp.status_code >= 400:
            logging.error("🔐 Refresh token falhou: %s %s", resp.status_code, resp.text)
            raise HTTPException(status_code=500, detail="Erro ao renovar token do ML.")
        js = resp.json()
        self._token = js.get("access_token")
        self._exp_ts = _now_utc_ts() + int(js.get("expires_in", 1800))

token_cache = TokenCache()

# =============================================================================
# N8N forwarding
# =============================================================================
async def post_to_n8n(payload: Dict[str, Any], idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    if not N8N_SALES_WEBHOOK_URL:
        logging.warning("⚠️ N8N_SALES_WEBHOOK_URL não configurada; simulando sucesso.")
        return {"n8n_status": 204, "n8n_body": None}

    assert _http is not None
    headers = {"Content-Type": "application/json"}
    if idempotency_key:
        headers["X-Idempotency-Key"] = idempotency_key

    delay = FORWARD_BACKOFF_BASE
    last_exc: Exception | None = None

    for attempt in range(1, FORWARD_MAX_RETRIES + 1):
        try:
            resp = await _http.post(
                N8N_SALES_WEBHOOK_URL, json=payload, headers=headers, timeout=FORWARD_TIMEOUT
            )
            ok = 200 <= resp.status_code < 300
            if ok:
                return {"n8n_status": resp.status_code, "n8n_body": _safe_json(resp)}
            else:
                logging.warning("➡️ n8n tentativa %s falhou: %s %s", attempt, resp.status_code, resp.text)
        except Exception as e:
            last_exc = e
            logging.warning("➡️ n8n tentativa %s exception: %r", attempt, e)

        await asyncio.sleep(delay)
        delay *= 2

    # deu ruim
    if last_exc:
        logging.error("❌ Falha ao encaminhar ao n8n: %r", last_exc)
    return {"n8n_status": 599, "n8n_body": None}

def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return resp.text

# =============================================================================
# Regra de decisão por “transição de estado”
# =============================================================================
def derive_status_key(order_json: Dict[str, Any]) -> Optional[str]:
    """
    Retorna "paid" ou "confirmed_approved" quando for estado-gatilho. Caso contrário, None.
    """
    status = (order_json or {}).get("status")
    if status == "paid":
        return "paid"
    if status == "confirmed":
        payments = order_json.get("payments") or []
        if any(p.get("status") == "approved" for p in payments):
            return "confirmed_approved"
    return None

def should_block_semantic(order_json: Dict[str, Any]) -> Optional[str]:
    """
    Bloqueios semânticos antes da regra principal (opcional).
    Ex.: pedido já cumprido/entregue/fat. autorizado → ignora.
    """
    if order_json.get("fulfilled") is True:
        return "already_fulfilled"
    tags = set(order_json.get("tags") or [])
    if "delivered" in tags:
        return "already_delivered"
    internal = set(order_json.get("internal_tags") or [])
    if "invoice_authorized" in internal:
        return "invoice_authorized"
    return None

# =============================================================================
# FastAPI lifecycle
# =============================================================================
@app.on_event("startup")
async def on_startup():
    global _http, _db
    # http client
    _http = httpx.AsyncClient(follow_redirects=True)
    # db
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    _db = await aiosqlite.connect(DB_PATH)
    await init_db(_db)
    logging.info("🚀 %s %s iniciado", SERVICE_NAME, SERVICE_VERSION)

@app.on_event("shutdown")
async def on_shutdown():
    global _http, _db
    try:
        if _http:
            await _http.aclose()
    finally:
        _http = None
    try:
        if _db:
            await _db.close()
    finally:
        _db = None

# =============================================================================
# Endpoints básicos
# =============================================================================
@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION}

@app.get("/")
async def root():
    return {"ok": True, "service": SERVICE_NAME, "version": SERVICE_VERSION}

@app.head("/")
async def head_root():
    return PlainTextResponse("", status_code=200)

# =============================================================================
# Webhook do ML
# =============================================================================
@app.post("/meli/webhook")
async def meli_webhook(
    request: Request,
    x_hub_signature_256: Optional[str] = Header(default=None, alias="X-Hub-Signature-256"),
):
    """
    Recebe notificações do Mercado Livre e decide se encaminha ao n8n.
    """
    raw = await request.body()

    # 1) HMAC opcional
    if WEBHOOK_SECRET:
        if not x_hub_signature_256 or not verify_hmac_sha256(WEBHOOK_SECRET, raw, x_hub_signature_256):
            logging.warning("🔒 HMAC inválido/ausente; ignorando.")
            return JSONResponse({"ok": True, "ignored": "bad_hmac"}, status_code=200)

    # 2) JSON do ML
    try:
        payload = await request.json()
    except Exception:
        logging.warning("⚠️ Body não-JSON; ignorando.")
        return JSONResponse({"ok": True, "ignored": "bad_json"}, status_code=200)

    topic = (payload or {}).get("topic")
    resource = (payload or {}).get("resource") or ""
    sent_iso = (payload or {}).get("sent")
    sent_unix = parse_iso_to_ts(sent_iso)

    if topic not in ALLOWED_TOPICS:
        return {"ok": True, "ignored": "topic_not_allowed", "topic": topic}

    order_id = extract_order_id(resource or "")
    if not order_id:
        return {"ok": True, "ignored": "no_order_id", "resource": resource}

    # 3) Dedup por evento (topic, resource, sent)
    is_first_delivery = await mark_delivery(topic, resource, sent_iso or "")
    if not is_first_delivery:
        return {"ok": True, "ignored": "duplicate_delivery", "order_id": order_id}

    # 4) Claim atômico por pedido (evita corrida entre eventos simultâneos)
    if not await claim_order(order_id):
        return {"ok": True, "ignored": "already_processing", "order_id": order_id}

    try:
        # 5) Busca do pedido no ML (include=payments)
        token = await token_cache.get_token()
        assert _http is not None
        ml_url = f"https://api.mercadolibre.com/orders/{order_id}?include=payments"
        resp = await _http.get(ml_url, headers={"Authorization": f"Bearer {token}"}, timeout=FORWARD_TIMEOUT)

        if resp.status_code >= 400:
            logging.warning("🟠 GET order falhou: %s %s", resp.status_code, resp.text)
            return {"ok": True, "status": "fetch_error", "order_id": order_id, "http": resp.status_code}

        order = resp.json() or {}

        # 6) Decisão por transição de estado
        status_key = derive_status_key(order)
        prev_status, first_ts = await get_order_state(order_id)
        
        # 6.1) Bloqueios semânticos (com opção de desativar/burlar em teste)
        if not DISABLE_SEMANTIC_BLOCKS:
            block_reason = should_block_semantic(order)
            if block_reason:
                # Se for 1º gatilho e queremos permitir mesmo se fulfilled/delivered, só em teste:
                if ALLOW_TRIGGER_WHEN_FULFILLED and status_key in ("paid", "confirmed_approved") and (prev_status != status_key):
                    pass  # deixa passar para teste controlado
                else:
                    return {"ok": True, "ignored": block_reason, "order_id": order_id}


        # (opcional) supressão temporal caso reentrega venha muito tarde
        if first_ts and sent_unix and LATE_DUPLICATE_SECONDS > 0:
            if sent_unix > first_ts + LATE_DUPLICATE_SECONDS:
                return {"ok": True, "ignored": "late_duplicate", "order_id": order_id}

        if status_key in ("paid", "confirmed_approved") and (prev_status != status_key):
            # 8) Encaminhar ao n8n
            idemp_key = f"order-{order_id}-{status_key}"
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
                # Se preferir enviar o JSON inteiro do pedido, descomente:
                # "order": order,
            }
            forward_result = await post_to_n8n(body, idempotency_key=idemp_key)

            # 9) Memorizar o estado para bloquear reentregas complementares
            await upsert_order_state(order_id, status_key)

            return {"ok": True, "forwarded": status_key, "order_id": order_id, **forward_result}

        # Nada novo de relevante
        return {"ok": True, "ignored": "no_state_transition", "order_id": order_id, "order_status": order.get("status")}

    finally:
        # 10) Libera claim do pedido
        await finalize_order(order_id)
