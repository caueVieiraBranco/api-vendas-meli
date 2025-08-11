
# Mercado Livre → n8n pre-filter (Render)

Filters Mercado Livre `orders_v2` notifications and forwards only *paid/approved* ones to your n8n webhook.

## Env vars
- ML_CLIENT_ID
- ML_CLIENT_SECRET
- ML_REFRESH_TOKEN
- N8N_WEBHOOK_URL  (e.g. https://your-n8n-host/webhook/meli/venda)
- FORWARD_SECRET   (choose a long random string; validate in n8n)

## Local run
pip install -r requirements.txt
uvicorn prefilter:app --reload --port 10000

## Render
- Create a new Web Service from this repo/zip
- Set env vars above
- Health check path: /healthz
- Start command: uvicorn prefilter:app --host 0.0.0.0 --port 10000
