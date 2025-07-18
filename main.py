from fastapi import FastAPI
import requests
from fastapi.responses import JSONResponse
import os

app = FastAPI()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

@app.get("/")
def root():
    return {"mensagem": "API Mercado Livre ativa"}


@app.get("/vendas")
def obter_vendas():
    try:
        if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
            return {"erro": "CLIENT_ID, CLIENT_SECRET ou REFRESH_TOKEN não definidos nas variáveis de ambiente."}

        # 1. Obter access_token via refresh_token
        token_url = "https://api.mercadolivre.com/oauth/token"
        payload = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN
        }
        token_response = requests.post(token_url, data=payload)
        
        if token_response.status_code != 200:
            return {"erro": f"Erro ao obter token. Status: {token_response.status_code}, Body: {token_response.text}"}
        
        tokens = token_response.json()


        if 'access_token' not in tokens:
            return {"erro": "Falha ao obter access_token", "detalhes": tokens}

        access_token = tokens['access_token']
        headers = {"Authorization": f"Bearer {access_token}"}

        # 2. Obter ID do usuário
        user_response = requests.get("https://api.mercadolibre.com/users/me", headers=headers)
        user_id = user_response.json()['id']

        # 3. Buscar pedidos com paginação até 2.000
        vendas = []
        limit = 50
        offset = 0
        max_vendas = 2000

        while True:
            url = f"https://api.mercadolibre.com/orders/search?seller={user_id}&order.status=paid&sort=date_desc&limit={limit}&offset={offset}"
            response = requests.get(url, headers=headers)
            data = response.json()
            results = data.get('results', [])

            if not results:
                break

            for o in results:
                vendas.append({
                    "pedido_id": o['id'],
                    "data": o['date_created'],
                    "comprador": o['buyer']['nickname'],
                    "total": o['total_amount'],
                    "status": o['status']
                })

            offset += limit
            if offset >= max_vendas:
                break

        return JSONResponse(content=vendas)

    except Exception as e:
        return {"erro": str(e)}
