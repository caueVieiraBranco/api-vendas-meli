from fastapi import FastAPI
import requests
from fastapi.responses import JSONResponse

app = FastAPI()

# === CONFIGURAÇÕES ===
CLIENT_ID = '6439275970401699'
CLIENT_SECRET = 'Pw9VVyEx4Wj3iYSeNFVvCWSd44I1j7hZ'
REFRESH_TOKEN = 'TG-685eefa4befee4000139f1cc-162089212'

@app.get("/vendas")
def obter_vendas():
    try:
        # 1. Obter access_token via refresh_token
        token_url = "https://api.mercadolibre.com/oauth/token"
        payload = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN
        }
        token_response = requests.post(token_url, data=payload)
        tokens = token_response.json()
        print(tokens)  # Debug opcional

        access_token = tokens['access_token']
        headers = {"Authorization": f"Bearer {access_token}"}

        # 2. Obter ID do usuário
        user_response = requests.get("https://api.mercadolibre.com/users/me", headers=headers)
        user_id = user_response.json()['id']

        # 3. Buscar pedidos
        url = f"https://api.mercadolibre.com/orders/search?seller={user_id}&order.status=paid&sort=date_desc&limit=50"
        orders_response = requests.get(url, headers=headers)
        orders = orders_response.json().get('results', [])

        # 4. Organizar dados
        vendas = []
        for o in orders:
            vendas.append({
                "pedido_id": o['id'],
                "data": o['date_created'],
                "comprador": o['buyer']['nickname'],
                "total": o['total_amount'],
                "status": o['status']
            })

        return JSONResponse(content=vendas)

    except Exception as e:
        return {"erro": str(e)}
