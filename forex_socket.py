import websocket
import simplejson as json
import time
from config import *
import ssl
import requests
from helpers.redis import getRedisClient
from app.RedisManager import RedisManager
    
ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
ws.connect(forex_ws_url)
    
login = {
    'event':'login',
    'data': {
        'apiKey': foreign_apiKey,
    }
}

redisManager = RedisManager(getRedisClient())

res = requests.get(url=forex_app_base_url)
resultData = res.json()

symbols = [item['symbol'].lower() for item in resultData['payload']]

# Prepare the subscription dictionary with the processed data
subscribe = {
    'event': 'subscribe',
    'data': {
        'ticker': symbols  # Set processed data as the ticker value
    }
}

# print(subscribe)

unsubscribe = {
    'event':'unsubscribe',
    'data': {
        'ticker': symbols,
    }
}
def connect_forex_ws():
    try:
        ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
        ws.connect(forex_ws_url)
        ws.send(json.dumps(login))
        time.sleep(1)
        ws.send(json.dumps(subscribe))
        time.sleep(1)
        return ws
    except Exception as e:
        print("WebSocket connection error:", e)
        return None

ws = connect_forex_ws()
if not ws:
    exit()

while True:
    try:
        message = ws.recv()
        if not message.strip():
            print("Received an empty message.")
            continue

        data = json.loads(message)
        if data.get('s'):
            symbol = data.get('s')
            row = next((item for item in resultData['payload'] if item.get('symbol') == symbol.upper()), None)
            if row:
                redisManager.updateForexSymbol(row, data)
    except websocket.WebSocketConnectionClosedException:
        print("Connection closed. Reconnecting...")
        ws = connect_forex_ws()
        if not ws:
            time.sleep(3)
        continue
    except Exception as e:
        print("Unexpected error:", e)
        continue
