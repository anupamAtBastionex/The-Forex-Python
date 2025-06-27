import websocket
from config import *
import simplejson as json
import time
import ssl
import requests
from helpers.redis import getRedisClient
from app.RedisManager import RedisManager
    
ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
ws.connect(us_ws_url)
    
login = {
    'event':'login',
    'data': {
        'apiKey': foreign_apiKey,
    }
}

redisManager = RedisManager(getRedisClient())

res = requests.get(url=us_app_base_url)
resultData = res.json()

# print("resultData", resultData)
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


    
ws.send(json.dumps(login))
     
time.sleep(1)
     
ws.send(json.dumps(subscribe))
     
# time.sleep(1)
     
# ws.send(json.dumps(unsubscribe))
    
while True:
    # print(ws.recv())
    message = ws.recv()
    data = json.loads(message)
    # print("Received data:", data)
    # for tick in ticks:
    if data.get('s') is not None:
        symbol = data.get('s')
        # Find the row in `resultData['payload']` that matches the received symbol
        row = next((item for item in resultData['payload'] if item.get('symbol') == symbol.upper()))
       
        # print(row)
        if row.get('symbol'):
            redisManager.updateUsSymbol(row, data)
