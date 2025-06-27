import websocket
import simplejson as json
import time
import ssl
import traceback
import threading
from config import *
import requests
from helpers.redis import getRedisClient
from app.RedisManager import RedisManager


def start_socket():
    while True:
        try:
            print("Connecting to WebSocket...")
            ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
            ws.connect(crypto_ws_url)

            login = {
                'event': 'login',
                'data': {
                    'apiKey': foreign_apiKey,
                }
            }

            redisManager = RedisManager(getRedisClient())

            res = requests.get(url=crypto_app_base_url)
            resultData = res.json()
            print(resultData)
            symbols = [item['symbol'].lower() for item in resultData['payload']]

            subscribe = {
                'event': 'subscribe',
                'data': {
                    'ticker': symbols,
                }
            }

            ws.send(json.dumps(login))
            time.sleep(1)
            ws.send(json.dumps(subscribe))
            time.sleep(1)

            def keep_alive():
                while True:
                    try:
                        ws.ping()
                        time.sleep(30)
                    except:
                        break

            threading.Thread(target=keep_alive, daemon=True).start()

            # Listen to messages
            while True:
                try:
                    message = ws.recv()
                    if not message.strip():
                        print("Received an empty message.")
                        continue

                    data = json.loads(message)

                    if data.get('s') is not None:
                        symbol = data.get('s')
                        row = next((item for item in resultData['payload'] if item.get('symbol') == symbol.upper()), None)
                        if row:
                            redisManager.updateCryptoSymbol(row, data)
                except websocket.WebSocketConnectionClosedException:
                    print("WebSocket disconnected. Reconnecting...")
                    break
                except Exception as e:
                    print("Error receiving message:", e)
                    traceback.print_exc()
                    break

        except Exception as e:
            print("Connection error:", e)
            traceback.print_exc()
        
        print("Retrying in 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    start_socket()
