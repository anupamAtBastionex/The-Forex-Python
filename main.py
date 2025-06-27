import requests, os, time
from config import *
from app.KiteManager import KiteManager
from kiteconnect import KiteConnect, KiteTicker
# from app.data import kiteManager
from helpers.redis import getRedisClient
from app.RedisManager import RedisManager


# initilaizations
# redis manager initilization
redisManager = RedisManager(getRedisClient())
# kiteManager initilization
res = requests.get(url=indian_app_base_url)
data = res.json()
# print("data \n", data)

kiteManager = KiteManager(data["payload"])
# kite Api from zerodha initilization
kite = KiteConnect(indian_api_key,access_token=indian_access_token)
# print(kite.login_url())
# data = kite.generate_session("hetjlePMcmWZ6V4Y5lleaD1yqQa6qKUh", api_secret="xia9dhmaw0qi0rsy9h8j94oepvkyq8mq")
# print(data['access_token'])
# print("hello")

kite.set_access_token(indian_access_token)
# kite.set_access_token("hetjlePMcmWZ6V4Y5lleaD1yqQa6qKUh")
# kite Socket handler from zerodha initilization
kws = KiteTicker(indian_api_key, indian_access_token)

# BUISNESS LOGIC STARTS FROM HERE
instrumentsTokens = []
instruments = kite.instruments()

# Filter and print only NFO instruments
# for instrument in instruments:
#     if instrument['exchange'] == 'SENSEX':
#         print(instrument)

mapper = kiteManager.getMapper(instruments)
# mapper = kiteManager.getMapper(instruments)
# print(mapper)
instrumentsTokens = [key for key,value in mapper.items()]
#state manage of all instruments for pre_bid_price or pre_ask_price etc
# state can also have other data but we remove it because redis only update passed columns and prevent other data from deleted
instrumentsState = {}     
# for token in mapper:
#     instrumentsState[mapper[token]] = redisManager.getInstrument(mapper[token])

for token, details in mapper.items():
    # redis_key = details['redis_key']
    # symbol = details['symbol']
    # exchange = details['exchange']

    # print(f"Token: {token}, Symbol: {symbol}, Exchange: {exchange}")
    # instrumentsState[redis_key] = redisManager.getInstrument(redis_key)
    instrumentsState[token] = redisManager.getInstrument(token)
#     if instrumentsState[token]['symbol'] != 'NIFTY':
#         if instrumentsState[token]['symbol'] != 'BANKNIFTY':
#             print(instrumentsState[token]['token'], instrumentsState[token]['symbol'], instrumentsState[token]['segment'])

# print(instrumentsState[token])
# exit()
updated_instruments = []
def on_ticks(ws, ticks):
    global updated_instruments

    for tick in ticks:
        try:
            token        = tick["instrument_token"]
            info         = mapper[token]
            actual_token = info["token"]               # Your app's Redis/internal token
            # Use the correct token to access instrumentsState
            instrument = kiteManager.convertKiteInstrumentForRedis(tick, instrumentsState.get(actual_token, {}), actual_token)
            # instrument = kiteManager.convertKiteInstrumentForRedis(tick, instrumentsState[mapper[token]], mapper[token])
            instrument["symbol"]          = info["symbol"]
            instrument["trading_symbol"]  = info["tradingsymbol"]
            instrument["exchange"]        = info["exchange"]
            instrument["segment"]         = info["segment"]
            instrument["lotsize"]         = info["lotsize"]
            instrument["tick_size"]       = info["tick_size"]
            instrument["expiry"]          = info["expiry"]
            instrument["currency"]        = "INR"
            # print(instrument)
            instrumentsState[instrument["token"]] = instrument
            if info["segment"] != 'INDICES':
                print(instrument)

            # Add to batch update list
            updated_instruments.append(instrument)

        except Exception as e:
            print("Error processing tick:", e)

    # Batch update Redis every 10 ticks
    if len(updated_instruments) >= 20:
        redisManager.updateInstrumentBatch(updated_instruments)
        # print(updated_instruments)
        updated_instruments = []

def on_connect(ws, response):
    if not hasattr(ws, 'subscribed'):
        ws.subscribe(instrumentsTokens)
        ws.set_mode(ws.MODE_FULL, instrumentsTokens)
        ws.subscribed = True  # Prevent multiple subscriptions

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
# kws.connect()


try:
    while True:
        kws.connect()
        time.sleep(2)
        continue
except KeyboardInterrupt:
    print("⛔ Program stopping... Closing WebSocket connection.")
    try:
        kws.close()
        print("✅ WebSocket closed successfully.")
    except Exception as e:
        print(f"⚠️ Error closing WebSocket: {e}")

    os._exit(0)  # Forcefully terminate process 


