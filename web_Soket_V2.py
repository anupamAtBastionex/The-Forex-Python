from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from SmartApi import SmartConnect
from logzero import logger
from datetime import datetime
import threading
from app.KiteManager import KiteManager
from kiteconnect import KiteConnect, KiteTicker
import time
import requests

# Your existing config and Redis setup
from config import *
from helpers.redis import getRedisClient
from app.RedisManager import RedisManager

# Smart API initialization
obj = SmartConnect(api_key=client_api_key)
data = obj.generateSession(Client_id, client_pin, client_totp)

# Tokens and credentials
refreshToken = data['data']['refreshToken']
AUTH_TOKEN = data['data']['jwtToken']
API_KEY = client_api_key
CLIENT_CODE = Client_id
FEED_TOKEN = obj.getfeedToken()
correlation_id = "Any_Text"
action = 1
mode = 3

# Fetch instrument mapping
res = requests.get(url="https://app.tradeyarr.com/api/public/script/fetchInstrument")
data = res.json()

# Initialize RedisManager
redisManager = RedisManager(getRedisClient())

# Extract tokens and map instrument data
# data["payload"] = ['112800', '112799']
# data["payload"].append({"token": "99926009"})
instruments = data["payload"]
# print(data)
instrumentsTokens = [instrument["token"] for instrument in instruments]
mapper = {instrument["token"]: instrument for instrument in instruments}
instrumentsState = {mapper[token]["token"]: redisManager.getInstrument(mapper[token]["token"]) for token in mapper}

# print(mapper)

# token_List_1 = [{"exchangeType": 1, "tokens": ['99926009', '112799', '112800', '112801', '112802', '112804', '112805', '112806', '112807', '112810', '112811', '112812', '112814', '112817', '112820', '112825', '112827', '112828', '112829', '112831', '112832', '112833', '112837', '112838', '112839', '112840', '112843', '112845', '112846', '112849', '112851', '112852', '112854', '112857', '112858', '112859', '112861', '112862', '112863', '112870', '112880', '112882', '112883', '112885', '112886', '112887', '112888', '112889', '112890', '112891', '112892', '112893', '112896', '112907', '112908', '112909', '112910', '112911', '112912', '112913', '112914', '112915', '112916', '341731', '356388', '366398', '367165', '367170', '367182', '369985', '374243', '374251', '374297', '374309', '374346', '374347', '374365', '374404', '374429', '374449', '376407', '376415', '376454', '376495', '376501', '376513', '376536', '376632', '376664', '376690', '376693', '376704', '376795', '376846', '376899', '376985', '377224', '377461', '377516', '377525', '377540', '377576', '377644', '377652', '377679', '377680', '377731', '377744', '377752', '377824', '377867', '377922', '377965', '377996', '378012', '378024', '378054', '378116', '378266', '378370', '378462', '378529', '378638', '378672', '378706', '378841', '378930', '378931', '378967', '378988', '379175', '379214', '379234', '379237', '379239', '379269', '379335', '379360', '379444', '379557', '379561', '379595', '379670', '379692', '379742', '379839', '379854', '379875', '379934', '379941', '379972', '380033', '380111', '380120', '380144', '380147', '380153', '380170', '380183', '380286', '380333', '380382', '380443', '380473', '380523', '380593', '380623', '380641', '380674', '380735', '380802', '380886', '380896', '380917', '381080', '381142', '381202', '381233', '381284', '381301', '381316', '381319', '381370', '381468', '381521', '381590', '381631', '381647', '381831', '381957', '381977', '382058', '382073', '382122', '382272', '382291', '382293', '382330', '382430', '382438', '382495', '382503', '382523', '382537', '382587', '382604', '382643', '382652', '382708', '382809', '382837', '382966', '382984', '383001', '383014', '383036', '383044', '383093', '383101', '383176', '383192', '383344', '383382', '383388', '383427', '383450', '383498', '383549', '383577', '383589', '383627', '383715', '383781', '383838', '383842', '383851', '383923', '383941', '383978', '383996', '384027', '384136', '384138', '384200', '384203', '384293', '384372', '384408', '384449', '384455', '384476', '384518', '384615', '384667', '384669', '384708', '386132', '386136', '386140', '386141', '386161', '386169', '386217', '386234', '386269', '386275', '386305', '386314', '386330', '386372', '386392', '386403', '386438', '386461', '386587', '386626', '386650', '386741', '386777', '386785', '386788', '386805', '386819', '386853', '386895', '386903', '387811', '387827', '388580', '389951', '389976', '408966']}]  # exchangeType 1 --> NSE Index

token_List_1 = [{"exchangeType": 2, "tokens": instrumentsTokens}]  # exchangeType 1 --> NSE Index
# Initialize WebSocket for SmartAPI (Angel One)
sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN, max_retry_attempt=3)
# print(f"WebSocket Object: {sws}")

# State management for instruments
LIVE_FEED_JSON = {}
#---------------------------------------------------------------------------
def convertKiteInstrumentForRedis(kiteInstrument,stateInstrument,token):
    instrument = {
        "token":token,
        "open_price":float(kiteInstrument["ohlc"]["open"]),
        "high_price":kiteInstrument["ohlc"]["high"],
        "low_price":kiteInstrument["ohlc"]["low"],
        "close_price":kiteInstrument["ohlc"]["close"],
        "volume":kiteInstrument["volume_traded"],
        "price_change":round(float(kiteInstrument["last_price"]) - float(kiteInstrument["ohlc"]["close"]),2),
        "ltp":kiteInstrument["last_price"],
        "ltpq":kiteInstrument["last_traded_quantity"],
        "percentage_change":round(float(kiteInstrument["change"]),2),
        "upper_circuit":0,
        "lower_circuit":0,
        # "tradable": "yes" if kiteInstrument["tradable"] else "no"
    }
    # instrument['percentage_change'] = ((instrument['price_change'] / instrument['open_price'])* 100) if instrument['open_price'] > 0 else 0
    try:
        # instrument["ltpt"]=kiteInstrument["last_trade_time"].timestamp()
        instrument["ltpt"]=int(kiteInstrument["last_trade_time"].timestamp())
    except:
        instrument["ltpt"]=0
    #MARKET DEPTH
    # SET BID SIDE ORDERBOOK
    for index,depth in enumerate(kiteInstrument["depth"]["buy"]):
        priceKey = "bid_price" if index == 0 else f"bid_price{index+1}"
        qtyKey = "bid_qty" if index == 0 else f"bid_qty{index+1}"
        instrument[priceKey] = depth["price"]
        instrument[qtyKey] = depth["quantity"]
    # SET ASK SIDE ORDERBOOK
    for index,depth in enumerate(kiteInstrument["depth"]["sell"]):
        priceKey = "ask_price" if index == 0 else f"ask_price{index+1}"
        qtyKey = "ask_qty" if index == 0 else f"ask_qty{index+1}"
        instrument[priceKey] = depth["price"]
        instrument[qtyKey] = depth["quantity"]

    # CHECK AND UPDATE PRE BID PRICE
    if not stateInstrument:
        instrument["pre_bid_price"] = instrument["bid_price"]
        instrument["pre_ask_price"] = instrument["ask_price"]
    else:
        # print(stateInstrument)
        if stateInstrument["bid_price"] != instrument["bid_price"]:
            instrument["pre_bid_price"] = stateInstrument["bid_price"]
        # else:
        #     instrument["pre_bid_price"] = instrument["pre_bid_price"]

        # CHECK AND UPDATE PRE ASK PRICE
        if stateInstrument["ask_price"] != instrument["ask_price"]:
            instrument["pre_ask_price"] = stateInstrument["ask_price"]
        # else:
        #     instrument["pre_ask_price"] = instrument["pre_ask_price"]

    return instrument

#----------------------------------------------------------------------------
def on_open(wsapp):
    print("on open")
    # print(token_List_1)
    sws.subscribe(correlation_id, mode, token_List_1) 


def on_data(wsapp, msg):
    try:
        
        # print("Ticks: {}".format(msg))
        #print(instrumentsState[mapper[msg["instrument_token"]]])
        instrument = convertKiteInstrumentForRedis(msg,instrumentsState[mapper[msg["instrument_token"]]], mapper[msg["instrument_token"]])

        # instrumentsState[instrument["token"]] = instrument
            # update redis
        # redisManager.updateInstrument(instrument)
        # LIVE_FEED_JSON[msg['token']] = {'token' :msg['token'] , 'ltp':msg['last_traded_price']/100 , 'exchange_timestamp':  datetime.fromtimestamp(msg['exchange_timestamp']/1000).isoformat() ,'oi':msg['open_interest']}
        # LIVE_FEED_JSON[msg['token']] = mapper

        # print(instrument)
    except Exception as e:
        print(e)




def on_error(wsapp, error):
    print(error)


def on_close(wsapp):
    print("Close")


# Assign the callbacks.
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close


threading.Thread(target = sws.connect).start()

print('Control Released')
time.sleep(2)
# print(len(LIVE_FEED_JSON))






# while True:
#     print(LIVE_FEED_JSON)
#     # if LIVE_FEED_JSON['99926009']['ltp'] > 27777:
#     #     print('Entry trigger' , LIVE_FEED_JSON['99926009']['ltp'])
#     time.sleep(10)
#     continue
    
    # print(LIVE_FEED_JSON)
    # time.sleep(2)
#------------------------------------end------------------------------------
# Define WebSocket Callbacks
# def on_ticks(ws, ticks):
#     print("Received Ticks")
#     for tick in ticks:
#         try:
#             token = tick["token"]
#             instrument = {
#                 "token": token,
#                 "ltp": tick["last_traded_price"] / 100,  # assuming price in paise
#                 "timestamp": datetime.fromtimestamp(tick["exchange_timestamp"] / 1000).isoformat(),
#                 "open_interest": tick["open_interest"]
#             }
#             instrumentsState[token] = instrument
#             redisManager.updateInstrument(instrument)
#             print(f"Updated instrument {token}: {instrument}")
#         except Exception as e:
#             print(f"Error processing tick: {e}")

# def on_connect(ws, response):
#     print("WebSocket connected")
#     sws.subscribe(correlation_id, mode=3, tokens=[{"exchangeType": 1, "tokens": instrumentsTokens}])

# def on_error(ws, error):
#     print(f"WebSocket error: {error}")

# def on_close(ws):
#     print("WebSocket closed")

# def on_data(ws, msg):
#     print(f"Received data: {msg}")

# # Assign the callbacks
# sws.on_ticks = on_ticks
# sws.on_connect = on_connect
# sws.on_error = on_error
# sws.on_close = on_close

# # Start WebSocket connection without threading to test
# sws.connect()

# # Infinite loop to process data
# while True:
#     try:
#         for token, instrument_data in LIVE_FEED_JSON.items():
#             if instrument_data['ltp'] > 27777:
#                 print(f"Entry trigger for token {token}: {instrument_data['ltp']}")
#         time.sleep(2)
#     except KeyError:
#         print("Waiting for data...")
#     except Exception as e:
#         print(f"Error in loop: {e}")
