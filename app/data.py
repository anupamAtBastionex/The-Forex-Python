import requests
import json
from KiteManager import KiteManager

# res = requests.get(url="https://app.tradeyarr.com/api/public/script/fetchInstrument")
res = requests.get(url="https://127.0.0.1:8000/api/public/script/fetchInstrument")
data = res.json()
kiteManager = KiteManager(data["payload"])
# symbolManager.getKiteTradingSymbols()