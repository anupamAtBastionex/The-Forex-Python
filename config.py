import json
TOKEN_FILE = "kite_token.json"

# indian_app_base_url = 'https://app.theforex.live/api/public/script/fetchInstrument' #'http://127.0.0.1:8000/api/public/script/fetchInstrument'
indian_app_base_url = 'http://127.0.0.1:8000/api/public/script/fetchInstrument'
crypto_app_base_url = 'https://app.theforex.live/api/public/script/fetchCryptoInstrument'
forex_app_base_url  = 'https://app.theforex.live/api/public/script/fetchForexInstrument'
us_app_base_url     = 'https://app.theforex.live/api/public/script/fetchUsInstrument'

redis_host = 'localhost'
redis_port = 6379
redis_db = 0

crypto_ws_url = 'wss://crypto.financialmodelingprep.com'
forex_ws_url = 'wss://forex.financialmodelingprep.com'
us_ws_url = 'wss://websockets.financialmodelingprep.com'

foreign_apiKey = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

# ############ Mehreen Account info #####################
# indian_api_key = 'xxxxxxxxxxxxxxxxxxxx'
# indian_api_secret = 'xxxxxxxxxxxxxxxxxxx'
# indian_userid = 'xxxxxxxxxxxxxxxxxxxx'
# indian_password = 'xxxxxx'
# Auth_2FA_Key = 'xxxxxxxxxxxxxxxxxxxxxxxx'

############ Laxman Account info #####################
indian_api_key = 'xxxxxxxxxxxxxxxxxxxx'
indian_api_secret = 'xxxxxxxxxxxxxxxxxxxxx'
indian_userid = 'xxxxxxxxxxxxxxxxxxxxxxxx'
indian_password = 'xxxxxxxxxxxxxxxxx@1993'
Auth_2FA_Key = 'xxxxxxxxxxxxxxxxxxxxx'

TOKEN_FILE = "kite_token.json"
with open(TOKEN_FILE, "r") as file:
    token_data = json.load(file)
indian_access_token = token_data.get("access_token", "")