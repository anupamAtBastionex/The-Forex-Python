from datetime import datetime
def getMonthNumber(date_str):
    # Parse the input date string into a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    # Extract the month number (1-12)
    month_number = date_obj.month
    return month_number

class KiteManager():
    segmentMap = {
        "MCX-FUT":"FUTCOM",
        "MCX-OPT":"OPTFUT",
        "NFO-FUT":"FUTSTK",
        "NFO-OPT":"OPTSTK",
        "BFO-OPT":"OPTSTK",
        "INDICES":"INDICES"
    }

    def __init__(self, instruments):
        self.instruments = instruments

    # def getMapper(self,kiteInstruments):
    #     # print(kiteInstruments)
    #     mapper = {}
    #     for kiteInstrument in kiteInstruments:
    #         expiry = kiteInstrument['expiry'].month if kiteInstrument['expiry'] else 0
    #         if not self.getSegment(kiteInstrument):
    #             continue
    #         token = self.getToken(kiteInstrument['name'],self.getSegment(kiteInstrument),kiteInstrument['exchange'],expiry,kiteInstrument["strike"],kiteInstrument["instrument_type"], kiteInstrument["tradingsymbol"], str(kiteInstrument['expiry']))
    #         # print(token)
    #         if token:
    #             mapper[kiteInstrument['instrument_token']] = token
    #     return mapper
    
    def getMapper(self, instruments):
        mapper = {}
        for inst in instruments:
            if inst['name'] == 'MIDCPNIFTY':
                print(inst)
            expiry = inst['expiry'].month if inst['expiry'] else 0
            # token = inst['instrument_token']
            if not self.getSegment(inst):
                continue
            
            token = self.getToken(inst['name'], self.getSegment(inst), self.getExchange(inst), expiry, inst["strike"], inst["instrument_type"], inst["tradingsymbol"], str(inst['expiry']))
            # print(inst)
            if token:
                segment = self.getSegment(inst)
                mapper[inst['instrument_token']] = {
                    'redis_key': f"instrument:{token}",
                    'token': token,
                    'symbol': inst['name'],
                    'lotsize': inst['lot_size'],
                    'tick_size': inst['tick_size'],
                    "expiry": inst["expiry"].strftime('%Y-%m-%d'),
                    'tradingsymbol': inst['tradingsymbol'],
                    'segment': segment, #inst['name'] in ['BANKNIFTY','NIFTY'] and inst['segment'] == "OPTSTK": return "OPTIDX",
                    'exchange': self.getExchange(inst)
                }
            else:
                
                # if inst['instrument_token'] == 265 or inst['instrument_token'] == 260105 or inst['instrument_token'] == 256265:
                # 265: Sensex, 260105: Banknifty, 256265: Nifty, 410633: Finnifty, 256777: Midcap Nifty 50 
                if inst['instrument_token'] in [265, 260105, 256265, 274441, 257801, 256777, 288009]:  
                    segment = "INDICES"
                    mapper[inst['instrument_token']] = {
                        'redis_key': f"instrument:{inst['instrument_token']}",
                        'token': inst['instrument_token'],
                        'symbol': inst['name'],
                        'lotsize': inst['lot_size'],
                        'tick_size': inst['tick_size'],
                        "expiry": "", #inst["expiry"].strftime('%Y-%m-%d'),
                        'tradingsymbol': inst['tradingsymbol'],
                        'segment': segment, #inst['name'] in ['BANKNIFTY','NIFTY'] and inst['segment'] == "OPTSTK": return "OPTIDX",
                        'exchange': "NFO"
                    } 
                    # print(segment)
        return mapper

    
    def getToken(self,symbol,segment,exchange,month,strikePrice=0,optionType="", trading_symbol="", expiry="0"):
        for instrument in self.instruments:
            if instrument['symbol'] == symbol and instrument['segment'] == segment and instrument['exchange'] == exchange and getMonthNumber(instrument['expiry']) == month:
                if segment in ["OPTSTK","OPTFUT","OPTIDX"]:
                    if not (str(instrument['strike_price']) == str(strikePrice).split(".")[0] and instrument['option_type'] == optionType):
                        continue
                    if segment == "OPTIDX":
                        if instrument['expiry'] != expiry:
                            continue
                return instrument["token"]
        else:
            return False
            
    @staticmethod
    def getSegment(kiteInstrument):
        segment = KiteManager.segmentMap.get(kiteInstrument['segment'])
        if not segment:
            return False
        elif kiteInstrument['name'] in ['BANKNIFTY','NIFTY'] and segment == "FUTSTK":
            return "FUTIDX"
        elif kiteInstrument['name'] in ['BANKNIFTY','NIFTY'] and segment == "OPTSTK":
            return "OPTIDX"
        elif kiteInstrument['name'] in ['BANKEX','SENSEX'] and segment == "OPTSTK":
            return "OPTIDX"
        else:
            return segment
        

    @staticmethod
    def getExchange(kiteInstrument):
        segment = KiteManager.segmentMap.get(kiteInstrument['segment'])
        if not segment:
            return False
        elif kiteInstrument['name'] in ['BANKEX','SENSEX'] and segment == "OPTSTK":
            return "NFO"
        else:
            return kiteInstrument['exchange']
        
    @staticmethod
    def convertKiteInstrumentForRedis(kiteInstrument, stateInstrument, token):
        is_index = kiteInstrument.get("segment") == "INDICES"

        instrument = {
            "token": token,
            "open_price": float(kiteInstrument["ohlc"]["open"]) if not is_index and kiteInstrument.get("ohlc") else 0.0,
            "high_price": kiteInstrument["ohlc"]["high"] if not is_index and kiteInstrument.get("ohlc") else 0.0,
            "low_price": kiteInstrument["ohlc"]["low"] if not is_index and kiteInstrument.get("ohlc") else 0.0,
            "close_price": kiteInstrument["ohlc"]["close"] if not is_index and kiteInstrument.get("ohlc") else 0.0,
            "volume": kiteInstrument["volume_traded"] if not is_index and kiteInstrument.get("volume_traded") else 0,
            "price_change": round(
                float(kiteInstrument["last_price"]) - float(kiteInstrument["ohlc"]["close"]),
                2
            ) if not is_index and kiteInstrument.get("ohlc") else 0.0,
            "ltp": kiteInstrument.get("last_price", 0.0),
            "ltpq": kiteInstrument["last_traded_quantity"] if kiteInstrument.get("last_traded_quantity") else 0,
            "percentage_change": round(float(kiteInstrument["change"]), 2) if kiteInstrument.get("change") else 0.0,
            "upper_circuit": 0,
            "lower_circuit": 0,
        }

        try:
            instrument["ltpt"] = int(kiteInstrument["last_trade_time"].timestamp())
        except:
            instrument["ltpt"] = 0

        # Add market depth only if not index and depth exists
        if not is_index and kiteInstrument.get("depth"):
            for index, depth in enumerate(kiteInstrument["depth"]["buy"]):
                priceKey = "bid_price" if index == 0 else f"bid_price{index+1}"
                qtyKey = "bid_qty" if index == 0 else f"bid_qty{index+1}"
                instrument[priceKey] = depth["price"]
                instrument[qtyKey] = depth["quantity"]

            for index, depth in enumerate(kiteInstrument["depth"]["sell"]):
                priceKey = "ask_price" if index == 0 else f"ask_price{index+1}"
                qtyKey = "ask_qty" if index == 0 else f"ask_qty{index+1}"
                instrument[priceKey] = depth["price"]
                instrument[qtyKey] = depth["quantity"]
        else:
            # Set dummy bid/ask price for indices
            instrument["bid_price"] = kiteInstrument.get("last_price", 0.0)
            instrument["bid_qty"] = 0
            instrument["ask_price"] = kiteInstrument.get("last_price", 0.0)
            instrument["ask_qty"] = 0

        # Previous bid/ask price logic
        if not stateInstrument:
            instrument["pre_bid_price"] = instrument.get("bid_price", 0.0)
            instrument["pre_ask_price"] = instrument.get("ask_price", 0.0)
        else:
            if stateInstrument.get("bid_price") != instrument.get("bid_price"):
                instrument["pre_bid_price"] = stateInstrument.get("bid_price", 0.0)
            if stateInstrument.get("ask_price") != instrument.get("ask_price"):
                instrument["pre_ask_price"] = stateInstrument.get("ask_price", 0.0)

        return instrument
