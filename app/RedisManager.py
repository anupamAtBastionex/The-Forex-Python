class RedisManager():
    def __init__(self, redisClient):
        self.redisClient = redisClient
    
    @staticmethod
    def keyDecodeToString(data):
        if not data:
            return None
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}

    @staticmethod
    def getKey(key):
        return f"tradeyar_database_{key}"


    def updateInstrumentBatch(self, instruments):
        """Batch update instruments in Redis."""
        if not self.redisClient:
            raise ValueError("❌ Redis client not initialized")

        pipe = self.redisClient.pipeline()
        for instrument in instruments:
            key = self.getKey(f"instrument:{instrument['token']}")
            pipe.hmset(key, instrument)  # Batch Redis update
            pipe.publish(self.getKey("priceUpdationChannel"), instrument["token"])  # Publish each update
        pipe.execute()
        print(f"✅ Batch updated {len(instruments)} instruments in Redis")
########################################### Indian Market ################################################
    
    def updateInstrument(self,instrument):
        self.redisClient.hmset(self.getKey(f"instrument:{instrument['token']}"),instrument)
        # self.redisClient.publish(self.getKey("priceUpdationChannel"),strinstrument["token"])
        self.redisClient.publish(self.getKey("priceUpdationChannel"), instrument["token"])

    def getInstrument(self,token):
        return self.keyDecodeToString(self.redisClient.hgetall(self.getKey(f"instrument:{token}")))
     
########################################### Forex ################################################

    def updateForexSymbol(self, rowData, liveData):

        instrument = self.convertForexLiveInstrumentForRedis(rowData, liveData)
        # Convert None values to an empty string or another default value
        cleaned_instrument = {k: (v if v is not None else "") for k, v in instrument.items()}
        
        # Store the cleaned instrument data in Redis
        self.redisClient.hmset(self.getKey(f"symbol:{cleaned_instrument['symbol'].lower()}"), cleaned_instrument)
        
        # Publish the symbol to the price update channel
        self.redisClient.publish(self.getKey("forexPriceUpdationChannel"), cleaned_instrument['symbol'].lower())
        print(instrument)

    def calculate_percentage_change(self, reference_price, new_price):
        return ((new_price - reference_price) / reference_price) * 100
    
    def convertForexLiveInstrumentForRedis(self, rowData, liveData):
        new_price = liveData.get('bp', 0)  # Bid price from live data
        previous_close = rowData.get("previous_close")  # Safely get previous close price

        # Calculate price change and percentage change
        if previous_close is not None:
            price_change = new_price - previous_close
            percentage_change = (price_change / previous_close) * 100
        else:
            price_change = 0
            percentage_change = 0

        ask_price       = float(liveData.get('ap', 0))
        bid_price       = float(liveData.get('bp', 0))
        current_high    = float(rowData.get("high_price", 0))
        current_low     = float(rowData.get("low_price", 0))
        ltp             = liveData.get('lp', 0) if liveData.get('type') in ['T', 'B'] else None
        ltpq            = liveData.get('ls', 0) if liveData.get('type') in ['T', 'B'] else None

        instrument = {
            "token": rowData['token'],
            "exchange": rowData['exchange'],
            "symbol": rowData['symbol'],
            "open_price": float(rowData["open_price"]),
            "close_price": rowData["close_price"],
            "previous_close": previous_close,  #rowData["previous_close"],
            "high_price": max(current_high, ask_price, bid_price),
            "low_price": min(current_low, ask_price, bid_price),
            "volume": rowData["volume"],
            "price_change": round(price_change, 3),
            "percentage_change": round(percentage_change, 3),
            "ltp": ltp,
            "ltpq": ltpq,
            "upper_circuit": 0,  # Set or remove based on your requirements
            "lower_circuit": 0,
        }

        # Set bid and ask price and quantity from live data
        instrument["bid_price"] = bid_price
        instrument["bid_qty"] = liveData.get("bs", 0)
        instrument["ask_price"] = ask_price
        instrument["ask_qty"] = liveData.get("as", 0)

        # Initialize or update pre-bid and pre-ask prices
        instrument["pre_bid_price"] = bid_price
        instrument["pre_ask_price"] = ask_price

        return instrument
    
########################################### US Market ################################################

    def updateUsSymbol(self, rowData, liveData):
        # Process the live data for US stock
        instrument = self.convertUsLiveInstrumentForRedis(rowData, liveData)

        # Convert None values to an empty string or another default value
        cleaned_instrument = {k: (v if v is not None else "") for k, v in instrument.items()}

        # Store the cleaned instrument data in Redis
        self.redisClient.hmset(self.getKey(f"symbol:{cleaned_instrument['symbol'].lower()}"), cleaned_instrument)

        # Publish the symbol to the price update channel
        self.redisClient.publish(self.getKey("UsPriceUpdationChannel"), cleaned_instrument['symbol'].lower())
        print(instrument)


    def calculate_percentage_change(self, reference_price, new_price):
        return ((new_price - reference_price) / reference_price) * 100
    
    def convertUsLiveInstrumentForRedis(self, rowData, liveData):
        new_price = float(liveData.get('bp', 0)) if liveData['type'] == 'Q' else None  # For Quote updates
        previous_close = float(rowData.get("previous_close", 0))

        # Calculate price change and percentage change if previous_close is available
        if previous_close:
            price_change = new_price - previous_close if new_price else 0
            percentage_change = (price_change / previous_close) * 100 if new_price else 0
        else:
            price_change = 0
            percentage_change = 0

        # Extract bid and ask prices, quantities, last price, and last size from live data
        ask_price = float(liveData.get('ap', 0)) if liveData['type'] == 'Q' else 0
        bid_price = float(liveData.get('bp', 0)) if liveData['type'] == 'Q' else 0
        last_price = float(liveData.get('lp', 0)) if liveData['type'] in ['T', 'B'] else None
        last_size = float(liveData.get('ls', 0)) if liveData['type'] in ['T', 'B'] else None

        # Define instrument data structure
        instrument = {
            "token": rowData['token'],
            "symbol": rowData['symbol'],
            "exchange": rowData['exchange'],
            "open_price": float(rowData.get("open_price", 0)),
            "close_price": rowData.get("close_price", 0),
            "previous_close": previous_close,
            "high_price": max(float(rowData.get("high_price", 0)), ask_price, bid_price, last_price or 0),
            "low_price": min(float(rowData.get("low_price", 0)), ask_price, bid_price, last_price or 0),
            "volume": rowData.get("volume", 0),
            "avg_volume": rowData.get("avg_volume", 0),
            "price_change": round(price_change, 3),
            "percentage_change": round(percentage_change, 3),
            "ltp": last_price,         # Last traded price
            "ltpq": last_size,         # Last traded quantity (volume)
            "upper_circuit": 0,        # Set if needed
            "lower_circuit": 0,
        }
        # Set bid and ask prices and quantities for Quote updates
        if liveData['type'] == 'Q':
            instrument["bid_price"] = bid_price
            instrument["bid_qty"] = liveData.get("bs", 0)
            instrument["ask_price"] = ask_price
            instrument["ask_qty"] = liveData.get("as", 0)

            # Update pre-bid and pre-ask prices since it's a new quote update
            instrument["pre_bid_price"] = bid_price
            instrument["pre_ask_price"] = ask_price
        else:
            # Keep previous bid/ask prices for non-Quote updates
            instrument["bid_price"] = instrument.get("pre_bid_price", None)
            instrument["bid_qty"] = instrument.get("bid_qty", 0)
            instrument["ask_price"] = instrument.get("pre_ask_price", None)
            instrument["ask_qty"] = instrument.get("ask_qty", 0)

        return instrument
    
########################################### CRYPTO Market ################################################
'''
    def updateCryptoSymbol(self, rowData, liveData):
        # Process real-time data
        instrument = self.convertCryptoLiveInstrumentForRedis(rowData, liveData)
        
        # Clean instrument data (replace None with empty strings or default values)
        cleaned_instrument = {k: (v if v is not None else "") for k, v in instrument.items()}
        
        # Save the processed instrument data in Redis
        self.redisClient.hmset(self.getKey(f"symbol:{cleaned_instrument['symbol'].lower()}"), cleaned_instrument)
        
        # Publish the symbol to the price update channel
        self.redisClient.publish(self.getKey("cryptoPriceUpdationChannel"), cleaned_instrument['symbol'].lower())
        print(instrument)


    def calculate_percentage_change(self, reference_price, new_price):
        return ((new_price - reference_price) / reference_price) * 100
    '''
    # def convertCryptoLiveInstrumentForRedis(self, rowData, liveData):
    #     # Get latest bid price from live data or set default
    #     new_price = float(liveData.get('lp', 0))  # Last traded price for crypto
    #     previous_close = float(rowData.get("previous_close", None) or new_price)  # Fix for zero issue

    #     # Calculate price change and percentage change
    #     price_change = new_price - previous_close
    #     percentage_change = (price_change / previous_close) * 100 if previous_close else 0

    #     print(liveData)


    #     ask_price = float(liveData.get('ap', 0)) if liveData.get('type') == 'Q' else 0  # Only for 'Q' updates
    #     bid_price = float(liveData.get('bp', 0)) if liveData.get('type') == 'Q' else 0
    #     last_price = new_price  # This represents the latest price from 'T' type updates
    #     last_size = float(liveData.get('ls', 0)) if liveData.get('type') == 'T' else 0

    #     # Update instrument data
    #     instrument = {
    #         "token": rowData['token'],
    #         "exchange": rowData['exchange'],
    #         "symbol": rowData['symbol'],
    #         "open_price": float(rowData.get("open_price", 0)),
    #         "close_price": rowData.get("close_price", 0),
    #         "previous_close": previous_close,
    #         "high_price": max(float(rowData.get("high_price", 0)), ask_price, bid_price, last_price),
    #         "low_price": min(float(rowData.get("low_price", 0)), ask_price, bid_price, last_price),
    #         "volume": rowData.get("volume", 0),
    #         "avg_volume": rowData.get("avg_volume", 0),
    #         "price_change": round(price_change, 3),
    #         "percentage_change": round(percentage_change, 3),
    #         "ltp": last_price,        # Last traded price
    #         "ltpq": last_size,        # Last traded quantity (size)
    #         "upper_circuit": 0,       # Not typically used in crypto, but could be set if required
    #         "lower_circuit": 0
    #     }

    #     # Set bid and ask price and quantity if available
    #     instrument["bid_price"] = bid_price
    #     instrument["bid_qty"] = liveData.get("bs", 0)
    #     instrument["ask_price"] = ask_price
    #     instrument["ask_qty"] = liveData.get("as", 0)

    #     # Update pre-bid and pre-ask prices based on the new data
    #     instrument["pre_bid_price"] = instrument["bid_price"]
    #     instrument["pre_ask_price"] = instrument["ask_price"]

    #     return instrument
'''
    def convertCryptoLiveInstrumentForRedis(self, rowData, liveData):
        # Get latest bid price from live data or set default
        print(liveData)
        new_price = float(liveData.get('lp', 0))  # Last traded price for crypto
        previous_close = float(rowData.get("previous_close", 0))  # Ensure it’s numeric

        # Avoid incorrect percentage calculations when LTP is zero
        if new_price > 0 and previous_close > 0:
            price_change = new_price - previous_close
            percentage_change = (price_change / previous_close) * 100
        else:
            price_change = 0
            percentage_change = 0  # Avoids misleading -100% changes


        valid_prices = [p for p in [float(rowData.get("high_price", 0)), float(rowData.get("low_price", 0)), new_price] if p > 0]

        instrument = {
            "token": rowData['token'],
            "exchange": rowData['exchange'],
            "symbol": rowData['symbol'],
            "open_price": float(rowData.get("open_price", 0)),
            "close_price": rowData.get("close_price", 0),
            "previous_close": previous_close,
            "high_price": max(valid_prices, default=previous_close),  # Avoids setting high_price to 0
            "low_price": min(valid_prices, default=previous_close),   # Avoids setting low_price to 0
            "volume": rowData.get("volume", 0),
            "avg_volume": rowData.get("avg_volume", 0),
            "price_change": round(price_change, 3),
            "percentage_change": round(percentage_change, 3),
            "ltp": new_price if new_price > 0 else previous_close,  # Avoids zero LTP
        }

        # ask_price = float(liveData.get('ap', 0)) if liveData.get('type') == 'Q' else 0  # Only for 'Q' updates
        # bid_price = float(liveData.get('bp', 0)) if liveData.get('type') == 'Q' else 0
    #     last_price = new_price  # This represents the latest price from 'T' type updates
    #     last_size = float(liveData.get('ls', 0)) if liveData.get('type') == 'T' else 0

        # Set bid and ask price and quantity if available
        instrument["bid_price"] = float(liveData.get('bp', 0)) if liveData.get('type') == 'Q' else 0
        instrument["bid_qty"]   = liveData.get("bs", 0)
        instrument["ask_price"] = float(liveData.get('ap', 0)) if liveData.get('type') == 'Q' else 0
        instrument["ask_qty"]   = liveData.get("as", 0)

        # Update pre-bid and pre-ask prices based on the new data
        instrument["pre_bid_price"] = instrument["bid_price"]
        instrument["pre_ask_price"] = instrument["ask_price"]

        return instrument
'''
########################################### End CRYPTO ################################################    
def updateCryptoSymbol(self, rowData, liveData):
        try:
            # Process real-time data with error handling
            instrument = self.convertCryptoLiveInstrumentForRedis(rowData, liveData)
            
            if not instrument:
                print("Error: Failed to convert instrument data")
                return

            # Clean instrument data (replace None with empty strings or default values)
            cleaned_instrument = {
                k: (v if v is not None else "" if isinstance(v, str) else 0 if isinstance(v, (int, float)) else "")
                for k, v in instrument.items()
            }

            # Validate symbol exists before proceeding
            if 'symbol' not in cleaned_instrument or not cleaned_instrument['symbol']:
                print("Error: Missing symbol in instrument data")
                return

            symbol_key = self.getKey(f"symbol:{cleaned_instrument['symbol'].lower()}")
            
            # Save the processed instrument data in Redis
            self.redisClient.hmset(symbol_key, cleaned_instrument)
            
            # Publish the symbol to the price update channel
            self.redisClient.publish(
                self.getKey("cryptoPriceUpdationChannel"), 
                cleaned_instrument['symbol'].lower()
            )
            
        except Exception as e:
            print(f"Error in updateCryptoSymbol: {str(e)}")
            raise

def calculate_percentage_change(self, reference_price, new_price):
        try:
            if reference_price == 0:  # Avoid division by zero
                return 0
            return ((new_price - reference_price) / reference_price) * 100
        except Exception as e:
            print(f"Error in calculate_percentage_change: {str(e)}")
            return 0

def convertCryptoLiveInstrumentForRedis(self, rowData, liveData):
        try:
            # Initialize default values
            instrument = {
                "token": rowData.get('token', 0),
                "exchange": rowData.get('exchange', ''),
                "symbol": rowData.get('symbol', ''),
                "open_price": 0,
                "close_price": 0,
                "previous_close": 0,
                "high_price": 0,
                "low_price": 0,
                "volume": 0,
                "avg_volume": 0,
                "price_change": 0,
                "percentage_change": 0,
                "ltp": 0,
                "bid_price": 0,
                "bid_qty": 0,
                "ask_price": 0,
                "ask_qty": 0,
                "pre_bid_price": 0,
                "pre_ask_price": 0
            }

            # Safely parse float values with defaults
            previous_close = self._parse_float(rowData.get("previous_close"))
            new_price = self._parse_float(liveData.get('lp'))
            open_price = self._parse_float(rowData.get("open_price"))
            high_price = self._parse_float(rowData.get("high_price"))
            low_price = self._parse_float(rowData.get("low_price"))

            # Price validation and calculation
            if new_price > 0 and previous_close > 0:
                price_change = new_price - previous_close
                percentage_change = self.calculate_percentage_change(previous_close, new_price)
                
                # Cap extreme percentage changes (e.g., > ±1000%)
                if abs(percentage_change) > 1000:
                    print(f"Warning: Extreme percentage change detected: {percentage_change}%")
                    percentage_change = 1000 if percentage_change > 0 else -1000
            else:
                price_change = 0
                percentage_change = 0

            # Get valid prices for high/low calculation
            valid_prices = [p for p in [high_price, low_price, new_price] if p > 0]
            
            # Update instrument data
            instrument.update({
                "open_price": open_price,
                "close_price": self._parse_float(rowData.get("close_price")),
                "previous_close": previous_close,
                "high_price": max(valid_prices, default=previous_close),
                "low_price": min(valid_prices, default=previous_close),
                "volume": self._parse_float(rowData.get("volume", 0)),
                "avg_volume": self._parse_float(rowData.get("avg_volume", 0)),
                "price_change": round(price_change, 8),
                "percentage_change": round(percentage_change, 6),
                "ltp": new_price if new_price > 0 else previous_close,
            })

            # Handle bid/ask data for quote updates
            if liveData.get('type') == 'Q':
                instrument.update({
                    "bid_price": self._parse_float(liveData.get('bp')),
                    "bid_qty": self._parse_float(liveData.get('bs')),
                    "ask_price": self._parse_float(liveData.get('ap')),
                    "ask_qty": self._parse_float(liveData.get('as')),
                    "pre_bid_price": self._parse_float(liveData.get('bp')),
                    "pre_ask_price": self._parse_float(liveData.get('ap'))
                })

            return instrument

        except Exception as e:
            print(f"Error in convertCryptoLiveInstrumentForRedis: {str(e)}")
            return None

def _parse_float(self, value, default=0):
        """Safe float parsing helper method"""
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default