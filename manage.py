from kiteconnect import KiteConnect
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time, datetime, json, requests
from urllib.parse import urlparse, parse_qs
from webdriver_manager.chrome import ChromeDriverManager
import re, pyotp, os
from config import *
import requests, json, datetime

# Credentials
TOKEN_FILE = "kite_token.json"


def load_token():
    """Load saved access token and date from file."""
    try:
        with open(TOKEN_FILE, "r") as f:
            data = json.load(f)
        return data.get("access_token"), data.get("date")
    except FileNotFoundError:
        return None, None

def save_token(access_token):
    """Save access token and today's date to file."""
    data = {
        "access_token": access_token,
        "date": str(datetime.date.today())  # Save today's date
    }
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f)

def my_function():
    """Get access token - reuse if valid, otherwise request a new one."""
    stored_token, stored_date = load_token()
    today = str(datetime.date.today())
    
    if stored_token and stored_date == today:
        print("Login from Stored Access Token")
        
        kite = KiteConnect(api_key=indian_api_key)
        kite.set_access_token(stored_token)

        try:
            profile = kite.profile()
            print("Authentication Successful:", profile)
        except Exception as e:
            print("Authentication Failed:", e, "Re-generating access token")
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.maximize_window()
            try:
                kite_login_url = f"https://kite.zerodha.com/connect/login?api_key={indian_api_key}&v=3"
                driver.get(kite_login_url)

                time.sleep(2)  # Wait for page to load

                # Enter User ID
                user_id_field = driver.find_element(By.XPATH, '//input[@type="text"]')
                user_id_field.send_keys(indian_userid)

                # Enter Password
                password_field = driver.find_element(By.XPATH, '//input[@type="password"]')
                password_field.send_keys(indian_password)
                password_field.send_keys(Keys.RETURN)  # Press Enter

                time.sleep(2)  # Wait for next page

                # Enter PIN (2FA)
                auth_key = pyotp.TOTP(Auth_2FA_Key)
                auth_key_field = driver.find_element(By.XPATH, '//input[@type="number"]')
                auth_key_field.send_keys(auth_key.now())
                time.sleep(20)
                current_url = driver.current_url  
                time.sleep(10)
                print("current url", current_url)  
                parsed_url = urlparse(current_url)  
                query_params = parse_qs(parsed_url.query)  

                # Get the request_token if present anywhere in the URL  
                request_token = query_params.get("request_token", [""])[0]  

                # Print the request_token  
                print("Request Token:", request_token)  

                if not request_token:
                    raise ValueError("❌ Request token cannot be empty.")

                # Initialize KiteConnect and generate session
                driver.quit()
                kite = KiteConnect(api_key=indian_api_key)
                data = kite.generate_session(request_token, api_secret=indian_api_secret)

                # Extract and save new access token
                new_access_token = data.get("access_token")
                print("new_access_token", new_access_token)
                save_token(new_access_token)
                print("✅ New access token saved.")
                
                # # ✅ Login with new access token
                # kite.set_access_token(new_access_token)
                # profile = kite.profile()  # Test login
                # print("✅ Logged in as:", profile["user_name"])

                return new_access_token

            except requests.exceptions.RequestException as net_err:
                print("❌ Network Error:", str(net_err))

        return stored_token
        
    else:
        print("Token expired, Re generating Access Token")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.maximize_window()
        try:
            kite_login_url = f"https://kite.zerodha.com/connect/login?api_key={indian_api_key}&v=3"
            driver.get(kite_login_url)

            time.sleep(2)  # Wait for page to load

            # Enter User ID
            user_id_field = driver.find_element(By.XPATH, '//input[@type="text"]')
            user_id_field.send_keys(indian_userid)

            # Enter Password
            password_field = driver.find_element(By.XPATH, '//input[@type="password"]')
            password_field.send_keys(indian_password)
            password_field.send_keys(Keys.RETURN)  # Press Enter

            time.sleep(2)  # Wait for next page

            # Enter PIN (2FA)
            auth_key = pyotp.TOTP(Auth_2FA_Key)
            auth_key_field = driver.find_element(By.XPATH, '//input[@type="number"]')
            auth_key_field.send_keys(auth_key.now())
            time.sleep(20)
            current_url = driver.current_url  
            time.sleep(10)
            print("current url", current_url)  
            parsed_url = urlparse(current_url)  
            query_params = parse_qs(parsed_url.query)  

            # Get the request_token if present anywhere in the URL  
            request_token = query_params.get("request_token", [""])[0]  

            # Print the request_token  
            print("Request Token:", request_token)  

            if not request_token:
                raise ValueError("❌ Request token cannot be empty.")

            # Initialize KiteConnect and generate session
            driver.quit()
            kite = KiteConnect(api_key=indian_api_key)
            data = kite.generate_session(request_token, api_secret=indian_api_secret)

            # Extract and save new access token
            new_access_token = data.get("access_token")
            print("new_access_token", new_access_token)
            save_token(new_access_token)
            print("✅ New access token saved.")
            
            # # ✅ Login with new access token
            # kite.set_access_token(new_access_token)
            # profile = kite.profile()  # Test login
            # print("✅ Logged in as:", profile["user_name"])



            return new_access_token

        except requests.exceptions.RequestException as net_err:
            print("❌ Network Error:", str(net_err))


my_function()
# try:
#     while True:
#         my_function()
#         time.sleep(2)
#         continue
# except KeyboardInterrupt:
#     print("⛔ Program stopping... Closing WebSocket connection.")
#     try:
#         print("✅ WebSocket closed successfully.")
#     except Exception as e:
#         print(f"⚠️ Error closing WebSocket: {e}")

#     os._exit(0)  # Forcefully terminate process    
       
                