import websocket
import json
from pymongo import MongoClient
from datetime import datetime
import requests

# MongoDB Setup
client = MongoClient('mongodb://localhost:27017')
db = client[DHAN]          
collection = db[DHAN]  

# Dhan API credentials
client_id= '1104639207'
access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMwMzQyOTA5LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNDYzOTIwNyJ9.4_DxpOulKrLVrq4Xm5VDd_l47Isfo5MmWpEV3eqYyIhBuDv9i89EmWqDNzuMfAPspjk-gG2L1SOKiRxTX9SvMg'  

# Function to handle incoming WebSocket messages
def on_message(ws, message):
    data = json.loads(message)  # Parse WebSocket data (JSON format from Dhan)
    
    # Assuming Dhan provides data like { "ltp": "last traded price", "symbol": "stock name" }
    last_price = data.get('ltp')  # LTP (last traded price)
    symbol = data.get('symbol')   # Stock/Instrument symbol
    timestamp = datetime.now()    # Current timestamp

    # Create document to store in MongoDB
    document = {
        "symbol": symbol,
        "price": last_price,
        "timestamp": timestamp
    }

    # Insert into MongoDB
    collection.insert_one(document)
    print(f"Inserted into MongoDB: {document}")

# Function to handle errors
def on_error(ws, error):
    print(f"Error: {error}")

# Function to handle WebSocket closure
def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

# Function to handle WebSocket connection opening
def on_open(ws):
    print("WebSocket opened")

    # Dhan WebSocket subscription format for real-time data
    subscribe_message = {
        "type": "subscribe",
        "token": TOKEN,
        "symbols": ["NSE:RELIANCE", "NSE:TCS"]  # Replace with the stock symbols you want to track
    }

    # Send subscription message
    ws.send(json.dumps(subscribe_message))

# WebSocket URL for Dhan real-time market data
ws_url = f"wss://api.dhan.co/market-data?api_key={API_KEY}"

# Create WebSocket object
ws = websocket.WebSocketApp(ws_url,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

# Start WebSocket connection
ws.on_open = on_open
ws.run_forever()
