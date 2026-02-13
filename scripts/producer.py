import ccxt
import os
import json
import time
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# 1. Configuration (Set these as Environment Variables)
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "raw"
SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']

exchange = ccxt.binance()

def fetch_and_upload(container_client):
    print(f"[{datetime.now()}] Fetching market data...")
    try:
        tickers = exchange.fetch_tickers(SYMBOLS)
        
        # Prepare data for ingestion
        data_to_store = []
        for symbol, ticker in tickers.items():
            data_to_store.append({
                "symbol": symbol,
                "price": ticker['last'],
                "volume": ticker['baseVolume'],
                "timestamp": ticker['timestamp'],
                "datetime": ticker['datetime']
            })

        # Generate unique filename for the stream
        filename = f"crypto_tickers_{int(time.time())}.json"

        # Write NDJSON for easier streaming ingestion.
        payload = "\n".join(json.dumps(record) for record in data_to_store)

        # Upload to Azure
        blob_client = container_client.get_blob_client(filename)
        blob_client.upload_blob(payload, overwrite=True)
        print(f"✅ Uploaded {filename} to {CONTAINER_NAME} container.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if not CONNECTION_STRING:
        print("Error: AZURE_STORAGE_CONNECTION_STRING not set.")
        exit(1)

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    print("🚀 Starting Crypto Producer Stream...")
    while True:
        fetch_and_upload(container_client)
        time.sleep(30)  # Fetch every 30 seconds
