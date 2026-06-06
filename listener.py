import os
import json
import time
import threading
from dotenv import load_dotenv
import certifi
from pymongo import MongoClient
from google.cloud import pubsub_v1

load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, tlsCAFile = certifi.where())
db = client["beautybyoa"]

# GCP settings
PROJECT_ID = os.getenv("PROJECT_ID")

# Pub/Sub publisher
publisher = pubsub_v1.PublisherClient.from_service_account_file("service-account.json")

# Topic paths
orders_topic = publisher.topic_path(PROJECT_ID, "orders-events")
inventory_topic = publisher.topic_path(PROJECT_ID, "inventory-events")

def watch_orders():
    """Watch orders collection and publish to orders-events topic"""
    print("Watching orders collection...")
    orders_collection = db["orders"]
    last_heartbeat = time.time()
    try:
        with orders_collection.watch(full_document = "updateLookup") as stream:
            while True:
                event = stream.try_next()
                
                if event is not None:
                    message = json.dumps(event, default=str)
                    message_bytes = message.encode("utf-8")
                    future = publisher.publish(orders_topic, message_bytes)
                    print(f"[ORDERS] Published event: {future.result()}")
                
                if time.time() - last_heartbeat >= 30:
                    print(f"[HEARTBEAT] watch_orders is alive - still watching orders collection")
                    last_heartbeat = time.time()
                
                time.sleep(0.1)
    except Exception as e:
        print(f"[ORDERS] Stream error: {e}")


def watch_inventory():
    """Watch inventory collection and publish to inventory-events topic"""
    print("Watching inventory collection...")
    inventory_collection = db["inventory"]
    last_heartbeat = time.time()
    try:
        with inventory_collection.watch(full_document="updateLookup") as stream:
            while True:
                event = stream.try_next()
                
                if event is not None:
                    message = json.dumps(event, default=str)
                    message_bytes = message.encode("utf-8")
                    future = publisher.publish(inventory_topic, message_bytes)
                    print(f"[INVENTORY] Published event: {future.result()}")
                
                if time.time() - last_heartbeat >= 30:
                    print(f"[HEARTBEAT] watch_inventory is alive - still watching inventory collection")
                    last_heartbeat = time.time()
                
                time.sleep(0.1)
    except Exception as e:
        print(f"[INVENTORY] Stream error: {e}")

if __name__ == "__main__":
    print("Starting Beauty by OA change stream listener...")
    
    t1 = threading.Thread(target = watch_orders)
    t2 = threading.Thread(target = watch_inventory)
    
    t1.start()
    t2.start()
    
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\nListener stopped.")