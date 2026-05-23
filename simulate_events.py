import os
from dotenv import load_dotenv
load_dotenv()
import random
import time
import uuid
from datetime import datetime
import certifi
from pymongo import MongoClient

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["beautybyoa"]
orders_collection = db["orders"]
inventory_collection = db["inventory"]

# Product catalogue with starting stock levels
PRODUCTS = [
    {"product_id": "PROD-001", "product_name": "Matte Full Coverage Foundation", "category": "Foundation", "price": 35.00},
    {"product_id": "PROD-002", "product_name": "Skin Tint Glow Foundation", "category": "Foundation", "price": 28.00},
    {"product_id": "PROD-003", "product_name": "Lip Gloss", "category": "Lip Products", "price": 15.00},
    {"product_id": "PROD-004", "product_name": "Matte Lipstick", "category": "Lip Products", "price": 18.00},
    {"product_id": "PROD-005", "product_name": "Lip Liner", "category": "Lip Products", "price": 12.00},
    {"product_id": "PROD-006", "product_name": "Concealer", "category": "Concealer", "price": 22.00},
    {"product_id": "PROD-007", "product_name": "Powder Blush", "category": "Blush", "price": 20.00},
    {"product_id": "PROD-008", "product_name": "Liquid Blush", "category": "Blush", "price": 24.00},
    {"product_id": "PROD-009", "product_name": "Mascara", "category": "Eye Products", "price": 19.00},
    {"product_id": "PROD-010", "product_name": "Eyeliner", "category": "Eye Products", "price": 14.00},
    {"product_id": "PROD-011", "product_name": "Eyelashes", "category": "Eye Products", "price": 16.00},
]

# Initialise stock levels — random between 20 and 100 units per product
stock_levels = {p["product_id"]: random.randint(20, 100) for p in PRODUCTS}

# Sales channels
CHANNELS = ["mobile_app", "web", "instagram", "tiktok", "in_store"]


def generate_order(product):
    """Generate a realistic order event for a given product"""
    quantity = random.randint(1, 3)
    current_stock = stock_levels[product["product_id"]]

    if quantity > current_stock:
        order_status = "failed"
        quantity = 0
    else:
        order_status = "completed"

    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "price": product["price"],
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "channel": random.choice(CHANNELS),
        "order_status": order_status,
        "timestamp": datetime.utcnow().isoformat()
    }


def generate_inventory_update(product, quantity_sold, movement_type="reduction"):
    """Generate an inventory update event after an order"""
    quantity_before = stock_levels[product["product_id"]]
    if movement_type == "reduction":
        quantity_after = max(0, quantity_before - quantity_sold)
    else:
        quantity_after = quantity_before + random.randint(20, 50)

    # Update in-memory stock level
    stock_levels[product["product_id"]] = quantity_after

    return {
        "inventory_id": f"INV-{uuid.uuid4().hex[:8].upper()}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity_before": quantity_before,
        "quantity_after": quantity_after,
        "movement_type": movement_type,
        "timestamp": datetime.utcnow().isoformat()
    }


def run_simulation():
    """Continuously generate and insert events into MongoDB"""
    print("Starting Beauty by OA event simulation...")
    print(f"Initial stock levels: {stock_levels}")

    last_restock_time = time.time()
    event_count = 0

    try:
        while True:
            # Pick a random product
            product = random.choice(PRODUCTS)

            # Generate and insert order event
            order = generate_order(product)
            orders_collection.insert_one(order)
            event_count += 1
            print(f"[ORDER] {order['product_name']} x{order['quantity']} via {order['channel']} | Status: {order['order_status']} | Stock remaining: {stock_levels[product['product_id']]}")

            # Only update inventory if order was completed
            if order["order_status"] == "completed":
                inventory = generate_inventory_update(product, order["quantity"], movement_type="reduction")
                inventory_collection.insert_one(inventory)
                print(f"[INVENTORY] {inventory['product_name']} | Before: {inventory['quantity_before']} → After: {inventory['quantity_after']} | {inventory['movement_type']}")
            else:
                print(f"[FAILED ORDER] {order['product_name']} — insufficient stock")

            # Every 30 seconds trigger a restock on a random product
            if time.time() - last_restock_time >= 30:
                restock_product = random.choice(PRODUCTS)
                restock = generate_inventory_update(restock_product, 0, movement_type="restock")
                inventory_collection.insert_one(restock)
                print(f"[RESTOCK] {restock['product_name']} | Before: {restock['quantity_before']} → After: {restock['quantity_after']}")
                last_restock_time = time.time()

            # Wait random interval between 2 and 5 seconds before next event
            wait_time = random.uniform(2, 5)
            time.sleep(wait_time)

    except KeyboardInterrupt:
        print(f"\nSimulation stopped. Total events generated: {event_count}")


# Run the simulation
if __name__ == "__main__":
    run_simulation()