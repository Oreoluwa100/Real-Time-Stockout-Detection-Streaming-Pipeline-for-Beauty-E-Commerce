import os
import json
import threading
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = "beautybyoa"

ORDERS_SUB = f"projects/{PROJECT_ID}/subscriptions/orders-events-sub"
INVENTORY_SUB = f"projects/{PROJECT_ID}/subscriptions/inventory-events-sub"


def parse_message(message):
    try:
        return {"status": "ok", "data": json.loads(message.decode("utf-8"))}
    except Exception as e:
        return {
            "status": "failed",
            "source": "parse",
            "raw_message": str(message),
            "failure_reason": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

def extract_order(event):
    try:
        print(f"[DEBUG] extract_order received: {event}")
        doc = event["data"]["fullDocument"]
        return {
            "status": "ok",
            "data": {
                 "id": doc["_id"],
                "order_id": doc["order_id"],
                "product_id": doc["product_id"],
                "product_name": doc["product_name"],
                "category": doc["category"],
                "quantity": doc["quantity"],
                "price": doc["price"],
                "customer_id": doc["customer_id"],
                "channel": doc["channel"],
                "order_status": doc["order_status"],
                "timestamp": doc["timestamp"]
            }
        }
    except Exception as e:
        return {
            "status": "failed",
            "source": "orders",
            "raw_message": json.dumps(event, default = str),
            "failure_reason": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

def extract_inventory(event):
    try:
        print(f"[DEBUG] extract_inventory received: {event}")
        doc = event["data"]["fullDocument"]
        return {
            "status": "ok",
            "data": {
                "id": doc["_id"],
                "inventory_id": doc["inventory_id"],
                "product_id": doc["product_id"],
                "product_name": doc["product_name"],
                "category": doc["category"],
                "quantity_before": doc["quantity_before"],
                "quantity_after": doc["quantity_after"],
                "movement_type": doc["movement_type"],
                "timestamp": doc["timestamp"]
            }
        }
    except Exception as e:
        return {
            "status": "failed",
            "source": "inventory",
            "raw_message": json.dumps(event, default=str),
            "failure_reason": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

class WriteToBigQueryDirect(beam.DoFn):
    def __init__(self, table, failed_table, project):
        self.table = table
        self.failed_table = failed_table
        self.project = project

    def setup(self):
        self.client = bigquery.Client(project=self.project)

    def process(self, element):
        if element["status"] == "ok":
            errors = self.client.insert_rows_json(self.table, [element["data"]])
            if errors:
                failed_row = {
                    "source": self.table,
                    "raw_message": json.dumps(element["data"]),
                    "failure_reason": str(errors),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                self.client.insert_rows_json(self.failed_table, [failed_row])
        else:
            failed_row = {k: v for k, v in element.items() if k != "status"}
            self.client.insert_rows_json(self.failed_table, [failed_row])


def run_orders_pipeline():
    options = PipelineOptions(
        streaming = True,
        project = PROJECT_ID,
        runner = "DirectRunner"
    )

    with beam.Pipeline(options = options) as p:
        parsed = (
            p
            | "Read Orders" >> beam.io.ReadFromPubSub(subscription = ORDERS_SUB)
            | "Parse Orders" >> beam.Map(parse_message)
        )

        good = parsed | "Filter Good Orders" >> beam.Filter(lambda x: x["status"] == "ok")
        failed = parsed | "Filter Failed Orders" >> beam.Filter(lambda x: x["status"] == "failed")

        (
            good
            | "Extract Orders" >> beam.Map(extract_order)
            | "Write Orders" >> beam.ParDo(
                WriteToBigQueryDirect(
                    table = f"{PROJECT_ID}.beautybyoa.orders",
                    failed_table = f"{PROJECT_ID}.beautybyoa.failed_events",
                    project = PROJECT_ID
                )
            )
        )

        (
            failed
            | "Write Failed Orders" >> beam.ParDo(
                WriteToBigQueryDirect(
                    table = f"{PROJECT_ID}.beautybyoa.failed_events",
                    failed_table = f"{PROJECT_ID}.beautybyoa.failed_events",
                    project = PROJECT_ID
                )
            )
        )


def run_inventory_pipeline():
    options = PipelineOptions(
        streaming = True,
        project = PROJECT_ID,
        runner = "DirectRunner"
    )

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "Read Inventory" >> beam.io.ReadFromPubSub(subscription = INVENTORY_SUB)
            | "Parse Inventory" >> beam.Map(parse_message)
        )

        good = parsed | "Filter Good Inventory" >> beam.Filter(lambda x: x["status"] == "ok")
        failed = parsed | "Filter Failed Inventory" >> beam.Filter(lambda x: x["status"] == "failed")

        (
            good
            | "Extract Inventory" >> beam.Map(extract_inventory)
            | "Write Inventory" >> beam.ParDo(
                WriteToBigQueryDirect(
                    table = f"{PROJECT_ID}.beautybyoa.inventory",
                    failed_table = f"{PROJECT_ID}.beautybyoa.failed_events",
                    project=PROJECT_ID
                )
            )
        )

        (
            failed
            | "Write Failed Inventory" >> beam.ParDo(
                WriteToBigQueryDirect(
                    table = f"{PROJECT_ID}.beautybyoa.failed_events",
                    failed_table = f"{PROJECT_ID}.beautybyoa.failed_events",
                    project=PROJECT_ID
                )
            )
        )


if __name__ == "__main__":
    print("Starting Beauty by OA streaming pipeline...")

    t1 = threading.Thread(target=run_orders_pipeline)
    t2 = threading.Thread(target=run_inventory_pipeline)

    t1.start()
    t2.start()

    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\nPipeline stopped.")