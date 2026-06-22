# Real-Time E-Commerce Streaming Pipeline

## Project Narrative

When I explain this project, I describe it as building the data infrastructure a beauty e-commerce company would need to stop losing revenue to stockouts they never saw coming.

The problem with overnight batch analytics is simple: by the time the report runs, the product is already out of stock, the orders have already failed, and the opportunity is already gone. This pipeline captures every order and inventory event the moment it happens and alerts the business before it becomes a problem.

---

## The Problem

A beauty e-commerce company processes orders daily. Their analytics runs overnight. By the time anyone sees the data, fast-selling products are already out of stock.

**The question:** How do you get real-time visibility into which products are selling fast and approaching stockout without waiting for a batch job?

**The answer:** A streaming pipeline that captures every event at the source and surfaces alerts in real time.

---

## Architecture

```
simulate_events.py (Python)
        ↓
MongoDB Atlas (raw event store)
        ↓
Change Stream Listener (CDC)
        ↓
Google Cloud Pub/Sub (message broker)
        ↓
Apache Beam / DirectRunner (stream processing)
        ↓
BigQuery (analytics layer)
        ↓
Cloud Run + Cloud Scheduler (alert engine)
        ↓
Email Alerts (stockout + pipeline failure notifications)
```

---

## The Journey, Step by Step

### Step 1: Event Design and Simulation

Since this project simulates a real business, the first decision was what events to capture and what they should look like.

Two event types drive the pipeline:

**Order event** - fires when a customer places an order:
```json
{
  "order_id": "ORD-A3F9B2C1",
  "product_id": "PROD-006",
  "product_name": "Concealer",
  "category": "Concealer",
  "quantity": 2,
  "price": 22.00,
  "customer_id": "CUST-4782",
  "channel": "instagram",
  "order_status": "completed",
  "timestamp": "2026-05-29T10:30:00"
}
```

**Inventory event** - fires when stock changes after a sale:
```json
{
  "inventory_id": "INV-B7D3E9F2",
  "product_id": "PROD-006",
  "product_name": "Concealer",
  "category": "Concealer",
  "quantity_before": 50,
  "quantity_after": 48,
  "movement_type": "reduction",
  "timestamp": "2026-05-29T10:30:01"
}
```

Key design decisions made here:
- `order_status` field added to distinguish completed vs failed orders. A failed order means a customer tried to buy a product with insufficient stock, which is a direct stockout signal
- `movement_type` field added to distinguish stock reductions from restocks. Restocking is a downstream business decision triggered by an alert, not simulated automatically
- Two separate MongoDB collections (`orders`, `inventory`) rather than one for cleaner separation, independent change streams

The simulator (`simulate_events.py`) generates continuous realistic events every 2–5 seconds across 11 beauty products in 5 categories: Foundation, Lip Products, Concealer, Blush, and Eye Products.

---

### Step 2: Change Data Capture with MongoDB Change Streams

The listener (`listener.py`) watches both MongoDB collections in real time using Change Streams, MongoDB's CDC mechanism. Every insert triggers an event that gets published to Google Cloud Pub/Sub.

Two collections, two topics, two threads running concurrently:

| Collection | Pub/Sub Topic | Thread |
|---|---|---|
| orders | orders-events | watch_orders() |
| inventory | inventory-events | watch_inventory() |

Threading was necessary because `collection.watch()` is a blocking call, a single-threaded listener would process one collection at a time, not both simultaneously.

**Observability addition:** A heartbeat log fires every 30 seconds from each watch function, confirming the listener is alive even when no events are flowing. This catches the silent failure where the listener crashes but no one notices because the store is quiet.

```
[HEARTBEAT] watch_orders is alive - still watching orders collection
[HEARTBEAT] watch_inventory is alive - still watching inventory collection
```

---

### Step 3: Stream Processing with Apache Beam

The pipeline (`pipeline.py`) reads from both Pub/Sub topics, transforms the raw MongoDB change events into clean BigQuery rows, and handles failures explicitly.

Each message travels through four stages:

```
Raw bytes from Pub/Sub
        ↓
parse_message()     → decode bytes → JSON → Python dictionary
        ↓
Filter (status)     → route ok messages forward, failed messages to dead letter
        ↓
extract_order()     → pull fullDocument fields, discard MongoDB wrapper
        ↓
WriteToBigQueryDirect → insert into orders or inventory table
```

**Dead letter pattern:** Rather than crashing on bad data or dropping it silently, every failure is routed to a `failed_events` table in BigQuery with the raw message, failure reason, source, and timestamp.

Three layers of dead letter handling:
1. Parse failure - corrupted bytes or invalid JSON → `failed_events`
2. Extract failure - missing or renamed field → `failed_events`
3. BigQuery insert failure - schema mismatch, network error → `failed_events`

This pattern was validated during development: a field name mismatch (`id` vs `_id`) caused messages to route to `failed_events` rather than crashing the pipeline, proving the pattern works as designed.

**Known limitation:** The pipeline runs on DirectRunner (local execution) rather than Google Dataflow. In production, switching to Dataflow would provide automatic scaling, fault tolerance, and managed restarts. The code change required is minimal, only the runner flag in `PipelineOptions` needs to change.

---

### Step 4: Observability: Monitoring and Alerting

Two silent failure points exist between the components that no application-level code can catch:

**Silent failure 1 - MongoDB → Pub/Sub gap**
If the listener crashes, events stop flowing to Pub/Sub with no error surfaced downstream. Addressed by: heartbeat logging in `listener.py` (Step 2).

**Silent failure 2 — Pub/Sub → Beam gap**
If the pipeline stops consuming messages, they pile up in Pub/Sub with no notification. Addressed by: a Google Cloud Monitoring alert on `subscription/oldest_unacked_message_age`. If either subscription has messages older than 5 minutes, an alert email fires automatically.

**Business alert — Stockout detection**
The alert script (`alerts.py`) runs every 5 minutes via Cloud Scheduler, querying BigQuery for:
- New rows in `failed_events` → pipeline health alert
- Orders with `order_status = 'failed'` → stockout alert

A failed order means a customer tried to buy a product that has insufficient stock. Even a single failed order is worth alerting on, a quantity of 1 failing means the stock is already critically low.

The alert engine is deployed to Cloud Run and triggered by Cloud Scheduler, meaning it runs 24/7 in GCP regardless of whether any local scripts are running.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event simulation | Python |
| Raw event store | MongoDB Atlas |
| Change Data Capture | MongoDB Change Streams |
| Message broker | Google Cloud Pub/Sub |
| Stream processing | Apache Beam (DirectRunner) |
| Analytics layer | Google BigQuery |
| Alert engine | Python, Flask, Cloud Run |
| Scheduling | Google Cloud Scheduler |
| Monitoring | Google Cloud Monitoring |
| Version control | Git, GitHub |
| Deployment | Docker, Google Cloud Run |

---

## Repository Structure

```
realtime-ecommerce-streaming-pipeline/
├── simulate_events.py    ← generates continuous order and inventory events
├── listener.py           ← MongoDB Change Streams → Pub/Sub
├── pipeline.py           ← Pub/Sub → Apache Beam → BigQuery
├── alerts.py             ← BigQuery alert logic (stockout + pipeline failure)
├── main.py               ← Flask wrapper for Cloud Run deployment
├── Dockerfile            ← container definition for Cloud Run
├── requirements.txt      ← Python dependencies
└── README.md
```

---

## Running Locally

**Prerequisites:**
- MongoDB Atlas cluster with `beautybyoa` database
- GCP project with BigQuery, Pub/Sub, and Cloud Run APIs enabled
- Service account JSON with appropriate permissions
- `.env` file with `MONGO_URI`, `PROJECT_ID`, `ALERT_EMAIL`, `EMAIL_PASSWORD`

**Start in this order:**

```bash
# Terminal 1 - start the listener
python listener.py

# Terminal 2 - start the event simulator
python simulate_events.py

# Terminal 3 - start the pipeline
python pipeline.py
```

The alert script runs automatically via Cloud Scheduler every 5 minutes once deployed to Cloud Run.

---

## Production Deployment Path

| Component | Development | Production |
|---|---|---|
| Event source | simulate_events.py | Real e-commerce platform |
| Listener | Local Python script | Cloud Run (always-on, --min-instances 1) |
| Pipeline | DirectRunner (local) | Google Dataflow (managed Beam) |
| Alerts | Cloud Run + Cloud Scheduler | Already production-grade ✓ |

---

## Key Engineering Decisions

**Dead letter pattern over silent failure**
Bad messages route to `failed_events` rather than being dropped or crashing the pipeline. Every failure is captured with enough context to debug and reprocess.

**Two Pub/Sub topics over one**
Separating orders and inventory into dedicated topics keeps the downstream routing logic simple and makes each stream independently observable.

**Heartbeat monitoring**
A 30-second heartbeat in the listener proves it's alive even when no events are flowing, distinguishing between "the store is quiet" and "the listener has crashed."

**order_status as a stockout signal**
A failed order (quantity > available stock) is treated as a direct stockout indicator. Even a single failed order triggers an alert because a quantity of 1 failing means stock is already at zero.

**Deduplication noted but handled downstream**
Pub/Sub provides at-least-once delivery, meaning duplicate messages are possible under failure conditions. In production, deduplication would be handled in a dbt staging model using `ROW_NUMBER()` partitioned by `order_id`, preserving raw event history for auditability while serving clean data downstream.

---

## Screenshots

### Pub/Sub Lag Alert — Firing
![Pub/Sub lag alert firing](docs/screenshots/pubsub-lag-alert-firing.png)

### Pub/Sub Lag Alert — Recovered
![Pub/Sub lag alert recovered](docs/screenshots/pubsub-lag-alert-recovered.png)

### Stockout Alert Email
![Stockout alert email](docs/screenshots/stockout-alert-email.png)

---

## What This Project Demonstrates

- End-to-end streaming pipeline from CDC to analytics layer
- Production observability: heartbeat monitoring, dead letter pattern, lag alerting
- Event-driven architecture with decoupled components
- GCP data stack: Pub/Sub, BigQuery, Cloud Run, Cloud Scheduler, Cloud Monitoring
- Professional Git workflow: feature branches, pull requests, clean commit history
- Business-driven engineering: every technical decision traces back to the problem of detecting stockouts before they impact revenue
