import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

bq_client = bigquery.Client(project=PROJECT_ID)

def send_email(subject, body):
    """Send an alert email via Gmail"""
    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = ALERT_EMAIL
        msg["To"] = ALERT_EMAIL
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(ALERT_EMAIL, EMAIL_PASSWORD)
            server.sendmail(ALERT_EMAIL, ALERT_EMAIL, msg.as_string())
        
        print(f"[ALERT] Email sent: {subject}")
    except Exception as e:
        print(f"[ALERT] Failed to send email: {e}")

def check_failed_events():
    """Check for new rows in failed_events in the last 5 minutes"""
    query = f"""
        SELECT COUNT(*) as count
        FROM `{PROJECT_ID}.beautybyoa.failed_events`
        WHERE CAST(timestamp AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    """
    result = bq_client.query(query).result()
    count = list(result)[0]["count"]
    
    if count > 0:
        send_email(
            subject=f"[PIPELINE ALERT] {count} failed events detected - Beauty by OA",
            body=f"{count} messages failed to process in the last 5 minutes and were routed to failed_events.\n\nCheck the failed_events table in BigQuery for details.\n\nTimestamp: {datetime.now(timezone.utc).isoformat()}"
        ) 

def check_stockout_orders():
    """Check for failed orders due to insufficient stock in the last 5 minutes"""
    query = f"""
        SELECT product_name, category, COUNT(*) as failed_orders
        FROM `{PROJECT_ID}.beautybyoa.orders`
        WHERE order_status = 'failed'
        AND CAST(timestamp AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
        GROUP BY product_name, category
    """
    results = list(bq_client.query(query).result())
    
    if results:
        product_lines = "\n".join([
            f"- {row['product_name']} ({row['category']}): {row['failed_orders']} failed orders"
            for row in results
        ])
        send_email(
            subject=f"[STOCKOUT ALERT] {len(results)} product(s) out of stock - Beauty by OA",
            body=f"The following products have insufficient stock and are failing orders:\n\n{product_lines}\n\nTimestamp: {datetime.now(timezone.utc).isoformat()}"
        )

if __name__ == "__main__":
    print(f"[{datetime.now(timezone.utc).isoformat()}] Running alert checks...")
    check_failed_events()
    check_stockout_orders()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Alert checks complete.")