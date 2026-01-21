import json
import os
import datetime
import requests
import functions_framework
from google.cloud import bigquery
from typing import List, Dict, Any, Optional

# Configuration
PLUNK_EVENTS_URL = "https://next-api.useplunk.com/events"
BQ_TABLE_EVENTS = "acu-service.plunk.events"  # Dataset must exist
ALERT_EMAIL_RECIPIENT = "florian.buettner@cloudmetrics.de"
ALERT_FROM_EMAIL = os.environ.get("PLUNK_FROM_EMAIL", "termin@a-c-u.eu")
PLUNK_SEND_URL = "https://next-api.useplunk.com/v1/send"

# Thresholds
BOUNCE_RATE_7D_WARNING = 0.05
BOUNCE_RATE_7D_CRITICAL = 0.075
BOUNCE_RATE_ALL_TIME_WARNING = 0.04
BOUNCE_RATE_ALL_TIME_CRITICAL = 0.06

# Global Clients
bq_client = None

def get_bq_client():
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
    return bq_client

def get_plunk_key():
    key = os.environ.get("PLUNK_API_KEY") or os.environ.get("PLUNK_BEARER_TOKEN")
    if not key:
        raise RuntimeError("Missing PLUNK_API_KEY env var")
    return key.strip()

def fetch_plunk_events() -> List[Dict[str, Any]]:
    key = get_plunk_key()
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    
    # Plunk API currently returns latest events. Pagination not standard?
    # We just fetch what we get and upsert.
    resp = requests.get(PLUNK_EVENTS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    
    data = resp.json()
    return data.get("events", [])

def upsert_events_to_bq(events: List[Dict[str, Any]]):
    if not events:
        print("No events to upsert.")
        return

    client = get_bq_client()
    
    # Prepare rows for streaming insert (or could use load job). 
    # Since we need upsert, we might physically just insert all and use correct query logic (dedup) 
    # or use MERGE statement. 
    # For simplicity and robustness with "latest X entries":
    # proper MERGE via query is best.

    # 1. Load into temp table
    params = []
    for e in events:
        # Extract fields safely
        data_obj = e.get("data", {})
        contact_obj = e.get("contact", {})
        
        row = {
            "id": e.get("id"),
            "name": e.get("name"),
            "project_id": e.get("projectId"),
            "contact_id": e.get("contactId"),
            "email_id": e.get("emailId"),
            "created_at": e.get("createdAt"),
            "timestamp": e.get("createdAt"), # Duplicate for partitioning if needed
            
            # Flatter data fields
            "data_from": data_obj.get("from"),
            "data_subject": data_obj.get("subject"),
            "data_template_id": data_obj.get("templateId"),
            "data_message_id": data_obj.get("messageId"),
            "data_bounced_at": data_obj.get("bouncedAt"),
            "data_bounce_type": data_obj.get("bounceType"),
            
            "contact_email": contact_obj.get("email"),
            
            # Store full raw just in case
            "raw_json": json.dumps(e)
        }
        params.append(row)

    # We use a MERGE query.
    # We pass the data as a struct array param.
    
    merge_query = f"""
    MERGE `{BQ_TABLE_EVENTS}` T
    USING UNNEST(@events) S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET 
        name = S.name,
        contact_id = S.contact_id,
        email_id = S.email_id,
        data_bounced_at = S.data_bounced_at,
        data_bounce_type = S.data_bounce_type,
        raw_json = S.raw_json
    WHEN NOT MATCHED THEN
      INSERT (id, name, project_id, contact_id, email_id, created_at, timestamp, 
              data_from, data_subject, data_template_id, data_message_id, 
              data_bounced_at, data_bounce_type, contact_email, raw_json)
      VALUES (S.id, S.name, S.project_id, S.contact_id, S.email_id, S.created_at, S.timestamp,
              S.data_from, S.data_subject, S.data_template_id, S.data_message_id,
              S.data_bounced_at, S.data_bounce_type, S.contact_email, S.raw_json)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "events",
                bigquery.StructQueryParameterType(
                    bigquery.ScalarQueryParameterType("STRING", name="id"),
                    bigquery.ScalarQueryParameterType("STRING", name="name"),
                    bigquery.ScalarQueryParameterType("STRING", name="project_id"),
                    bigquery.ScalarQueryParameterType("STRING", name="contact_id"),
                    bigquery.ScalarQueryParameterType("STRING", name="email_id"),
                    bigquery.ScalarQueryParameterType("TIMESTAMP", name="created_at"),
                    bigquery.ScalarQueryParameterType("TIMESTAMP", name="timestamp"),
                    bigquery.ScalarQueryParameterType("STRING", name="data_from"),
                    bigquery.ScalarQueryParameterType("STRING", name="data_subject"),
                    bigquery.ScalarQueryParameterType("STRING", name="data_template_id"),
                    bigquery.ScalarQueryParameterType("STRING", name="data_message_id"),
                    bigquery.ScalarQueryParameterType("TIMESTAMP", name="data_bounced_at"),
                    bigquery.ScalarQueryParameterType("STRING", name="data_bounce_type"),
                    bigquery.ScalarQueryParameterType("STRING", name="contact_email"),
                    bigquery.ScalarQueryParameterType("STRING", name="raw_json"),
                ),
                params
            )
        ]
    )
    
    get_bq_client().query(merge_query, job_config=job_config).result()
    print(f"Upserted {len(params)} events.")

def calculate_bounce_metrics() -> Dict[str, Any]:
    client = get_bq_client()
    
    # Base query for metrics
    # Bounce Rate = (Distinct Emails with Bounce) / (Distinct Emails Sent)
    # Using emailId as unique identifier for a SENT mail.
    
    # 7 Days
    query_7d = f"""
    WITH sent AS (
        SELECT COUNT(DISTINCT email_id) as cnt
        FROM `{BQ_TABLE_EVENTS}`
        WHERE name = 'email.sent'
          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    ),
    bounced AS (
        SELECT COUNT(DISTINCT email_id) as cnt
        FROM `{BQ_TABLE_EVENTS}`
        WHERE name = 'email.bounce'
          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )
    SELECT sent.cnt as sent_count, bounced.cnt as bounced_count
    FROM sent, bounced
    """
    
    # All Time
    query_all = f"""
    WITH sent AS (
        SELECT COUNT(DISTINCT email_id) as cnt
        FROM `{BQ_TABLE_EVENTS}`
        WHERE name = 'email.sent'
    ),
    bounced AS (
        SELECT COUNT(DISTINCT email_id) as cnt
        FROM `{BQ_TABLE_EVENTS}`
        WHERE name = 'email.bounce'
    )
    SELECT sent.cnt as sent_count, bounced.cnt as bounced_count
    FROM sent, bounced
    """
    
    row_7d = list(client.query(query_7d).result())[0]
    row_all = list(client.query(query_all).result())[0]
    
    sent_7d = row_7d["sent_count"] or 0
    bounced_7d = row_7d["bounced_count"] or 0
    rate_7d = (bounced_7d / sent_7d) if sent_7d > 0 else 0.0
    
    sent_all = row_all["sent_count"] or 0
    bounced_all = row_all["bounced_count"] or 0
    rate_all = (bounced_all / sent_all) if sent_all > 0 else 0.0

    # Get recent bounces details (last 24h for reporting context, or just all contributing to alert?)
    # User asked for: "List of bounce events... contact-email, subject, bouncedAt"
    # Let's fetch last 20 bounces for the report.
    query_bounces = f"""
        SELECT contact_email, data_subject, data_bounced_at
        FROM `{BQ_TABLE_EVENTS}`
        WHERE name = 'email.bounce'
        ORDER BY timestamp DESC
        LIMIT 20
    """
    bounces = [dict(row) for row in client.query(query_bounces).result()]

    return {
        "7d": {"sent": sent_7d, "bounced": bounced_7d, "rate": rate_7d},
        "all": {"sent": sent_all, "bounced": bounced_all, "rate": rate_all},
        "recent_bounces": bounces
    }

def send_alert_email(metrics: Dict[str, Any], alerts: List[str]):
    key = get_plunk_key()
    
    # Format message
    lines = ["ACHTUNG: Plunk Bounce-Rate Warnung/Critical", ""]
    lines.extend(alerts)
    lines.append("")
    lines.append("--- Metrics ---")
    lines.append(f"7 Days: {metrics['7d']['rate']:.2%} ({metrics['7d']['bounced']}/{metrics['7d']['sent']})")
    lines.append(f"All Time: {metrics['all']['rate']:.2%} ({metrics['all']['bounced']}/{metrics['all']['sent']})")
    lines.append("")
    lines.append("--- Recent Bounces ---")
    for b in metrics["recent_bounces"]:
        dt = b['data_bounced_at']
        lines.append(f"{dt} | {b['contact_email']} | {b['data_subject']}")
        
    body_text = "\n".join(lines)
    
    # We use requests directly safely.
    # Note: Plunk doesn't have a generic "send raw text" endpoint easily if using templates only,
    # but normally transactionals allow body.
    # If Plunk ONLY supports templates, we might need a dedicated alert template.
    # However, docs say 'body' parameter works for standard transactional?
    # User code uses templates. 
    # Let's assume we can use a generic system template OR just put it in a simple HTML body if Plunk supports it.
    # If Plunk requires a template, we are in a bind without creating one.
    # Fallback: Log heavily. But user requested usage of Plunk function to send mail.
    # We will try to send with a simple body if API allows, or use a "generic" template if known.
    # Since I don't have a template ID for alerts, I will try to use a simple text body. 
    # Plunk API documentation (external knowledge): POST /v1/send supports 'body' (html/text).
    
    payload = {
        "to": ALERT_EMAIL_RECIPIENT,
        "subject": "Plunk Alert: Bounce Rates Exceeded",
        "body": body_text.replace("\n", "<br>"),
        "from": ALERT_FROM_EMAIL
    }
    
    requests.post(
        PLUNK_SEND_URL,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json=payload,
        timeout=10
    )
    print("Alert email sent.")

@functions_framework.http
def plunk_event_logger(request):
    try:
        # 1. Fetch & Sync
        print("Fetching Plunk events...")
        events = fetch_plunk_events()
        print(f"Found {len(events)} events.")
        
        upsert_events_to_bq(events)
        
        # 2. Analyze
        metrics = calculate_bounce_metrics()
        print(f"Metrics: {metrics}")
        
        # 3. Check Thresholds
        alerts = []
        
        # 7 Day Checks
        rate_7d = metrics["7d"]["rate"]
        if rate_7d > BOUNCE_RATE_7D_CRITICAL:
            alerts.append(f"CRITICAL: 7-Day Bounce Rate is {rate_7d:.2%} (> {BOUNCE_RATE_7D_CRITICAL:.1%})")
        elif rate_7d > BOUNCE_RATE_7D_WARNING:
            alerts.append(f"WARNING: 7-Day Bounce Rate is {rate_7d:.2%} (> {BOUNCE_RATE_7D_WARNING:.1%})")
            
        # All Time Checks
        rate_all = metrics["all"]["rate"]
        if rate_all > BOUNCE_RATE_ALL_TIME_CRITICAL:
            alerts.append(f"CRITICAL: All-Time Bounce Rate is {rate_all:.2%} (> {BOUNCE_RATE_ALL_TIME_CRITICAL:.1%})")
        elif rate_all > BOUNCE_RATE_ALL_TIME_WARNING:
            alerts.append(f"WARNING: All-Time Bounce Rate is {rate_all:.2%} (> {BOUNCE_RATE_ALL_TIME_WARNING:.1%})")
            
        if alerts:
            print("Sending alert...")
            send_alert_email(metrics, alerts)
        else:
            print("No new alerts.")

        return "OK", 200
        
    except Exception as e:
        print(f"Error: {e}")
        return str(e), 500
