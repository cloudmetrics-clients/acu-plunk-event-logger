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
ALERT_EMAIL_RECIPIENTS = os.environ.get("ALERT_EMAIL_RECIPIENTS", "florian.buettner@cloudmetrics.de").split(",")
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
        
        row = bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter("id", "STRING", e.get("id")),
            bigquery.ScalarQueryParameter("name", "STRING", e.get("name")),
            bigquery.ScalarQueryParameter("project_id", "STRING", e.get("projectId")),
            bigquery.ScalarQueryParameter("contact_id", "STRING", e.get("contactId")),
            bigquery.ScalarQueryParameter("email_id", "STRING", e.get("emailId")),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", e.get("createdAt")),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", e.get("createdAt")),
            bigquery.ScalarQueryParameter("data_from", "STRING", data_obj.get("from")),
            bigquery.ScalarQueryParameter("data_subject", "STRING", data_obj.get("subject")),
            bigquery.ScalarQueryParameter("data_template_id", "STRING", data_obj.get("templateId")),
            bigquery.ScalarQueryParameter("data_message_id", "STRING", data_obj.get("messageId")),
            bigquery.ScalarQueryParameter("data_bounced_at", "TIMESTAMP", data_obj.get("bouncedAt")),
            bigquery.ScalarQueryParameter("data_bounce_type", "STRING", data_obj.get("bounceType")),
            bigquery.ScalarQueryParameter("contact_email", "STRING", contact_obj.get("email")),
            bigquery.ScalarQueryParameter("raw_json", "STRING", json.dumps(e)),
        )
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
                bigquery.StructQueryParameterType([
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
                ]),
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

    # Recent 16 minutes check (User request: Always alert if any bounce in last 16 mins)
    query_16m = f"""
        SELECT COUNT(DISTINCT email_id) as cnt
        FROM `{BQ_TABLE_EVENTS}`
        WHERE name = 'email.bounce'
          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 16 MINUTE)
    """
    row_16m = list(client.query(query_16m).result())[0]
    bounced_16m = row_16m["cnt"] or 0

    return {
        "7d": {"sent": sent_7d, "bounced": bounced_7d, "rate": rate_7d},
        "all": {"sent": sent_all, "bounced": bounced_all, "rate": rate_all},
        "recent_bounces": bounces,
        "recent_bounces_16m_count": bounced_16m
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
    lines.append(f"Recent 16min Bounces: {metrics['recent_bounces_16m_count']}")
    lines.append("")
    lines.append("--- Recent Bounces ---")
    for b in metrics["recent_bounces"]:
        dt = b['data_bounced_at']
        lines.append(f"{dt} | {b['contact_email']} | {b['data_subject']}")
        
    body_text = "\n".join(lines)
    
    # Loop over recipients
    for recipient in ALERT_EMAIL_RECIPIENTS:
        recipient = recipient.strip()
        if not recipient:
            continue
            
        payload = {
            "to": recipient,
            "subject": "Plunk Alert: Bounce Rates Exceeded",
            "body": body_text.replace("\n", "<br>"),
            "from": ALERT_FROM_EMAIL
        }
        
        try:
            requests.post(
                PLUNK_SEND_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=10
            )
            print(f"Alert email sent to {recipient}.")
        except Exception as e:
            print(f"Failed to send alert to {recipient}: {e}")

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
            
        # Recent Bounces Check (16m)
        if metrics.get("recent_bounces_16m_count", 0) > 0:
            alerts.append(f"INFO: {metrics['recent_bounces_16m_count']} bounce(s) detected in last 16 minutes.")

        if alerts:
            print("Sending alert...")
            send_alert_email(metrics, alerts)
        else:
            print("No new alerts.")

        return "OK", 200
        
    except Exception as e:
        print(f"Error: {e}")
        return str(e), 500
