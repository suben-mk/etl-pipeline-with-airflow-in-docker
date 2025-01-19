import os
import pandas as pd
from slack_sdk.webhook import WebhookClient

slack_webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
webhook = WebhookClient(slack_webhook_url)

def send_success_notify(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    execution_date_local = pd.Timestamp(execution_date).astimezone(tz='Asia/Bangkok')
    
    response = webhook.send(
        text="fallback",
        blocks=[
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"""
                                *DAG ID:* {dag_id}\n*Task ID:* {task_id}\n*Notification*: ✅ Dag Success\n*Execution Date:* {execution_date_local}
                            """
                }
            },        
            {
                "type": "divider"
            },
        ]
    )

def send_failed_notify(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    execution_date_local = pd.Timestamp(execution_date).astimezone(tz='Asia/Bangkok')
    log_url = context.get('task_instance').log_url

    response = webhook.send(
        text="fallback",
        blocks=[
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"""
                                *DAG ID:* {dag_id}\n*Task ID:* {task_id}\n*Notification*: ❌ Dag Validation Failed\n*Execution Date:* {execution_date_local}\n*Log Url:* {log_url}
                            """
                }
            },        
            {
                "type": "divider"
            },
        ]
    )