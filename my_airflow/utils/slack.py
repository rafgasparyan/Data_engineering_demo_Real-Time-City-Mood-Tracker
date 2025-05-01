# my_airflow/utils/slack.py
import os
import requests

def notify_slack_failure(context):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("No Slack webhook configured!")
        return

    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')

    message = (
        f":x: *Task Failed!*\n"
        f"*Task*: `{task_instance.task_id}`\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Execution Time*: `{execution_date}`"
    )

    response = requests.post(webhook_url, json={"text": message})
    if response.status_code != 200:
        print(f"Failed to send Slack notification: {response.text}")
