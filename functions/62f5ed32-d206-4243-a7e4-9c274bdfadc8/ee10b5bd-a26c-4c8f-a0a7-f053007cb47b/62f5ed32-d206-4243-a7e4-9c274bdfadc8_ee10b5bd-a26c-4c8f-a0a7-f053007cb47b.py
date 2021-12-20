from datetime import datetime
import numpy as np
from xpms_storage.db_handler import DBProvider
import time
import requests
import json
from xpms_storage.utils import get_env


def hma_feedback_notification_inprogress(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
            DOMAIN_NAME = get_env("DOMAIN_NAME", "enterprise.xpms.ai", False)
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            BE_URL = get_env('CLAIMS_AUDIT_APIS_URL', None, True)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)

            file_name = objects["document"][0]["metadata"]["properties"]["filename"]
            db = DBProvider.get_instance(db_name=ENV_DATABASE)
            notifications = {
                "group": "feedback_status",
                "message": {
                    "body": f'Feedback for {file_name} is started.',
                    "status": "info",
                    "title": file_name,
                    "icon": "started"
                },
                "metadata": {
                    "file_name": file_name,
                    "current_status": "in-progress",
                    "previous_status": "started"
                },
                "created_timestamp": int(time.time())
            }

            s = db.insert(table='notifications', rows=[notifications])
            if s:
                url = f'https://{BE_URL}/send_notification'
                headers = {
                    'Content-Type': 'application/json'
                }

                resp = requests.request("POST", url, headers=headers, data=json.dumps(notifications, default=str))

            return objects
        except Exception as e:
            counter += 1