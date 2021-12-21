from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
import time
from xpms_storage.db_handler import DBProvider
import uuid
from datetime import datetime
import json
import requests
from xpms_storage.utils import get_env


def split_hma_metadata(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            BE_URL = get_env('CLAIMS_AUDIT_APIS_URL', None, True)

            file_path_list, file_name_list = [], []
            for datum in objects["document"]:
                file_paths = datum["metadata"]["properties"]["file_metadata"]["file_path"]
                file_names = datum["metadata"]["properties"]["filename"]
                file_path_list.append(file_paths)
                file_name_list.append(file_names)

            for file_path, filename in zip(file_path_list, file_name_list):
                start_time = int(time.time())
                start_time_1 = int(datetime.today().strftime('%Y%m%d%H%M%S'))
                batch_name = "{0}_{1}".format(filename, start_time_1)
                converted_start_time = datetime.utcfromtimestamp(start_time)
                input_source = objects["document"][0]["metadata"]["properties"]["extension"]
                status = "to-do"

                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                try:
                    data = db.find(table='global_settings')
                    threshold = data[0]['confidence_score']
                    config['context']['threshold'] = threshold

                except :
                    threshold = 50
                    config['context']['threshold'] = threshold

                config["context"]["batch_name"] = batch_name
                config["context"]["start_time"] = start_time
                config["context"]["input_file_name"] = objects["document"][0]["metadata"]["properties"]["filename"]

                batch_ob = {
                    "batch_name": batch_name,
                    "input_source": input_source,
                    "audit_needed": None,
                    "audit_not_needed": None,
                    "batch_start_date": start_time,
                    "converted_start_time": converted_start_time,
                    "status": status,
                    "threshold": threshold,
                    "file_name": objects["document"][0]["metadata"]["properties"]["filename"]
                }

                try:
                    db = DBProvider.get_instance(db_name=ENV_DATABASE)
                    s = db.insert(table='batch_metadata', rows=batch_ob)
                except Exception as e:
                    return 'e is ' + str(e)

                try:
                    notification = {
                        "group": "batch_status",
                        "message": {
                            "body": f'{batch_name} is started.',
                            "status": "info",
                            "title": batch_name,
                            "icon": "started"
                        },
                        "metadata": {
                            "batch_name": batch_name,
                            "current_status": "to-do",
                            "previous_status": "started"
                        },
                        "created_timestamp": start_time
                    }
                    db = DBProvider.get_instance(db_name=ENV_DATABASE)
                    s = db.insert(table='notifications', rows=[notification])

                    if s:
                        url = f'https://{BE_URL}/send_notification'
                        headers = {
                            'Content-Type': 'application/json'
                        }

                        resp = requests.request("POST", url, headers=headers, data=json.dumps(notification, default=str))

                except Exception as e:
                    return "error is " + str(e)

            return objects
        except Exception as e:
            counter += 1