from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
import time
import json
import requests
from distutils.util import strtobool
from datetime import datetime
import os
from xpms_storage.utils import get_env
from xpms_storage.db_handler import DBProvider

def hma_check_feedback_file_exists(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            BE_URL = get_env('CLAIMS_AUDIT_APIS_URL', None, True)

            file_path = "minio://{0}/claimsaudit-ingestfiles/feedback-inputs".format(AMAZON_AWS_BUCKET)

            def generate_notification(body, group='batch_status', status='failed', title='File Ingestion',
                                      icon='failure'):
                notification = {
                    "group": group,
                    "message": {
                        "body": body,
                        "status": status,
                        "title": title,
                        "icon": icon
                    },
                    "created_timestamp": int(time.time())
                }
                return notification

            def send_notification_UI(notification):
                url = f'https://{BE_URL}/send_notification'
                headers = {
                    'Content-Type': 'application/json'
                }
                resp = requests.request("POST", url, headers=headers,
                                        verify=bool(strtobool(get_env("SSL_VERIFY", "False", False))),
                                        data=json.dumps(notification, default=str))
                return resp

            xr = XpmsResource()
            minio_resource = xr.get(urn=file_path)
            if minio_resource.exists():
                all_files_list = minio_resource.list()
                invalid_list = [(path.filename) for path in all_files_list if
                                (".csv" not in path.fullpath and path.filename != "README.md")]
                if (len(invalid_list)):
                    file_name = invalid_list[0]
                    xrm = XpmsResource()
                    mr = xrm.get(urn=file_path + '/' + file_name)
                    notification = generate_notification(body="Error in the ingested feedback File. Only csv files are accepted")
                    db = DBProvider.get_instance(db_name=ENV_DATABASE)
                    s = db.insert(table='notifications', rows=[notification])
                    if s:
                        send_notification_UI(notification)
                        backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/feedback-completed".format(
                            AMAZON_AWS_BUCKET)
                        backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                        backup_rm = XpmsResource()
                        backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                        if mr.exists():
                            mr.copy(backup_mr)
                            mr.delete()
                        raise AssertionError("Invalid input(feedback) File Provided.")
                files_list = [(path.filename) for path in all_files_list if ".csv" in path.fullpath]
                if len(files_list) == 0:

                    return {
                        "file_path": "na"
                    }
                # elif len(files_list) > 1:
                #     return {
                #         'message': 'More than one file present in ' + file_path
                #     }
                else:
                    file_name = files_list[0]
                    local_path = '/tmp/local_' + file_name
                    lr = LocalResource(key=local_path)
                    xrm = XpmsResource()
                    mr = xrm.get(urn=file_path + '/' + file_name)
                    mr.copy(lr)
                    backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/feedback-inprogress".format(AMAZON_AWS_BUCKET)

                    backup_filename = file_name

                    filename, file_extension = os.path.splitext(file_name)

                    backup_rm = XpmsResource()
                    backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                    mr.copy(backup_mr)
                    mr.delete()
                    return {

                        "file_path": backup_path + '/' + backup_filename

                    }

            else:
                return {

                    "file_path": "na"
                }
        except Exception as e:
            counter += 1