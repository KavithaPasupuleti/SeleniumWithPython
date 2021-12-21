from xpms_storage.db_handler import DBProvider
from xpms_storage.utils import get_env
import time
import json
import requests
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource


def hma_split_files_status(config=None, **objects):

    ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
    BE_URL = get_env('CLAIMS_AUDIT_APIS_URL', None, True)
    AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
    try:
        db = DBProvider.get_instance(db_name=ENV_DATABASE)

        aggregate = [
            {
                "$match": {
                    "no_of_chunk": {"$exists": True},
                    "status": "to-do"
                }

            },
            {
                "$project": {
                    "batch_name": "$batch_name",
                    "no_of_chunk": "$no_of_chunk",
                    "file_name": "$file_name"
                }
            }
        ]
        batch_metadata = db.find(table='batch_metadata', aggregate=aggregate)
        batch_name_chunk_map = {datum['batch_name']: [datum['no_of_chunk'], datum['file_name']] for datum in
                                batch_metadata}

        aggregate_1 = [
            {"$match": {'batch_name': {"$in": list(batch_name_chunk_map.keys())},
                        "status": "completed"}},
            {
                "$group": {
                    "_id": "$batch_name",
                    "total": {"$sum": 1},
                    "audit_needed": {'$sum': '$audit_needed'},
                    "audit_not_needed": {'$sum': '$audit_not_needed'}
                }
            },
            {
                "$project": {
                    '_id': 0,
                    'batch_name': "$_id",
                    "total": 1,
                    'audit_needed': 1,
                    'audit_not_needed': 1
                }
            }
        ]
        batch_metadata_chunk = db.find(table='batch_metadata_chunk', aggregate=aggregate_1)
        for obj in batch_metadata_chunk:
            batch_name = obj['batch_name']
            file_name = batch_name_chunk_map[batch_name][1]
            if obj['total'] == batch_name_chunk_map[batch_name][0]:
                filter_ob = {'batch_name': batch_name}
                update_ob = {
                    "status": "in-progress",
                    "audit_needed": obj['audit_needed'],
                    "audit_not_needed": obj['audit_not_needed'],
                    "batch_end_date": int(time.time())
                }
                s = db.update(table='batch_metadata', update_obj=update_ob, filter_obj=filter_ob)
                notification = {
                    "group": "batch_status",
                    "message": {
                        "body": f'{batch_name} is in-progress.',
                        "status": "info",
                        "title": batch_name,
                        "icon": "processing"
                    },
                    "metadata": {
                        "batch_name": batch_name,
                        "current_status": "in-progress",
                        "previous_status": "to-do"
                    },
                    "created_timestamp": int(time.time())
                }

                db.insert(table='notifications', rows=[notification])
                file_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-input-csv-inprogress".format(
                    AMAZON_AWS_BUCKET)
                xr = XpmsResource()
                minio_resource = xr.get(urn=file_path)
                if minio_resource.exists():
                    all_files_list = minio_resource.list()
                    files_list = [(path.filename) for path in all_files_list if ".csv" in path.fullpath]
                    if len(files_list) == 0:

                        return {
                            "file_path": "na"
                        }

                    else:
                        backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-batches_completed".format(
                            AMAZON_AWS_BUCKET)
                        for file_name_big in files_list:
                            if file_name == file_name_big:
                                xrm = XpmsResource()
                                mr = xrm.get(urn=file_path + '/' + file_name_big)

                                backup_urn = backup_path + '/' + file_name_big
                                backup_rm = XpmsResource()
                                backup_mr = backup_rm.get(urn=backup_urn)
                                mr.copy(backup_mr)
                                mr.delete()
                celery_batch_url = f"https://{BE_URL}/celery/batch-ingested-calculation"

                payload = {}
                headers = {
                    'Content-Type': 'application/json'
                }

                response = requests.request("GET", celery_batch_url, headers=headers, data=payload)

                print(response.text.encode('utf8'))

                url = f'https://{BE_URL}/send_notification'
                headers = {
                    'Content-Type': 'application/json'
                }

                requests.request("POST", url, headers=headers, data=json.dumps(notification, default=str))

    except Exception as e:
        return 'e is ' + str(e)
