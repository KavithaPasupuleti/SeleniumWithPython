from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
import time
import requests
import json
from xpms_storage.db_handler import DBProvider
from datetime import datetime
from xpms_storage.utils import get_env


def hma_preprocess_feedback(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
            DOMAIN_NAME = get_env("DOMAIN_NAME", "enterprise.xpms.ai", False)
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            BE_URL = get_env('CLAIMS_AUDIT_APIS_URL', None, True)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)

            config["context"]["source_file_path"] = objects["document"][0]["metadata"]["properties"]["file_metadata"][
                "file_path"]
            config["context"]["filename"] = objects["document"][0]["metadata"]["properties"]["filename"]

            file_path = objects["document"][0]["metadata"]["properties"]["file_metadata"]["file_path"]
            local_csv_path = "/tmp/" + objects["document"][0]["metadata"]["properties"]["filename"]
            minio_resource = XpmsResource.get(urn=file_path)
            local_res = LocalResource(key=local_csv_path)
            minio_resource.copy(local_res)

            dataset = pd.read_csv(local_csv_path)
            dataset.fillna("NA", inplace=True)
            claim_ids = dataset["claim_id"].tolist()
            audit_time = int(time.time())
            converted_audit_submitted_date = datetime.utcfromtimestamp(audit_time)

            db = DBProvider.get_instance(db_name=ENV_DATABASE)
            is_claim_present = db.find(table="claims_data", filter_obj={"data.CLAIM NUMBER": {'$in': claim_ids}})
            is_line_level_claim_present = db.find(table="line_level_claims_data",
                                                  filter_obj={"data.CLAIM_NUMBER_Mask": {'$in': claim_ids}})

            for claim in is_claim_present:
                # print(claim["data"]["CLAIM_NUMBER_Mask"])
                claim["manual_audit"] = {
                    'manual_audit_result':
                        dataset.loc[dataset['claim_id'] == claim["data"]["CLAIM NUMBER"], 'manual_audit_result'].iloc[0],
                    'manual_audit_error_bucket':
                        dataset.loc[dataset['claim_id'] == claim["data"]["CLAIM NUMBER"], 'manual_audit_error_bucket'].iloc[0],
                    'audit_submitted_date': audit_time,
                    'converted_audit_submitted_date': converted_audit_submitted_date
                }
                claim["batch_status"] = "in-progress"

            for claim in is_line_level_claim_present:
                # print(claim["data"]["CLAIM_NUMBER_Mask"])
                claim["manual_audit"] = {
                    'manual_audit_result':
                        dataset.loc[dataset['claim_id'] == claim["data"]["CLAIM_NUMBER_Mask"], 'manual_audit_result'].iloc[0],
                    'manual_audit_error_bucket':
                        dataset.loc[
                            dataset['claim_id'] == claim["data"]["CLAIM_NUMBER_Mask"], 'manual_audit_error_bucket'].iloc[0],
                    'audit_submitted_date': audit_time,
                    'converted_audit_submitted_date': converted_audit_submitted_date
                }
                claim["batch_status"] = "in-progress"

            r1 = db.delete(table='claims_data', filter_obj={"data.CLAIM NUMBER": {'$in': claim_ids}})
            r2 = db.delete(table='line_level_claims_data', filter_obj={"data.CLAIM_NUMBER_Mask": {'$in': claim_ids}})

            if all([r1, r2, is_claim_present, is_line_level_claim_present]):
                s1 = db.insert(table='claims_data', rows=is_claim_present)
                s2 = db.insert(table='line_level_claims_data', rows=is_line_level_claim_present)

            agg = [
                {"$match": {"batch_status": {'$ne': 'completed'}}},
                {"$group": {'_id': "$batch_name",
                            'count': {'$sum': {'$cond': [{"$ifNull": ['$manual_audit', False]}, 0, 1]}}}},
                {"$project": {'batch_name': 1, 'count': 1}}
            ]
            completed_time = int(time.time())
            batch_count = db.find(table="claims_data", aggregate=agg)
            completed_batches = [item['_id'] for item in batch_count if item['count'] == 0]
            if len(completed_batches) > 0:
                s3 = db.update(table='batch_metadata',
                               update_obj={'$set': {'status': 'completed', 'batch_completed_date': completed_time}},
                               filter_obj={'batch_name': {'$in': completed_batches}})
                s4 = db.update(table='claims_data', update_obj={'$set': {'batch_status': 'completed'}},
                               filter_obj={'batch_name': {'$in': completed_batches}})
                s5 = db.update(table='line_level_claims_data', update_obj={'$set': {'batch_status': 'completed'}},
                               filter_obj={'batch_name': {'$in': completed_batches}})

                #         notifications = [{
                #                 "group": "batch_status",
                #                 "message": {
                #                     "batch_name": batch_name,
                #                     "current_status": "completed",
                #                     "previous_status": "in-progress"
                #                 },
                #                 "created_timestamp":int(time.time())
                #             } for batch_name in completed_batches]
                #         s = db.insert(table='notifications', rows=notifications)

                for batch_name in completed_batches:
                    notification = {
                        "group": "batch_status",
                        "message": {
                            "body": f'Feedback for {batch_name} is completed.',
                            "status": "success",
                            "title": batch_name,
                            "icon": "completed"
                        },
                        "metadata": {
                            "batch_name": batch_name,
                            "current_status": "completed",
                            "previous_status": "in-progress"
                        },

                        "created_timestamp": int(time.time())
                    }

                    s = db.insert(table='notifications', rows=[notification])

                    url = f'https://{BE_URL}/send_notification'
                    headers = {
                        'Content-Type': 'application/json'
                    }

                    resp = requests.request("POST", url, headers=headers, data=json.dumps(notification, default=str))

                celery_feedback_url = f"https://{BE_URL}/celery/feedback-ingested-calculation"
                payload = {}
                headers = {
                    'Content-Type': 'application/json'
                }
                response = requests.request("GET", celery_feedback_url, headers=headers, data=payload)
                print(response.text.encode('utf8'))
                file_name = objects["document"][0]["metadata"]["properties"]["filename"]
                feedback_notifications = {
                    "group": "feedback_status",
                    "message": {
                        "body": f'Feedback for {file_name} is completed.',
                        "status": "success",
                        "title": file_name,
                        "icon": "completed"
                    },
                    'metadata': {
                        "file_name": file_name,
                        "current_status": "completed",
                        "previous_status": "in-progress"
                    },
                    "created_timestamp": int(time.time())
                }

                ss = db.insert(table='notifications', rows=[feedback_notifications])
                send_notification_url = f'https://{BE_URL}/send_notification'
                headers = {
                    'Content-Type': 'application/json'
                }

                resp = requests.request("POST", send_notification_url, headers=headers,
                                        data=json.dumps(feedback_notifications, default=str))

                return {
                    "status": "completed",
                    "batch_metadata_response": s3,
                    "claims_data_response": s1 and s4,
                    "line_level response": s2 and s5,
                    "notification_response": resp.text
                }
            else:

                # celery_feedback_url = f"https://{BE_URL}/celery/feedback-ingested-calculation"
                # payload = {}
                # headers = {
                #     'Content-Type': 'application/json'
                # }
                # response = requests.request("GET", celery_feedback_url, headers=headers, data=payload)
                # print(response.text.encode('utf8'))
                filename = objects["document"][0]["metadata"]["properties"]["filename"]
                feedback_notifications = {
                    "group": "feedback_status",
                    "message": {
                        "body": f'Feedback for {filename} is failed because of respective batch not found',
                        "status": "success",
                        "title": filename,
                        "icon": "completed"
                    },
                    "metadata": {
                        "file_name": filename,
                        "current_status": "completed",
                        "previous_status": "in-progress"
                    },
                    "created_timestamp": int(time.time())
                }

                ss = db.insert(table='notifications', rows=[feedback_notifications])
                send_notification_url = f'https://{BE_URL}/send_notification'
                headers = {
                    'Content-Type': 'application/json'
                }

                resp = requests.request("POST", send_notification_url, headers=headers,
                                        data=json.dumps(feedback_notifications, default=str))

                return {
                    "status": "failed",
                    "batch_metadata_response": False,
                    "notification_response": resp.text
                }
        except Exception as e:
            counter += 1
        finally:
            local_res.delete()