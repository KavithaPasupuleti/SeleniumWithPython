from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
import time
from xpms_storage.db_handler import DBProvider
import uuid
from datetime import datetime
import json
import requests
from xpms_storage.utils import get_env


def hma_metadata_chunk(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            file_name = objects["document"][0]["metadata"]["properties"]["filename"]
            start_time = int(time.time())
            converted_start_time = datetime.utcfromtimestamp(start_time)
            input_source = objects["document"][0]["metadata"]["properties"]["extension"]
            status = "processing"
            try:
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                aggregate = [
                    {
                        "$match": {
                            "threshold": {"$exists": True},
                            "status": "processing",
                            "file_name_chunk": file_name
                        }

                    },
                    {
                        "$project": {
                            "batch_name": "$batch_name",
                            "threshold": "$threshold"

                        }
                    }
                ]
                data = db.find(table='batch_metadata_chunk', aggregate=aggregate)
                threshold = data[0]['threshold']
                batch_name = data[0]['batch_name']

            except Exception as e:
                return 'e is ' + str(e)
            config['context']['threshold'] = threshold
            config["context"]["batch_name"] = batch_name
            config["context"]["start_time"] = start_time
            config["context"]["input_file_name"] = objects["document"][0]["metadata"]["properties"]["filename"]

            update_ob = {
                "input_source": input_source,
                "audit_needed": None,
                "audit_not_needed": None,
                "batch_start_date": start_time,
                "converted_start_time": converted_start_time,
                "status": status,
                "file_name": objects["document"][0]["metadata"]["properties"]["filename"]
            }
            filter_ob = {"file_name_chunk": file_name}
            try:
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                s = db.update(table='batch_metadata_chunk', update_obj=update_ob, filter_obj=filter_ob)
            except Exception as e:
                return 'e is ' + str(e)

            return objects
        except Exception as e:
            counter += 1
