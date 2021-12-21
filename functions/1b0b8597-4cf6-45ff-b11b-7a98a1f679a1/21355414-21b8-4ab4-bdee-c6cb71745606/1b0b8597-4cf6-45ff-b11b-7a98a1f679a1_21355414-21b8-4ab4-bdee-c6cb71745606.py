import json

from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
from xpms_storage.db_handler import DBProvider
from datetime import datetime
import os
from xpms_storage.utils import get_env

def hma_read_csv_ts(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            number_of_files = json.loads(config['number_of_files'])

            file_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-batches-input-csv-pending".format(AMAZON_AWS_BUCKET)
            n = int(number_of_files)
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
                    backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-input-csv-inprogress_batches".format(AMAZON_AWS_BUCKET)
                    path_list = []
                    for file_name in files_list[:n]:
                        xrm = XpmsResource()
                        mr = xrm.get(urn=file_path + '/' + file_name)
                        backup_urn = backup_path + '/' + file_name
                        backup_rm = XpmsResource()
                        backup_mr = backup_rm.get(urn=backup_urn)
                        mr.copy(backup_mr)
                        mr.delete()
                        path_list.append(backup_urn)
                        filter_ob = {"file_name_chunk": file_name}
                        update_ob = {
                            "status": "processing"
                        }
                        try:
                            db = DBProvider.get_instance(db_name=ENV_DATABASE)
                            s = db.update(table='batch_metadata_chunk', update_obj=update_ob, filter_obj=filter_ob)
                        except Exception as e:
                            return 'e is ' + str(e)

                    return {

                        "file_path": path_list
                    }

            else:
                return {

                    "file_path": "na"
                }
        except Exception as e:
            counter += 1