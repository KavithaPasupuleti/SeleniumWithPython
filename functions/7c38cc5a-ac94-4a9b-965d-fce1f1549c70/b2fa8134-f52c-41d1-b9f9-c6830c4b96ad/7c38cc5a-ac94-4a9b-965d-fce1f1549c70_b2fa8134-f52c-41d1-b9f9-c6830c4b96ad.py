from datetime import datetime
import json
from xpms_file_storage.file_handler import XpmsResource, LocalResource
from xpms_storage.utils import get_env
from xpms_storage.db_handler import DBProvider


def hma_move_to_completed(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
            DOMAIN_NAME = get_env("DOMAIN_NAME", "enterprise.xpms.ai", False)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            file_path = config["context"]["source_file_path"]
            # file_path = "minio://claims-audit/aclaimsauditpoc/highmark/highmark_inputs_backup/1601454901_hma.csv"
            file_name = file_path.split("/")[-1]
            if objects["status"].lower() == "completed":
                xrm = XpmsResource()
                mr = xrm.get(urn=file_path)
                print(mr)
                backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-input-csv-inprogress_batches_completed".format(
                    AMAZON_AWS_BUCKET)
                backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                backup_rm = XpmsResource()
                backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                if mr.exists():
                    mr.copy(backup_mr)
                    mr.delete()
                    filter_ob = {"file_name": file_name}
                    update_ob = {
                        "status": 'completed',
                    }
                    try:
                        db = DBProvider.get_instance(db_name=ENV_DATABASE)
                        s = db.update(table='batch_metadata_chunk', update_obj=update_ob, filter_obj=filter_ob)
                    except Exception as e:
                        return 'e is ' + str(e)
                    return {
                        "status": "moved"
                    }
                else:
                    return {
                        "status": "file not exist"
                    }
            else:
                return {
                    "status": "not moved"
                }
        except Exception as e:
            counter += 1