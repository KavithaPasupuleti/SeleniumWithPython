from datetime import datetime
import json
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
from xpms_storage.utils import get_env

def hma_move_feedback_to_completed(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
            DOMAIN_NAME = get_env("DOMAIN_NAME", "enterprise.xpms.ai", False)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)

            file_path = config["context"]["source_file_path"]
            # file_path = "minio://claims-audit/aclaimsauditpoc/highmark/highmark_inputs_backup/1601454901_hma.csv"
            file_name = file_path.split("/")[-1]
            if objects["status"].lower() == "completed":
                xrm = XpmsResource()
                mr = xrm.get(urn=file_path)
                print(mr)
                backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/feedback-completed".format(AMAZON_AWS_BUCKET)
                backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                backup_rm = XpmsResource()
                backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                if mr.exists():
                    mr.copy(backup_mr)
                    mr.delete()
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