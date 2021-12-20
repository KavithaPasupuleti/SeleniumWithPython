from datetime import datetime
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
import numpy as np
from xpms_storage.db_handler import DBProvider
import json
from datetime import datetime
import time
import requests
import copy
import pickle
from distutils.util import strtobool
from xpms_storage.utils import get_env


def hma_postprocess_error_model(config=None, **obj):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            batch_name = config['context']['batch_name']
            start_time = int(config['context']['start_time'])
            threshold = float(config['context']['threshold'])
            file_name = config["context"]["cfe_source_file"].split('/')[-1]
            CLAIM_NUMBER = "CLAIM_NUMBER_Mask"
            converted_start_time = datetime.utcfromtimestamp(start_time)
            file_path = config["context"]["cfe_source_file"]
            local_csv_path = "/tmp/cfe_data.csv"

            xr1 = XpmsResource()
            minio_resource = xr1.get(urn=file_path)
            local_res = LocalResource(key=local_csv_path)
            minio_resource.copy(local_res)
            df = pd.read_csv(local_csv_path)
            # Result 1
            result_path = obj['result_path']
            local_csv_path = "/tmp/vmAuditTest.csv"
            xr1 = XpmsResource()
            minio_resource = xr1.get(urn=result_path)
            local_res_1 = LocalResource(key=local_csv_path)
            minio_resource.copy(local_res_1)
            df1 = pd.read_csv(local_csv_path)
            df1 = df1.apply(lambda x: (round(x * 100, 2)) / 100)

            final_df = pd.concat([df, df1.drop(df1.columns[0], axis=1)], axis=1)

            labels = {
                '0': 'Coding Error', '1': 'Frequency - Claim Error',
                '2': 'Frequency - Money Claim Error', '3': 'Internal Error'
            }
            claim_error_columns = ['Coding Error', 'Frequency - Claim Error', 'Frequency - Money Claim Error', 'Internal Error']

            final_df.rename(columns=labels, inplace=True)
            final_df['Error Result'] = final_df[claim_error_columns].idxmax(axis=1)
            agg_df = final_df.copy()
            agg_df['error_bucket'] = agg_df[claim_error_columns].to_dict(orient='records')
            claim_ids = df[CLAIM_NUMBER].unique().tolist()
            try:
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                is_claim_present = db.find(table="claims_data", filter_obj={"data.CLAIM NUMBER": {'$in': claim_ids}})
                is_line_level_claim_present = db.find(table="line_level_claims_data",
                                                      filter_obj={"data.CLAIM_NUMBER_Mask": {'$in': claim_ids}})

                for claim in is_claim_present:
                    claim["data"]["Error Result"] = \
                    agg_df[agg_df[CLAIM_NUMBER] == claim["data"]["CLAIM NUMBER"]]["Error Result"].iloc[0]
                    claim["data"]["error_bucket"] = \
                    agg_df[agg_df[CLAIM_NUMBER] == claim["data"]["CLAIM NUMBER"]]['error_bucket'].iloc[0]
                    # pass

                for claim in is_line_level_claim_present:
                    claim["data"]["Error Result"] = \
                    final_df[final_df[CLAIM_NUMBER] == claim["data"]['CLAIM_NUMBER_Mask']]['Error Result'].iloc[0]

                r1 = db.delete(table='claims_data', filter_obj={"data.CLAIM NUMBER": {'$in': claim_ids}})
                r2 = db.delete(table='line_level_claims_data', filter_obj={"data.CLAIM_NUMBER_Mask": {'$in': claim_ids}})

                if r1 and r2:
                    s1 = db.insert(table='claims_data', rows=is_claim_present)
                    s3 = db.insert(table='line_level_claims_data', rows=is_line_level_claim_present)

                return {
                    "status": "completed",
                    "claims_db_inserted": s1,
                    "line_level_claims_data_inserted": s3,
                    # "notification_resp": resp.text,
                }

            except Exception as e:
                return {
                    "status": "failed",
                    "claims_db_inserted": False,
                    "line_level_claims_data_inserted": False,
                    "error_message": str(e)
                }
        except Exception as e:
            counter += 1
        finally:
            local_res.delete()
            local_res_1.delete()