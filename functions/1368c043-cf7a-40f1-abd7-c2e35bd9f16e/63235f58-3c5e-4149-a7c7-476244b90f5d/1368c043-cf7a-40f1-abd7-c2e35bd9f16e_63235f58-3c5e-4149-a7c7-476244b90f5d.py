from datetime import datetime
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
import numpy as np
from xpms_storage.db_handler import DBProvider
import json
from datetime import datetime
import time
import requests
from xpms_storage.utils import get_env


def hma_postprocess_audit_model(config=None, **obj):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            batch_name = config['context']['batch_name']
            start_time = int(config['context']['start_time'])
            threshold = float(config['context']['threshold'])
            file_name = config['context']['input_file_name']
            converted_start_time = datetime.utcfromtimestamp(start_time)
            file_path = config["context"]["source_file_path"]
            local_csv_path = config["context"]["local_source_file_path"]
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
            df1.rename(columns={"0": "CFE", "1": "CAF"}, inplace=True)

            final_df = pd.concat([df, df1.drop(df1.columns[0], axis=1)], axis=1)
            aggregated_df = final_df.groupby('CLAIM_NUMBER_Mask').agg(
                lambda x: list(x) if len(set(x)) == 1 else list(x)).replace(
                'nan', '').reset_index()

            lcols = ['CAF', 'CFE']
            aggregated_df['index_choice'] = aggregated_df['CFE'].apply(lambda x: x.index(max(x)))
            for col in lcols:
                aggregated_df[col + '_confidence'] = aggregated_df.apply(lambda row: row[col][row['index_choice']],
                                                                         axis=1)
            rename_cols = {"CAF": "aggregated_caf",
                           "CFE": "aggregated_cfe",

                           "CAF_confidence": "CAF",
                           "CFE_confidence": "CFE",
                           }
            aggregated_df.rename(columns=rename_cols, inplace=True)

            aggregated_df["AFV_PRV_CRG_AMT_1"] = [sum(x) for x in aggregated_df["AFV_PRV_CRG_AMT_1"]]

            aggregated_df["Audit Result"] = aggregated_df["CAF"].apply(
                lambda x: "CAF" if x >= float(threshold) / 100 else "CFE")

            aggregated_df["system_recommended_result"] = aggregated_df[["CAF", "CFE"]].to_dict(orient='records')

            final_df["Audit Result"] = final_df['CLAIM_NUMBER_Mask'].map(
                aggregated_df.set_index('CLAIM_NUMBER_Mask')['Audit Result'])

            final_df["index_choice"] = final_df['CLAIM_NUMBER_Mask'].map(
                aggregated_df.set_index('CLAIM_NUMBER_Mask')['index_choice'])

            aggregated_df_copy = aggregated_df.copy(deep=True)
            final_df_copy = final_df.copy(deep=True)

            rename_c = {
                "CLAIM_NUMBER_Mask": "CLAIM NUMBER",
                "AFV_PRV_CRG_AMT_1": "TOT PROV CHARGE",
            }
            aggregated_df_copy.rename(columns=rename_c, inplace=True)
            # final_df_copy.rename(columns=rename_c,inplace=True)

            aggregated_obj = json.loads(aggregated_df_copy.to_json(orient='records'))
            line_level_obj = json.loads(final_df.to_json(orient='records'))

            clean_ob_lst = [
                {'batch_name': batch_name, 'data': item, 'start_time': start_time,
                 "converted_start_time": converted_start_time,
                 'threshold': float(threshold / 100),
                 'file_name': file_name, 'flag': 'untrained'} for item in aggregated_obj]

            line_level_lst = [
                {'batch_name': batch_name, 'data': item, 'start_time': start_time,
                 "converted_start_time": converted_start_time,
                 'threshold': float(threshold / 100),
                 'file_name': file_name, 'flag': 'untrained'} for item in line_level_obj]

            try:
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                s1 = db.insert(table='claims_data', rows=clean_ob_lst)
                s2 = db.insert(table='line_level_claims_data', rows=line_level_lst)
                filter_ob = {'file_name_chunk': file_name}
                update_ob = {
                    "$set": {
                        'audit_not_needed': aggregated_df_copy[aggregated_df_copy['CAF'] >= threshold / 100].shape[0],
                        'audit_needed': aggregated_df_copy[aggregated_df_copy['CAF'] < threshold / 100].shape[0],
                        "status": "in-progress"}}

                s3 = db.update(table='batch_metadata_chunk', update_obj=update_ob, filter_obj=filter_ob)

                if aggregated_df_copy[aggregated_df_copy['CAF'] < threshold / 100].shape[0] == 0:
                    return {
                        "status": "completed",
                        "claims_db_inserted": s1,
                        "line_level_claims_data_inserted": s3,
                        "batch_metadata_updated": s2,
                        "dataset": {
                            "data_format": "csv",
                            "value": "na"
                        }
                    }
                else:
                    cfe_df = final_df[final_df["Audit Result"] == "CFE"]
                    file_name = file_path.split('/')[-1]
                    csv_minio_urn = "minio://{}/ml_input/cfe_".format(AMAZON_AWS_BUCKET) + file_name
                    local_csv_path = "/tmp/cfe_" + file_name
                    minio_resource = XpmsResource.get(urn=csv_minio_urn)
                    cfe_df.to_csv(local_csv_path, index=False)
                    local_res_2 = LocalResource(key=local_csv_path)
                    local_res_2.copy(minio_resource)
                    config["context"]["cfe_source_file"] = csv_minio_urn
                    local_res_2.delete()
                    return {
                        "dataset": {
                            "data_format": "csv",
                            "value": csv_minio_urn
                        }
                    }

            except Exception as e:
                return {
                    "status": "failed",
                    "claims_db_inserted": False,
                    "line_level_claims_data_inserted": False,
                    "batch_metadata_updated": False,
                    "error_message": str(e)
                }
        except Exception as e:
            counter += 1
        finally:
            local_res.delete()
            local_res_1.delete()
