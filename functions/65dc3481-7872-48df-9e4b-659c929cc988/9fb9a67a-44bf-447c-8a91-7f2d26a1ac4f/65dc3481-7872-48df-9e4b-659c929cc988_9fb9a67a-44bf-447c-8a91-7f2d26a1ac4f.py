from datetime import datetime
import pandas as pd
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import json
import numpy as np
from sklearn.preprocessing import StandardScaler
import pickle
from pandas.core.common import flatten
import numpy as np
from xpms_storage.utils import get_env


def hma_preprocess(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            flag_columns = config['context']['flag_columns']
            excess_columns = config['context']['excess_columns']
            encode_columns = config['context']['encode_columns']
            left_out_cols = config["context"]["left_out_cols"]
            cols_span = config['context']['cols_span']
            ADJU_EXEC_CD_LIST = config['context']['ADJU_EXEC_CD_LIST']
            CLM_SAC_CD_LIST = config['context']['CLM_SAC_CD_LIST']
            AGD_CODE_LIST = config['context']['AGD_CODE_LIST']
            NSIR_CD_LIST = config['context']['NSIR_CD_LIST']

            df_lst = objects["data"]
            headers = df_lst.pop(0)
            df = pd.DataFrame(df_lst, columns=headers)

            df.drop(excess_columns, axis=1, inplace=True)
            df = df[df['CLAIM_NUMBER_Mask'].notna()]

            flag = []
            for item in flag_columns:
                df['{}_flag'.format(item)] = np.where(df[item].notna(), 1, 0)
                flag.append('{}_flag'.format(item))
                df = df.drop(columns=[item], axis=1)

            ADJU_EXC_CD_DF = pd.DataFrame(columns=['ADJU_EXC_CD_' + str(code) for code in ADJU_EXEC_CD_LIST])
            for code in ADJU_EXEC_CD_LIST:
                mask = df[cols_span["ADJU_EXC_CD"]].isin([code]).any(axis=1)
                ADJU_EXC_CD_DF['ADJU_EXC_CD_' + str(code)] = [1 if code else 0 for code in mask.values]

            CLM_SAC_CD_DF = pd.DataFrame(columns=['CLM_SAC_CD_' + str(code) for code in sorted(CLM_SAC_CD_LIST)])

            for code in CLM_SAC_CD_LIST:
                mask = df[cols_span["CLM_SAC_CD"]].isin([code]).any(
                    axis=1)
                CLM_SAC_CD_DF['CLM_SAC_CD_' + str(code)] = [1 if code else 0 for code in mask.values]

            AGD_CODE_DF = pd.DataFrame(columns=['AGD_CODE_' + str(code) for code in sorted(AGD_CODE_LIST)])
            for code in AGD_CODE_LIST:
                mask = df[cols_span["AGD_CODE"]].isin(
                    [code]).any(axis=1)
                AGD_CODE_DF['AGD_CODE_' + str(code)] = [1 if code else 0 for code in mask.values]

            NSIR_CD_DF = pd.DataFrame(columns=['NSIR_CD_' + str(code) for code in NSIR_CD_LIST])
            for code in NSIR_CD_LIST:
                mask = df[cols_span["NSIR_CD"]].isin([code]).any(axis=1)
                NSIR_CD_DF['NSIR_CD_' + str(code)] = [1 if code else 0 for code in mask.values]

            ohe_dfs = pd.concat([ADJU_EXC_CD_DF, CLM_SAC_CD_DF, AGD_CODE_DF, NSIR_CD_DF], axis=1)

            cols_span_list = list(flatten(cols_span.values()))
            df.drop(cols_span_list, axis=1, inplace=True)

            df['AFV_BGN_02_DATE'] = pd.to_datetime(df['AFV_BGN_02_DATE'])
            df['HSCBMP_SVCE_END_DT'] = pd.to_datetime(df['HSCBMP_SVCE_END_DT'])
            df['FINAL_DATE'] = pd.to_datetime(df['FINAL_DATE'])
            df['DAYS_SERVICED'] = df['HSCBMP_SVCE_END_DT'] - df['AFV_BGN_02_DATE']
            df['FINAL_AFTER'] = df['FINAL_DATE'] - df['HSCBMP_SVCE_END_DT']
            df.drop(['AFV_BGN_02_DATE', 'HSCBMP_SVCE_END_DT', 'FINAL_DATE'], axis=1, inplace=True)
            df['DAYS_SERVICED'] = df['DAYS_SERVICED'].astype('int64')
            df['FINAL_AFTER'] = df['FINAL_AFTER'].astype('int64')

            enc_df = df[encode_columns]
            ip_df = df[left_out_cols]
            flag_df = df[flag]

            enc_df.fillna("unknown", inplace=True)

            file_path = "minio://{}/label_encoder/label_encoder.pkl".format(AMAZON_AWS_BUCKET)
            local_pkl_path = "/tmp/scaler.pkl"
            minio_resource = XpmsResource.get(urn=file_path)
            local_res = LocalResource(key=local_pkl_path)
            minio_resource.copy(local_res)

            loaded_label_encoder = pickle.load(open(local_pkl_path, "rb"))
            enc_df_transformed = loaded_label_encoder.transform(enc_df)

            final_df = pd.concat([ip_df, enc_df_transformed, ohe_dfs, flag_df], axis=1)
            final_df.drop('CLAIM_NUMBER_Mask', axis=1, inplace=True)

            print("final shape:-", final_df.shape)

            file_name = config['context']['input_file_name']
            csv_minio_urn = "minio://{}/ml_input/mapped_".format(AMAZON_AWS_BUCKET) + file_name
            local_csv_path = "/tmp/mapped_" + file_name
            minio_resource = XpmsResource.get(urn=csv_minio_urn)
            final_df.to_csv(local_csv_path, index=False)
            local_res_1 = LocalResource(key=local_csv_path)
            local_res_1.copy(minio_resource)
            return {
                "dataset": {
                    "data_format": "csv",
                    "value": csv_minio_urn
                }
            }
        except Exception as e:
            counter += 1
        finally:
            local_res.delete()
            local_res_1.delete()
