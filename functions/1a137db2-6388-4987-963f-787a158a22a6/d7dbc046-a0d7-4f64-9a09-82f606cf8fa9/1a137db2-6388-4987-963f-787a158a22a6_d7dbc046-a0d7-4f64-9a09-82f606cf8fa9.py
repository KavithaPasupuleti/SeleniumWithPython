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
from xpms_storage.utils import get_env


def hma_preprocess_error_model(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            file_path = objects["dataset"]["value"]
            local_csv_path = "/tmp/cfe_df"
            xr1 = XpmsResource()
            minio_resource = xr1.get(urn=file_path)
            local_res = LocalResource(key=local_csv_path)
            minio_resource.copy(local_res)
            df = pd.read_csv(local_csv_path)
            useless_columns = ['ADJU_EXC_CD_07', 'ADJU_EXC_CD_08', 'ADJU_EXC_CD_09', 'ADJU_EXC_CD_10', 'CLM_SAC_CD_6',
                               'AGD_CODE_7',
                               'PBSC_SAE_CD_6', 'EOB_PRINT_CD', 'AB7_FEE_PAID_IND', 'SBMD_HSCRMPRV_CTY_AD_2',
                               'ABG_RSN_CODE',
                               'PBSC_GP_SPL_CD', 'AAE_CODE_ODO_4', 'AAE_CODE_ODO_5', 'AAE_CODE_ODO_6',
                               'PBSC_MAN_MRG_CD_2',
                               'PBSC_MAN_MRG_CD_1', 'ABG_EMP_ID_Mask', 'CH_INS_MBR_ID_GP_Mask', 'GROUP_NUMBER',
                               'LINE_ITEM_NO',
                               'EAMEE_ID',
                               'LINE_BLIND_KEY', 'ABK_FNL_DATE', 'AFV_BILL_PRV_UVFY_ID_1', 'AFV_BILL_PRV_UVFY_ID_2',
                               'AFV_PRV_CRG_AMT_2',
                               'ABK_PNT_PERS_ID_1', 'CAR_VFY_ID', 'AF4_HIC_ID', 'ENR_BEN_LVL_VFY_ID',
                               'ABL_ATTY_FEE_AMT', 'ABK_PRNL_DLPS_IND', 'PBSC_RPC_CLM_ID', 'SBMD_HSCRMPRV_NM_GP',
                               'ADA_PRS_DATE',
                               'SBMD_HSCRMPRV_ZIP_AD', 'GRP_COL_VFY_ID', 'PBSC_SAE_CD_3', 'AB7_FEE_PAID_AMT_2',
                               'SBMD_HSCRMPRV_STE_AD',
                               'ABK_ENR_SCE_CODE_1', 'ABK_ENR_SCE_CODE_2', 'PAOWNS_CD', 'PBSC_RLS_YR_DT',
                               'PBSC_RLS_DAY_DT',
                               'SPER_NPI_MPEI_ID', 'ANE_ID', 'HSCBMP_SVCE_BGN_DT', 'ABK_PNT_REL_APP_CODE_2',
                               'AFV_BILL_CLM_PRV_ID',
                               'ABK_PNT_PERS_ID_2', 'AFV_PRV_CRG_AMT_2', 'PBSC_SAE_CD_2', 'ABK_FNL_DATE',
                               'PBSC_SAE_CD_5',
                               'ABK_STA_CODE_2',
                               'ABR_RSN_CODE', 'PBSC_SAE_CD_1', 'ABK_VFY_ENR_GRP_ID', 'ABK_STA_CODE_1', 'PBSC_SAE_CD_4',
                               'VFY_PROD_LN_CODE',
                               'PBSC_VFYD_ACTB_CD',
                               ]
            initial_col_list = df.columns.tolist()
            df.drop(useless_columns, axis=1, inplace=True)
            df = df[df['CLAIM_NUMBER_Mask'].notna()]

            flag_cols = [
                'AC1_RSN_CODE', 'PBS_CLM_ORIG_CD', 'PBIPC_OSC_PAST_CD', 'PBSC_CLM_LK_NO', 'ABR_DATE',
                'ITS_DLV_MTH_CD', 'ALL_REJ_RSN_CODE', 'ADO_REF_BY_PRV_ID', 'SBMD_HSCRMPRV_ANCL_AFF_IN',
                'VFY_CTL_PLN_CODE', 'AHR_RATE_MTH_CODE', 'AAA_CODE', 'PBSC_INN_CD', 'INPL_DTA_PGM_CD',
                'BRY_CODE', 'BM4_CODE', 'DF_MSG_CD'
            ]

            flag = []
            for item in flag_cols:
                df['{}_flag'.format(item)] = np.where(df[item].notna(), 1, 0)
                flag.append('{}_flag'.format(item))
                df = df.drop(columns=[item], axis=1)
            df.shape

            encode_cols = [
                "AFV_FNL_STA_CODE_2", "AB7_TYPE_CODE",
                "ABK_PNT_SEX_CODE", "ACKRC_CD", "ALL_GRP_PROD_LINE_CODE",
                "AFV_BCBSA_PL_CODE_1", "AFV_BCBSA_PL_CODE_2", "PBSC_INP_MDM_CD", "AGD_UVFY_CODE",
                "ADB_CODE", "NS_FTP_CD", "ABK_SUB_ENR_CLS_CODE", "PCOWNS_CD",
                "ABE_RCP_CLM_AMT", "PR_ID", "AB7_ID", "AB7_SUB_ASG_BEN_CODE", "AB7_MED_ASG_ACP_CODE",
                "RPA_ID", "PRV_MSG_DTR_CODE", "ANE_TYPE_CODE",
                "PBSC_MEGA_CLM_IN", "AS5_CODE",
                "VFYD_PRDID_PDI_CD",
            ]
            cols_span = [
                "ADJU_EXC_CD_01", "ADJU_EXC_CD_02", "ADJU_EXC_CD_03", "ADJU_EXC_CD_04", "ADJU_EXC_CD_05",
                "ADJU_EXC_CD_06",
                "CLM_SAC_CD_1", "CLM_SAC_CD_2", "CLM_SAC_CD_3", "CLM_SAC_CD_4", "CLM_SAC_CD_5",
                "AGD_CODE_1", "AGD_CODE_2", "AGD_CODE_3", "AGD_CODE_4", "AGD_CODE_5", "AGD_CODE_6",
                "NSIR_CD_1", "NSIR_CD_2", "NSIR_CD_3", "NSIR_CD_4", "NSIR_CD_5",
            ]

            drop_more = [
                "ALL_PRI_IND", "AB7_EOB_CODE", "ABK_HIS_SCE_CODE", "AB7_EOB_CODE", "ABR_TYPE_CODE_1",
                "AFV_FNL_STA_CODE_1", "ABR_TYPE_CODE_2", "AAE_CODE_ODO_1", "AAE_CODE_ODO_2", "AAE_CODE_ODO_3"
            ]

            left_out_cols = [
                'CLAIM_NUMBER_Mask', 'PRN_ACC_VFY_ID_Mask', 'VFYD_CL_N_Mask', 'ACC_VFY_ID_Mask', 'ENR_SRC_CODE',
                'ABK_PNT_REL_APP_CODE_1', 'AFV_PRV_CRG_AMT_1', 'ABK_TYPE_CODE', 'ABK_AUTM_CLM_SCE_ID',
                'PBSC_GVN_ETY_CD', 'VFYD_BPD_ID', 'BLPLN_PYR_ID', 'PCIND_ACS_FEE_AT', 'ABL_NET_AMT',
                'AB7_FEE_PAID_AMT_1',
                'AB7_PFN_PRV_SPL_CODE', 'AB7_CLS_CODE', 'AB7_PFN_CRG_CLS_CODE', 'HSCBMP_TOT_MBR_LIAB_AT_1',
                'HSCBMP_TOT_MBR_LIAB_AT_2', 'ANE_CLS_CODE', 'ANE_PFN_PRV_SPL_CODE', 'SBMD_HSCRMPRV_ZIP_AD_1',
                'PELG_STD_INDS_CD', 'ALL_TYPE_CODE', 'INPL_SF_LN_ID', 'PRV_ASOC_STA_CODE', 'HSCBMP_STA_CD',
                'DAYS_SERVICED', 'FINAL_AFTER'
            ]

            ADJU_EXC_CD_LIST = ["S21", "B18", "S29", "B30", "B83", "S17", "B71", "BCV", "S16", "S90", "S09", "BEW",
                                "B1X",
                                "B81", "B1B",
                                "S1C", "B3U", "S56", "BB9", "S19"]
            CLM_SAC_CD_LIST = ['S35', 'S44', 'S62', 'S64', 'S79', 'S21', 'S65', 'S92', 'S10', 'S78', 'S45', 'S49',
                               'S29', 'S69',
                               'S15', 'S56',
                               'S63', 'S1K', 'S68', 'S99', 'S04', 'SA1', 'B1Q', 'S1E', 'S1A', 'S1F', 'S05', 'S71',
                               'S61', 'S08',
                               'S93', 'S60', 'S59', 'S07', 'S37',
                               'S1T', 'S90', 'S53', 'S1H', 'S1S']
            AGD_CODE_LIST = ['00', 'Z8', 'TC', '25', '59', 'GP', '26', 'WD', 'RT', 'LT', 'W4', 'PO', '95', 'KX', 'XU',
                             'WJ',
                             'NU', 'GT', 'ET',
                             'GO']
            NSIR_CD_LIST = ['N219', 'N25', 'MA15']

            ADJU_EXC_CD_DF = pd.DataFrame(columns=['ADJU_EXC_CD_' + str(code) for code in ADJU_EXC_CD_LIST])
            for code in ADJU_EXC_CD_LIST:
                mask = df[["ADJU_EXC_CD_01", "ADJU_EXC_CD_02", "ADJU_EXC_CD_03", "ADJU_EXC_CD_04", "ADJU_EXC_CD_05",
                           "ADJU_EXC_CD_06"]].isin([code]).any(axis=1)
                ADJU_EXC_CD_DF['ADJU_EXC_CD_' + str(code)] = [1 if code else 0 for code in mask.values]

            CLM_SAC_CD_DF = pd.DataFrame(columns=['CLM_SAC_CD_' + str(code) for code in sorted(CLM_SAC_CD_LIST)])

            for code in CLM_SAC_CD_LIST:
                mask = df[["CLM_SAC_CD_1", "CLM_SAC_CD_2", "CLM_SAC_CD_3", "CLM_SAC_CD_4", "CLM_SAC_CD_5"]].isin(
                    [code]).any(
                    axis=1)
                CLM_SAC_CD_DF['CLM_SAC_CD_' + str(code)] = [1 if code else 0 for code in mask.values]

            AGD_CODE_DF = pd.DataFrame(columns=['AGD_CODE_' + str(code) for code in sorted(AGD_CODE_LIST)])
            for code in AGD_CODE_LIST:
                mask = df[["AGD_CODE_1", "AGD_CODE_2", "AGD_CODE_3", "AGD_CODE_4", "AGD_CODE_5", "AGD_CODE_6"]].isin(
                    [code]).any(axis=1)
                AGD_CODE_DF['AGD_CODE_' + str(code)] = [1 if code else 0 for code in mask.values]

            NSIR_CD_DF = pd.DataFrame(columns=['NSIR_CD_' + str(code) for code in NSIR_CD_LIST])
            for code in NSIR_CD_LIST:
                mask = df[["NSIR_CD_1", "NSIR_CD_2", "NSIR_CD_3", "NSIR_CD_4", "NSIR_CD_5"]].isin([code]).any(axis=1)
                NSIR_CD_DF['NSIR_CD_' + str(code)] = [1 if code else 0 for code in mask.values]

            ohe_dfs = pd.concat([ADJU_EXC_CD_DF, CLM_SAC_CD_DF, AGD_CODE_DF, NSIR_CD_DF], axis=1)

            df.drop(drop_more, axis=1, inplace=True)
            df.drop(cols_span, axis=1, inplace=True)

            df['AFV_BGN_02_DATE'] = pd.to_datetime(df['AFV_BGN_02_DATE'])
            df['HSCBMP_SVCE_END_DT'] = pd.to_datetime(df['HSCBMP_SVCE_END_DT'])
            df['FINAL_DATE'] = pd.to_datetime(df['FINAL_DATE'])
            df['DAYS_SERVICED'] = df['HSCBMP_SVCE_END_DT'] - df['AFV_BGN_02_DATE']
            df['FINAL_AFTER'] = df['FINAL_DATE'] - df['HSCBMP_SVCE_END_DT']
            df.drop(['AFV_BGN_02_DATE', 'HSCBMP_SVCE_END_DT', 'FINAL_DATE'], axis=1, inplace=True)
            df['DAYS_SERVICED'] = df['DAYS_SERVICED'].astype('int64')
            df['FINAL_AFTER'] = df['FINAL_AFTER'].astype('int64')

            ignored_cols_for_now = ['AB7_PFN_PRV_ST_CODE_1', 'AB7_PFN_PRV_ST_CODE_2', 'PBSC_RENO_RLS_DT',
                                    'PBSC_RLS_CEN_DT']
            df.drop(ignored_cols_for_now, axis=1, inplace=True)

            enc_df = df[encode_cols]
            ip_df = df[left_out_cols]
            flag_df = df[flag]
            enc_df.fillna("unknown", inplace=True)

            file_path = "minio://{}/label_encoder/label_encoder.pkl".format(AMAZON_AWS_BUCKET)
            local_pkl_path = "/tmp/scaler.pkl"
            minio_resource = XpmsResource.get(urn=file_path)
            local_res_1 = LocalResource(key=local_pkl_path)
            minio_resource.copy(local_res_1)

            loaded_label_encoder = pickle.load(open(local_pkl_path, "rb"))
            enc_df_transformed = loaded_label_encoder.transform(enc_df)
            final_df = pd.concat([ip_df, enc_df_transformed, ohe_dfs, flag_df], axis=1)
            final_df.drop('CLAIM_NUMBER_Mask', axis=1, inplace=True)

            print("final shape:-", final_df.shape)

            file_name = objects["dataset"]["value"].split("/")[-1]
            csv_minio_urn = "minio://{}/ml_input/mapped_".format(AMAZON_AWS_BUCKET) + file_name
            local_csv_path = "/tmp/proc_cfe_" + file_name
            minio_resource = XpmsResource.get(urn=csv_minio_urn)
            final_df.to_csv(local_csv_path, index=False)
            local_res_2 = LocalResource(key=local_csv_path)
            local_res_2.copy(minio_resource)
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
            local_res_2.delete()
