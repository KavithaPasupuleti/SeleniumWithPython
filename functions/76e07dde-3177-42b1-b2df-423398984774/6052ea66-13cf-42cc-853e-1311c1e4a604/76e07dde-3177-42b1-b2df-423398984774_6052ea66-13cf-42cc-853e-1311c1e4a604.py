from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import pandas as pd
from datetime import datetime
import os
from xpms_storage.utils import get_env
import time
from distutils.util import strtobool
from xpms_storage.db_handler import DBProvider
import requests
import json
from minio import ResponseError


def split_hma_check_files(config=None, **objects):
    AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
    # number_of_files = json.loads(config['number_of_files'])
    ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
    BE_URL = get_env('CLAIMS_AUDIT_APIS_URL', None, True)
    file_path = "minio://{0}/claimsaudit-ingestfiles/split-input-csv".format(AMAZON_AWS_BUCKET)

    # xr = XpmsResource()
    # minio_resource = xr.get(urn=file_path)
    # n = int(number_of_files)
    # if minio_resource.exists():
    #     all_files_list = minio_resource.list()
    #     files_list = [(path.filename) for path in all_files_list if ".csv" in path.fullpath]
    #     if len(files_list) == 0:
    #
    #         return {
    #             "file_path": "na"
    #         }
    #
    #     else:
    #         backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-input-csv-inprogress".format(
    #             AMAZON_AWS_BUCKET)
    #         path_list = []
    #         for file_name in files_list[:n]:
    #             xrm = XpmsResource()
    #             mr = xrm.get(urn=file_path + '/' + file_name)
    #             backup_urn = backup_path + '/' + file_name
    #             backup_rm = XpmsResource()
    #             backup_mr = backup_rm.get(urn=backup_urn)
    #             mr.copy(backup_mr)
    #             mr.delete()
    #             path_list.append(backup_urn)
    #         return {
    #
    #             "file_path": path_list
    #         }
    #
    # else:
    #     return {
    #
    #         "file_path": "na"
    #     }
    def generate_notification(body, group='batch_status', status='failed', title='File Ingestion',
                              icon='failure'):
        notification = {
            "group": group,
            "message": {
                "body": body,
                "status": status,
                "title": title,
                "icon": icon
            },
            "created_timestamp": int(time.time())
        }
        return notification

    def send_notification_UI(notification):
        url = f'https://{BE_URL}/send_notification'
        headers = {
            'Content-Type': 'application/json'
        }
        resp = requests.request("POST", url, headers=headers,
                                verify=bool(strtobool(get_env("SSL_VERIFY", "False", False))),
                                data=json.dumps(notification, default=str))
        return resp

    xr = XpmsResource()
    minio_resource = xr.get(urn=file_path)
    if minio_resource.exists():
        all_files_list = minio_resource.list()
        invalid_list = [(path.filename) for path in all_files_list if
                        (".csv" not in path.fullpath and path.filename != "README.md")]
        if (len(invalid_list)):
            file_name = invalid_list[0]
            xrm = XpmsResource()
            mr = xrm.get(urn=file_path + '/' + file_name)
            notification = generate_notification(body="Error in the ingested ingestion File. Only csv files are accepted")
            db = DBProvider.get_instance(db_name=ENV_DATABASE)
            s = db.insert(table='notifications', rows=[notification])
            if s:
                send_notification_UI(notification)
                backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-batches_completed".format(
                    AMAZON_AWS_BUCKET)
                backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                backup_rm = XpmsResource()
                backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                if mr.exists():
                    mr.copy(backup_mr)
                    mr.delete()
                raise AssertionError("Invalid input(ingestion) File Provided.")
        files_list = [(path.filename) for path in all_files_list if ".csv" in path.fullpath]
        if len(files_list) == 0:
            return {
                "file_path": "na"
            }
        # elif len(files_list) > 1:
        #     return {
        #         'message': 'More than one file present in ' + file_path
        #     }
        else:
            file_name = files_list[0]
            local_path = '/tmp/local_' + file_name
            lr = LocalResource(key=local_path)
            xrm = XpmsResource()
            mr = xrm.get(urn=file_path + '/' + file_name)
            mr.copy(lr)

            # verify column headers

            xdf = pd.read_csv(local_path)
            x = list(xdf.columns)
            mapper_dict = {
                'CLAIM_NUMBER_Mask': 'CLAIM_NUMBER_Mask',
                'ABG_EMP_ID_Mask': 'ABG_EMP_ID_Mask',
                'CH_INS_MBR_ID_GP_Mask': 'CH_INS_MBR_ID_GP_Mask',
                'PRN_ACC_VFY_ID_Mask': 'PRN_ACC_VFY_ID_Mask',
                'VFYD_CL_N_Mask': 'VFYD_CL_N_Mask',
                'ACC_VFY_ID_Mask': 'ACC_VFY_ID_Mask',
                'FINAL_DATE': 'FINAL_DATE',
                'ENR_SRC_CODE': 'ENR_SRC_CODE',
                'GROUP_NUMBER': 'GROUP_NUMBER',
                'PROCESS_STAT': 'PROCESS_STAT',
                'LINE_ITEM_NO': 'LINE_ITEM_NO',
                'EAMEE_ID': 'EAMEE_ID',
                'LINE_BLIND_KEY': 'LINE_BLIND_KEY',
                'ACKRC_CD': 'ACKRC_CD',
                'ADJU_EXC_CD_01': 'ADJU_EXC_CD_01',
                'ADJU_EXC_CD_02': 'ADJU_EXC_CD_02',
                'ADJU_EXC_CD_03': 'ADJU_EXC_CD_03',
                'ADJU_EXC_CD_04': 'ADJU_EXC_CD_04',
                'ADJU_EXC_CD_05': 'ADJU_EXC_CD_05',
                'ADJU_EXC_CD_06': 'ADJU_EXC_CD_06',
                'ADJU_EXC_CD_07': 'ADJU_EXC_CD_07',
                'ADJU_EXC_CD_08': 'ADJU_EXC_CD_08',
                'ADJU_EXC_CD_09': 'ADJU_EXC_CD_09',
                'ADJU_EXC_CD_10': 'ADJU_EXC_CD_10',
                'CLM_SAC_CD_1': 'CLM_SAC_CD_1',
                'CLM_SAC_CD_2': 'CLM_SAC_CD_2',
                'CLM_SAC_CD_3': 'CLM_SAC_CD_3',
                'CLM_SAC_CD_4': 'CLM_SAC_CD_4',
                'CLM_SAC_CD_5': 'CLM_SAC_CD_5',
                'CLM_SAC_CD_6': 'CLM_SAC_CD_6',
                'ABK_PNT_PERS_ID_1': 'ABK_PNT_PERS_ID_1',
                'ALL_GRP_PROD_LINE_CODE': 'ALL_GRP_PROD_LINE_CODE',
                'AFV_FNL_STA_CODE_1': 'AFV_FNL_STA_CODE_1',
                'AFV_FNL_STA_CODE_2': 'AFV_FNL_STA_CODE_2',
                'ABK_FNL_DATE': 'ABK_FNL_DATE',
                'AFV_BILL_PRV_UVFY_ID_1': 'AFV_BILL_PRV_UVFY_ID_1',
                'AFV_BILL_PRV_UVFY_ID_2': 'AFV_BILL_PRV_UVFY_ID_2',
                'ABK_PNT_REL_APP_CODE_1': 'ABK_PNT_REL_APP_CODE_1',
                'AFV_BGN_02_DATE': 'AFV_BGN_02_DATE',
                'AFV_BCBSA_PL_CODE_1': 'AFV_BCBSA_PL_CODE_1',
                'AFV_BCBSA_PL_CODE_2': 'AFV_BCBSA_PL_CODE_2',
                'AGD_CODE_1': 'AGD_CODE_1',
                'AGD_CODE_2': 'AGD_CODE_2',
                'AGD_CODE_3': 'AGD_CODE_3',
                'AGD_CODE_4': 'AGD_CODE_4',
                'AGD_CODE_5': 'AGD_CODE_5',
                'AGD_CODE_6': 'AGD_CODE_6',
                'AGD_CODE_7': 'AGD_CODE_7',
                'AGD_UVFY_CODE': 'AGD_UVFY_CODE',
                'AFV_PRV_CRG_AMT_1': 'AFV_PRV_CRG_AMT_1',
                'AC1_RSN_CODE': 'AC1_RSN_CODE',
                'ABK_STA_CODE_1': 'ABK_STA_CODE_1',
                'ABK_STA_CODE_2': 'ABK_STA_CODE_2',
                'PBSC_INP_MDM_CD': 'PBSC_INP_MDM_CD',
                'PBS_CLM_ORIG_CD': 'PBS_CLM_ORIG_CD',
                'PBIPC_OSC_PAST_CD': 'PBIPC_OSC_PAST_CD',
                'PBSC_CLM_LK_NO': 'PBSC_CLM_LK_NO',
                'ABK_HIS_SCE_CODE': 'ABK_HIS_SCE_CODE',
                'ABK_TYPE_CODE': 'ABK_TYPE_CODE',
                'PBSC_SAE_CD_1': 'PBSC_SAE_CD_1',
                'PBSC_SAE_CD_2': 'PBSC_SAE_CD_2',
                'PBSC_SAE_CD_3': 'PBSC_SAE_CD_3',
                'PBSC_SAE_CD_4': 'PBSC_SAE_CD_4',
                'PBSC_SAE_CD_5': 'PBSC_SAE_CD_5',
                'PBSC_SAE_CD_6': 'PBSC_SAE_CD_6',
                'ABR_RSN_CODE': 'ABR_RSN_CODE',
                'ABR_TYPE_CODE_1': 'ABR_TYPE_CODE_1',
                'ABR_TYPE_CODE_2': 'ABR_TYPE_CODE_2',
                'ABR_DATE': 'ABR_DATE',
                'ADB_CODE': 'ADB_CODE',
                'EOB_PRINT_CD': 'EOB_PRINT_CD',
                'ABK_AUTM_CLM_SCE_ID': 'ABK_AUTM_CLM_SCE_ID',
                'PBSC_GVN_ETY_CD': 'PBSC_GVN_ETY_CD',
                'NS_FTP_CD': 'NS_FTP_CD',
                'AFV_BILL_CLM_PRV_ID': 'AFV_BILL_CLM_PRV_ID',
                'ABK_PNT_REL_APP_CODE_2': 'ABK_PNT_REL_APP_CODE_2',
                'ABK_PNT_SEX_CODE': 'ABK_PNT_SEX_CODE',
                'ABK_PNT_PERS_ID_2': 'ABK_PNT_PERS_ID_2',
                'ABK_ENR_SCE_CODE_1': 'ABK_ENR_SCE_CODE_1',
                'AF4_HIC_ID': 'AF4_HIC_ID',
                'ABK_ENR_SCE_CODE_2': 'ABK_ENR_SCE_CODE_2',
                'ABK_VFY_ENR_GRP_ID': 'ABK_VFY_ENR_GRP_ID',
                'VFY_PROD_LN_CODE': 'VFY_PROD_LN_CODE',
                'ABK_SUB_ENR_CLS_CODE': 'ABK_SUB_ENR_CLS_CODE',
                'CAR_VFY_ID': 'CAR_VFY_ID',
                'ENR_BEN_LVL_VFY_ID': 'ENR_BEN_LVL_VFY_ID',
                'GRP_COL_VFY_ID': 'GRP_COL_VFY_ID',
                'PBSC_VFYD_ACTB_CD': 'PBSC_VFYD_ACTB_CD',
                'VFYD_BPD_ID': 'VFYD_BPD_ID',
                'PCOWNS_CD': 'PCOWNS_CD',
                'BLPLN_PYR_ID': 'BLPLN_PYR_ID',
                'PAOWNS_CD': 'PAOWNS_CD',
                'PCIND_ACS_FEE_AT': 'PCIND_ACS_FEE_AT',
                'ITS_DLV_MTH_CD': 'ITS_DLV_MTH_CD',
                'ABE_RCP_CLM_AMT': 'ABE_RCP_CLM_AMT',
                'ABL_NET_AMT': 'ABL_NET_AMT',
                'ABL_ATTY_FEE_AMT': 'ABL_ATTY_FEE_AMT',
                'AFV_PRV_CRG_AMT_2': 'AFV_PRV_CRG_AMT_2',
                'PR_ID': 'PR_ID',
                'AB7_FEE_PAID_AMT_1': 'AB7_FEE_PAID_AMT_1',
                'ALL_REJ_RSN_CODE': 'ALL_REJ_RSN_CODE',
                'ALL_PRI_IND': 'ALL_PRI_IND',
                'AB7_ID': 'AB7_ID',
                'AB7_PFN_PRV_SPL_CODE': 'AB7_PFN_PRV_SPL_CODE',
                'AB7_FEE_PAID_AMT_2': 'AB7_FEE_PAID_AMT_2',
                'AB7_TYPE_CODE': 'AB7_TYPE_CODE',
                'AB7_CLS_CODE': 'AB7_CLS_CODE',
                'AB7_FEE_PAID_IND': 'AB7_FEE_PAID_IND',
                'AB7_PFN_PRV_ST_CODE_1': 'AB7_PFN_PRV_ST_CODE_1',
                'AB7_PFN_PRV_ST_CODE_2': 'AB7_PFN_PRV_ST_CODE_2',
                'AB7_PFN_CRG_CLS_CODE': 'AB7_PFN_CRG_CLS_CODE',
                'AB7_SUB_ASG_BEN_CODE': 'AB7_SUB_ASG_BEN_CODE',
                'AB7_EOB_CODE': 'AB7_EOB_CODE',
                'AB7_MED_ASG_ACP_CODE': 'AB7_MED_ASG_ACP_CODE',
                'RPA_ID': 'RPA_ID',
                'PRV_MSG_DTR_CODE': 'PRV_MSG_DTR_CODE',
                'PBSC_RENO_RLS_DT': 'PBSC_RENO_RLS_DT',
                'PBSC_RLS_CEN_DT': 'PBSC_RLS_CEN_DT',
                'PBSC_RLS_YR_DT': 'PBSC_RLS_YR_DT',
                'PBSC_RLS_DAY_DT': 'PBSC_RLS_DAY_DT',
                'NSIR_CD_1': 'NSIR_CD_1',
                'NSIR_CD_2': 'NSIR_CD_2',
                'NSIR_CD_3': 'NSIR_CD_3',
                'NSIR_CD_4': 'NSIR_CD_4',
                'NSIR_CD_5': 'NSIR_CD_5',
                'HSCBMP_TOT_MBR_LIAB_AT_1': 'HSCBMP_TOT_MBR_LIAB_AT_1',
                'HSCBMP_TOT_MBR_LIAB_AT_2': 'HSCBMP_TOT_MBR_LIAB_AT_2',
                'HSCBMP_SVCE_BGN_DT': 'HSCBMP_SVCE_BGN_DT',
                'HSCBMP_SVCE_END_DT': 'HSCBMP_SVCE_END_DT',
                'ANE_ID': 'ANE_ID',
                'ANE_TYPE_CODE': 'ANE_TYPE_CODE',
                'ANE_CLS_CODE': 'ANE_CLS_CODE',
                'ANE_PFN_PRV_SPL_CODE': 'ANE_PFN_PRV_SPL_CODE',
                'SPER_NPI_MPEI_ID': 'SPER_NPI_MPEI_ID',
                'ADO_REF_BY_PRV_ID': 'ADO_REF_BY_PRV_ID',
                'SBMD_HSCRMPRV_NM_GP': 'SBMD_HSCRMPRV_NM_GP',
                'SBMD_HSCRMPRV_STE_AD': 'SBMD_HSCRMPRV_STE_AD',
                'SBMD_HSCRMPRV_ZIP_AD': 'SBMD_HSCRMPRV_ZIP_AD',
                'SBMD_HSCRMPRV_ZIP_AD_1': 'SBMD_HSCRMPRV_ZIP_AD_1',
                'SBMD_HSCRMPRV_CTY_AD_2': 'SBMD_HSCRMPRV_CTY_AD_2',
                'SBMD_HSCRMPRV_ANCL_AFF_IN': 'SBMD_HSCRMPRV_ANCL_AFF_IN',
                'ABK_PRNL_DLPS_IND': 'ABK_PRNL_DLPS_IND',
                'VFY_CTL_PLN_CODE': 'VFY_CTL_PLN_CODE',
                'AHR_RATE_MTH_CODE': 'AHR_RATE_MTH_CODE',
                'AAA_CODE': 'AAA_CODE',
                'PBSC_INN_CD': 'PBSC_INN_CD',
                'PBSC_MEGA_CLM_IN': 'PBSC_MEGA_CLM_IN',
                'AS5_CODE': 'AS5_CODE',
                'ADA_PRS_DATE': 'ADA_PRS_DATE',
                'INPL_DTA_PGM_CD': 'INPL_DTA_PGM_CD',
                'ABG_RSN_CODE': 'ABG_RSN_CODE',
                'BRY_CODE': 'BRY_CODE',
                'BM4_CODE': 'BM4_CODE',
                'VFYD_PRDID_PDI_CD': 'VFYD_PRDID_PDI_CD',
                'DF_MSG_CD': 'DF_MSG_CD',
                'PBSC_GP_SPL_CD': 'PBSC_GP_SPL_CD',
                'AAE_CODE_ODO_1': 'AAE_CODE_ODO_1',
                'AAE_CODE_ODO_2': 'AAE_CODE_ODO_2',
                'AAE_CODE_ODO_3': 'AAE_CODE_ODO_3',
                'AAE_CODE_ODO_4': 'AAE_CODE_ODO_4',
                'AAE_CODE_ODO_5': 'AAE_CODE_ODO_5',
                'AAE_CODE_ODO_6': 'AAE_CODE_ODO_6',
                'PBSC_RPC_CLM_ID': 'PBSC_RPC_CLM_ID',
                'PBSC_MAN_MRG_CD_2': 'PBSC_MAN_MRG_CD_2',
                'PBSC_MAN_MRG_CD_1': 'PBSC_MAN_MRG_CD_1',
                'PELG_STD_INDS_CD': 'PELG_STD_INDS_CD',
                'ALL_TYPE_CODE': 'ALL_TYPE_CODE',
                'INPL_SF_LN_ID': 'INPL_SF_LN_ID',
                'PRV_ASOC_STA_CODE': 'PRV_ASOC_STA_CODE',
                'HSCBMP_STA_CD': 'HSCBMP_STA_CD',
            }
            Missed_columns = []
            for v in (mapper_dict.values()):
                if v not in x:
                    Missed_columns.append(v)
            if len(Missed_columns) > 0:
                if len(Missed_columns) == 1:
                    body = "Error in the ingested csv file. {} column is Missing.".format(Missed_columns[0])
                elif (len(Missed_columns) == 2):
                    body = "Error in the ingested csv file. {} and {} columns are missing.".format(
                        Missed_columns[0],
                        Missed_columns[1])
                else:
                    body = "Error in the ingested csv file. {} and other {} columns are missing.".format(
                        Missed_columns[0], len(Missed_columns) - 1)
                notification = generate_notification(body=body)
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                s = db.insert(table='notifications', rows=[notification])
                if s:
                    send_notification_UI(notification)
                    backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-batches_completed".format(
                        AMAZON_AWS_BUCKET)
                    backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                    backup_rm = XpmsResource()
                    backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                    if mr.exists():
                        mr.copy(backup_mr)
                        mr.delete()
                    raise AssertionError("Invalid file provided. Some of the columns are missing")
            # Verify the file format
            if (xdf[mapper_dict["CLAIM_NUMBER_Mask"]].isnull().values.any()):
                notification = generate_notification(
                    body="Error in the ingested csv file. Data is missing in {} column".format(
                        mapper_dict["CLAIM_NUMBER_Mask"]))
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                s = db.insert(table='notifications', rows=[notification])
                if s:
                    send_notification_UI(notification)
                    backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-batches_completed".format(
                        AMAZON_AWS_BUCKET)
                    backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                    backup_rm = XpmsResource()
                    backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                    if mr.exists():
                        mr.copy(backup_mr)
                        mr.delete()
                    raise AssertionError("Invalid File Provided.")
            if (xdf.shape[0] == 0):
                notification = generate_notification(
                    body="Error in the ingested csv file. No data present in the rows")
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                s = db.insert(table='notifications', rows=[notification])
                if s:
                    send_notification_UI(notification)
                    backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-batches_completed".format(
                        AMAZON_AWS_BUCKET)
                    backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name
                    backup_rm = XpmsResource()
                    backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
                    if mr.exists():
                        mr.copy(backup_mr)
                        mr.delete()
                    raise AssertionError("Invalid File Provided.")
            xrm = XpmsResource()
            mr = xrm.get(urn=file_path + '/' + file_name)
            mr.copy(lr)

            backup_path = "minio://{0}/claimsaudit-ingestfiles/archive/split-input-csv-inprogress".format(
                AMAZON_AWS_BUCKET)

            backup_filename = str(int(datetime.now().timestamp())) + '_' + file_name

            filename, file_extension = os.path.splitext(file_name)

            backup_rm = XpmsResource()
            backup_mr = backup_rm.get(urn=backup_path + '/' + backup_filename)
            try:
                mr.copy(backup_mr)
                mr.delete()
            except ResponseError as err:
                notification = generate_notification(body=err.message)
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                db.insert(table='notifications', rows=[notification])
                raise err

            return {
                "file_path": backup_path + '/' + backup_filename
            }

    else:
        return {

            "file_path": "na"
        }

