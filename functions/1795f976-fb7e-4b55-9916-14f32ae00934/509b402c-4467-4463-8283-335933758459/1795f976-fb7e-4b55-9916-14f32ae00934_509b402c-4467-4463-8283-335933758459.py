from datetime import datetime
import pandas as pd
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
import json
from pandas.core.common import flatten
import numpy as np


def hma_mapper(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
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

            file_path = objects["document"][0]["metadata"]["properties"]["file_metadata"]["file_path"]
            local_csv_path = "/tmp/" + objects["document"][0]["metadata"]["properties"]["filename"]
            config["context"]["source_file_path"] = file_path
            config["context"]["local_source_file_path"] = local_csv_path
            config["context"]["doc_id"] = objects["document"][0]["doc_id"]
            config["context"]["solution_id"] = objects["document"][0]["solution_id"]
            config["context"]["root_id"] = objects["document"][0]["root_id"]
            minio_resource = XpmsResource.get(urn=file_path)
            local_res = LocalResource(key=local_csv_path)
            minio_resource.copy(local_res)
            hma_df = pd.read_csv(local_csv_path)
            hma_df.columns = [x.strip() for x in hma_df.columns]
            generic_df = pd.DataFrame(columns=(mapper_dict.keys()))
            for k, v in (mapper_dict.items()):
                if v is not None:
                    generic_df[k] = hma_df[v]
                else:
                    generic_df[k] = 0
                # generic_df[k] = hma_df[v] or 0

            local_res.delete()
            flag_cols = [
                'AC1_RSN_CODE', 'PBS_CLM_ORIG_CD', 'PBIPC_OSC_PAST_CD', 'PBSC_CLM_LK_NO', 'ABR_DATE',
                'ITS_DLV_MTH_CD', 'ALL_REJ_RSN_CODE', 'ADO_REF_BY_PRV_ID', 'SBMD_HSCRMPRV_ANCL_AFF_IN',
                'VFY_CTL_PLN_CODE', 'AHR_RATE_MTH_CODE', 'AAA_CODE', 'PBSC_INN_CD', 'INPL_DTA_PGM_CD',
                'BRY_CODE', 'BM4_CODE', 'DF_MSG_CD'
            ]

            excess_columns = ['ADJU_EXC_CD_07', 'ADJU_EXC_CD_08', 'ADJU_EXC_CD_09', 'ADJU_EXC_CD_10', 'CLM_SAC_CD_6',
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
                              'PBSC_VFYD_ACTB_CD', 'AB7_PFN_PRV_ST_CODE_1', 'AB7_PFN_PRV_ST_CODE_2', 'PBSC_RENO_RLS_DT',
                              'PBSC_RLS_CEN_DT',
                              "ALL_PRI_IND", "AB7_EOB_CODE", "ABK_HIS_SCE_CODE", "AB7_EOB_CODE", "ABR_TYPE_CODE_1",
                              "AFV_FNL_STA_CODE_1", "ABR_TYPE_CODE_2", "AAE_CODE_ODO_1", "AAE_CODE_ODO_2",
                              "AAE_CODE_ODO_3"
                              ]

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

            cols_span = {
                "ADJU_EXC_CD": ["ADJU_EXC_CD_01", "ADJU_EXC_CD_02", "ADJU_EXC_CD_03", "ADJU_EXC_CD_04",
                                "ADJU_EXC_CD_05",
                                "ADJU_EXC_CD_06"],
                "CLM_SAC_CD": ["CLM_SAC_CD_1", "CLM_SAC_CD_2", "CLM_SAC_CD_3", "CLM_SAC_CD_4", "CLM_SAC_CD_5"],
                "AGD_CODE": ["AGD_CODE_1", "AGD_CODE_2", "AGD_CODE_3", "AGD_CODE_4", "AGD_CODE_5", "AGD_CODE_6"],
                "NSIR_CD": ["NSIR_CD_1", "NSIR_CD_2", "NSIR_CD_3", "NSIR_CD_4", "NSIR_CD_5"],
            }

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

            # Load these from db ideally.

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

            config["context"]["flag_columns"] = flag_cols
            config["context"]["excess_columns"] = excess_columns
            config["context"]["encode_columns"] = encode_cols
            config["context"]["cols_span"] = cols_span
            config["context"]["left_out_cols"] = left_out_cols
            config["context"]["ADJU_EXEC_CD_LIST"] = ADJU_EXC_CD_LIST
            config["context"]["CLM_SAC_CD_LIST"] = CLM_SAC_CD_LIST
            config["context"]["AGD_CODE_LIST"] = AGD_CODE_LIST
            config["context"]["NSIR_CD_LIST"] = NSIR_CD_LIST
            return {
                "batch_length": len(generic_df),
                "data": [generic_df.columns.values.tolist()] + generic_df.values.tolist()
            }
        except Exception as e:
            counter += 1
        finally:
            local_res.delete()
