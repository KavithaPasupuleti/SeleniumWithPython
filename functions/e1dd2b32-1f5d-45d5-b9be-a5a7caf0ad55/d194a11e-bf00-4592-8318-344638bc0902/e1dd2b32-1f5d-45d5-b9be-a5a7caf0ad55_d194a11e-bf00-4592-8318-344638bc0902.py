import json

import pandas as pd

pd.options.display.max_columns = None
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
from xpms_storage.db_handler import DBProvider
from xpms_storage.utils import get_env

df_info = []


# NUM_CLAIMS_PER_FILE


def split_hma_batches_record(config=None, **objects):
    retries = 3
    counter = 1
    while counter < retries:
        try:
            NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
            AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
            ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)

            NUM_CLAIMS_PER_FILES = json.loads(config['NUM_CLAIMS_PER_FILES'])

            NUM_CLAIMS_PER_FILE = int(NUM_CLAIMS_PER_FILES)
            file_path = objects["document"][0]["metadata"]["properties"]["file_metadata"]["file_path"]
            local_csv_path = "/tmp/" + objects["document"][0]["metadata"]["properties"]["filename"]
            minio_resource = XpmsResource.get(urn=file_path)
            local_res = LocalResource(key=local_csv_path)
            minio_resource.copy(local_res)
            # split file name from config
            file_name = objects["document"][0]["metadata"]["properties"]["filename"].split(".")[0]
            batch_name = config["context"]["batch_name"]
            threshold = config["context"]["threshold"]

            # three funtion for split big file to small one

            def file_sequence(batch_name, local_csv_path_df, threshold):
                NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
                AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)
                ENV_DATABASE = get_env('DATABASE_PARAPHRASE', None, True)
                # file_name = file_name.split(".")[-2]
                local_res = LocalResource(key=local_csv_path_df)
                csv_minio_urn = "minio://{0}/".format(
                    AMAZON_AWS_BUCKET) + "claimsaudit-ingestfiles/archive/split-batches-input-csv-pending/" + local_res.filename
                minio_resource = XpmsResource.get(urn=csv_minio_urn)

                local_res.copy(minio_resource)
                batch_ob = {

                    "batch_name": batch_name,
                    "file_name_chunk": local_res.filename,
                    "threshold": threshold,
                    "status": "pending"

                }
                try:
                    db = DBProvider.get_instance(db_name=ENV_DATABASE)
                    s = db.insert(table='batch_metadata_chunk', rows=batch_ob)
                except Exception as e:
                    return 'e is ' + str(e)

            def create_csv_with_num_claims(file_name, batch_name, remaining_dataframes, current_df, unique_claim_count,
                                           threshold):
                """ Create csv files from the provided chunks of dataframes by satisfying
                the number of claims per file

                Args:
                    remaining_dataframes ([list of df]): list of dataframes which are to be
                    written to the csv file
                    current_df ([df]): dataframe of the current chunk of file
                    unique_claim_count ([int]): maintains the number of unique claims throughout
                    the input file

                Returns:
                    remaining_dataframes ([list of df]): list of dataframes which are to be
                    written to the csv file
                    unique_claim_count ([int]): maintains the number of unique claims throughout
                    the input file
                    :param unique_claim_count:
                    :param current_df:
                    :param remaining_dataframes:
                    :param batch_name:
                    :param file_name:
                """

                remaining_dataframes.append(current_df)

                df_ = pd.concat(remaining_dataframes)
                claims = list(df_.CLAIM_NUMBER_Mask.unique())
                current_unique_claims_count = len(claims)

                if current_unique_claims_count >= NUM_CLAIMS_PER_FILE:

                    remaining_dataframes = []

                    remaining_ids = current_unique_claims_count % NUM_CLAIMS_PER_FILE
                    claim_sets = [claims[s:s + NUM_CLAIMS_PER_FILE]
                                  for s in range(0, current_unique_claims_count, NUM_CLAIMS_PER_FILE)]
                    if remaining_ids != 0:
                        last_claims_df = df_[df_['CLAIM_NUMBER_Mask'].isin(claim_sets[-1])]
                        remaining_dataframes.append(last_claims_df)
                        claim_sets.pop(-1)

                    for _ in claim_sets:
                        unique_claim_count += len(_)
                        df_sub = df_[df_['CLAIM_NUMBER_Mask'].isin(_)]
                        df_name = "{0}_{1}.csv".format(batch_name, unique_claim_count + 1)
                        # filename = "fixed_claims_file_{}.csv".format(unique_claim_count + 1)
                        path = r"/tmp/" + df_name
                        df_sub.to_csv(path, index=False)
                        file_sequence(batch_name, path, threshold)
                else:
                    pass

                return remaining_dataframes, unique_claim_count

            # functions of create chunks files
            def create_chunks(file_name, batch_name, chunkfile_location, threshold):
                """Split the input file in multiple files making sure following conditions are satified:
                1. one claim number does not occur in multiple files
                2. each file has maximum of NUM_CLAIMS_PER_FILE
                3. no claims are missed

                Args:
                    chunkfile_location ([string]): input file location
                    :param chunkfile_location:
                    :param batch_name:
                    :param file_name:
                """
                claim_rows = 0
                remaining_dfs = []
                chunksize = 1000
                unique_claim_count = 0
                for chunk_num, chunk in enumerate(pd.read_csv(chunkfile_location, chunksize=chunksize)):
                    #         if chunk_num%10==0: print(chunk_num)
                    if chunk_num == 0:
                        initial_df = chunk
                    else:
                        last_claim_initial = initial_df.CLAIM_NUMBER_Mask.iloc[-1]
                        first_claim_current = chunk.CLAIM_NUMBER_Mask.iloc[0]
                        if last_claim_initial == first_claim_current:
                            claim_to_move = initial_df[initial_df['CLAIM_NUMBER_Mask']
                                                       == last_claim_initial]
                            initial_df = initial_df[initial_df['CLAIM_NUMBER_Mask']
                                                    != last_claim_initial]
                            if initial_df.shape[0] != 0:
                                remaining_dfs, unique_claim_count = create_csv_with_num_claims(file_name, batch_name,
                                                                                               remaining_dfs,
                                                                                               initial_df,
                                                                                               unique_claim_count,
                                                                                               threshold)
                                claim_rows += initial_df.shape[0]
                            chunk = claim_to_move.append(chunk)
                            initial_df = chunk
                        else:
                            remaining_dfs, unique_claim_count = create_csv_with_num_claims(file_name, batch_name,
                                                                                           remaining_dfs,
                                                                                           initial_df,
                                                                                           unique_claim_count,
                                                                                           threshold)
                            claim_rows += initial_df.shape[0]
                            initial_df = chunk
                if chunk.shape[0] != chunksize:
                    remaining_dfs, unique_claim_count = create_csv_with_num_claims(file_name, batch_name,
                                                                                   remaining_dfs,
                                                                                   chunk,
                                                                                   unique_claim_count, threshold)
                    if len(remaining_dfs) != 0:
                        rem_df = pd.concat(remaining_dfs)
                        # filename = "fixed_claims_file_{}.csv".format(unique_claim_count)
                        df_name = "{0}_{1}.csv".format(batch_name, unique_claim_count)
                        path = r"/tmp/" + df_name
                        rem_df.to_csv(path, index=False)
                        file_sequence(batch_name, path, threshold)
                        current_unique_claim_ids = rem_df.CLAIM_NUMBER_Mask.unique()
                        unique_claim_count += len(current_unique_claim_ids)
                    claim_rows += chunk.shape[0]
                print("claim rows ", claim_rows)
                print("unique_claim_count ", unique_claim_count)
                return unique_claim_count

            unique_claim = create_chunks(file_name, batch_name, local_csv_path, threshold)

            if unique_claim % NUM_CLAIMS_PER_FILE == 0:
                no_chunk = unique_claim // NUM_CLAIMS_PER_FILE
            else:
                no_chunk = unique_claim // NUM_CLAIMS_PER_FILE + 1

            filter_ob = {"batch_name": batch_name}
            update_ob = {
                "no_of_chunk": no_chunk,
                "batch_volume": unique_claim
            }
            try:
                db = DBProvider.get_instance(db_name=ENV_DATABASE)
                s = db.update(table='batch_metadata', update_obj=update_ob, filter_obj=filter_ob)
            except Exception as e:
                return 'e is ' + str(e)

            return objects

        except Exception as e:
            counter += 1
        finally:
            local_res.delete()
