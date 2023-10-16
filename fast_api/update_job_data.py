import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import pyathena
from pyathena import connect
from pyathena.pandas.util import as_pandas
import pandas as pd
from pandas.tseries.offsets import BDay
from git_utils import get_file, get_repo_file_paths
from common import CONF_DICT, DATATYPE_MANAGER
import boto3
from botocore.exceptions import ClientError


def store_old_job_date():
    set_environment = CONF_DICT["hartree_set_environment"]
    common_config = CONF_DICT["common_config"]
    for env_var in set_environment:
        os.environ[env_var] = set_environment[env_var]
    all_file_paths = get_repo_file_paths(access_code=CONF_DICT["git_access_code"],
                                         repository_name=CONF_DICT["git_dags_conf_repo"],
                                         branch_name=CONF_DICT["git_dags_conf_ref"])
    table_name_list = [file_path.replace("_job.json", "") for file_path in all_file_paths if
                       file_path.endswith("_job.json")]
    conn = connect()
    cursor = conn.cursor()
    back_quoted_database_reference = DATATYPE_MANAGER["table"]["back_quoted_database_reference"].format(
        catalog_name=common_config['catalog_name'],
        database_name=common_config['database_name']
    )
    table_list_query = DATATYPE_MANAGER["table"]["table_list_query"].format(
        back_quoted_database_reference=back_quoted_database_reference)
    cursor.execute(table_list_query)
    available_table = [item[0] for item in cursor.fetchall()]
    final_table_name_list = [item for item in available_table if item in table_name_list]
    print(final_table_name_list)
    query_file_string = get_file(access_code=CONF_DICT["git_access_code"],
                                 repository_name=CONF_DICT["git_dags_conf_repo"],
                                 branch_name=CONF_DICT["git_dags_conf_ref"],
                                 file_path=CONF_DICT["query_conf"])

    query_dict = json.loads(query_file_string)
    error_job_list = []
    # final_table_name_list = ["allegro_realized_ytd_daily_recon"]
    tmp_path = Path("tmp")
    for job_name in final_table_name_list:
        tmp_path.joinpath(job_name).mkdir(exist_ok=True)
        if query_dict[job_name].get("date_column"):
            date_column_string = query_dict[job_name]["date_column"]
        else:
            date_column_string = f"case when {query_dict[job_name]['source1_date_column']} ='' then " \
                                 f" {query_dict[job_name]['source2_date_column']} " \
                                 f"else {query_dict[job_name]['source1_date_column']} end"
        query = f"select distinct {date_column_string} as job_date, run_id, \"$path\" as path" \
                f", run_date from {common_config['catalog_name']}.{common_config['database_name']}.{job_name}"
        print(query)
        try:
            cursor.execute(query)
            all_data_df = as_pandas(cursor)
            print(all_data_df)
            for job_date in all_data_df["job_date"].unique():
                data = all_data_df.loc[all_data_df["job_date"] == job_date, ["job_date", "run_id", "path"]]
                data.to_csv(tmp_path.joinpath(job_name, job_date.split(" ")[0] + ".csv"), index=False)
                job_file_string = get_file(access_code=CONF_DICT["git_access_code"],
                                           repository_name=CONF_DICT["git_dags_conf_repo"],
                                           branch_name=CONF_DICT["git_dags_conf_ref"],
                                           file_path=job_name + "_job.json")

                job_dict = json.loads(job_file_string)
                source1 = job_dict["source1"]
                source2 = job_dict["source2"]
                table_out_path = CONF_DICT["table_out_path"]
                if table_out_path[-1] == "/":
                    table_out_path = table_out_path[:-1]
                for index, row in data.iterrows():
                    if table_out_path + "/" + job_name + "/" + job_name + "_" + row["run_id"] + ".csv" == row["path"]:
                        print(row["path"], True)
                        print(table_out_path + "/" + job_name + "/" + job_name + "_" + row["run_id"] + ".csv")
                        print(table_out_path + "/" + job_name + "/job_date=" + row["job_date"] + "/" + job_name + "_" +
                              row["run_id"] + ".csv")
                        print(
                            table_out_path + "/" + job_name + "___" + source1 + "/" + job_name + "___" + source1 + "_" +
                            row["run_id"] + ".csv")
                        print(table_out_path + "/" + job_name + "___" + source1 + "/job_date=" + row[
                            "job_date"] + "/" + job_name + "___" + source1 + "_" +
                              row["run_id"] + ".csv")
                        print(
                            table_out_path + "/" + job_name + "___" + source2 + "/" + job_name + "___" + source2 + "_" +
                            row["run_id"] + ".csv")
                        print(table_out_path + "/" + job_name + "___" + source2 + "/job_date=" + row[
                            "job_date"] + "/" + job_name + "___" + source2 + "_" +
                              row["run_id"] + ".csv", end="\n\n")
                        # upload_dict = [
                        #     {"src": row["path"],
                        #      "dest": "/".join(row["path"].split("/")[:-1]) + "/job_date=" + row["job_date"] + "/" +
                        #              row["path"].split("/")[-1]}
                        #     for index, row in data.iterrows()]
                    else:
                        print(row["path"], False)
                        print(table_out_path + "/" + job_name + "/" + job_name + "_" + row["run_id"] + ".csv",
                              end="\n\n")
                # print(upload_dict)
        except pyathena.error.OperationalError as err:
            print(err)
            error_job_list.append((job_name, str(err)))
            continue
        exit()
    print(error_job_list)


store_old_job_date()
