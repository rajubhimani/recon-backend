import uuid
from pytz import timezone
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import pyathena
from pyathena import connect
from pyathena.pandas.util import as_pandas
import pandas as pd
from pandas.tseries.offsets import BDay
from git_utils import get_file, get_files, update_file, get_repo_file_paths, check_path_exists, create_file
from common import CONF_DICT, DATATYPE_MANAGER
import boto3
from botocore.exceptions import ClientError


def is_business_date(current_date):
    return bool(len(pd.bdate_range(current_date, current_date)))


def get_bucket_and_path(full_path):
    path_split = full_path.replace("s3://", "").split("/")
    bucket = path_split[0]
    s3_path = "/".join(path_split[1:])
    return bucket, s3_path


def upload_file(bucket, s3_file_path, file_path, extra_args=None):
    """
    Upload a file to an S3 bucket
    :param file_path: File to upload path
    :param bucket: Bucket to upload to
    :param s3_file_path: S3 path
    :param extra_args: dictionary object name. Check available extra parameters allowed here -
    boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
    :return: True if file was uploaded, else False
    """

    # Upload the file
    print("S3 Client creating")
    s3_client = boto3.client('s3')
    print("S3 Client created")
    try:
        s3_file_path = s3_file_path.replace("//", "/")
        if extra_args:
            print(f"File Uploading to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
            s3_client.upload_file(file_path,
                                  bucket,
                                  s3_file_path,
                                  ExtraArgs=extra_args
                                  )
            print(f"File Uploaded to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
        else:
            print(f"File Uploading to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
            s3_client.upload_file(file_path,
                                  bucket,
                                  s3_file_path
                                  )
            print(f"File Uploaded to s3 - from {file_path} to s3://{bucket}/{s3_file_path}")
    except ClientError as e:
        print(e)
        return False
    return True


def date_generator(job_filter):
    if job_filter["type"] == "day-wise":
        current_date = datetime.now() - timedelta(days=job_filter["days_ago"])
        current_date = datetime(day=current_date.day, month=current_date.month, year=current_date.year)
    elif job_filter["type"] == "prior-business-day":
        current_date = datetime.now() - timedelta(days=job_filter["days_ago"])
        current_date = datetime(day=current_date.day, month=current_date.month, year=current_date.year)
        if not is_business_date(current_date):
            current_date = current_date - BDay(n=1)
    elif job_filter["type"] == "previous-month-last-business-day":
        current_date = datetime.now()
        month_calc = current_date.month - job_filter["months_ago"] + 1
        max_dt = datetime(day=1, month=month_calc, year=current_date.year)
        current_date = max_dt - timedelta(days=1)
        if not is_business_date(current_date):
            current_date = current_date - BDay(n=1)
    return current_date


def store_old_job_info():
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
    for job_name in final_table_name_list:
        if query_dict[job_name].get("date_column"):
            date_column_string = query_dict[job_name]["date_column"]
        else:
            date_column_string = f"case when {query_dict[job_name]['source1_date_column']} ='' then " \
                                 f" {query_dict[job_name]['source2_date_column']} " \
                                 f"else {query_dict[job_name]['source1_date_column']} end"
        query = f"select distinct {date_column_string} as date, run_id, " \
                f"run_date from {common_config['catalog_name']}.{common_config['database_name']}.{job_name}"
        print(query)
        try:
            cursor.execute(query)
            all_data_df = as_pandas(cursor)
            # all_data_df.info()
            print(all_data_df["date"].unique())
            for date in all_data_df["date"].unique():
                # print(date)
                date = date.split(" ")[0]
                job_info_filepath = CONF_DICT["common_config"]["job_info"]["job_info_filepath"].format(
                    job_name=job_name,
                    date=date)
                print(job_info_filepath)
                tmp = Path("/tmp")
                tmp = tmp.joinpath(job_info_filepath[:-1].split("/")[-2], job_info_filepath[:-1].split("/")[-1])
                os.makedirs(tmp, exist_ok=True)
                for run_id in all_data_df.loc[all_data_df["date"] == date, ["run_id"]]["run_id"]:
                    df = all_data_df.loc[
                        (all_data_df["date"] == date) & (all_data_df["run_id"] == run_id), ["run_id", "run_date"]]
                    df = df.sort_values("run_date")
                    file_name = "job_info_" + job_name + "_" + run_id + ".csv"
                    df.to_csv(str(tmp.joinpath(file_name)), index=False)
                    bucket, s3_path = get_bucket_and_path(job_info_filepath + file_name)
                    upload_file(bucket=bucket, s3_file_path=s3_path, file_path=str(tmp.joinpath(file_name)))
                    # df.info()
                table_reference = CONF_DICT["common_config"]["job_info"]["job_info_table"]
                partition_query = "alter table {table_reference} add if not exists {partitions}".format(
                    table_reference=table_reference,
                    partitions="partition (job_name='{job_name}', date='{date}')".format(job_name=job_name, date=date)
                )
                print(partition_query)
                cursor.execute(partition_query)
                conn.commit()

        except pyathena.error.OperationalError as err:
            print(err)
            error_job_list.append((job_name, str(err)))
            continue
    print(error_job_list)


def store_job_info(job_conf, run_info):
    job_description = job_conf["job_description"]
    job_name = job_description["job_name"]
    run_id = run_info["run_id"]
    run_date = run_info["run_date"]
    date = date_generator(job_description["filter"])
    job_info_filepath = CONF_DICT["common_config"]["job_info"]["job_info_filepath"].format(
        job_name=job_name,
        date=date.strftime("%Y-%m-%d"))
    print(job_info_filepath)
    tmp = Path("/tmp")
    df = pd.DataFrame(data=[[run_id, run_date]], columns=["run_id", "run_date"])
    file_name = "job_info_" + job_name + "_" + run_id + ".csv"
    df.to_csv(str(tmp.joinpath(file_name)), index=False)
    df.info()
    bucket, s3_path = get_bucket_and_path(job_info_filepath + file_name)
    upload_file(bucket=bucket, s3_file_path=s3_path, file_path=str(tmp.joinpath(file_name)))
    print(df)
    table_reference = CONF_DICT["common_config"]["job_info"]["job_info_table"]
    partition_query = "alter table {table_reference} add if not exists {partitions}".format(
        table_reference=table_reference,
        partitions="partition (job_name='{job_name}', date='{date}')".format(job_name=job_name, date=date)
    )
    print(partition_query)



# with open(r"H:\Repos\dev\recon-dags-config\epsilon_oz_realized_mtd_fx_recon_job.json", "r") as file_reader:
#     job_dict = json.load(file_reader)
#     est = timezone("US/Eastern")
#     common_config = CONF_DICT["common_config"]
#     current_datetime = datetime.now(est)
#     run_id = str(uuid.UUID(int=int(datetime.utcnow().strftime(common_config["run_id_date_format"]))))
#     run_date = current_datetime.strftime(common_config["python_log_time_format"])
#     run_info = {
#         "run_id": run_id,
#         "run_date": run_date
#     }
#     store_job_info(job_dict, run_info)

store_old_job_info()
