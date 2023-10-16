import sys
import os
import time
from pathlib import PurePath, Path
import json
import uuid
from datetime import datetime
from pytz import timezone
from botocore.exceptions import ClientError
import pandas as pd
from pyathena.error import OperationalError
from common import CONF_DICT, LOGGER, DATATYPE_MANAGER
from get_latest_data import get_latest_data
from job_utils import (
    select_query, grouping_query, get_bucket_and_path, get_value_comparison, repair_partition, get_column_names,
    date_generator
)
from keystone.pipeline import Pipeline
from keystone.sources import AthenaSource, FileSource
from keystone.sinks import FileSink
from keystone.transformers import RenameColumn

debug_query = False


def run(job_conf_path, source_path):
    sleep_time = 20
    LOGGER.debug('pwd')
    os.system('pwd')
    LOGGER.debug(f'{job_conf_path} {source_path}')
    with open(job_conf_path, "r") as job_reader:
        job_conf = json.load(job_reader)
        LOGGER.debug(f'{job_conf}')
        common_config = CONF_DICT["common_config"]
        set_environment = CONF_DICT["hartree_set_environment_new"]
        pipline_env = {
            "MODIN_ENGINE": set_environment["MODIN_ENGINE"],
            "AWS_REGION_NAME": set_environment["AWS_REGION_NAME"],
            "AWS_S3_STAGING_DIR": set_environment["AWS_S3_STAGING_DIR"],
            "AWS_WORK_GROUP": set_environment["AWS_WORK_GROUP"]
        }
        for env_var in pipline_env:
            os.environ[env_var] = pipline_env[env_var]
        LOGGER.debug(f'{os.environ}')

        pipeline = Pipeline({})
        final_query = ""
        source_path = PurePath(source_path)
        source_no = "source1"
        est = timezone("US/Eastern")
        current_datetime = datetime.now(est)
        run_id = str(uuid.UUID(int=int(datetime.utcnow().strftime(common_config["run_id_date_format"]))))
        run_date = current_datetime.strftime(common_config["python_log_time_format"])
        run_info = {
            "run_id": run_id,
            "run_date": run_date
        }
        data1_pre_query, data1_query, data1_columns_list, s3_data1_query = select_query(job_conf, source_path,
                                                                                        source_no, run_info, pipeline)

        table1_alias = job_conf[source_no]
        f_table1_alias = table1_alias
        final_query += "with pre_" + table1_alias + " as (" + data1_pre_query + ")"
        final_query += ", " + table1_alias + " as ( " + data1_query + " ) "
        final_columns = list()
        aggregation_status = job_conf["aggregation"]["status"]
        if aggregation_status:
            agg1_query, column_list = grouping_query(job_conf, source_no, source_path)

            final_query += ", g_" + table1_alias + " as ( " + agg1_query + " ) "
            final_columns.append(table1_alias + "___row_count")
            f_table1_alias = "g_" + f_table1_alias
        else:
            final_columns.append("1 as " + table1_alias + "___row_count")

        source_no = "source2"
        data2_pre_query, data2_query, data2_columns_list, s3_data2_query = select_query(job_conf, source_path,
                                                                                        source_no, run_info, pipeline)

        table2_alias = job_conf[source_no]
        final_query += ", pre_" + table2_alias + " as ( " + data2_pre_query + " ) "
        final_query += ", " + table2_alias + " as ( " + data2_query + " ) "
        f_table2_alias = table2_alias
        if aggregation_status:
            agg2_query, column_list = grouping_query(job_conf, source_no, source_path)

            final_query += ", g_" + table2_alias + " as ( " + agg2_query + " ) "
            final_columns.append(table2_alias + "___row_count")
            f_table2_alias = "g_" + f_table2_alias
        else:
            final_columns.append("1 as " + table2_alias + "___row_count")

        reconcile_query = ""

        condition_list = list()
        for key in job_conf["keys"]:
            s1_column = f_table1_alias + "." + table1_alias + "___" + key["source1"]
            s2_column = f_table2_alias + "." + table2_alias + "___" + key["source2"]
            final_columns.append("cast(" + s1_column + " as varchar) as " + s1_column.split(".")[-1])
            final_columns.append("cast(" + s2_column + " as varchar) as " + s2_column.split(".")[-1])
            condition_list.append(s1_column + ' = ' + s2_column)
        recon_policy = DATATYPE_MANAGER["recon_policy"]
        reconciliation_amount_dict = recon_policy["reconciliation_amount"]
        reconciliation_amount_status_dict = recon_policy["reconciliation_amount_status"]
        value_to_matched = job_conf["value_to_matched"]
        value_comparison_list = value_to_matched["value"]["source1"]
        s1_tmp = get_value_comparison(f_table1_alias + "." + table1_alias + "___", value_comparison_list)
        value_comparison_list = value_to_matched["value"]["source2"]
        s2_tmp = get_value_comparison(f_table2_alias + "." + table2_alias + "___", value_comparison_list)
        final_columns.extend(s1_tmp[2])
        final_columns.extend(s2_tmp[2])
        reconcile_query += " select " + ", ".join(final_columns)
        math_operation = value_to_matched["operation"]
        if math_operation not in reconciliation_amount_dict["operation_available"].values():
            LOGGER.debug(f'Invalid reconciliation operation selected "{math_operation}"')
            sys.exit(1)
        job_description = job_conf["job_description"]
        job_recon_policy = job_description["recon_policy"]
        recon_precision = job_recon_policy["recon_precision"]
        recon_type = job_recon_policy["recon_type"]
        reconciliation_amount_string = reconciliation_amount_dict["query"].format(source1_value=s1_tmp[1],
                                                                                  recon_operation=math_operation,
                                                                                  source2_value=s2_tmp[1],
                                                                                  recon_precision=recon_precision)
        recon_condition_dict = reconciliation_amount_status_dict[recon_type]
        if recon_type == "exact-match":
            recon_condition = recon_condition_dict["query"].format(reconciliation_amount=reconciliation_amount_string)
        elif recon_type == "percentage-range-match":
            lower_value = recon_condition_dict["lower_value"].format(source_value=s1_tmp[1],
                                                                     range_value=job_recon_policy["range_value"],
                                                                     recon_precision=recon_precision)
            upper_value = recon_condition_dict["upper_value"].format(source_value=s1_tmp[1],
                                                                     range_value=job_recon_policy["range_value"],
                                                                     recon_precision=recon_precision)
            recon_condition = recon_condition_dict["query"].format(reconciliation_amount=reconciliation_amount_string,
                                                                   lower_value=lower_value,
                                                                   upper_value=upper_value)
        elif recon_type == "value-range-match":
            lower_value = recon_condition_dict["lower_value"].format(source_value=s1_tmp[1],
                                                                     range_value=job_recon_policy["range_value"],
                                                                     recon_precision=recon_precision)
            upper_value = recon_condition_dict["upper_value"].format(source_value=s1_tmp[1],
                                                                     range_value=job_recon_policy["range_value"],
                                                                     recon_precision=recon_precision)
            recon_condition = recon_condition_dict["query"].format(reconciliation_amount=reconciliation_amount_string,
                                                                   lower_value=lower_value,
                                                                   upper_value=upper_value)

        reconciliation_amount_status = reconciliation_amount_status_dict["column"].format(condition=recon_condition)
        reconcile_query += ", " + reconciliation_amount_string + " as " + reconciliation_amount_dict["name_as"]
        reconcile_query += ", " + reconciliation_amount_status
        reconcile_query += ", '" + run_date + "' as run_date, "
        reconcile_query += " '" + run_id + "' as run_id from "
        reconcile_query += f_table1_alias
        reconcile_query += " full outer join " + f_table2_alias

        final_query = final_query + " " + reconcile_query + " on " + " and ".join(condition_list)
        LOGGER.debug(f's3_data1_query - {s3_data1_query}')
        LOGGER.debug(f's3_data2_query - {s3_data2_query}')
        LOGGER.debug(f'final_query - {final_query}')

        try:
            with open(source_path.joinpath(job_conf["source1"] + '_source.json'), 'r') as source_reader:
                source1_conf = json.load(source_reader)
            with open(source_path.joinpath(job_conf["source2"] + '_source.json'), 'r') as source_reader:
                source2_conf = json.load(source_reader)
            repair_partition(source1_conf, source_path, pipeline, sleep_time)
            repair_partition(source2_conf, source_path, pipeline, sleep_time)

            linux_timestamp = str(int(time.time()))

            if debug_query:
                LOGGER.debug("executing s3_data1_query")
                s1_data_path = "/tmp/" + table1_alias + "_" + linux_timestamp + ".csv"
                s3_data1 = pipeline > AthenaSource(s3_data1_query)
                s3_data1 > RenameColumn(
                    get_column_names(s3_data1.as_dataframe(), alias=table1_alias + "___")) > FileSink(s1_data_path)
                LOGGER.debug("s3_data1_query execution completed")
                LOGGER.debug("executing s3_data2_query")
                s2_data_path = "/tmp/" + table2_alias + "_" + linux_timestamp + ".csv"
                s3_data2 = pipeline > AthenaSource(s3_data2_query)
                s3_data2 > RenameColumn(
                    get_column_names(s3_data2.as_dataframe(), alias=table2_alias + "___")) > FileSink(s2_data_path)
                LOGGER.debug("s3_data2_query execution completed")
                LOGGER.debug("executing final_query")
                final_data_path = "/tmp/" + job_description["job_name"] + "_" + linux_timestamp + ".csv"
                pipeline > AthenaSource(final_query) > FileSink(final_data_path)
                LOGGER.debug("final_query execution completed")
                exit()

            job_s3_path = CONF_DICT["table_out_path"]
            bucket, path = get_bucket_and_path(job_s3_path)

            try:
                table_dict = DATATYPE_MANAGER["table"]
                default_s3_table_datatype = table_dict["default_s3_table_datatype"]
                create_query = table_dict["create_query"]
                column_detail = table_dict["column_detail"]
                table_properties = table_dict["table_properties"]
                skip_header = table_properties["skip_header"].format(no_of_lines=table_dict["no_of_lines"])
                s3_uri = table_dict["s3_uri"]
                table_reference_string = table_dict["back_quoted_table_reference"]
                catalog_name = common_config["catalog_name"]
                database_name = common_config["database_name"]

                # Uploading data and creating tables.

                LOGGER.debug("executing s3_data1_query")
                s1_upload_path = path + "/" + job_description["job_name"] + "___"
                s1_upload_path += table1_alias + "/" + job_description["job_name"] + "___" + table1_alias
                s1_upload_path += "_" + run_id + ".csv"
                s1_upload_path = s1_upload_path.replace("//", "/")
                s1_data = pipeline > AthenaSource(s3_data1_query)
                s1_data = s1_data > RenameColumn(
                    get_column_names(s1_data.as_dataframe(), alias=table1_alias + "___")) > FileSink(
                    f"s3://{bucket}/{s1_upload_path}")
                LOGGER.debug("s3_data1_query execution completed")

                LOGGER.debug("executing s3_data2_query")
                s2_upload_path = path + "/" + job_description["job_name"] + "___"
                s2_upload_path += table2_alias + "/" + job_description["job_name"] + "___" + table2_alias
                s2_upload_path += "_" + run_id + ".csv"
                s2_upload_path = s2_upload_path.replace("//", "/")
                s2_data = pipeline > AthenaSource(s3_data2_query)
                s2_data = s2_data > RenameColumn(
                    get_column_names(s2_data.as_dataframe(), alias=table2_alias + "___")) > FileSink(
                    f"s3://{bucket}/{s2_upload_path}")
                LOGGER.debug("s3_data2_query execution completed")

                LOGGER.debug("executing final_query")
                final_upload_path = path + "/" + job_description["job_name"] + "/"
                final_upload_path += job_description["job_name"] + "_" + run_id + ".csv"
                final_upload_path = final_upload_path.replace("//", "/")
                final_data = pipeline > AthenaSource(final_query) > FileSink(f"s3://{bucket}/{final_upload_path}")
                LOGGER.debug("final_query execution completed")

                job_table_exist = False
                check_table_exist_query = common_config["check_table_exist_query"].format(
                    catalog_name=catalog_name,
                    database_name=database_name,
                    table_name=job_description["job_name"]
                )
                LOGGER.debug(check_table_exist_query)
                table_exist_data = pipeline > AthenaSource(check_table_exist_query)

                if table_exist_data.as_dataframe().shape[0] == 3:
                    job_table_exist = True
                if job_table_exist:
                    LOGGER.debug("Job related tables already exists!!")
                else:
                    final_table_reference = table_reference_string.format(catalog_name=catalog_name,
                                                                          database_name=database_name,
                                                                          table_name=job_description["job_name"])
                    column_details = [column_detail.format(column_name=column, column_type=default_s3_table_datatype)
                                      for column in final_data.as_dataframe().columns]
                    column_details = ", ".join(column_details)
                    final_s3_uri = s3_uri.format(bucket=bucket,
                                                 s3_path="/".join(final_upload_path.split("/")[:-1]))

                    final_create_query = create_query.format(table_name=final_table_reference,
                                                             column_details=column_details,
                                                             s3_uri=final_s3_uri,
                                                             table_properties=skip_header)
                    LOGGER.debug(final_create_query)
                    pipeline > AthenaSource(final_create_query)

                    s1_table_name = job_description["job_name"] + "___" + table1_alias
                    s1_table_reference = table_reference_string.format(catalog_name=catalog_name,
                                                                       database_name=database_name,
                                                                       table_name=s1_table_name)
                    s1_column_details = [column_detail.format(column_name=column, column_type=default_s3_table_datatype)
                                         for column in s1_data.as_dataframe().columns]
                    s1_column_details = ", ".join(s1_column_details)
                    s1_s3_uri = s3_uri.format(bucket=bucket,
                                              s3_path="/".join(s1_upload_path.split("/")[:-1]))
                    s1_create_query = create_query.format(table_name=s1_table_reference,
                                                          column_details=s1_column_details,
                                                          s3_uri=s1_s3_uri,
                                                          table_properties=skip_header)
                    LOGGER.debug(s1_create_query)
                    pipeline > AthenaSource(s1_create_query)

                    s2_table_name = job_description["job_name"] + "___" + table2_alias
                    s2_table_reference = table_reference_string.format(catalog_name=catalog_name,
                                                                       database_name=database_name,
                                                                       table_name=s2_table_name)
                    s2_column_details = [column_detail.format(column_name=column, column_type=default_s3_table_datatype)
                                         for column in s2_data.as_dataframe().columns]
                    s2_column_details = ", ".join(s2_column_details)
                    s2_s3_uri = s3_uri.format(bucket=bucket,
                                              s3_path="/".join(s2_upload_path.split("/")[:-1]))
                    s1_create_query = create_query.format(table_name=s2_table_reference,
                                                          column_details=s2_column_details,
                                                          s3_uri=s2_s3_uri,
                                                          table_properties=skip_header)
                    LOGGER.debug(s1_create_query)
                    pipeline > AthenaSource(s1_create_query)

                if final_data.as_dataframe().empty:
                    LOGGER.debug("No Records found")
                    sys.exit(0)
                else:
                    run_id = run_info["run_id"]
                    run_date = run_info["run_date"]
                    job_name = job_description["job_name"]
                    date = date_generator(job_description["filter"])
                    job_info_filepath = CONF_DICT["common_config"]["job_info"]["job_info_filepath"].format(
                        job_name=job_name,
                        date=date.strftime("%Y-%m-%d"))
                    if job_info_filepath[-1] != "/":
                        job_info_filepath = job_info_filepath + "/"
                    LOGGER.debug(job_info_filepath)
                    tmp = Path("/tmp")
                    df = pd.DataFrame(data=[[run_id, run_date]], columns=["run_id", "run_date"])
                    file_name = "job_info_" + job_name + "_" + run_id + ".csv"
                    file_path = str(tmp.joinpath(file_name))
                    df.to_csv(file_path, index=False)
                    pipeline > FileSource(file_path) > FileSink(job_info_filepath + file_name)
                    table_reference = CONF_DICT["common_config"]["job_info"]["job_info_table"]
                    add_partition_query = CONF_DICT["common_config"]["job_info"]["add_partition_query"].format(
                        table_reference=table_reference,
                        job_name=job_name,
                        date=date.strftime("%Y-%m-%d")
                    )
                    LOGGER.debug(add_partition_query)
                    pipeline > AthenaSource(add_partition_query)
            except ClientError as e:
                LOGGER.debug(e)
                sys.exit(1)

        except (OSError, OperationalError) as error:
            LOGGER.debug(error)
            sys.exit(1)
        time.sleep(sleep_time)
        get_latest_data(job_description["job_name"])


if __name__ == '__main__':
    run(job_conf_path=sys.argv[1], source_path=sys.argv[2])
