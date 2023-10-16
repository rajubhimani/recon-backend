"""
job_runner Module
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path, PurePath
import pandas as pd
from botocore.exceptions import ClientError
from pyathena import connect
from pyathena.error import OperationalError
from pytz import timezone
from pendulum import DateTime
from job_runner.common import CONF_DICT, DATATYPE_MANAGER, LOGGER
from job_runner.get_latest_data import get_latest_data
from job_runner.job_utils import (date_generator, get_bucket_and_path, get_column_names,
                                  get_value_comparison, grouping_query, repair_partition,
                                  select_query)
from job_runner.aws_s3_utils import upload_file

DEBUG_QUERY = False


def run(job_conf_path: str, source_path: str, schedule_as_run_id: bool = False, schedule_info: dict = None, api_cache: bool = True):
    """
    run
    """
    sleep_time = 20
    LOGGER.debug('pwd')
    os.system('pwd')
    LOGGER.debug(f'{job_conf_path} {source_path}')
    with open(job_conf_path, "r", encoding="utf-8") as job_reader:
        job_conf = json.load(job_reader)
        LOGGER.debug(f'{job_conf}')
        job_description = job_conf["job_description"]
        job_name = job_description["job_name"]
        common_config = CONF_DICT["common_config"]
        if job_name in CONF_DICT["external_hartree_environment"]:
            set_environment = CONF_DICT["external_hartree_environment"][job_name]
        else:
            set_environment = CONF_DICT["hartree_set_environment"]
        LOGGER.debug(f'{set_environment}')
        for env_var in set_environment:
            os.environ[env_var] = set_environment[env_var]
        LOGGER.debug(f'{os.environ}')

        job_filter_type = job_description["filter"]["type"]
        if job_filter_type == "all":
            iter_param = "dummy"
            iter_list = [1]
        else:
            parameters = list(
                DATATYPE_MANAGER["job_filter_policy"][job_filter_type].keys())
            parameters.remove("source1")
            parameters.remove("source2")
            iter_param = parameters[0]
            start_iter = job_description["filter"][iter_param]
            iter_dict = job_description["recon_policy"].get(
                "iterate", {})
            iter_step = iter_dict.get("step", 1)
            no_of_steps = iter_dict.get("no_of_steps", 1)
            end_iter = start_iter + \
                (iter_step * (no_of_steps-1)) + int((iter_step/abs(iter_step)))
            iter_list = list(range(start_iter, end_iter, iter_step))
        LOGGER.debug(f"Iterate {iter_param} - {iter_list}")
        for i in iter_list:
            job_description["filter"][iter_param] = i
            LOGGER.debug(f'{job_description["filter"]}')

            final_query = ""
            source_path = PurePath(source_path)
            time_zone = CONF_DICT["time_zone"]
            est = timezone(time_zone)
            current_datetime = datetime.now(est)
            LOGGER.info("schedule_info - %s", schedule_info)
            if schedule_as_run_id:
                # 2018-01-01T00:00:00+00:00
                if len(iter_list) > 1:
                    schedule = current_datetime.strftime(common_config["schedule_read_date_format"])
                elif schedule_info["external_trigger"]:
                    schedule = schedule_info["ts"]
                else:
                    schedule = schedule_info["data_interval_end"]
                LOGGER.debug("schedule date string - %s", schedule)
                try:
                    LOGGER.debug("Parsing schedule format - %s",
                                 common_config["schedule_read_date_format"])
                    schedule = datetime.strptime(
                        schedule, common_config["schedule_read_date_format"]).astimezone(est)
                except ValueError:
                    LOGGER.debug("Parsing schedule format - %s",
                                 common_config["schedule_read_date_format_fraction"])
                    schedule = datetime.strptime(
                        schedule, common_config["schedule_read_date_format_fraction"]).astimezone(est)

                run_id = schedule.strftime(common_config["run_id_schedule_date_format"])
                file_name_sufix = schedule.strftime(common_config["sufix_schedule_date_format"])
            else:
                run_id = str(uuid.UUID(int=int(datetime.utcnow().strftime(common_config["run_id_date_format"]))))
                file_name_sufix = run_id
            LOGGER.info("run_id - %s", run_id)
            run_date = current_datetime.strftime(
                common_config["python_log_time_format"])
            job_run_date = current_datetime.strftime(
                common_config["python_job_run_date_format"])
            run_info = {
                "run_id": run_id,
                "run_date": run_date,
                "job_run_date": job_run_date
            }
            data1_pre_query, data1_query, data1_columns_list, s3_data1_query = select_query(
                job_conf, source_path, "source1", run_info, time_zone)

            table1_alias = job_conf["source1"]

            data2_pre_query, data2_query, data2_columns_list, s3_data2_query = select_query(
                job_conf, source_path, "source2", run_info, time_zone)

            table2_alias = job_conf["source2"]

            LOGGER.info(
                f"Source '{table1_alias}' selected column - {data1_columns_list}")
            f_table1_alias = table1_alias
            final_query += "with pre_" + table1_alias + \
                " as (" + data1_pre_query + ")"

            LOGGER.info(
                f"Source '{table2_alias}' selected column - {data2_columns_list}")
            final_query += ", pre_" + table2_alias + \
                " as ( " + data2_pre_query + " ) "

            final_query += ", " + table1_alias + " as ( " + data1_query + " ) "
            final_columns = []
            aggregation_status = job_conf["aggregation"]["status"]
            if aggregation_status:
                agg1_query, column_list = grouping_query(
                    job_conf, "source1", source_path)
                LOGGER.info(
                    f"Final {table1_alias} column after aggregation - {column_list}")
                final_query += ", g_" + table1_alias + \
                    " as ( " + agg1_query + " ) "
                final_columns.append(table1_alias + "___row_count")
                f_table1_alias = "g_" + f_table1_alias
            else:
                final_columns.append("1 as " + table1_alias + "___row_count")

            final_query += ", " + table2_alias + " as ( " + data2_query + " ) "
            f_table2_alias = table2_alias
            if aggregation_status:
                agg2_query, column_list = grouping_query(
                    job_conf, "source2", source_path)
                LOGGER.info(
                    f"Final {table2_alias} column after aggregation - {column_list}")
                final_query += ", g_" + table2_alias + \
                    " as ( " + agg2_query + " ) "
                final_columns.append(table2_alias + "___row_count")
                f_table2_alias = "g_" + f_table2_alias
            else:
                final_columns.append("1 as " + table2_alias + "___row_count")

            reconcile_query = ""

            condition_list = []
            for key in job_conf["keys"]:
                s1_column = f_table1_alias + "." + \
                    table1_alias + "___" + key["source1"]
                s2_column = f_table2_alias + "." + \
                    table2_alias + "___" + key["source2"]
                final_columns.append(
                    "cast(" + s1_column + " as varchar) as " + s1_column.split(".")[-1])
                final_columns.append(
                    "cast(" + s2_column + " as varchar) as " + s2_column.split(".")[-1])
                condition_list.append(s1_column + ' = ' + s2_column)
            recon_policy = DATATYPE_MANAGER["recon_policy"]
            reconciliation_amount_dict = recon_policy["reconciliation_amount"]
            reconciliation_amount_status_dict = recon_policy["reconciliation_amount_status"]
            value_to_matched = job_conf["value_to_matched"]
            value_comparison_list = value_to_matched["value"]["source1"]
            s1_tmp = get_value_comparison(
                f_table1_alias + "." + table1_alias + "___", value_comparison_list)
            value_comparison_list = value_to_matched["value"]["source2"]
            s2_tmp = get_value_comparison(
                f_table2_alias + "." + table2_alias + "___", value_comparison_list)
            final_columns.extend(s1_tmp[2])
            final_columns.extend(s2_tmp[2])
            reconcile_query += " select " + ", ".join(final_columns)
            math_operation = value_to_matched["operation"]
            if math_operation not in reconciliation_amount_dict["operation_available"].values():
                LOGGER.debug(
                    f'Invalid reconciliation operation selected "{math_operation}"')
                sys.exit(1)

            job_recon_policy = job_description["recon_policy"]
            recon_precision = job_recon_policy["recon_precision"]
            recon_type = job_recon_policy["recon_type"]
            force_variance = job_recon_policy.get("force_variance", False)

            if force_variance:
                reconciliation_amount_query = reconciliation_amount_dict["query_force_variance"]
            else:
                reconciliation_amount_query = reconciliation_amount_dict["query"]

            reconciliation_amount_string = reconciliation_amount_query.format(
                source1_value=s1_tmp[1],
                recon_operation=math_operation,
                source2_value=s2_tmp[1],
                recon_precision=recon_precision)

            recon_condition_dict = reconciliation_amount_status_dict[recon_type]
            if recon_type == "exact-match":
                recon_condition = recon_condition_dict["query"].format(
                    reconciliation_amount=reconciliation_amount_string)
            elif recon_type == "percentage-range-match":
                lower_value = recon_condition_dict["lower_value"].format(
                    source_value=s1_tmp[1], range_value=job_recon_policy["range_value"],
                    recon_precision=recon_precision)
                upper_value = recon_condition_dict["upper_value"].format(
                    source_value=s1_tmp[1], range_value=job_recon_policy["range_value"],
                    recon_precision=recon_precision)
                recon_condition = recon_condition_dict["query"].format(
                    reconciliation_amount=reconciliation_amount_string, lower_value=lower_value,
                    upper_value=upper_value)
            elif recon_type == "value-range-match":
                lower_value = recon_condition_dict["lower_value"].format(
                    source_value=s1_tmp[1], range_value=job_recon_policy["range_value"],
                    recon_precision=recon_precision)
                upper_value = recon_condition_dict["upper_value"].format(
                    source_value=s1_tmp[1], range_value=job_recon_policy["range_value"],
                    recon_precision=recon_precision)
                recon_condition = recon_condition_dict["query"].format(
                    reconciliation_amount=reconciliation_amount_string, lower_value=lower_value,
                    upper_value=upper_value)

            reconciliation_amount_status = reconciliation_amount_status_dict["column"].format(
                condition=recon_condition)
            reconcile_query += ", " + reconciliation_amount_string + \
                " as " + reconciliation_amount_dict["name_as"]
            reconcile_query += ", " + reconciliation_amount_status
            reconcile_query += ", '" + run_date + "' as run_date, "
            reconcile_query += " '" + run_id + "' as run_id from "
            reconcile_query += f_table1_alias
            reconcile_query += " full outer join " + f_table2_alias

            final_query = final_query + " " + reconcile_query + \
                " on " + " and ".join(condition_list)
            # LOGGER.debug(f's3_data1_query - {s3_data1_query}')
            # LOGGER.debug(f's3_data2_query - {s3_data2_query}')
            # LOGGER.debug(f'final_query - {final_query}')

            if CONF_DICT.get("hartree_set_environment_" + job_name):
                set_environment = CONF_DICT["hartree_set_environment_" +
                                            job_name]
                for env_var in set_environment:
                    os.environ[env_var] = set_environment[env_var]
                LOGGER.debug(f'{os.environ}')
            try:
                conn = connect()
                cursor = conn.cursor()
                # repairing partition
                with open(source_path.joinpath(job_conf["source1"] + '_source.json'),
                          'r', encoding="utf-8") as source_reader:
                    source1_conf = json.load(source_reader)
                with open(source_path.joinpath(job_conf["source2"] + '_source.json'),
                          'r', encoding="utf-8") as source_reader:
                    source2_conf = json.load(source_reader)
                repair_partition(source1_conf, source_path, cursor, sleep_time)
                repair_partition(source2_conf, source_path, cursor, sleep_time)

                linux_timestamp = str(int(time.time()))
                LOGGER.debug(f"executing final_query - {final_query}")
                cursor.execute(final_query)
                LOGGER.debug("final_query execution completed")
                time.sleep(sleep_time)
                final_s3_path = cursor.output_location
                final_data = pd.read_csv(final_s3_path, dtype=str)
                if final_data.empty:
                    LOGGER.debug("No Records found")
                    sys.exit(0)
                final_data_path = "/tmp/" + \
                    job_name + "_" + linux_timestamp + ".csv"

                LOGGER.debug(f"executing s3_data1_query - {s3_data1_query}")
                cursor.execute(s3_data1_query)
                LOGGER.debug("s3_data1_query execution completed")
                time.sleep(sleep_time)

                s1_s3_path = cursor.output_location
                s1_data = pd.read_csv(s1_s3_path, dtype=str)
                s1_data_columns = get_column_names(
                    s1_data, alias=table1_alias + "___")
                s1_data.columns = s1_data_columns
                s1_data_path = "/tmp/" + table1_alias + "_" + linux_timestamp + ".csv"

                LOGGER.debug(f"executing s3_data2_query - {s3_data2_query}")
                cursor.execute(s3_data2_query)
                LOGGER.debug("s3_data2_query execution completed")
                time.sleep(sleep_time)
                s2_s3_path = cursor.output_location
                s2_data = pd.read_csv(s2_s3_path, dtype=str)
                s2_data_columns = get_column_names(
                    s2_data, alias=table2_alias + "___")
                s2_data.columns = s2_data_columns
                s2_data_path = "/tmp/" + table2_alias + "_" + linux_timestamp + ".csv"

                if DEBUG_QUERY:
                    s1_data.to_csv(s1_data_path, index=False)
                    s2_data.to_csv(s2_data_path, index=False)
                    final_data.to_csv(final_data_path, index=False)
                    LOGGER.debug("Job Execution Completed")
                    sys.exit()

                job_s3_path = CONF_DICT["table_out_path"]
                bucket, path = get_bucket_and_path(job_s3_path)

                try:
                    table_dict = DATATYPE_MANAGER["table"]
                    default_s3_table_datatype = table_dict["default_s3_table_datatype"]
                    create_query = table_dict["create_query_with_partition"]
                    column_detail = table_dict["column_detail"]
                    partition_detail = table_dict["partition_detail"]
                    table_properties = table_dict["table_properties"]
                    repair_partition_query = table_dict["repair_partition_query"]
                    partition_string_query = table_dict["partition_string_query"]
                    partition_query = table_dict["partition_query"]
                    skip_header = table_properties["skip_header"].format(
                        no_of_lines=table_dict["no_of_lines"])
                    s3_uri = table_dict["s3_uri"]
                    table_reference_string = table_dict["back_quoted_table_reference"]
                    catalog_name = common_config["catalog_name"]
                    database_name = common_config["database_name"]
                    job_run_date_partition_string = partition_string_query.format(
                        key="job_run_date", value=run_info["job_run_date"])
                    job_run_date_partitions = partition_query.format(
                        partition_string=job_run_date_partition_string)

                    # checking job tabes exist or not.
                    job_table_exist = False
                    check_table_exist_query = common_config["check_table_exist_query"].format(
                        catalog_name=catalog_name,
                        database_name=database_name,
                        job_name=job_name,
                        table1_alias=table1_alias,
                        table2_alias=table2_alias
                    )
                    LOGGER.debug(check_table_exist_query)
                    cursor.execute(check_table_exist_query)
                    if len(cursor.fetchall()) == 3:
                        job_table_exist = True

                    # Uploading data and creating tables.
                    final_data.to_csv(final_data_path, index=False)

                    job_file_name = job_name + "_" + file_name_sufix + ".csv"
                    final_upload_path = DATATYPE_MANAGER["job_path"].format(
                        path=path, job_name=job_name,
                        job_run_date=run_info["job_run_date"],
                        job_file_name=job_file_name)

                    final_upload_path = final_upload_path.replace("//", "/")
                    final_table_reference = table_reference_string.format(
                        catalog_name=catalog_name, database_name=database_name,
                        table_name=job_name)
                    if not job_table_exist:

                        column_details = [
                            column_detail.format(column_name=column,
                                                 column_type=default_s3_table_datatype)
                            for column in final_data.columns]
                        column_details = ", ".join(column_details)
                        final_s3_uri = s3_uri.format(
                            bucket=bucket,
                            s3_path="/".join(final_upload_path.split("/")[:-2]))
                        partition_details = partition_detail.format(
                            column_name="job_run_date", column_type=default_s3_table_datatype)
                        d_query = create_query.format(table_name=final_table_reference,
                                                      column_details=column_details,
                                                      partition_details=partition_details,
                                                      s3_uri=final_s3_uri,
                                                      table_properties=skip_header)
                        LOGGER.debug(d_query)
                        cursor.execute(d_query)
                        time.sleep(sleep_time)
                        conn.commit()

                    status = upload_file(
                        bucket=bucket, file_path=final_data_path, s3_file_path=final_upload_path)

                    if status:
                        final_repair_partition_query = repair_partition_query.format(
                            table_reference=final_table_reference,
                            partitions=job_run_date_partitions
                        )
                        LOGGER.debug(final_repair_partition_query)
                        cursor.execute(final_repair_partition_query)
                        time.sleep(sleep_time)
                        conn.commit()
                    else:
                        LOGGER.error("Last Upload Failed.")
                        sys.exit(1)

                    s1_data.to_csv(s1_data_path, index=False)

                    s1_job_name = job_name + "___" + table1_alias
                    s1_job_file_name = s1_job_name + "_" + file_name_sufix + ".csv"
                    s1_upload_path = DATATYPE_MANAGER["job_path"].format(
                        path=path, job_name=s1_job_name,
                        job_run_date=run_info["job_run_date"],
                        job_file_name=s1_job_file_name)
                    s1_upload_path = s1_upload_path.replace("//", "/")

                    s1_table_name = job_name + "___" + table1_alias
                    s1_table_reference = table_reference_string.format(
                        catalog_name=catalog_name, database_name=database_name,
                        table_name=s1_table_name)
                    if not job_table_exist:
                        s1_column_details = [
                            column_detail.format(column_name=column,
                                                 column_type=default_s3_table_datatype)
                            for column in s1_data.columns]
                        s1_column_details = ", ".join(s1_column_details)
                        s1_s3_uri = s3_uri.format(bucket=bucket,
                                                  s3_path="/".join(s1_upload_path.split("/")[:-2]))
                        partition_details = partition_detail.format(
                            column_name="job_run_date", column_type=default_s3_table_datatype)
                        s1_query = create_query.format(table_name=s1_table_reference,
                                                       column_details=s1_column_details,
                                                       partition_details=partition_details,
                                                       s3_uri=s1_s3_uri,
                                                       table_properties=skip_header)
                        LOGGER.debug(s1_query)
                        cursor.execute(s1_query)
                        time.sleep(sleep_time)
                        conn.commit()

                    status = upload_file(
                        bucket=bucket, file_path=s1_data_path, s3_file_path=s1_upload_path)
                    if status:
                        s1_repair_partition_query = repair_partition_query.format(
                            table_reference=s1_table_reference,
                            partitions=job_run_date_partitions
                        )
                        LOGGER.debug(s1_repair_partition_query)
                        cursor.execute(s1_repair_partition_query)
                        time.sleep(sleep_time)
                        conn.commit()
                    else:
                        LOGGER.error("Last Upload Failed.")
                        sys.exit(1)

                    s2_data.to_csv(s2_data_path, index=False)

                    s2_job_name = job_name + \
                        "___" + table2_alias
                    s2_job_file_name = s2_job_name + "_" + file_name_sufix + ".csv"
                    s2_upload_path = DATATYPE_MANAGER["job_path"].format(
                        path=path, job_name=s2_job_name,
                        job_run_date=run_info["job_run_date"],
                        job_file_name=s2_job_file_name)

                    s2_upload_path = s2_upload_path.replace("//", "/")
                    s2_table_name = job_name + \
                        "___" + table2_alias
                    s2_table_reference = table_reference_string.format(
                        catalog_name=catalog_name, database_name=database_name,
                        table_name=s2_table_name)

                    if not job_table_exist:
                        s2_column_details = [column_detail.format(
                            column_name=column, column_type=default_s3_table_datatype)
                            for column in s2_data.columns]
                        s2_column_details = ", ".join(s2_column_details)
                        s2_s3_uri = s3_uri.format(bucket=bucket,
                                                  s3_path="/".join(s2_upload_path.split("/")[:-2]))
                        partition_details = partition_detail.format(
                            column_name="job_run_date", column_type=default_s3_table_datatype)
                        s2_query = create_query.format(table_name=s2_table_reference,
                                                       column_details=s2_column_details,
                                                       partition_details=partition_details,
                                                       s3_uri=s2_s3_uri,
                                                       table_properties=skip_header)
                        LOGGER.debug(s2_query)
                        cursor.execute(s2_query)
                        time.sleep(sleep_time)
                        conn.commit()

                    status = upload_file(
                        bucket=bucket, file_path=s2_data_path, s3_file_path=s2_upload_path)
                    if status:
                        s2_repair_partition_query = repair_partition_query.format(
                            table_reference=s2_table_reference,
                            partitions=job_run_date_partitions
                        )
                        LOGGER.debug(s2_repair_partition_query)
                        cursor.execute(s2_repair_partition_query)
                        time.sleep(sleep_time)
                        conn.commit()
                    else:
                        LOGGER.error("Last Upload Failed.")
                        sys.exit(1)

                    run_id = run_info["run_id"]
                    run_date = run_info["run_date"]
                    date = date_generator(job_description["filter"], time_zone)
                    tmp = CONF_DICT["common_config"]["job_info"]["job_info_filepath"]
                    job_info_filepath = tmp.format(job_name=job_name,
                                                   date=date.strftime("%Y-%m-%d"))
                    LOGGER.debug(job_info_filepath)
                    tmp = Path("/tmp")
                    job_info_df = pd.DataFrame(data=[[run_id, run_date, job_run_date]], columns=[
                        "run_id", "run_date", "job_run_date"])
                    file_name = "job_info_" + job_name + "_" + file_name_sufix + ".csv"
                    job_info_df.to_csv(
                        str(tmp.joinpath(file_name)), index=False)
                    bucket, s3_path = get_bucket_and_path(
                        job_info_filepath + file_name)
                    upload_file(bucket=bucket, s3_file_path=s3_path,
                                file_path=str(tmp.joinpath(file_name)))
                    table_reference = CONF_DICT["common_config"]["job_info"]["job_info_table"]
                    tmp = CONF_DICT["common_config"]["job_info"]["add_partition_query"]
                    add_partition_query = tmp.format(table_reference=table_reference,
                                                     job_name=job_name,
                                                     date=date.strftime(
                                                         "%Y-%m-%d")
                                                     )
                    LOGGER.debug(add_partition_query)
                    cursor.execute(add_partition_query)
                    time.sleep(sleep_time)
                    conn.commit()
                except ClientError as error:
                    LOGGER.debug(error)
                    sys.exit(1)
                if os.path.exists(final_data_path):
                    LOGGER.debug(f"Deleting file - {final_data_path}")
                    os.remove(final_data_path)
                if os.path.exists(s1_data_path):
                    LOGGER.debug(f"Deleting file - {s1_data_path}")
                    os.remove(s1_data_path)
                if os.path.exists(s2_data_path):
                    LOGGER.debug(f"Deleting file - {s2_data_path}")
                    os.remove(s2_data_path)
                LOGGER.debug("Job Execution Completed")
            except (OSError, OperationalError) as error:
                LOGGER.debug(error)
                sys.exit(1)
            cursor.close()
            conn.close()
            time.sleep(sleep_time)
        if api_cache:
            get_latest_data(job_name)


if __name__ == '__main__':
    run(job_conf_path=sys.argv[1], source_path=sys.argv[2])
