import sys
import os
import time
import json
import base64
import logging
import requests
from pandas import DataFrame
import pandas as pd
from croniter import croniter
import pyathena
from botocore.exceptions import ClientError
from pyathena.error import OperationalError
from github import GithubException
from fast_api.git_utils import GitUtils
from fast_api.common import DATATYPE_MANAGER, DATATYPE_MAPPER
from fast_api.aws_s3_utils import S3Utils


def get_bucket_and_path(full_path):
    """
    get_bucket_and_path
    """
    path_split = full_path.replace("s3://", "").split("/")
    bucket = path_split[0]
    s3_path = "/".join(path_split[1:])
    return bucket, s3_path


def dataframe_difference(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Find rows which are different between two DataFrames.
    :param df1: first dataframe.
    :param df2: second dataframe.
    :return:    if there is different between both dataframes.
    """
    comparison_df = df1.merge(df2, indicator=True, how='outer')
    diff_df = comparison_df[comparison_df['_merge'] != 'both']
    return diff_df


def compare_two_lists(list1: list, list2: list, logger: logging = None) -> bool:
    """
    Compare two lists and logs the difference.
    :param list1: first list.
    :param list2: second list.
    :param logger: logging instance
    :return:      if there is difference between both lists.
    """
    if logger:
        logger.debug(f'list1 -  {bool(list1)}, list2 - {bool(list2)}')
    if (bool(list1) is False) and (bool(list2) is False):
        result = True
    elif bool(list1) != bool(list2):
        result = False
    else:
        df1 = pd.DataFrame(list1)
        df2 = pd.DataFrame(list2)
        diff = dataframe_difference(df1, df2)
        result = len(diff) == 0
        if not result and logger:
            logger.debug(f'There are {len(diff)} differences:\n{diff.head()}')
    return result


def build_filter_value(condition_operator, filter_values, date_format, filter_value_string):
    if condition_operator == "in":
        tmp = ",".join([filter_value_string.format(
            value=value, format=date_format) for value in filter_values])
    else:
        tmp = filter_value_string.format(
            value=filter_values, format=date_format)
    return tmp


def build_condition(filter_dict, column_dict, datatype_mapper, logger, previous_filter):
    if (previous_filter["type"] == "start" and filter_dict["type"] == "operation") or \
            (previous_filter["type"] == "filter" and filter_dict["type"] == "filter") or \
            (previous_filter["type"] == "combine" and filter_dict["type"] == "operation") or \
            (previous_filter["type"] == "operation" and filter_dict["type"] == "operation") or \
            (previous_filter["type"] == "filter" and filter_dict["type"] == "combine") or \
            (previous_filter["type"] == "combine" and filter_dict["type"] == "combine"):
        logger.debug(
            f'previous filter - {previous_filter} current filter - {filter_dict}')
        logger.debug("wrong filter configuration structure ")
        sys.exit(1)

    previous_filter = filter_dict
    condition_string = ""

    def mode(m):
        tmp = ""
        if not m:
            tmp = ' not '
        return tmp

    value = filter_dict['value']

    if filter_dict["type"] == "filter":

        filter_available = datatype_mapper[column_dict[value["column_name"]]
                                           ["type"]]["filter_available"]
        if not value["condition_operator"] in filter_available:
            logger.debug(f'wrong filter selected for  {value["column_name"]} '
                         f'{column_dict[value["column_name"]]["type"]} type - {value["condition_operator"]}')
            return False, "", previous_filter
        date_format = datatype_mapper.get(
            column_dict[value["column_name"]]["type"], {}).get("date_format", "")
        filter_value_string = datatype_mapper[column_dict[value["column_name"]]
                                              ['type']]["filter_value"]
        filter_value = build_filter_value(condition_operator=value["condition_operator"],
                                          filter_values=value["filter_values"],
                                          date_format=date_format,
                                          filter_value_string=filter_value_string)
        if value["condition_operator"] in DATATYPE_MANAGER["filter_query"].keys():
            condition_string = DATATYPE_MANAGER["filter_query"][value["condition_operator"]]["query"]

        else:
            condition_string = DATATYPE_MANAGER["filter_query"]["default"]["query"]
        condition_string = condition_string.format(column_name=column_dict[value["column_name"]]["cast_cond"],
                                                   filter_mode=mode(
                                                       value['filter_mode']),
                                                   condition_operator=value["condition_operator"],
                                                   filter_value=filter_value)
    elif filter_dict["type"] == "operation":
        if value not in DATATYPE_MANAGER["filter_operation_available"].values():
            logger.debug(f'Available operation {DATATYPE_MANAGER["filter_query"]["operation_available"].values()}. '
                         f'Wrong operation selected - {value}')
            return False, "", previous_filter
        condition_string += value
    else:
        for item in value:
            status, partial_condition_string, previous_filter = build_condition(item, column_dict,
                                                                                datatype_mapper,
                                                                                logger,
                                                                                previous_filter)
            if not status:
                return False, "", previous_filter
            condition_string += " " + partial_condition_string + " "

        condition_string = DATATYPE_MANAGER["filter_query"]["combine"]["query"].format(
            condition_string=condition_string)

    return True, condition_string, previous_filter


def build_condition_string(filters, column_dict, datatype_mapper, logger):
    condition_string = ""
    previous_filter = {"type": "start"}

    for filter_dict in filters:
        status, partial_condition_string, previous_filter = build_condition(filter_dict,
                                                                            column_dict,
                                                                            datatype_mapper,
                                                                            logger,
                                                                            previous_filter)
        if not status:
            return False, ""
        condition_string += " " + partial_condition_string
    return status, condition_string


def build_view_query(union_conf, git_access_code, git_dags_conf_repo, git_dags_conf_ref, logger):
    view_name = union_conf['table_name']
    operator = union_conf['operator']
    operator = " " + operator + " "
    query = ""
    git_utils = GitUtils(access_code=git_access_code)
    for source in union_conf['sources']:
        source_name = source['source_name']
        source_content = git_utils.get_file(
            repository_name=git_dags_conf_repo,
            branch_name=git_dags_conf_ref,
            file_path=source_name + "_source.json")
        logger.debug(source_content)
        source['source_conf'] = json.loads(source_content)
    columns_matched = False
    for i in range(len(union_conf['sources']) - 1):
        columns_in_sequence1 = union_conf['sources'][i]['source_conf']['columns_in_sequence']
        columns_in_sequence2 = union_conf['sources'][i +
                                                     1]['source_conf']['columns_in_sequence']
        static_column1 = union_conf['sources'][i]['source_conf'].get(
            'static_column', [])
        static_column2 = union_conf['sources'][i +
                                               1]['source_conf'].get('static_column', [])
        status1 = compare_two_lists(
            columns_in_sequence1, columns_in_sequence2, logger)
        status2 = compare_two_lists(static_column1, static_column2, logger)
        status = (status1 and status2)
        if not status:
            break
    else:
        columns_matched = True
    status = columns_matched
    if columns_matched:
        view_name = DATATYPE_MANAGER["table"]["table_name"].format(
            table_name=view_name)
        query += "create view " + "{database_reference}." + view_name + " as "
        table_query_list = list()
        for source in union_conf['sources']:
            source_conf = source['source_conf']
            source_type = source_conf.get("source_type")
            file_name_filter = source_conf.get("file_name_filter", "")
            cast_selector = "s3" if source_type == "s3" else "table"
            for column in source_conf["columns_in_sequence"]:
                column_name = column["name"]
                date_format = column.get("format", "")
                cast_col = DATATYPE_MAPPER[column["type"]]["view_" + cast_selector + "_cast_ref"].format(
                    reference_name=source_conf["source_name"],
                    column_name=column_name,
                    column_type=column["type"],
                    format=date_format)
                cast_col += " as " + column_name
                cast_cond = DATATYPE_MAPPER[column["type"]]["view_" + cast_selector + "_cond_cast_ref"].format(
                    reference_name=source_conf["source_name"],
                    column_name=column_name,
                    column_type=column["type"],
                    format=date_format)
                column.update(
                    {"cast_col": cast_col,
                     "cast_cond": cast_cond
                     })

            column_dict = {
                column["name"]: column for column in source_conf["columns_in_sequence"]}
            filters = source['filters']
            status, final_condition = build_condition_string(
                filters, column_dict, DATATYPE_MANAGER, logger)
            if not status:
                break
            cast_columns = [column_dict[column["name"]]["cast_col"] for column in
                            source_conf["columns_in_sequence"]]

            def get_static_colum(static_column):
                tmp = ""
                static_date_format = DATATYPE_MAPPER[static_column["type"]].get(
                    "format", "")
                tmp += DATATYPE_MAPPER[static_column["type"]]["static_column"].format(value=static_column["value"],
                                                                                      format=static_date_format)
                static_column_name = static_column["name"]
                tmp += " as " + static_column_name

                return tmp
            if source_conf.get("source_type") == "s3" or source_conf.get("source_type") == "view":
                reference = "{database_reference}"
            else:
                source_catalog_name = source_conf["catalog_name"]
                source_database_name = source_conf["database_name"]
                reference = source_catalog_name + "." + source_database_name

            cast_columns.extend([get_static_colum(column)
                                for column in source_conf["static_column"]])
            final_query = "select " + ", ".join(cast_columns) + " from "
            final_query += reference + "." + source_conf['table_name'] + " as "
            final_query += source_conf["source_name"]
            if final_condition and file_name_filter:
                final_query += " where ( " + final_condition + \
                    " ) and \"$path\" like '" + file_name_filter + "'"
            elif final_condition:
                final_query += " where " + final_condition
            elif file_name_filter:
                final_query += " where " + " \"$path\" like '" + file_name_filter + "'"
            # logger.info(final_query)
            table_query_list.append(final_query)

        union_query = operator.join(table_query_list)
        query += union_query
        # logger.info(query)

    return query, status


def fill_template(template_path, params):
    template_content = ""
    with open(template_path, "rt", encoding="utf-8") as file_reader:
        template_content += file_reader.read()
        for key in params:
            template_content = template_content.replace(key, params[key])
    return template_content


def check_cron_string(val):
    valid_strings = ("@once", "@hourly", "@daily",
                     "@weekly", "@monthly", "@yearly")
    if val:
        cron_string = val.strip()
    return cron_string in valid_strings or croniter.is_valid(cron_string)


def _encode_base64(string: str) -> str:
    """_encode_base64

    Args:
        string (str): string

    Returns:
        string: base64 encoded string
    """
    combine_string_bytes = string.encode("ascii")
    encode_string_bytes = base64.b64encode(combine_string_bytes)
    encode_string = encode_string_bytes.decode("ascii")
    return encode_string


def delete_job(job_config: dict, conf_dict: dict, sleep_time: int,  logger: logging = None) -> bool:

    if not logger:
        logger = logging.getLogger(__name__)
    job_name = job_config["job_description"]["job_name"]
    source1 = job_config["source1"]
    source2 = job_config["source2"]

    job_file_name = job_name + "_job" + ".json"
    try:
        git_utils = GitUtils(access_code=conf_dict["git_access_code"])
        prod_query_file_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                    branch_name=conf_dict["git_dags_conf_ref"],
                                                    file_path=conf_dict["prod_query_conf"])

        prod_query_dict = json.loads(prod_query_file_string)
        prod_query_dict.pop(job_name)
        comment = f"Deleted Query - {job_name}"
        prod_file_content = json.dumps(prod_query_dict, indent=4)
        git_utils.update_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=conf_dict["prod_query_conf"],
                              comment=comment, file_content=prod_file_content)
        dev_query_file_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                   branch_name=conf_dict["git_dags_conf_ref"],
                                                   file_path=conf_dict["dev_query_conf"])

        dev_query_dict = json.loads(dev_query_file_string)
        dev_query_dict.pop(job_name)
        comment = f"Deleted Query - {job_name}"
        dev_file_content = json.dumps(dev_query_dict, indent=4)
        git_utils.update_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=conf_dict["dev_query_conf"],
                              comment=comment, file_content=dev_file_content)

        comment = "Deleting job config file - " + job_file_name
        logger.debug(comment)
        git_utils.delete_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name,
                              comment=comment)
        dag_file_name = job_name + ".py"

        comment = "Deleting dag file - " + dag_file_name
        logger.debug(comment)
        git_utils.delete_file(repository_name=conf_dict["git_dags_repo"],
                              branch_name=conf_dict["git_dags_repo_ref"], file_path=dag_file_name,
                              comment=comment)
    except GithubException as error:
        logger.debug(error)
        return False

    set_environment = conf_dict["hartree_set_environment"]
    logger.debug(f'{set_environment}')
    for env_var in set_environment:
        os.environ[env_var] = set_environment[env_var]
    logger.debug(f'{os.environ}')
    table_dict = DATATYPE_MANAGER["table"]
    drop_table = table_dict["drop_table"]
    common_config = conf_dict["common_config"]
    catalog_name = common_config["catalog_name"]
    database_name = common_config["database_name"]
    table_reference_string = table_dict["back_quoted_table_reference"]
    final_table_reference = table_reference_string.format(
        catalog_name=catalog_name, database_name=database_name,
        table_name=job_name)
    s1_table_name = job_name + "___" + source1
    s1_table_reference = table_reference_string.format(
        catalog_name=catalog_name, database_name=database_name,
        table_name=s1_table_name)
    s2_table_name = job_name + "___" + source2
    s2_table_reference = table_reference_string.format(
        catalog_name=catalog_name, database_name=database_name,
        table_name=s2_table_name)

    try:
        conn = pyathena.connect()
        cursor = conn.cursor()
        s3_utils = S3Utils(logger=logger)
        job_s3_path = conf_dict["table_out_path"]
        bucket, path = get_bucket_and_path(job_s3_path)
        logger.debug(drop_table.format(table_name=final_table_reference))
        cursor.execute(drop_table.format(table_name=final_table_reference))
        time.sleep(sleep_time)
        conn.commit()
        final_job_path = (path + "/" + job_name).replace("//", "/")
        logger.debug(f"deleting all files - s3://{bucket}/{final_job_path}")
        s3_utils.delete_all_files_from_folder(bucket_name=bucket,
                                              prefix=final_job_path,
                                              match_prefix=False)
        time.sleep(sleep_time)

        logger.debug(drop_table.format(table_name=s1_table_reference))
        cursor.execute(drop_table.format(table_name=s1_table_reference))
        time.sleep(sleep_time)
        conn.commit()
        s1_job_path = (path + "/" + s1_table_name).replace("//", "/")
        logger.debug(f"deleting all files - s3://{bucket}/{s1_job_path}")
        s3_utils.delete_all_files_from_folder(bucket_name=bucket,
                                              prefix=s1_job_path,
                                              match_prefix=False)
        time.sleep(sleep_time)

        logger.debug(drop_table.format(table_name=s2_table_reference))
        cursor.execute(drop_table.format(table_name=s2_table_reference))
        time.sleep(sleep_time)
        conn.commit()
        s2_job_path = (path + "/" + s2_table_name).replace("//", "/")
        logger.debug(f"deleting all files - s3://{bucket}/{s2_job_path}")
        s3_utils.delete_all_files_from_folder(bucket_name=bucket,
                                              prefix=s2_job_path,
                                              match_prefix=False)
        time.sleep(sleep_time)

        job_info_table_reference = conf_dict["common_config"]["job_info"]["job_info_table"]
        tmp = conf_dict["common_config"]["job_info"]["drop_partition_query"]
        drop_partition_query = tmp.format(table_reference=job_info_table_reference,
                                          job_name=job_name)
        logger.debug(drop_partition_query)
        cursor.execute(drop_partition_query)
        time.sleep(sleep_time)
        conn.commit()
        job_info_s3path = conf_dict["common_config"]["job_info"]["job_info_s3path"]
        job_info_bucket, job_info_s3_path = get_bucket_and_path(
            job_info_s3path)
        job_info_s3_path = (job_info_s3_path +
                            f"/job_name={job_name}/").replace("//", "/")
        logger.debug(
            f"deleting all files - s3://{job_info_bucket}/{job_info_s3_path}")
        s3_utils.delete_all_files_from_folder(bucket_name=job_info_bucket,
                                              prefix=job_info_s3_path,
                                              match_prefix=False)
        time.sleep(sleep_time)
        
        airflow_url = conf_dict["airflow_url"]
        combine_string = conf_dict["airflow_username"] + \
            ":" + conf_dict["airflow_password"]
        encode_string = _encode_base64(combine_string)
        url = conf_dict["delete_dag_url"].format(
            airflow_url=airflow_url, dag_id=job_name)
        logger.debug("airflow dag delete url - %s", url)
        try:
            response = requests.delete(
                url, headers={"Authorization": "Basic " + encode_string}, timeout=60)
        except requests.exceptions.SSLError:
            response = requests.delete(url, headers={
                                       "Authorization": "Basic " + encode_string}, verify=False, timeout=60)
        if response.status_code != 204:
            logger.error("Unable to delete dag - %s", job_name)
            logger.error(response.text)
        else:
            logger.error("Deleted dag - %s", job_name)
    except (ClientError, OperationalError, requests.exceptions.ConnectionError) as error:
        logger.debug(error)
        return False
    return True
