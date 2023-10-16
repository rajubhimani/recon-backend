"""
util
"""
import calendar
import json
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from pandas.tseries.offsets import BDay
from pytz import timezone
from job_runner.aws_s3_utils import list_objects, upload_file
from job_runner.common import (
    CONF_DICT, DATATYPE_MANAGER, DATATYPE_MAPPER, LOGGER
)
from utils import build_table_source_config




def sql_round(column, precision):
    """
    sql_round
    """
    return " round(" + column + ", " + str(precision) + ") "


def find_distinct(item_list):
    """
    find_distinct
    """
    return [i for n, i in enumerate(item_list) if i not in item_list[:n]]


def is_business_date(date):
    """
    is_business_date
    """
    return bool(len(pd.bdate_range(date, date)))


def capitalize(column):
    """
    capitalize
    """
    tmp = "regexp_replace(column, '(\\w)(\\w*)', x -> upper(x[1]) || lower(x[2]))"
    return tmp.replace("column", column)


def upper(column):
    """
    upper
    """
    tmp = "upper(column)"
    return tmp.replace("column", column)


def get_bucket_and_path(full_path):
    """
    get_bucket_and_path
    """
    path_split = full_path.replace("s3://", "").split("/")
    bucket = path_split[0]
    s3_path = "/".join(path_split[1:])
    return bucket, s3_path


def date_generator(job_filter, time_zone):
    """
    date_generator
    """
    time_zone_obj = timezone(time_zone)
    if job_filter["type"] == "day-wise":
        current_date = datetime.now(
            time_zone_obj) - timedelta(days=job_filter["days_ago"])
        current_date = datetime(day=current_date.day, month=current_date.month,
                                year=current_date.year)
    elif job_filter["type"] == "prior-business-day":
        current_date = datetime.now(
            time_zone_obj) - timedelta(days=job_filter["days_ago"])
        current_date = datetime(day=current_date.day, month=current_date.month,
                                year=current_date.year)
        if not is_business_date(current_date):
            current_date = current_date - BDay(n=1)
    elif job_filter["type"] == "previous-month-last-business-day":
        current_date = datetime.now(time_zone_obj)
        current_date = datetime(
            day=1, month=current_date.month, year=current_date.year)
        current_date = current_date - relativedelta(
            months=job_filter["months_ago"]-1) - timedelta(days=1)
        if not is_business_date(current_date):
            current_date = current_date - BDay(n=1)
    elif job_filter["type"] == "previous-month-last-day":
        current_date = datetime.now(time_zone_obj)
        current_date = datetime(day=1, month=current_date.month,
                                year=current_date.year)
        current_date = current_date - relativedelta(
            months=job_filter["months_ago"]-1) - timedelta(days=1)
    elif job_filter["type"] == "previous-month-period":
        current_date = datetime.now(
            time_zone_obj) - relativedelta(months=job_filter["months_ago"])
        current_date = datetime(day=1, month=current_date.month,
                                year=current_date.year)
    else:
        current_date = datetime.now(time_zone_obj)
    return current_date


def date_filter_generator(datatype_mapper, column_details, filter_details, job_filter, time_zone,
                          table_alias=None):
    """
    date_filter_generator
    """
    time_zone_obj = timezone(time_zone)
    python_format = datatype_mapper[column_details["type"]]["python_format"]
    date_format = datatype_mapper[column_details["type"]]["date_format"]
    column_name = column_details["cast_cond"]
    if table_alias:
        column_name = table_alias + "___" + column_details["name"]
    if filter_details["type"] == "day-wise":
        current_date = datetime.now(
            time_zone_obj) - timedelta(days=job_filter["days_ago"])
        current_date = datetime(
            day=current_date.day, month=current_date.month, year=current_date.year)
        filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=current_date.strftime(python_format), format=date_format)
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name, filter_value=filter_value)
    elif filter_details["type"] == "min-max":
        min_dt = job_filter["min"]
        max_dt = job_filter["max"]
        min_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=min_dt, format=date_format)
        max_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=max_dt, format=date_format)
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name,
            min_filter_value=min_filter_value,
            max_filter_value=max_filter_value)
    elif filter_details["type"] == "window":
        min_dt = datetime.now(time_zone_obj) - \
            timedelta(days=job_filter["days_ago"])
        min_dt = datetime(day=min_dt.day, month=min_dt.month, year=min_dt.year)
        min_dt = min_dt.strftime(python_format)
        max_dt = datetime.now(time_zone_obj).strftime(python_format)
        min_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=min_dt, format=date_format)
        max_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=max_dt, format=date_format)
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name, min_filter_value=min_filter_value,
            max_filter_value=max_filter_value
        )
    elif filter_details["type"] == "prior-business-day-period":
        current_date = datetime.now(
            time_zone_obj) - timedelta(days=job_filter["days_ago"])
        if not is_business_date(current_date):
            current_date = current_date - BDay(n=1)
        max_dt = current_date
        min_dt = datetime(day=1, month=max_dt.month, year=max_dt.year)
        max_dt = max_dt.strftime(python_format)
        min_dt = min_dt.strftime(python_format)
        min_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=min_dt, format=date_format)
        max_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=max_dt, format=date_format)
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name, min_filter_value=min_filter_value,
            max_filter_value=max_filter_value
        )
    elif filter_details["type"] == "prior-business-day":
        current_date = datetime.now(
            time_zone_obj) - timedelta(days=job_filter["days_ago"])
        prior_business_date = datetime(
            day=current_date.day, month=current_date.month, year=current_date.year)
        if not is_business_date(prior_business_date):
            prior_business_date = current_date - BDay(n=1)
        filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=prior_business_date.strftime(python_format),
            format=date_format)
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name,
            filter_value=filter_value)
    elif filter_details["type"] == "previous-month-period":
        current_date = datetime.now(time_zone_obj
                                    ) - relativedelta(months=job_filter["months_ago"])
        week_day, max_day = calendar.monthrange(
            current_date.year, current_date.month)
        max_dt = datetime(day=max_day, month=current_date.month,
                          year=current_date.year, hour=23, minute=59, second=59)
        min_dt = datetime(day=1, month=current_date.month,
                          year=current_date.year)
        max_dt = max_dt.strftime(python_format)
        min_dt = min_dt.strftime(python_format)
        min_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=min_dt, format=date_format)
        max_filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=max_dt, format=date_format)
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name,
            min_filter_value=min_filter_value,
            max_filter_value=max_filter_value
        )
    elif filter_details["type"] == "previous-month-last-business-day":
        current_date = datetime.now(time_zone_obj)
        current_date = datetime(
            day=1, month=current_date.month, year=current_date.year)
        current_date = current_date - \
            relativedelta(
                months=job_filter["months_ago"]-1) - timedelta(days=1)
        if not is_business_date(current_date):
            current_date = current_date - BDay(n=1)
        filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=current_date.strftime(python_format),
            format=date_format
        )
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name,
            filter_value=filter_value
        )
    elif filter_details["type"] == "previous-month-last-day":
        current_date = datetime.now(time_zone_obj)
        current_date = datetime(
            day=1, month=current_date.month, year=current_date.year)
        current_date = current_date - \
            relativedelta(
                months=job_filter["months_ago"]-1) - timedelta(days=1)
        filter_value = datatype_mapper[column_details["type"]]["filter_value"].format(
            value=current_date.strftime(python_format),
            format=date_format
        )
        job_filter_query = DATATYPE_MANAGER["filter_query"][filter_details["type"]]["query"].format(
            column_name=column_name,
            filter_value=filter_value
        )

    return job_filter_query


def create_value_string(alias, value_comparison, previous_comparison):
    """
    create_value_string
    """
    value_comparisons_string = ""
    value_column_list = []
    if ((previous_comparison["type"] == "column" and value_comparison["type"] == "column") or
            (previous_comparison["type"] == "numeric" and value_comparison["type"] == "numeric") or
            (previous_comparison["type"] == "operation" and value_comparison["type"] == "operation") or
            (previous_comparison["type"] == "combine" and value_comparison["type"] == "operation") or
            ((previous_comparison["type"] == "column" or previous_comparison["type"] == "numeric" or
              previous_comparison["type"] == "combine") and
             value_comparison["type"] == "combine") or
            (previous_comparison["type"] == "start" and value_comparison["type"] == "operation")):
        LOGGER.debug(
            f'{previous_comparison["type"]} {value_comparison["type"]}')
        LOGGER.debug(f'{value_comparison}')
        LOGGER.debug("wrong value comparison configuration structure")
        return value_comparisons_string, False
    previous_comparison = value_comparison
    if value_comparison["type"] == "column":
        value_comparisons_string += alias + value_comparison["value"]
        value_column_list.append(alias + value_comparison["value"])
    elif value_comparison["type"] == "numeric":
        value_comparisons_string += str(value_comparison["value"])
    elif value_comparison["type"] == "operation":
        value_comparisons_string += " " + value_comparison["value"] + " "
    else:
        value_comparisons_string += " ( "
        for item in value_comparison["value"]:
            partial_status, pvcs, pcvl, previous_comparison = create_value_string(
                alias, item, previous_comparison)
            partial_value_comparisons_string = pvcs
            partial_column_value_list = pcvl
            if partial_status:
                value_comparisons_string += partial_value_comparisons_string
                value_column_list.extend(partial_column_value_list)
            else:
                return False, value_comparisons_string, partial_column_value_list
        value_comparisons_string += " ) "
    return True, value_comparisons_string, value_column_list, previous_comparison


def get_value_comparison(alias, value_comparison_list):
    """
    get_value_comparison
    """
    value_comparisons_string = ""
    status = False
    previous_filter = {"type": "start"}
    value_column_list = []
    for value_comparison in value_comparison_list:
        partial_status, pvcs, pcvl, previous_filter = create_value_string(
            alias, value_comparison, previous_filter)
        partial_value_comparisons_string = pvcs
        partial_column_value_list = pcvl
        if partial_status:
            value_column_list.extend(partial_column_value_list)
            value_comparisons_string += partial_value_comparisons_string
        else:
            break
    else:
        status = True
    return status, value_comparisons_string, value_column_list


class Lookup:
    """Lookup
    """

    def __init__(self, source_path, source_name, conf_dict):
        self.source_path = source_path
        self.source_name = source_name
        self.conf_dict = conf_dict
        with open(self.source_path.joinpath(self.source_name + '_source.json'),
                  'r', encoding="utf-8") as lookup_reader:
            self.lookup_conf = json.load(lookup_reader)
        self.lookup_source_name = self.lookup_conf["source_name"]
        self.lookup_type = self.lookup_conf["source_type"]

    def update_lookup(self):
        """_summary_
        """
        lookup_name = self.lookup_conf.get("lookup_name", None)
        if lookup_name:
            lookup_url = self.conf_dict["lookup_url"]
            source_s3_path = self.conf_dict["lookup_out_path"]
            bucket, lookup_s3_path = get_bucket_and_path(source_s3_path)
            url = lookup_url.format(lookup_name=lookup_name)
            LOGGER.debug("requesting - " + url)
            response = requests.get(url, verify=False, timeout=120)
            if response.status_code != 200:
                LOGGER.error(f"Lookup request failed - {url}")
                sys.exit(1)
            else:
                temp_path = "/tmp/"
                lookup_s3_path = lookup_s3_path + "/" + \
                    self.lookup_source_name + "/" + self.lookup_source_name + ".csv"
                lookup_s3_path = lookup_s3_path.replace("//", "/")
                temp_file_path = os.path.join(
                    temp_path, self.lookup_source_name + ".csv")
                LOGGER.debug(f'{temp_file_path}  {lookup_s3_path}')
                with open(temp_file_path, "wt", encoding="utf-8") as file_writer:
                    file_writer.write(response.text)
                try:
                    data = pd.read_csv(temp_file_path)
                    if data.empty:
                        LOGGER.error(f"No data in '{lookup_name}'")
                        sys.exit(1)
                except (pd.errors.ParserError, pd.errors.EmptyDataError) as error:
                    LOGGER.error(
                        f"Invalid data in '{lookup_name}', error - {str(error)}")
                    sys.exit(1)
                LOGGER.debug(f"temporary file path - {temp_file_path}")
                LOGGER.debug(
                    f"Uploading to bucket - {bucket} and path - {lookup_s3_path}")
                status = upload_file(bucket=bucket, file_path=temp_file_path,
                                     s3_file_path=lookup_s3_path)
                if not status:
                    LOGGER.error(
                        f"Upload Failed to bucket - {bucket} and path - {lookup_s3_path}")
                    sys.exit(1)

    def get_lookup_dict(self):
        """_summary_

        Returns:
            dict: lookup_column_dict
        """
        common_config = self.conf_dict["common_config"]
        precision = common_config["precision"]

        lookup_cast_selector = "s3" if self.lookup_type == "s3" else "table"
        lookup_cast_selector += "_lookup"

        for column in self.lookup_conf["columns_in_sequence"]:
            column_name = column["name"]
            date_format = column.get("format", "")
            cast_cond = DATATYPE_MAPPER[column["type"]][lookup_cast_selector + "_cast_ref"].format(
                reference_name=self.lookup_source_name, column_name=column_name,
                column_type=column["type"], precision=precision, format=date_format)

            column.update(
                {
                    "cast_cond": cast_cond
                }
            )
        lookup_column_dict = {
            column["name"]: column for column in self.lookup_conf["columns_in_sequence"]}
        self.lookup_conf["lookup_column_dict"] = lookup_column_dict
        return lookup_column_dict


def build_filter_value(condition_operator, filter_values, date_format, filter_value_string,
                       source_path, column_type):
    """
    build_filter_value
    """
    if condition_operator == "in":
        tmp = ",".join([filter_value_string.format(
            value=value, format=date_format) for value in filter_values])
    elif condition_operator == "in_with_column":
        catalog_name = CONF_DICT["common_config"]["catalog_name"]
        database_name = CONF_DICT["common_config"]["database_name"]
        # updated maping file
        lookup_obj = Lookup(source_path=source_path, source_name=filter_values["source_name"],
                            conf_dict=CONF_DICT)
        lookup_obj.update_lookup()
        lookup_column_dict = lookup_obj.get_lookup_dict()
        filter_column_type = lookup_column_dict[filter_values["column_name"]]["type"]
        filter_core_type = DATATYPE_MANAGER[filter_column_type]["input_type"]
        column_core_type = DATATYPE_MANAGER[column_type]["input_type"]
        if filter_core_type != column_core_type:
            LOGGER.debug('incompatible filter column types')
            LOGGER.debug(
                f"column_type - {column_core_type} and filter_column_type - {filter_column_type}")
        table_reference_string = DATATYPE_MANAGER["table"]["table_reference"]
        sub_table_reference = table_reference_string.format(
            catalog_name=catalog_name, database_name=database_name,
            table_name=filter_values["source_name"]
        )
        filter_value_string = DATATYPE_MANAGER["filter_query"][condition_operator]["filter_value"]
        tmp = lookup_column_dict[filter_values["column_name"]]["cast_cond"]
        tmp = filter_value_string.format(
            column_name=tmp, table_name=sub_table_reference)
    else:
        tmp = filter_value_string.format(
            value=filter_values, format=date_format)
    return tmp


def build_condition(filter_dict, column_dict, datatype_mapper, job_filter, table_alias,
                    previous_filter, source_path, time_zone):
    """
    build_condition
    """
    filter_column_list = []
    # if (previous_filter["type"] == "start" and filter_dict["type"] == "operation") or \
    #         (previous_filter["type"] == "filter" and filter_dict["type"] == "filter") or \
    #         (previous_filter["type"] == "combine" and filter_dict["type"] == "operation") or \
    #         (previous_filter["type"] == "operation" and filter_dict["type"] == "operation") or \
    #         (previous_filter["type"] == "filter" and filter_dict["type"] == "combine") or \
    #         (previous_filter["type"] == "combine" and filter_dict["type"] == "combine"):
    #     LOGGER.debug(
    #         f'previous filter - {previous_filter} current filter - {filter_dict}')
    #     LOGGER.debug("wrong filter configuration structure ")
    #     sys.exit(1)
    LOGGER.debug("filter_dict - %s", filter_dict)
    previous_filter = filter_dict
    condition_string = ""

    def mode(mode):
        tmp = ""
        if not mode:
            tmp = ' not '
        return tmp

    value = filter_dict['value']

    if filter_dict["type"] == "filter":
        column_type = column_dict[value["column_name"]]["type"]
        tmp = DATATYPE_MANAGER["job_filter_compatible_date_filter"][job_filter["type"]]
        if value["condition_operator"] in tmp:
            compatible_datatype = DATATYPE_MANAGER["date_filter_compatible_datatype"]
            if column_type not in compatible_datatype:
                LOGGER.debug(
                    f' Datatype "{column_type}" is not compatible with '
                    '"{value["condition_operator"]}" filter')
                sys.exit(1)
            filter_column_list.append(value["column_name"])
            column_details = column_dict[value["column_name"]]
            filter_details = {
                "type": value["condition_operator"]
            }
            condition_string = mode(value['filter_mode']) + date_filter_generator(
                datatype_mapper, column_details, filter_details, job_filter, time_zone,
                table_alias)
        else:
            filter_available = datatype_mapper[column_dict[value["column_name"]]
                                               ["type"]]["filter_available"]
            if not value["condition_operator"] in filter_available:
                LOGGER.debug(f'wrong filter selected for  {value["column_name"]} '
                             f'{column_type} type - {value["condition_operator"]}')
                sys.exit(1)
            filter_column_list.append(value["column_name"])
            date_format = datatype_mapper.get(
                column_type, {}).get("date_format", "")
            filter_value_string = datatype_mapper[column_dict[value["column_name"]]
                                                  ['type']]["filter_value"]
            LOGGER.debug(
                f'Building filter value for column - {value["column_name"]}')
            filter_value = build_filter_value(
                condition_operator=value["condition_operator"],
                filter_values=value["filter_values"], date_format=date_format,
                filter_value_string=filter_value_string, source_path=source_path,
                column_type=column_type)
            if value["condition_operator"] in DATATYPE_MANAGER["filter_query"].keys():
                tmp = DATATYPE_MANAGER["filter_query"][value["condition_operator"]]["query"]
                condition_string = tmp
            else:
                condition_string = DATATYPE_MANAGER["filter_query"]["default"]["query"]
            if value["condition_operator"] == "in_with_column":
                operation = "in"
            else:
                operation = value["condition_operator"]
            condition_string = condition_string.format(
                column_name=table_alias + "___" + value["column_name"],
                filter_mode=mode(value['filter_mode']),
                condition_operator=operation, filter_value=filter_value)
    elif filter_dict["type"] == "operation":
        if value not in DATATYPE_MANAGER["filter_operation_available"].values():
            tmp = DATATYPE_MANAGER["filter_query"]["operation_available"].values(
            )
            LOGGER.debug(
                f'Available operation {tmp}. Wrong operation selected - {value}')
            sys.exit(1)
        condition_string += value
    else:
        for item in value:
            partial_condition_string, partial_filter_column_list, previous_filter = build_condition(
                item, column_dict, datatype_mapper, job_filter, table_alias, previous_filter,
                source_path, time_zone)

            condition_string += " " + partial_condition_string + " "
            filter_column_list.extend(partial_filter_column_list)

        condition_string = DATATYPE_MANAGER["filter_query"]["combine"]["query"].format(
            condition_string=condition_string)

    return condition_string, filter_column_list, previous_filter


def build_condition_string(filters, column_dict, datatype_mapper,
                           job_filter, table_alias, source_path, time_zone):
    """
    build_condition_string
    """
    condition_string = ""
    previous_filter = {"type": "start"}
    filter_column_list = []

    for filter_dict in filters:
        partial_condition_string, partial_filter_column_list, previous_filter = build_condition(
            filter_dict, column_dict, datatype_mapper, job_filter, table_alias, previous_filter,
            source_path, time_zone)
        condition_string += " " + partial_condition_string
        filter_column_list.extend(partial_filter_column_list)
    return condition_string, filter_column_list


def build_subquery_condition(filter_dict, column_dict, datatype_mapper,
                             condition_query, table_reference, table_alias,
                             previous_filter, operation_required):
    """
    build_subquery_condition
    """
    filter_column_list = []
    if (previous_filter["type"] == "start" and filter_dict["type"] == "filter"
        and operation_required) or \
            (previous_filter["type"] == "filter" and filter_dict["type"] == "filter") or \
            (previous_filter["type"] == "combine" and filter_dict["type"] == "operation") or \
            (previous_filter["type"] == "operation" and filter_dict["type"] == "operation") or \
            (previous_filter["type"] == "filter" and filter_dict["type"] == "combine") or \
            (previous_filter["type"] == "combine" and filter_dict["type"] == "combine"):
        LOGGER.debug(
            f'previous filter - {previous_filter} current filter - {filter_dict}')
        LOGGER.debug("wrong filter configuration structure ")
        sys.exit(1)

    previous_filter = filter_dict
    condition_string = ""

    def mode(mode):
        tmp = ""
        if not mode:
            tmp = ' not '
        return tmp

    value = filter_dict['value']

    if filter_dict["type"] == "filter":
        filter_available = datatype_mapper[column_dict[value["column_name"]]
                                           ["type"]]["sub_query_filter_available"]
        if not value["condition_operator"] in filter_available:
            LOGGER.debug(f'wrong filter selected for  {value["column_name"]} '
                         f'{column_dict[value["column_name"]]["type"]} type')
            sys.exit(1)
        filter_column_list.append(value["column_name"])
        if condition_query:
            condition_string = DATATYPE_MANAGER["filter_query"]["sub_query"]["query_cond"]

        else:
            condition_string = DATATYPE_MANAGER["filter_query"]["sub_query"]["query"]
        condition_string = condition_string.format(
            column_name=table_alias + "___" + value["column_name"],
            filter_mode=mode(value['filter_mode']),
            condition_operator=value["condition_operator"],
            table_name="pre_" + table_alias,
            condition_query=condition_query
        )
    elif filter_dict["type"] == "operation":
        if value not in DATATYPE_MANAGER["filter_operation_available"].values():
            tmp = DATATYPE_MANAGER["filter_query"]["operation_available"].values(
            )
            LOGGER.debug(
                f'Available operation {tmp}. Wrong operation selected - {value}')
            sys.exit(1)
        condition_string += value
    else:
        for item in value:
            partial_condition_string, partial_filter_column_list, previous_filter = build_subquery_condition(
                item, column_dict, datatype_mapper, condition_query, table_reference, table_alias,
                previous_filter, operation_required)
            condition_string += " " + partial_condition_string + " "
            filter_column_list.extend(partial_filter_column_list)
        condition_string = datatype_mapper["filter_query"]["combine"]["query"].format(
            condition_string=condition_string)

    return condition_string, filter_column_list, previous_filter


def build_subquery_condition_string(subquery_filters, operation_required, column_dict,
                                    datatype_mapper, condition_query, table_reference,
                                    table_alias):
    """
    build_subquery_condition_string
    """
    condition_string = ""
    previous_filter = {"type": "start"}
    filter_column_list = []
    for filter_dict in subquery_filters:
        partial_condition_string, partial_filter_column_list, previous_filter = build_subquery_condition(
            filter_dict, column_dict, datatype_mapper, condition_query, table_reference,
            table_alias, previous_filter, operation_required)
        condition_string += " " + partial_condition_string
        filter_column_list.extend(partial_filter_column_list)
    return condition_string, filter_column_list


def select_query(job_conf, source_path, source_no, run_info, time_zone):
    """
    select_query
    """
    run_id = run_info["run_id"]
    run_date = run_info["run_date"]
    with open(source_path.joinpath(job_conf[source_no] + '_source.json'),
              'r', encoding="utf-8") as source_reader:
        source_conf = json.load(source_reader)
        if source_conf["source_type"] == "table":
            temp = build_table_source_config(
                source_conf["catalog_name"],
                source_conf["database_name"],
                source_conf["table_name"], LOGGER)
            if temp is None:
                LOGGER.error("Source not found - %s", job_conf[source_no])
                sys.exit(1)
            source_conf["columns_in_sequence"] = temp["columns_in_sequence"]
    table_alias = source_conf["source_name"]
    common_config = CONF_DICT["common_config"]
    precision = common_config["precision"]
    source_type = source_conf.get("source_type")
    if source_type == "s3" or source_conf.get("source_type") == "view":
        catalog_name = common_config["catalog_name"]
        database_name = common_config["database_name"]
    else:
        catalog_name = source_conf["catalog_name"]
        database_name = source_conf["database_name"]
    cast_selector = "s3" if source_type == "s3" else "table"
    for column in source_conf["columns_in_sequence"]:
        column_name = column["name"]
        date_format = column.get("format", "")
        LOGGER.debug(f"{column_name} - {date_format}")
        cast_col = DATATYPE_MAPPER[column["type"]][cast_selector + "_cast_ref"].format(
            reference_name=table_alias, column_name=column_name, column_type=column["type"],
            precision=precision, format=date_format)
        cast_col += " as " + table_alias + "___" + column_name
        cast_cond = DATATYPE_MAPPER[column["type"]][cast_selector + "_cast_ref"].format(
            reference_name=table_alias, column_name=column_name, column_type=column["type"],
            precision=precision, format=date_format)
        column.update({"cast_col": cast_col, "cast_cond": cast_cond})
    column_dict = {
        column["name"]: column for column in source_conf["columns_in_sequence"]}

    pre_column_list = []
    pre_condition_list = []
    column_list = []
    condition_list = []
    pre_column_list.extend(
        [column_dict[key[source_no]]["cast_col"]
         for key in job_conf["keys"] if key[source_no] in column_dict])
    column_list.extend(
        [table_alias + "___" + key[source_no]
         for key in job_conf["keys"] if key[source_no] in column_dict])

    value_comparison_list = job_conf["value_to_matched"]["value"][source_no]
    _, _, values_column_list = get_value_comparison("", value_comparison_list)
    column_list.extend([table_alias + "___" + values_column for values_column in values_column_list
                        if values_column in column_dict])
    values_column_list = [column_dict[values_column]["cast_col"]
                          for values_column in values_column_list
                          if values_column in column_dict]
    pre_column_list.extend(values_column_list)

    if source_conf.get('static_column', None):
        for static_column in source_conf['static_column']:
            tmp = ""
            date_format = DATATYPE_MAPPER[static_column["type"]].get(
                "format", "")
            tmp += DATATYPE_MAPPER[static_column["type"]]["static_column"].format(
                value=static_column["value"], format=date_format)
            cast_cond = tmp
            static_column_name = static_column["name"]
            tmp += " as " + table_alias + "___" + static_column_name
            cast_col = tmp
            pre_column_list.append(tmp)
            column_dict[static_column_name] = static_column
            column_dict[static_column_name].update(
                {"cast_col": cast_col, "cast_cond": cast_cond})
            column_list.append(table_alias + "___" + static_column_name)
    for mapper in job_conf["mapping"][source_no]:
        grp_col_query = ""
        grp_column_name = mapper["column_name"]
        pre_column_list.append(column_dict[grp_column_name]["cast_col"])
        column_list.append(table_alias + "___" + grp_column_name)
        grp_col_query += "case "

        for condition in mapper["group_condition"]:
            grp_col_query += "when " + \
                column_dict[grp_column_name]["cast_cond"]
            date_format = DATATYPE_MAPPER[column_dict[grp_column_name]["type"]].get(
                "format", "")
            tmp = DATATYPE_MAPPER[column_dict[grp_column_name]["type"]]["mapper_value"].format(
                value=condition["value"], format=date_format)
            grp_col_query += " = " + tmp + " "
            date_format = DATATYPE_MAPPER[mapper["type"]].get("format", "")
            tmp = DATATYPE_MAPPER[mapper["type"]]["mapper_value"].format(
                value=condition["group_value"], format=date_format)
            grp_col_query += "then " + tmp + " "
        if mapper.get("default"):
            date_format = DATATYPE_MAPPER[mapper["type"]].get("format", "")
            tmp = DATATYPE_MAPPER[mapper["type"]]["mapper_value"].format(
                value=mapper["default"], format=date_format)
            grp_col_query += " else " + tmp + " end "
        else:
            if mapper["type"] == column_dict[grp_column_name]["type"]:
                grp_col_query += " else " + \
                    column_dict[grp_column_name]["cast_cond"] + " end "
            else:
                grp_col_query += " else "
                grp_col_query += DATATYPE_MAPPER[mapper["type"]
                                                 ]["mapper_default_value"] + " end "

        grp_col_query += " as " + table_alias + "___"
        grp_col_query += mapper["group_name"]
        pre_column_list.append(grp_col_query)
        column_list.append(table_alias + "___" + mapper["group_name"])
    custom_column_dict = {}
    for custom_column in job_conf["custom_column"][source_no]:
        custom_query = custom_column["custom_column_query"]
        custom_query += " as " + table_alias + "___"
        custom_query += custom_column["column_name"]
        pre_column_list.append(custom_query)
        custom_column_dict[custom_column["column_name"]] = {
            "cast_col": custom_query,
            "cast_cond": custom_column["custom_column_query"],
            "type": custom_column.get("type", None)
        }
        column_list.append(table_alias + "___" + custom_column["column_name"])
    column_dict.update(custom_column_dict)
    if source_conf.get("file_name_filter", None):
        file_name_query = " \"$path\" like '%" + \
            source_conf["file_name_filter"].replace("*", "%") + "'"
        pre_column_list.append("\"$path\" as " + table_alias + "___path")
        pre_condition_list.append(file_name_query)
        column_list.append(table_alias + "___path")

    filters = job_conf["filters"][source_no]
    job_desc = job_conf["job_description"]
    job_filter = job_desc["filter"]
    table_condition_query, filter_columns = build_condition_string(
        filters, column_dict, DATATYPE_MAPPER, job_filter,
        table_alias, source_path, time_zone)

    job_filter_query = ""
    if job_filter["type"] != "all":
        job_filter_query = date_filter_generator(datatype_mapper=DATATYPE_MAPPER,
                                                 column_details=column_dict[job_filter[source_no]],
                                                 filter_details=job_filter,
                                                 job_filter=job_filter,
                                                 time_zone=time_zone
                                                 )
    table_reference_string = DATATYPE_MANAGER["table"]["table_reference"]
    table_reference = table_reference_string.format(
        catalog_name=catalog_name,
        database_name=database_name,
        table_name=source_conf["table_name"]
    )
    table_reference += " as " + table_alias
    subquery_filters = job_conf["subquery_filters"][source_no]
    operation_required = len(filters) > 0

    table_subquery_condition_query, subquery_filter_columns = build_subquery_condition_string(
        subquery_filters,
        operation_required,
        column_dict,
        DATATYPE_MAPPER,
        "",
        table_reference,
        table_alias)

    column_list.extend(
        [table_alias + "___" + filter_column for filter_column in filter_columns])

    column_list.extend(
        [table_alias + "___" + subquery_filter_column
         for subquery_filter_column in subquery_filter_columns])
    filter_columns = [column_dict[filter_column]["cast_col"]
                      for filter_column in filter_columns]
    subquery_filter_columns = [column_dict[subquery_filter_column]["cast_col"]
                               for subquery_filter_column in subquery_filter_columns]

    pre_column_list.extend(filter_columns)
    pre_column_list.extend(subquery_filter_columns)
    if table_subquery_condition_query and table_condition_query:
        condition_list.append(table_condition_query +
                              table_subquery_condition_query)
    elif table_condition_query:
        condition_list.append(table_condition_query)
    elif table_subquery_condition_query:
        condition_list.append(table_subquery_condition_query)

    lookup_columns = []
    join_list = []
    all_lookup_column_dict = {}
    for lookup in job_conf["lookups"][source_no]:
        lookup_obj = Lookup(source_path=source_path, source_name=lookup["source_name"],
                            conf_dict=CONF_DICT)
        lookup_obj.update_lookup()
        lookup_column_dict = lookup_obj.get_lookup_dict()
        lookup_source_name = lookup_obj.lookup_source_name
        lookup_type = lookup_obj.lookup_type
        lookup_conf = lookup_obj.lookup_conf
        all_lookup_column_dict[lookup["source_name"]] = lookup_column_dict
        lookup_conditions = []

        reference_columns = lookup["reference_columns"]
        key_columns = lookup["key_columns"]
        for count, value in enumerate(reference_columns):
            tmp = table_alias + "___" + value + " = "
            tmp += lookup_column_dict[key_columns[count]]["cast_cond"]
            lookup_conditions.append(tmp)
            pre_column_list.append(column_dict[value]["cast_col"])
            lookup_columns.append(table_alias + "___" + value)
            lookup_alias = lookup_source_name + "___" + key_columns[count]
            tmp = lookup_column_dict[key_columns[count]]["cast_cond"] + \
                " as " + table_alias + "___" + lookup_alias
            lookup_columns.append(tmp)
        if lookup_type in ("s3", "view"):
            lookup_catalog_name = common_config["catalog_name"]
            lookup_database_name = common_config["database_name"]
        else:
            lookup_catalog_name = lookup_conf["catalog_name"]
            lookup_database_name = lookup_conf["database_name"]
        lookup_table_reference = table_reference_string.format(
            catalog_name=lookup_catalog_name,
            database_name=lookup_database_name,
            table_name=lookup_conf["table_name"]
        )
        lookup_table_reference += " as " + lookup_source_name
        join_query = "left join " + lookup_table_reference + \
            " on " + " and ".join(lookup_conditions)
        if lookup.get("value_columns"):
            for value_column in lookup["value_columns"]:
                lookup_alias = lookup_source_name + "___" + value_column
                default_column = lookup.get(
                    "default_column", {}).get(value_column, None)
                if default_column:
                    tmp = "coalesce(" + \
                        lookup_column_dict[value_column]["cast_cond"] + ", "
                    tmp += table_alias + "___" + default_column + ")"
                    tmp += " as " + table_alias + "___" + lookup_alias
                else:
                    tmp = lookup_column_dict[value_column]["cast_cond"]
                    tmp += " as " + table_alias + "___" + lookup_alias
                lookup_columns.append(tmp)
        if lookup.get("conversion_factor"):
            for cf_detail in lookup.get("conversion_factor"):
                column_name = cf_detail["column_name"]
                conversion_factor_column = cf_detail["conversion_factor_column"]
                name_as = cf_detail["name_as"]
                lookup_alias = lookup_source_name + "___" + conversion_factor_column
                pre_column_list.append(column_dict[column_name]["cast_col"])
                lookup_columns.append(table_alias + "___" + column_name)
                tmp = lookup_column_dict[conversion_factor_column]["cast_cond"]
                tmp += " as " + table_alias + "___" + lookup_alias
                lookup_columns.append(tmp)
                tmp = "coalesce((" + table_alias + "___" + column_name + " * "
                tmp += lookup_column_dict[conversion_factor_column]["cast_cond"] + \
                    "), ("
                tmp += table_alias + "___" + column_name + ")" + ")"
                tmp = "round(" + tmp + "," + str(precision) + ")"
                tmp += " as " + table_alias + "___" + name_as
                lookup_columns.append(tmp)
        join_list.append(join_query)

    column_list.extend(lookup_columns)
    if job_conf.get("coalesce"):
        coalesce_cols = job_conf.get("coalesce")[source_no]
        for coalesce_col in coalesce_cols:
            coalesce_sequence = []
            for column in coalesce_cols[coalesce_col]:
                if "___" in column:
                    l_source_name = column.split("___")[0]
                    l_column_name = column.split("___")[1]
                    lookup_alias = l_source_name + "___coalesce__" + l_column_name
                    tmp = all_lookup_column_dict[l_source_name][l_column_name]["cast_cond"]
                    column_list.append(
                        tmp + " as " + table_alias + "___" + lookup_alias)
                else:
                    tmp = table_alias + "___" + column
                    pre_column_list.append(column_dict[column]["cast_col"])
                    column_list.append(table_alias + "___" + column)
                coalesce_sequence.append(tmp)
            coalesce_col_string = "coalesce(" + ", ".join(
                coalesce_sequence) + ") as " + table_alias + "___"
            coalesce_col_string += coalesce_col
            column_list.append(coalesce_col_string)

    if job_filter_query:
        # condition_list.append(job_filter_query)
        pre_condition_list.append(job_filter_query)
        pre_column_list.append(column_dict[job_filter[source_no]]["cast_col"])
        column_list.append(table_alias + "___" + job_filter[source_no])
    if job_conf.get("extra_column"):
        if job_conf["extra_column"].get(source_no):
            extra_column = job_conf["extra_column"][source_no]
            pre_column_list.extend(
                [column_dict[column]["cast_col"] for column in extra_column])
            column_list.extend(
                [table_alias + "___" + column for column in extra_column])

    column_list.append("'" + run_date + "' as run_date")
    column_list.append("'" + run_id + "' as run_id")

    column_list = find_distinct(column_list)
    condition_list = find_distinct(condition_list)
    condition_query = ""
    if condition_list:
        condition_query = "(" + ") and (".join(condition_list) + ")"
    pre_query = "select " + ", ".join(find_distinct(pre_column_list))
    pre_query += " from " + table_reference
    if pre_condition_list:
        pre_query += " where " + \
            " and ".join(find_distinct(pre_condition_list))
    query = "select " + ", ".join(column_list)
    query += " from pre_" + table_alias
    if join_list:
        query += " " + " ".join(join_list)
    if condition_query:
        query += " where " + condition_query
    column_name_list = [column.split(" as ")[-1].strip()
                        for column in column_list]

    def cast_varchar(column_string):
        if " as " in column_string:
            tmp_column_string = "cast(" + " as ".join(
                column_string.split(" as ")[:-1]).strip() + " as varchar) as "
            tmp_column_string += column_string.split(" as ")[-1].strip()
        else:
            tmp_column_string = "cast(" + \
                column_string.strip() + " as varchar) as "
            tmp_column_string += column_string.strip()
        return tmp_column_string

    data_query = "select " + \
        ", ".join([cast_varchar(column) for column in column_list])
    data_query += " from " + "from".join(query.split("from")[1:])
    data_query = "with pre_" + table_alias + \
        " as (" + pre_query + ") " + data_query
    return pre_query, query, column_name_list, data_query


def grouping_query(job_conf, source_no, source_path):
    """
    grouping_query
    """
    with open(source_path.joinpath(job_conf[source_no] + '_source.json'),
              'r', encoding="utf-8") as source_reader:
        source_conf = json.load(source_reader)
    precision = CONF_DICT["common_config"]["precision"]
    table_alias = source_conf["source_name"]
    column_list = []
    keys = [table_alias + "___" + key[source_no] for key in job_conf["keys"]]
    column_list.extend(keys)
    group_by = ", ".join(keys)

    value_comparison_list = job_conf["value_to_matched"]["value"][source_no]
    _, _, values_column_list = get_value_comparison("", value_comparison_list)

    query = ""
    aggregate_fun = job_conf["aggregation"]["operation"]
    if aggregate_fun not in DATATYPE_MANAGER["aggregate"]["operation_available"].values():
        LOGGER.error(f"Invalid Aggregate function - {aggregate_fun}")
        sys.exit(1)

    aggregate_query = DATATYPE_MANAGER["aggregate"]["query"]
    aggr_string_list = [
        aggregate_query.format(aggregate_fun=aggregate_fun,
                               column_name=table_alias + "___" + values_column,
                               precision=precision)
        for values_column in values_column_list]
    column_list.extend(aggr_string_list)
    query += "select count(*) as " + table_alias + "___row_count" + ", "
    query += ", ".join(column_list) + " from " + \
        table_alias + " group by " + group_by
    column_list = [column.split(" as ")[-1] for column in column_list]
    column_list = [(table_alias + "___row_count")] + column_list
    return query, column_list


def generate_partition_add_query(catalog_name, database_name, source_conf):
    """
    generate_partition_add_query
    """
    partition_columns = [column.get("name") for column in source_conf["columns_in_sequence"] if
                         column.get("partitioned")]
    no_of_partition = len(partition_columns)
    back_quoted_table_reference = DATATYPE_MANAGER["table"]["back_quoted_table_reference"]
    bucket, path = get_bucket_and_path(source_conf["source_s3_path"])
    prefix = path.replace("//", "/")
    prefix = prefix + "/" if prefix[-1] != "/" else prefix
    partition_list = {"/".join(key["Key"].replace(prefix, "").split("/")[:no_of_partition])
                      for key in list_objects(bucket, prefix=prefix)
                      if key["Key"].replace(prefix, "")}
    LOGGER.debug(f"{partition_list}")

    table_name = source_conf["table_name"]
    table_reference = back_quoted_table_reference.format(
        catalog_name=catalog_name,
        database_name=database_name,
        table_name=table_name
    )
    repair_partition_query = DATATYPE_MANAGER["table"]["repair_partition_query"]
    partition_query = DATATYPE_MANAGER["table"]["partition_query"]
    partition_string_query = DATATYPE_MANAGER["table"]["partition_string_query"]
    all_partition_string = ""
    for partition in partition_list:
        if "=" in partition:
            split_partitions = partition.split("/")
            split_partitions = [data for data in split_partitions if data]
            if len(split_partitions) == no_of_partition:
                key_value = {split_partition.split("=")[0]: split_partition.split("=")[1]
                             for split_partition in split_partitions}
                partition_string_list = [partition_string_query.format(
                    key=key, value=key_value[key])
                    for key in key_value]
                final_partition_string = ", ".join(partition_string_list)
                all_partition_string += partition_query.format(
                    partition_string=final_partition_string)
    query = repair_partition_query.format(
        table_reference=table_reference, partitions=all_partition_string)
    LOGGER.debug(query)
    return query


def repair_partition(source_conf, source_path, cursor, sleep_time):
    """
    repair_partition
    """
    common_config = CONF_DICT["common_config"]
    catalog_name = common_config["catalog_name"]
    database_name = common_config["database_name"]
    if source_conf["source_type"] == "view":
        source_name_list = [source["source_name"]
                            for source in source_conf["sources"]]
        for source_name in source_name_list:
            with open(source_path.joinpath(source_name + '_source.json'),
                      'r', encoding="utf-8") as file_reader:
                child_source_conf = json.load(file_reader)
                if child_source_conf.get("source_type") == "s3":
                    if any(column.get("partitioned")
                            for column in child_source_conf["columns_in_sequence"]):
                        repair_query = generate_partition_add_query(
                            catalog_name=catalog_name, database_name=database_name,
                            source_conf=child_source_conf)
                        LOGGER.debug(f"Executing query - {repair_query}")
                        cursor.execute(repair_query)
                        LOGGER.debug("Execution complete")
                        time.sleep(sleep_time)
    else:
        if source_conf.get("source_type") == "s3":
            if any(column.get("partitioned") for column in source_conf["columns_in_sequence"]):
                repair_query = generate_partition_add_query(
                    catalog_name=catalog_name, database_name=database_name,
                    source_conf=source_conf)
                LOGGER.debug(f"Executing query - {repair_query}")
                cursor.execute(repair_query)
                LOGGER.debug("Execution complete")
                time.sleep(sleep_time)


def get_column_name(column_name: str, alias: str):
    """
    get_column_name
    """
    if column_name.replace(alias, "") not in ("run_id", "run_date", "job_run_date"):
        column_name = column_name.replace(alias, "")
    return column_name


def get_column_names(data_frame: DataFrame, alias: str = "") -> list:
    """
    get_column_names
    """
    column_name_list = [get_column_name(column, alias)
                        for column in data_frame.columns]
    return column_name_list
