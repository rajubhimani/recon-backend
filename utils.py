import sys
import logging
import re
import pyathena
from pyathena.error import (
    OperationalError, NotSupportedError, DataError, DatabaseError)
import pandas as pd
LOGGER = None

def get_default_logger(level: int = None, log_format: str = None) -> logging.Logger:
    """get_default_logger

    Args:
        level (int, optional): _description_. Defaults to None.
        log_format (str, optional): _description_. Defaults to None.

    Returns:
        logging.Logger: _description_
    """
    global LOGGER
    level = level if level else logging.INFO
    if LOGGER:
        logger = LOGGER
    else:
        logger = logging.getLogger(__name__)
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        # handler.setLevel(level)
        log_format = log_format if log_format else "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(log_format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        LOGGER = logger
    return logger


def build_table_source_config(catalog_name: str, database_name: str, table_name: str,
                              logger: logging = None) -> dict:
    """get_source_config

    Args:
        catalog_name (str): athena catalog_name
        database_name (str): database_name
        table_name (str): table_name
        logger (logging, optional): logger. Defaults to None.

    Returns:
        dict: source config
    """    """"""
    if not logger:
        logger = get_default_logger()
    conn = pyathena.connect()
    cursor = conn.cursor()
    catalog_name = catalog_name.lower()
    database_name = database_name.lower()
    table_name = table_name.lower()
    logger.info("Selected schema - %s.%s.%s", database_name,table_name,catalog_name)
    source_dict = dict()
    source_dict.update({
        "source_type": "table",
        "source_name": catalog_name + "__" + database_name + "__" + table_name,
        "catalog_name": catalog_name,
        "database_name": database_name,
        "table_name": table_name
    })
    source_dict["columns_in_sequence"] = list()
    try:
        if catalog_name.lower() == "awsdatacatalog":
            query = f"SELECT * FROM information_schema.columns WHERE"
            query += f" table_schema = '{database_name}' AND table_name = '{table_name}'"
            query += f" AND table_catalog = '{catalog_name}'"
            logger.info(query)
            cursor.execute(query)
            logger.debug(cursor.description)
            columns = [column[0] for column in cursor.description]

            data = cursor.fetchall()
            dataframe = pd.DataFrame(
                data, columns=[column_name for column_name in columns])
            if dataframe.empty:
                logger.error("Table Not found - %s.%s.%s", catalog_name, database_name, table_name)
                return None
        else:
            query = f"SELECT * FROM \"{catalog_name}\".\"{database_name}\".\"{table_name}\" limit 1"
            logger.info(query)
            cursor.execute(query)
            logger.debug(cursor.description)
            columns = ["column_name", "data_type",
                    "col3", "col4", "col5", "col6", "col7"]
            dataframe = pd.DataFrame(cursor.description, columns=columns)
            dataframe["extra_info"] = None
    except (OperationalError, NotSupportedError, DataError, DatabaseError):
        logger.error("Table Not found - %s.%s.%s", catalog_name, database_name, table_name)
        return None
    for index, row in dataframe.iterrows():
        logger.debug(f"column sequuence - {index}")
        logger.debug(f"column info - {row.to_dict()}")
        column = dict()
        column["name"] = row["column_name"]
        column["type"] = row["data_type"][:row["data_type"].index(
            "(")] if "(" in row["data_type"] else row["data_type"]
        column["partitioned"] = True if "partition key" == row["extra_info"] else False
        if 'date' == column["type"]:
            column["format"] = "%Y-%m-%d"
        elif 'timestamp' == column["type"]:
            column["format"] = "%Y-%m-%d %H:%i:%S.%f"
        elif column["type"].startswith("row"):
            column["type"] = "row"
        elif column["type"].startswith("map"):
            column["type"] = "map"
        elif column["type"].startswith("array"):
            column["type"] = "array"
        source_dict["columns_in_sequence"].append(column)
    source_dict["static_column"] = []
    return source_dict


def build_s3_source_config(s3_file_path: str, logger: logging = None) -> dict:
    """_summary_

    Args:
        s3_file_path (str): s3_file_path
        logger (logging, optional): logger. Defaults to None.

    Returns:
        dict: source conf
    """
    if not logger:
        logger = get_default_logger()
    try:
        if s3_file_path.endswith(".gz"):
            data = pd.read_csv(s3_file_path, parse_dates=True,
                            infer_datetime_format=True, nrows=10000,
                            compression='gzip')
        else:
            data = pd.read_csv(s3_file_path, parse_dates=True,
                            infer_datetime_format=True, nrows=10000)
    except FileNotFoundError:
        logger.error("File Not Found Error - %s", s3_file_path)
        return None

    source_dict = dict()
    splitted = s3_file_path.split("/")
    source_dict.update({
        "source_type": "s3",
        "source_name": "source_name",
        "table_name": "table_name",
        "source_s3_path": "/".join(splitted[:-1]),
        "file_name_filter": splitted[-1],
    })
    source_dict["columns_in_sequence"] = list()
    for index, dtype in data.dtypes.items():
        logger.debug(f"column name - '{index}', type - '{dtype}'")
        column = dict()
        col_regex = r"[a-z_][a-z0-9_]*"
        column["name"] = "_".join(re.findall(col_regex, index.lower()))
        column["partitioned"] = False
        if pd.api.types.is_datetime64_any_dtype(dtype):
            column["type"] = "timestamp"
        elif pd.api.types.is_integer_dtype(dtype):
            column["type"] = "bigint"
        elif pd.api.types.is_float_dtype(dtype):
            column["type"] = "double"
        else:
            column["type"] = "varchar"
        source_dict["columns_in_sequence"].append(column)
    source_dict["static_column"] = []
    return source_dict
