"""Recon Backend"""
import json
import os
import logging
from datetime import datetime, timedelta
import time
from typing import Union, Literal
import pendulum
from fastapi import BackgroundTasks, FastAPI, status, Response, Request
from fastapi import APIRouter
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger
from fastapi_redis_cache import FastApiRedisCache, cache
from pyathena import connect
from pyathena.error import (
    OperationalError, NotSupportedError, DataError, DatabaseError)
import pandas as pd
from github import GithubException
from fast_api.common import CONFIG_PATH, DATATYPE_MANAGER, DATATYPE_MAPPER
from fast_api.git_utils import GitUtils
from fast_api.api_utils import (
    build_view_query, fill_template, check_cron_string,
    delete_job
)
from utils import build_table_source_config, build_s3_source_config
from fast_api.api_models import (
    Message, SourceList, Source, View, Job, JobList, JobData, JobTimeList, LatestJobTime, DefaultJobName,
    ImportSourceList, JobConfigList, UpdateDagDetails, ReconPolicy, QuerySource,
)

DEBUG = True
if DEBUG:
    # app = FastAPI(debug=True, title="recon-backend", docs_url="/", redoc_url="/redoc")
    app = FastAPI(debug=True, title="recon-backend")
    logger.setLevel(logging.DEBUG)
else:
    app = FastAPI(title="recon-backend")
router = APIRouter(
    # prefix="/v1",
    responses={404: {"description": "Not found"}},
)


with open(CONFIG_PATH, "r", encoding="utf-8") as file_reader:
    conf_dict = json.load(file_reader)
redis_host = os.environ.get("REDIS_HOST", "localhost")
port = os.environ.get("REDIS_PORT", "6379")
REDIS_URL = f"redis://{redis_host}:{port}"
set_environment = conf_dict["hartree_set_environment"]
for env_var in set_environment:
    os.environ[env_var] = set_environment[env_var]


@app.on_event("startup")
def startup():
    """startup
    """
    time.sleep(int(os.environ.get("REDIS_DELAY", "60")))
    flush_command = f"redis-cli -h {redis_host} -p {port} flushall"
    logger.debug(f"command: {flush_command}")
    os.system(flush_command)
    redis_cache = FastApiRedisCache()
    logger.debug(f"REDIS_URL - {REDIS_URL}")
    redis_cache.init(
        host_url=os.environ.get("REDIS_URL", REDIS_URL),
        prefix="reconapi-cache",
        response_header="X-ReconAPI-Cache",
        ignore_arg_types=[Request, Response]
    )


@app.get("/", response_class=HTMLResponse)
def root():
    """root
    """
    html_content = """
    <html>
        <head>
            <title>Recon Backend</title>
        </head>
        <body>
            <h1>Welcome to Recon Backend</h1>
            <a href="/docs">Swagger UI</a>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)


# @router.delete("/clean_cache")
# def clean_cache():
#     flush_command = "redis-cli -h {host_name} -p {port} flushall".format(host_name=redis_host, port=port)
#     logger.debug(f"command: {flush_command}")
#     os.system(flush_command)
#     return {"message": "all cache cleared"}


@router.get("/source", status_code=status.HTTP_200_OK,
            response_model=Source,
            response_model_exclude_unset=True,
            responses={404: {"model": Message}}, tags=["Source"])
def get_source(source_name: str, response: Response):
    """get_source

    Args:
        source_name (str): Source Name
        response (Response): _description_

    Returns:
        Response: response
    """
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    source_dict_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                            branch_name=conf_dict["git_dags_conf_ref"],
                                            file_path=source_name + "_source.json")

    if source_dict_string:
        source_config = json.loads(source_dict_string)
        if source_config["source_type"] == "table":
            temp = build_table_source_config(
                source_config["catalog_name"],
                source_config["database_name"],
                source_config["table_name"], logger)
            source_config["columns_in_sequence"] = temp["columns_in_sequence"]
        client_response = source_config
    else:
        client_response = JSONResponse(status_code=404, content={
                                       "message": "Source Not Found - " + source_name})
        response.status_code = status.HTTP_404_NOT_FOUND
    return client_response


@router.post("/source", status_code=status.HTTP_201_CREATED,
             response_model=Message,
             response_model_exclude_unset=True,
             responses={409: {"model": Message}}, tags=["Source"])
def post_source(source: Source, response: Response):
    """post_source

    Args:
        source (Source): source object
        response (Response): response object

    Returns:
        Response: response
    """
    logger.debug(source)
    logger.debug(jsonable_encoder(source, exclude_none=True))
    common_config = conf_dict["common_config"]
    source_file_name = source.source_name + "_source" + ".json"
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    if git_utils.check_path_exists(repository_name=conf_dict["git_dags_conf_repo"],
                                   branch_name=conf_dict["git_dags_conf_ref"], file_path=source_file_name):
        client_response = JSONResponse(status_code=409, content={"message": "Source Already Exists - "
                                                                            + source.source_name})
        response.status_code = status.HTTP_409_CONFLICT
    else:
        conn = connect()
        cursor = conn.cursor()

        if source.source_type == "s3":
            if source.source_s3_path or source.lookup_name:
                if source.lookup_name:
                    source_s3_path = conf_dict["lookup_out_path"] + \
                        source.source_name
                else:
                    source_s3_path = source.source_s3_path
                table_dict = DATATYPE_MANAGER["table"]
                default_data_type = table_dict["default_s3_table_datatype"]
                table_properties = table_dict["table_properties"]
                skip_header = table_properties["skip_header"].format(
                    no_of_lines=table_dict["no_of_lines"])

                back_quoted_table_reference = table_dict["back_quoted_table_reference"].format(
                    catalog_name=common_config["catalog_name"],
                    database_name=common_config["database_name"],
                    table_name=source.table_name)
                cols_data_type = ", ".join(
                    [table_dict["column_detail"].format(column_name=column.name, column_type=default_data_type) for
                     column
                     in
                     source.columns_in_sequence if
                     not column.partitioned])

                partition_columns = [column.name for column in
                                     source.columns_in_sequence if
                                     column.partitioned]
                if len(partition_columns) > 0:
                    partition_details = ", ".join(
                        [table_dict["partition_detail"].format(column_name=column, column_type=default_data_type) for
                         column
                         in
                         partition_columns])
                    query = table_dict["create_query_with_partition"].format(table_name=back_quoted_table_reference,
                                                                             column_details=cols_data_type,
                                                                             partition_details=partition_details,
                                                                             s3_uri=source_s3_path,
                                                                             table_properties=skip_header)
                else:
                    query = table_dict["create_query"].format(table_name=back_quoted_table_reference,
                                                              column_details=cols_data_type,
                                                              s3_uri=source_s3_path,
                                                              table_properties=skip_header)

                logger.debug(query)
                cursor.execute(query)
                conn.commit()
                logger.debug("Table created successfully.")
            else:
                client_response = JSONResponse(status_code=400, content={
                                               "message": "Bad Request"})
                response.status_code = status.HTTP_400_BAD_REQUEST
                return client_response
        else:
            logger.debug("testing table source")
            query = f"show tables in {source.catalog_name}.{source.database_name} '{source.table_name}'"
            cursor.execute(query)
            if not cursor.fetchall():
                client_response = JSONResponse(status_code=400, content={
                                               "message": "Bad Request"})
                response.status_code = status.HTTP_400_BAD_REQUEST
                return client_response
        content = json.dumps(jsonable_encoder(
            source, exclude_none=True), indent=4)
        logger.debug("Uploading source conf - " + source_file_name)
        comment = "Add " + source_file_name
        git_utils.create_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=source_file_name,
                              comment=comment, file_content=content)
        client_response = JSONResponse(status_code=201, content={"message": "Source Created Successfully - "
                                                                            + source.source_name})
        response.status_code = status.HTTP_201_CREATED
    return client_response


@router.get("/view", status_code=status.HTTP_200_OK, response_model=View, response_model_exclude_unset=True,
            responses={404: {"model": Message}}, tags=["Source"])
def get_view(view_name: str, response: Response):
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    view_dict_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                          branch_name=conf_dict["git_dags_conf_ref"],
                                          file_path=view_name + "_source.json")
    if view_dict_string:
        view_config = json.loads(view_dict_string)
        client_response = view_config
    else:
        client_response = JSONResponse(status_code=404, content={
                                       "message": "Source Not Found - " + view_name})
        response.status_code = status.HTTP_404_NOT_FOUND
    return client_response


@router.post("/view", status_code=status.HTTP_201_CREATED,
             response_model=Message,
             response_model_exclude_unset=True,
             responses={409: {"model": Message}, 400: {"model": Message}}, tags=["Source"])
def create_view(view: View, response: Response):
    logger.debug(view)
    logger.debug(jsonable_encoder(view, exclude_none=True))
    source_file_name = view.source_name + "_source" + ".json"
    view = jsonable_encoder(view, exclude_none=True)
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    if git_utils.check_path_exists(repository_name=conf_dict["git_dags_conf_repo"],
                                   branch_name=conf_dict["git_dags_conf_ref"],
                                   file_path=source_file_name):
        client_response = JSONResponse(status_code=409, content={"message": "View Already Exists - "
                                                                            + view["source_name"]})
        response.status_code = status.HTTP_409_CONFLICT
    else:
        source_1_name = view["sources"][0]["source_name"] + "_source.json"
        source_1_content = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                              branch_name=conf_dict["git_dags_conf_ref"],
                                              file_path=source_1_name)
        source_1_conf = json.loads(source_1_content)
        view["columns_in_sequence"] = source_1_conf["columns_in_sequence"]
        for column in source_1_conf["static_column"]:
            del column["value"]
            column["partitioned"] = False
            column["name"] = column["name"]
            view["columns_in_sequence"].append(column)
        query, query_status = build_view_query(view, conf_dict["git_access_code"],
                                               conf_dict["git_dags_conf_repo"],
                                               conf_dict["git_dags_conf_ref"], logger)
        if query_status:
            view["view_query"] = query
            try:
                conn = connect()
                cursor = conn.cursor()
                logger.debug(query)
                common_config = conf_dict["common_config"]
                database_reference = DATATYPE_MANAGER["table"]["database_reference"].format(
                    catalog_name=common_config['catalog_name'],
                    database_name=common_config['database_name'])
                cursor.execute(query.format(
                    database_reference=database_reference))
                conn.commit()
                logger.debug("View created successfully.")
                content = json.dumps(view, indent=4)
                logger.debug("Uploading source conf - " + source_file_name)
                comment = "Add " + source_file_name
                git_utils.create_file(repository_name=conf_dict["git_dags_conf_repo"],
                                      branch_name=conf_dict["git_dags_conf_ref"], file_path=source_file_name,
                                      comment=comment, file_content=content)
                client_response = JSONResponse(status_code=201, content={"message": "View Created Successfully - "
                                                                                    + view["source_name"]})
                response.status_code = status.HTTP_201_CREATED
            except (OperationalError, NotSupportedError, DataError, DatabaseError) as error:
                logger.error(error.__str__())
                client_response = JSONResponse(status_code=400, content={
                                               "message": "Bad Request"})
                response.status_code = status.HTTP_400_BAD_REQUEST
        else:
            client_response = JSONResponse(status_code=400, content={
                                           "message": "Bad Request"})
            response.status_code = status.HTTP_400_BAD_REQUEST
    return client_response


@router.get("/job", status_code=status.HTTP_200_OK, response_model=Job, response_model_exclude_unset=True,
            responses={404: {"model": Message}}, tags=["Job"])
def get_job(job_name: str, response: Response):
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    job_dict_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                         branch_name=conf_dict["git_dags_conf_ref"],
                                         file_path=job_name + "_job.json")

    if job_dict_string:
        job_config = json.loads(job_dict_string)
        client_response = job_config
    else:
        client_response = JSONResponse(status_code=404, content={
                                       "message": "Job Not Found - " + job_name})
        response.status_code = status.HTTP_404_NOT_FOUND
    return client_response


@router.post("/job", status_code=status.HTTP_201_CREATED, response_model=Message, response_model_exclude_unset=True,
             responses={409: {"model": Message}}, tags=["Job"])
def post_job(job: Job, response: Response):
    common_config = conf_dict["common_config"]
    job = jsonable_encoder(job, exclude_none=True)
    job_description = job["job_description"]
    job_file_name = job_description["job_name"] + "_job" + ".json"
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    if git_utils.check_path_exists(repository_name=conf_dict["git_dags_conf_repo"],
                                   branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name):
        logger.debug("Job Already exist")
        client_response = JSONResponse(status_code=409, content={"message": "Job Already Exists - "
                                                                            + job_description["job_name"]})
        response.status_code = status.HTTP_409_CONFLICT
    else:
        content = json.dumps(job, indent=4)
        if not git_utils.check_path_exists(repository_name=conf_dict["git_dags_conf_repo"],
                                           branch_name=conf_dict["git_dags_conf_ref"],
                                           file_path=conf_dict["prod_query_conf"]):
            comment = "Creating query conf file."
            git_utils.create_file(repository_name=conf_dict["git_dags_conf_repo"],
                                  branch_name=conf_dict["git_dags_conf_ref"],
                                  file_path=conf_dict["prod_query_conf"],
                                  comment=comment, file_content="{}")

        prod_query_file_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                    branch_name=conf_dict["git_dags_conf_ref"],
                                                    file_path=conf_dict["prod_query_conf"])

        prod_query_dict = json.loads(prod_query_file_string)
        prod_query_dict[job_description['job_name']] = {}
        table_name = DATATYPE_MANAGER["table"]["database_reference"]
        table_name += "." + \
            DATATYPE_MANAGER["table"]["table_name"].format(
                table_name=job_description["job_name"])
        tmp = "select * from " + table_name + " where " + \
            "run_id='{run_id}' and job_run_date='{job_run_date}'"
        prod_query_dict[job_description['job_name']]['data_query'] = tmp
        if job_description['filter']['type'] == 'all':
            prod_query_dict[job_description['job_name']
                            ]['date_column'] = 'run_date'
            athena_log_time_format = common_config["athena_log_time_format"]
            prod_query_dict[job_description['job_name']
                            ]['format'] = athena_log_time_format
        else:
            source1 = job['source1']
            source1_conf_str = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                  branch_name=conf_dict["git_dags_conf_ref"],
                                                  file_path=source1 + "_source.json")
            source1_conf = json.loads(source1_conf_str)
            source1_columns = source1_conf['columns_in_sequence']
            source2 = job['source2']
            source2_conf_str = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                  branch_name=conf_dict["git_dags_conf_ref"],
                                                  file_path=source2 + "_source.json")
            source2_conf = json.loads(source2_conf_str)
            source2_columns = source2_conf['columns_in_sequence']
            for column in source1_columns:
                if column['name'] == job_description['filter']['source1']:
                    tmp = source1 + "___" + column['name']
                    prod_query_dict[job_description['job_name']
                                    ]['source1_date_column'] = tmp
                    tmp = column['format']
                    prod_query_dict[job_description['job_name']
                                    ]['source1_format'] = tmp
                    break
            for column in source2_columns:
                if column['name'] == job_description['filter']['source2']:
                    tmp = source2 + "___" + column['name']
                    prod_query_dict[job_description['job_name']
                                    ]['source2_date_column'] = tmp
                    tmp = column['format']
                    prod_query_dict[job_description['job_name']
                                    ]['source2_format'] = tmp
                    break
        comment = "Updating Query configuration"
        prod_file_content = json.dumps(prod_query_dict, indent=4)
        git_utils.update_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=conf_dict["prod_query_conf"],
                              comment=comment, file_content=prod_file_content)
        if not git_utils.check_path_exists(repository_name=conf_dict["git_dags_conf_repo"],
                                           branch_name=conf_dict["git_dags_conf_ref"],
                                           file_path=conf_dict["dev_query_conf"]):
            comment = "Creating query conf file."
            git_utils.create_file(repository_name=conf_dict["git_dags_conf_repo"],
                                  branch_name=conf_dict["git_dags_conf_ref"],
                                  file_path=conf_dict["dev_query_conf"],
                                  comment=comment, file_content="{}")

        dev_query_file_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                   branch_name=conf_dict["git_dags_conf_ref"],
                                                   file_path=conf_dict["dev_query_conf"])

        dev_query_dict = json.loads(dev_query_file_string)
        dev_query_dict[job_description['job_name']] = {}
        table_name = DATATYPE_MANAGER["table"]["database_reference"]
        table_name += "." + \
            DATATYPE_MANAGER["table"]["table_name"].format(
                table_name=job_description["job_name"])
        tmp = "select * from " + table_name + " where " + \
            "run_id='{run_id}' and job_run_date='{job_run_date}'"
        dev_query_dict[job_description['job_name']]['data_query'] = tmp
        if job_description['filter']['type'] == 'all':
            dev_query_dict[job_description['job_name']
                           ]['date_column'] = 'run_date'
            athena_log_time_format = common_config["athena_log_time_format"]
            dev_query_dict[job_description['job_name']
                           ]['format'] = athena_log_time_format
        else:
            source1 = job['source1']
            source1_conf_str = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                  branch_name=conf_dict["git_dags_conf_ref"],
                                                  file_path=source1 + "_source.json")
            source1_conf = json.loads(source1_conf_str)
            source1_columns = source1_conf['columns_in_sequence']
            source2 = job['source2']
            source2_conf_str = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                                  branch_name=conf_dict["git_dags_conf_ref"],
                                                  file_path=source2 + "_source.json")
            source2_conf = json.loads(source2_conf_str)
            source2_columns = source2_conf['columns_in_sequence']
            for column in source1_columns:
                if column['name'] == job_description['filter']['source1']:
                    tmp = source1 + "___" + column['name']
                    dev_query_dict[job_description['job_name']
                                   ]['source1_date_column'] = tmp
                    tmp = column['format']
                    dev_query_dict[job_description['job_name']
                                   ]['source1_format'] = tmp
                    break
            for column in source2_columns:
                if column['name'] == job_description['filter']['source2']:
                    tmp = source2 + "___" + column['name']
                    dev_query_dict[job_description['job_name']
                                   ]['source2_date_column'] = tmp
                    tmp = column['format']
                    dev_query_dict[job_description['job_name']
                                   ]['source2_format'] = tmp
                    break
        comment = "Updating Query configuration"
        dev_file_content = json.dumps(dev_query_dict, indent=4)
        git_utils.update_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=conf_dict["dev_query_conf"],
                              comment=comment, file_content=dev_file_content)

        logger.debug("Uploading job conf file - " + job_file_name)
        comment = "Add " + job_file_name
        git_utils.create_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name,
                              comment=comment, file_content=content)
        template_path = conf_dict["dag"]["template_path"]
        template_params = dict(conf_dict["dag"])
        template_params["dag_name"] = job_description["job_name"]
        template_params["dag_schedule"] = job_description["schedule"]
        template_params["dag_group_name"] = job_description["group_name"]
        template_params["dag_source_path"] = conf_dict["dag_config_path"]
        job_conf_path = conf_dict["dag_config_path"]
        template_params["dag_job_config_path"] = os.path.join(
            job_conf_path, job_file_name)
        start_date = pendulum.now(conf_dict["dag"]["dag_timezone"])
        start_date -= timedelta(days=int(conf_dict["dag"]
                                ["dag_start_days_ago"]))
        template_params["dag_start_year"] = str(start_date.year)
        template_params["dag_start_month"] = str(start_date.month)
        template_params["dag_start_day"] = str(start_date.day)
        template_params["dag_tmp_job_path"] = template_params["dag_tmp_job_path"].format(
            job_file_name=job_file_name)
        dag_content = fill_template(template_path, template_params)
        dag_file_name = template_params["dag_name"] + ".py"
        logger.debug("Uploading dag file - " + dag_file_name)
        comment = "Add " + dag_file_name
        git_utils.create_file(repository_name=conf_dict["git_dags_repo"],
                              branch_name=conf_dict["git_dags_repo_ref"], file_path=dag_file_name,
                              comment=comment, file_content=dag_content)
        client_response = JSONResponse(status_code=201, content={"message": "Job Created Successfully - "
                                                                            + job_description["job_name"]})
        response.status_code = status.HTTP_201_CREATED
    return client_response


@router.get("/get_default_job", status_code=status.HTTP_200_OK, response_model=DefaultJobName,
            responses={404: {"model": Message}}, tags=["Job"])
@cache(expire=300)
def get_default_job(response: Response):
    query_conf_file = conf_dict["dev_query_conf"]
    if "prod" in CONFIG_PATH:
        query_conf_file = conf_dict["prod_query_conf"]
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    query_file_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                           branch_name=conf_dict["git_dags_conf_ref"],
                                           file_path=query_conf_file)

    query_dict = json.loads(query_file_string)
    if query_dict.get("default_job_name"):
        client_response = {"default_job_name": query_dict["default_job_name"]}
    else:
        client_response = JSONResponse(status_code=404, content={
                                       "message": "default_job_name info not found."})
        response.status_code = status.HTTP_404_NOT_FOUND
    return client_response


@router.get("/import_source", status_code=status.HTTP_200_OK, response_model=ImportSourceList, tags=["Source"])
def get_import_source(response: Response):
    try:
        conn = connect()
        cursor = conn.cursor()
        common_config = conf_dict["common_config"]
        back_quoted_database_reference = DATATYPE_MANAGER["table"]["back_quoted_database_reference"].format(
            catalog_name=common_config['catalog_name'],
            database_name=common_config['database_name']
        )
        table_list_query = DATATYPE_MANAGER["table"]["table_list_query"].format(
            back_quoted_database_reference=back_quoted_database_reference)
        cursor.execute(table_list_query)
        source_table_list = [item[0] for item in cursor.fetchall()]
        git_utils = GitUtils(access_code=conf_dict["git_access_code"])
        all_file_paths = git_utils.get_files(repository_name=conf_dict["git_dags_conf_repo"],
                                             branch_name=conf_dict["git_dags_conf_ref"],
                                             file_path="/")
        source_name_list = [file_path for file_path in all_file_paths if
                            file_path.endswith("_source.json")]
        table_name_list = {}
        for source_name in source_name_list:
            source_content = all_file_paths[source_name]
            source_dict = json.loads(source_content)
            if source_dict["source_type"] == "s3" or source_dict["source_type"] == "view":
                table_name_list[source_dict["table_name"]] = source_dict

        importable_tables = set(table_name_list.keys()) - \
            set(source_table_list)
        client_response = {
            "source_list": [table_name_list[table_name]["source_name"] for table_name in table_name_list if
                            table_name in importable_tables]
        }
    except (OperationalError, NotSupportedError, DataError, DatabaseError) as error:
        logger.error(error.__str__())
        client_response = JSONResponse(status_code=500, content={
                                       "message": "Internal Server Error"})
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

    return client_response


@router.post("/import_source", status_code=status.HTTP_200_OK, response_model=Message,
             responses={400: {"model": Message}, 500: {"model": Message}}, tags=["Source"])
def post_import_source(import_source_list: ImportSourceList, response: Response):
    source_list = import_source_list.source_list
    logger.info(source_list)
    common_config = conf_dict["common_config"]
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    try:
        conn = connect()
        cursor = conn.cursor()
        common_config = conf_dict["common_config"]
        back_quoted_database_reference = DATATYPE_MANAGER["table"]["back_quoted_database_reference"].format(
            catalog_name=common_config['catalog_name'],
            database_name=common_config['database_name']
        )
        table_list_query = DATATYPE_MANAGER["table"]["table_list_query"].format(
            back_quoted_database_reference=back_quoted_database_reference)
        cursor.execute(table_list_query)
        source_table_list = [item[0] for item in cursor.fetchall()]
        all_status = list()
        for source_name in source_list:
            source_file_name = source_name + "_source.json"

            source_content = git_utils.get_file(file_path=source_file_name,
                                                branch_name=conf_dict["git_dags_conf_ref"],
                                                repository_name=conf_dict["git_dags_conf_repo"])
            if source_content:
                source_conf = json.loads(source_content)

                def _create_source(source_dict):
                    if source_dict.get("lookup_name"):
                        source_s3_path = conf_dict["lookup_out_path"] + \
                            source_dict["source_name"]
                    else:
                        source_s3_path = source_dict["source_s3_path"]
                    table_dict = DATATYPE_MANAGER["table"]
                    default_data_type = table_dict["default_s3_table_datatype"]
                    table_properties = table_dict["table_properties"]
                    skip_header = table_properties["skip_header"].format(
                        no_of_lines=table_dict["no_of_lines"])

                    back_quoted_table_reference = table_dict["back_quoted_table_reference"].format(
                        catalog_name=common_config["catalog_name"],
                        database_name=common_config["database_name"],
                        table_name=source_dict["table_name"])
                    cols_data_type = ", ".join(
                        [table_dict["column_detail"].format(column_name=column["name"], column_type=default_data_type)
                         for column in source_dict["columns_in_sequence"] if not column["partitioned"]])

                    partition_columns = [column["name"] for column in source_dict["columns_in_sequence"] if
                                         column["partitioned"]]
                    if len(partition_columns) > 0:
                        partition_details = ", ".join(
                            [table_dict["partition_detail"].format(column_name=column, column_type=default_data_type)
                             for column in partition_columns])
                        create_query = table_dict["create_query_with_partition"].format(
                            table_name=back_quoted_table_reference,
                            column_details=cols_data_type,
                            partition_details=partition_details,
                            s3_uri=source_s3_path,
                            table_properties=skip_header)
                    else:
                        create_query = table_dict["create_query"].format(table_name=back_quoted_table_reference,
                                                                         column_details=cols_data_type,
                                                                         s3_uri=source_s3_path,
                                                                         table_properties=skip_header)
                    logger.debug(create_query)
                    cursor.execute(create_query)
                    conn.commit()
                    logger.debug("Table created successfully.")

                if source_conf.get("source_type") == "s3":
                    if source_conf["table_name"] in source_table_list:
                        all_status.append(True)
                        continue
                    _create_source(source_dict=source_conf)

                elif source_conf.get("source_type") == "view":
                    if source_conf["table_name"] in source_table_list:
                        all_status.append(True)
                        continue
                    for view_source in source_conf["sources"]:
                        view_source_file_name = view_source["source_name"] + \
                            "_source.json"
                        view_source_content = git_utils.get_file(
                            file_path=view_source_file_name,
                            branch_name=conf_dict["git_dags_conf_ref"],
                            repository_name=conf_dict["git_dags_conf_repo"])
                        view_source_conf = json.loads(view_source_content)
                        if view_source_conf.get("source_type") == "s3":
                            if view_source_conf["table_name"] in source_table_list:
                                all_status.append(True)
                                continue

                            _create_source(source_dict=view_source_conf)
                    view_query = source_conf["view_query"]
                    common_config = conf_dict["common_config"]
                    database_reference = DATATYPE_MANAGER["table"]["database_reference"].format(
                        catalog_name=common_config['catalog_name'],
                        database_name=common_config['database_name'])
                    view_query = view_query.format(
                        database_reference=database_reference)
                    conn = connect()
                    cursor = conn.cursor()
                    logger.debug(view_query)
                    cursor.execute(view_query)
                    conn.commit()
                    logger.debug("View created successfully.")
                    all_status.append(True)
            else:
                all_status.append(False)

        if all(all_status):
            client_response = JSONResponse(status_code=202, content={
                                           "message": "Success imported"})
            response.status_code = status.HTTP_200_OK
        else:
            client_response = JSONResponse(status_code=400, content={
                                           "message": "Imported Failed"})
            response.status_code = status.HTTP_400_BAD_REQUEST
    except (OperationalError, NotSupportedError, DataError, DatabaseError, GithubException) as error:
        logger.error(error.__str__())
        client_response = JSONResponse(status_code=500, content={
                                       "message": "Internal Server Error"})
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return client_response


@router.get("/get_job_data", status_code=status.HTTP_200_OK, response_model=JobData,
            responses={500: {"model": Message}}, tags=["Job"])
@cache(expire=86400)
def get_job_data(job_name: str, run_id: str, job_run_date: str, response: Response):
    start_time = time.time()
    common_config = conf_dict["common_config"]
    query_conf_file = conf_dict["dev_query_conf"]
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    if "prod" in CONFIG_PATH:
        query_conf_file = conf_dict["prod_query_conf"]
    try:
        conn = connect()
        cursor = conn.cursor()
        query_conf_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                               branch_name=conf_dict["git_dags_conf_ref"],
                                               file_path=query_conf_file)
        table_reference = DATATYPE_MANAGER["table"]["table_reference"].format(
            catalog_name=common_config["catalog_name"],
            database_name=common_config["database_name"],
            table_name=job_name)
        query = "select * from " + table_reference + " where " + "run_id='" + \
            run_id + "'" + " and job_run_date='" + job_run_date + "'"
        if query_conf_string:
            query_dict = json.loads(query_conf_string)
            if query_dict.get(job_name, {}).get("data_query", None):
                query = query_dict[job_name]["data_query"].format(run_id=run_id,
                                                                  job_run_date=job_run_date,
                                                                  catalog_name=common_config["catalog_name"],
                                                                  database_name=common_config["database_name"])
        logger.debug(query)
        cursor.execute(query)
        logger.debug(
            f"data_scanned_in_bytes - {cursor.data_scanned_in_bytes} bytes {cursor.data_scanned_in_bytes/1000000} MB {cursor.data_scanned_in_bytes/1000000000} GB")
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        # df = pd.read_csv(cursor.output_location)
        df.fillna("", inplace=True)
        response_data = df.to_dict(orient="records")
        client_response = {
            "job_data": response_data
        }
    except (OperationalError, NotSupportedError, DataError, DatabaseError, GithubException) as error:
        logger.error(error.__str__())
        client_response = JSONResponse(status_code=500, content={
                                       "message": "Internal Server Error"})
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    logger.debug(f"Response time - {time.time()-start_time}")
    return client_response


@router.get("/get_latest_job_time", status_code=status.HTTP_200_OK, response_model=LatestJobTime,
            responses={500: {"model": Message}}, tags=["Job"])
def get_latest_job_time(job_name: str, response: Response):
    common_config = conf_dict["common_config"]
    try:
        conn = connect()
        cursor = conn.cursor()
        query_dict = {}
        table_reference = common_config["job_info"]["job_info_table"]
        query_dict["table_reference"] = table_reference
        query_dict["job_name"] = job_name
        query_dict["date_format"] = common_config["job_info"]["date_format"]
        query = DATATYPE_MANAGER["table"]["latest_data_query"].format(
            **query_dict)
        logger.debug(query)
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        if df.empty:
            client_response = JSONResponse(status_code=404, content={
                                           "message": "No Data Found"})
            response.status_code = status.HTTP_404_NOT_FOUND
        else:
            response_data = df.loc[:0, [
                "run_id", "date", "run_date", "job_run_date"]].to_dict(orient="records")
            client_response = response_data[0]
    except (OperationalError, NotSupportedError, DataError, DatabaseError) as error:
        logger.error(error.__str__())
        client_response = JSONResponse(status_code=500, content={
                                       "message": "Internal Server Error"})
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return client_response


@router.get("/get_job_time", status_code=status.HTTP_200_OK, response_model=JobTimeList,
            responses={400: {"model": Message}, 500: {"model": Message}}, tags=["Job"])
def get_job_time(job_name: str, date: str, response: Response):
    common_config = conf_dict["common_config"]
    try:
        datetime.strptime(date, "%Y-%m-%d")
        try:
            conn = connect()
            cursor = conn.cursor()

            query_dict = {}
            table_reference = common_config["job_info"]["job_info_table"]
            query_dict["table_reference"] = table_reference
            query_dict["job_name"] = job_name
            query_dict["date_format"] = common_config["job_info"]["date_format"]
            query_dict["date"] = date
            query = DATATYPE_MANAGER["table"]["job_time_query"].format(
                **query_dict)
            logger.debug(query)
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            response_data = df.to_dict(orient="records")
            client_response = {
                "job_time_list": response_data
            }
        except (OperationalError, NotSupportedError, DataError, DatabaseError) as error:
            logger.error(error.__str__())
            client_response = JSONResponse(status_code=500, content={
                                           "message": "Internal Server Error"})
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    except ValueError as error:
        logger.error(error.__str__())
        client_response = JSONResponse(status_code=400, content={
                                       "message": "Bad Request"})
        response.status_code = status.HTTP_400_BAD_REQUEST
    return client_response


@router.get("/job_list", status_code=status.HTTP_200_OK, response_model=JobList, tags=["Job"])
@cache(expire=300)
def get_job_list():
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    all_file_paths = git_utils.get_repo_file_paths(
        repository_name=conf_dict["git_dags_conf_repo"],
        branch_name=conf_dict["git_dags_conf_ref"])
    table_name_list = [file_path.replace("_job.json", "") for file_path in all_file_paths if
                       file_path.endswith("_job.json")]
    common_config = conf_dict["common_config"]
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
    final_table_name_list = [
        item for item in available_table if item in table_name_list]
    client_response = {"job_list": final_table_name_list}
    return client_response


@router.get("/job_config_list", status_code=status.HTTP_200_OK, response_model=JobConfigList, tags=["Job"])
@cache(expire=300)
def get_job_config_list():
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    all_file_paths = git_utils.get_repo_file_paths(repository_name=conf_dict["git_dags_conf_repo"],
                                                   branch_name=conf_dict["git_dags_conf_ref"])
    table_name_list = [file_path.replace("_job.json", "") for file_path in all_file_paths if
                       file_path.endswith("_job.json")]

    client_response = {"job_config_list": table_name_list}
    return client_response


@router.get("/source_list", status_code=status.HTTP_200_OK, response_model=SourceList, tags=["Source"])
def get_source_list():
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    all_file_paths = git_utils.get_repo_file_paths(
        repository_name=conf_dict["git_dags_conf_repo"],
        branch_name=conf_dict["git_dags_conf_ref"])
    table_name_list = [file_path.replace("_source.json", "") for file_path in all_file_paths if
                       file_path.endswith("_source.json")]
    client_response = {"source_list": table_name_list}
    return client_response


@router.post("/update_dag", status_code=status.HTTP_200_OK, response_model=Message,
             response_model_exclude_unset=True, responses={400: {"model": Message}, 404: {"model": Message}}, tags=["Job"])
def post_update_dag(job_name: str, update_dag_details: UpdateDagDetails, response: Response):
    schedule = update_dag_details.schedule
    group_name = update_dag_details.group_name
    execution_minutes = update_dag_details.execution_minutes
    if not group_name and not schedule and not execution_minutes:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return JSONResponse(status_code=400, content={"message": "Bad Request"})
    job_file_name = job_name + "_job.json"
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    job_content = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                     branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name)
    if not job_content:
        response.status_code = status.HTTP_404_NOT_FOUND
        return JSONResponse(status_code=404, content={"message": "Job Not Found"})
    job_conf = json.loads(job_content)
    job_description = job_conf["job_description"]
    update_test_list = list()
    if group_name is not None:
        if job_description["group_name"] == group_name:
            update_test_list.append(False)
        else:
            update_test_list.append(True)
            job_description["group_name"] = group_name
    if schedule is not None:
        if check_cron_string(schedule):
            if job_description["schedule"] == schedule:
                update_test_list.append(False)
            else:
                update_test_list.append(True)
                job_description["schedule"] = schedule
        else:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return JSONResponse(status_code=400, content={"message": "Invalid schedule string"})
    if execution_minutes is not None:
        update_test_list.append(True)
    else:
        update_test_list.append(False)
        execution_minutes = conf_dict["dag"]["dag_execution_timeout_min"]

    logger.debug(update_test_list)
    if any(update_test_list):
        job_config_content = json.dumps(job_conf, indent=4)
        with open(job_file_name, "wt", encoding="utf-8") as file_writer:
            file_writer.write(job_config_content)

        logger.debug("Uploading job config file - " + job_file_name)
        comment = "Update " + job_file_name
        git_utils.update_file(
            repository_name=conf_dict["git_dags_conf_repo"],
            branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name,
            comment=comment, file_content=job_config_content)

        template_path = conf_dict["dag"]["template_path"]
        template_params = dict(conf_dict["dag"])
        template_params["dag_execution_timeout_min"] = str(execution_minutes)
        template_params["dag_name"] = job_description["job_name"]
        template_params["dag_schedule"] = job_description["schedule"]
        template_params["dag_group_name"] = job_description["group_name"]
        template_params["dag_source_path"] = conf_dict["dag_config_path"]
        job_conf_path = conf_dict["dag_config_path"]
        template_params["dag_job_config_path"] = os.path.join(
            job_conf_path, job_file_name)
        start_date = pendulum.now(conf_dict["dag"]["dag_timezone"])
        start_date -= timedelta(days=int(conf_dict["dag"]
                                ["dag_start_days_ago"]))
        template_params["dag_start_year"] = str(start_date.year)
        template_params["dag_start_month"] = str(start_date.month)
        template_params["dag_start_day"] = str(start_date.day)
        template_params["dag_tmp_job_path"] = template_params["dag_tmp_job_path"].format(
            job_file_name=job_file_name)
        dag_content = fill_template(template_path, template_params)
        dag_file_name = template_params["dag_name"] + ".py"
        with open(dag_file_name, "wt", encoding="utf-8") as file_writer:
            file_writer.write(dag_content)
        logger.debug("Uploading dag file - " + dag_file_name)
        comment = "Update " + dag_file_name

        git_utils.update_file(
            repository_name=conf_dict["git_dags_repo"],
            branch_name=conf_dict["git_dags_repo_ref"], file_path=dag_file_name,
            comment=comment, file_content=dag_content)
        client_response = JSONResponse(status_code=200, content={"message": f"Updated dag - {dag_file_name} and "
                                                                            f"Job conf - {job_file_name} Successfully!!"})
        response.status_code = status.HTTP_200_OK
    else:
        response.status_code = status.HTTP_200_OK
        client_response = JSONResponse(status_code=200, content={
                                       "message": "No Update done"})

    return client_response


@router.post("/update_recon_policy", status_code=status.HTTP_200_OK, response_model=Message,
             response_model_exclude_unset=True,
             responses={400: {"model": Message}, 404: {"model": Message}}, tags=["Job"])
def post_update_recon_policy(job_name: str, recon_policy: ReconPolicy, response: Response):
    job_file_name = job_name + "_job" + ".json"
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    job_content = git_utils.get_file(
        repository_name=conf_dict["git_dags_conf_repo"],
        branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name)
    if not job_content:
        response.status_code = status.HTTP_404_NOT_FOUND
        return JSONResponse(status_code=404, content={"message": "Job Not Found"})
    recon_policy = jsonable_encoder(recon_policy)
    job_conf = json.loads(job_content)
    job_description = job_conf["job_description"]
    if recon_policy == job_description.get("recon_policy"):
        response.status_code = status.HTTP_200_OK
        client_response = JSONResponse(status_code=200, content={
                                       "message": "No Update done"})
    else:
        if recon_policy.get("recon_type") not in DATATYPE_MANAGER["recon_policy"]["recon_type"]:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return JSONResponse(status_code=400, content={"message": "Bad Request"})
        job_description["recon_policy"] = recon_policy
        job_config_content = json.dumps(job_conf, indent=4)
        logger.debug("Uploading job config file - " + job_file_name)
        comment = "Update " + job_file_name
        git_utils.update_file(repository_name=conf_dict["git_dags_conf_repo"],
                              branch_name=conf_dict["git_dags_conf_ref"], file_path=job_file_name,
                              comment=comment, file_content=job_config_content)
        client_response = JSONResponse(status_code=200,
                                       content={"message": f"Updated Job conf - {job_file_name} Successfully!!"})
        response.status_code = status.HTTP_200_OK
    return client_response


@app.get("/datatype_manager", status_code=status.HTTP_200_OK)
def get_datatype_manager():
    datatype_manager = {}
    datatype_mapper = {}

    for datatype in DATATYPE_MAPPER:
        datatype_mapper[datatype] = {}
        datatype_mapper[datatype]["filter_available"] = DATATYPE_MAPPER[datatype]["filter_available"]
        datatype_mapper[datatype]["sub_query_filter_available"] = DATATYPE_MAPPER[datatype][
            "sub_query_filter_available"]
        datatype_mapper[datatype]["input_type"] = DATATYPE_MAPPER[datatype]["input_type"]
        if DATATYPE_MAPPER[datatype].get("date_picker_format"):
            datatype_mapper[datatype]["date_picker_format"] = DATATYPE_MAPPER[datatype]["date_picker_format"]

    datatype_manager["datatype_mapper"] = datatype_mapper
    datatype_manager["available_s3_datatype"] = DATATYPE_MANAGER["available_s3_datatype"]
    datatype_manager["available_table_datatype"] = DATATYPE_MANAGER["available_table_datatype"]
    datatype_manager["available_static_type"] = DATATYPE_MANAGER["available_static_type"]
    datatype_manager["available_mapper_type"] = DATATYPE_MANAGER["available_mapper_type"]
    datatype_manager["job_filter_compatible_datatype"] = DATATYPE_MANAGER["job_filter_compatible_datatype"]
    datatype_manager["job_filter_policy"] = DATATYPE_MANAGER["job_filter_policy"]
    datatype_manager["date_filter_compatible_datatype"] = DATATYPE_MANAGER["date_filter_compatible_datatype"]
    datatype_manager["job_filter_compatible_date_filter"] = DATATYPE_MANAGER["job_filter_compatible_date_filter"]
    datatype_manager["aggregate_operation_available"] = DATATYPE_MANAGER["aggregate"]["operation_available"]
    datatype_manager["filter_operation_available"] = DATATYPE_MANAGER["filter_operation_available"]
    recon_policy = DATATYPE_MANAGER["recon_policy"]
    reconciliation_operation_available = recon_policy["reconciliation_amount"]["operation_available"]
    datatype_manager["reconciliation_operation_available"] = reconciliation_operation_available
    datatype_manager["recon_type"] = recon_policy["recon_type"]
    client_response = {"datatype_manager": datatype_manager}
    return client_response


@router.get("/get_source_config", status_code=status.HTTP_200_OK, response_model=Source,
            response_model_exclude_unset=True,
            responses={400: {"model": Message}, 500: {"model": Message},404: {"model": Message}}, tags=["Source"])
def get_source_config(response: Response, source_type: str = "table",
                      catalog_name: Union[str, None] = "awsdatacatalog",
                      database_name: Union[str, None] = None,
                      table_name: Union[str, None] = None,
                      s3_file_path: Union[str, None] = None):
    try:
        logger.info(f"source_type - {source_type}")
        if source_type == "table":
            logger.info(f"catalog_name - {catalog_name}")
            logger.info(f"database_name - {database_name}")
            logger.info(f"table_name - {table_name}")
            if (not catalog_name) or (not database_name) or (not table_name):
                response.status_code = status.HTTP_400_BAD_REQUEST
                return JSONResponse(status_code=400, content={"message": "Bad Request - Please provide catalog_name, database_name and table_name."})
            client_response = build_table_source_config(
                catalog_name, database_name, table_name, logger)
            response.status_code = status.HTTP_200_OK
        elif source_type == "s3":
            logger.info(f"s3_file_path - {s3_file_path}")
            if not s3_file_path:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return JSONResponse(status_code=400, content={"message": "Bad Request - Please provide s3_file_path."})
            elif not (s3_file_path.endswith("csv") or (s3_file_path.endswith(".gz") and not s3_file_path.endswith("tar.gz"))):
                response.status_code = status.HTTP_400_BAD_REQUEST
                return JSONResponse(status_code=400, content={"message": "Bad Request - File Type not supported"})
            client_response = build_s3_source_config(s3_file_path, logger)
            response.status_code = status.HTTP_200_OK
        else:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return JSONResponse(status_code=400, content={"message": "Bad Request"})
        if client_response is None:
            client_response = JSONResponse(status_code=404, content={
                                       "message": "Source Not Found"})
            response.status_code = status.HTTP_404_NOT_FOUND
    except (OperationalError, NotSupportedError, DataError, DatabaseError, FileNotFoundError) as error:
        logger.error(error.__str__())
        client_response = JSONResponse(status_code=500, content={
                                       "message": "Internal Server Error"})
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    except UnicodeDecodeError as uerror:
        logger.error(uerror.__str__())
        response.status_code = status.HTTP_400_BAD_REQUEST
        return JSONResponse(status_code=400, content={"message": "Bad Request - File is not utf-8 encoded"})
    return client_response


@router.delete("/delete_recon_job", status_code=status.HTTP_202_ACCEPTED, response_model=Message,
               responses={400: {"model": Message}, 500: {"model": Message}}, tags=["Job"])
def delete_recon_job(job_name: str, response: Response, background_tasks: BackgroundTasks):
    git_utils = GitUtils(access_code=conf_dict["git_access_code"])
    job_dict_string = git_utils.get_file(repository_name=conf_dict["git_dags_conf_repo"],
                                         branch_name=conf_dict["git_dags_conf_ref"],
                                         file_path=job_name + "_job.json")

    if job_dict_string:
        job_config = json.loads(job_dict_string)
        background_tasks.add_task(delete_job,
                                  job_config=job_config,
                                  conf_dict=conf_dict,
                                  sleep_time=5,
                                  logger=logger)
        client_response = JSONResponse(status_code=202, content={
                                       "message": "Deletion Accepted - " + job_name})
        response.status_code = status.HTTP_202_ACCEPTED
    else:
        client_response = JSONResponse(status_code=404, content={
                                       "message": "Job Not Found - " + job_name})
        response.status_code = status.HTTP_404_NOT_FOUND
    return client_response


@router.post("/create_source_by_query", status_code=status.HTTP_200_OK, response_model=Message,
             responses={400: {"model": Message}, 500: {"model": Message}}, tags=["Source"])
def create_source_by_query(query_source: QuerySource, response: Response):
    try:
        conn = connect()
        cursor = conn.cursor()
        git_utils = GitUtils(access_code=conf_dict["git_access_code"])
        try:
            common_config = conf_dict["common_config"]
            table_reference = DATATYPE_MANAGER["table"]["table_reference"].format(
                catalog_name=common_config["catalog_name"],
                database_name=common_config["database_name"],
                table_name=query_source.view_name)
            check_view_exists = common_config["check_view_exists_query"].format(
                database_name=common_config["database_name"],
                view_name=query_source.view_name
            )
            cursor.execute(check_view_exists)
            view_list = [item for item in cursor.fetchall()]
            if view_list:
                logger.debug("View Already exist")
                client_response = JSONResponse(status_code=409, content={"message": "View Already Exists - "
                                                                                    + query_source.view_name})
                response.status_code = status.HTTP_409_CONFLICT
            else:

                view_query = f"create view {table_reference} as {query_source.select_query}"
                logger.debug(view_query)
                cursor.execute(view_query)
                conn.commit()
                source_dict = build_table_source_config(catalog_name=common_config["catalog_name"],
                                                        database_name=common_config["database_name"],
                                                        table_name=query_source.view_name,
                                                        logger=logger)
                source_dict["source_name"] = query_source.view_name
                source_dict["select_query"] = query_source.select_query
                content = json.dumps(source_dict, indent=4)
                source_file_name = query_source.view_name + "_source.json"
                logger.debug("Uploading source conf - %s", source_file_name)
                comment = "Add " + source_file_name
                git_utils.create_file(repository_name=conf_dict["git_dags_conf_repo"],
                                      branch_name=conf_dict["git_dags_conf_ref"], file_path=source_file_name,
                                      comment=comment, file_content=content)
                client_response = JSONResponse(status_code=201, content={"message": "Source Created Successfully - "
                                                                                    + query_source.view_name})
                response.status_code = status.HTTP_201_CREATED
        except (OperationalError, NotSupportedError, DataError, DatabaseError, FileNotFoundError) as error:
            logger.debug(error.__str__())
            client_response = JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={
                "message": "Bad Request"})
            response.status_code = status.HTTP_400_BAD_REQUEST
    except (OperationalError, NotSupportedError, DataError, DatabaseError, FileNotFoundError, GithubException) as error:
        logger.debug(error.__str__())
        client_response = JSONResponse(status_code=500, content={
            "message": "Internal Server Error"})
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return client_response


app.include_router(router)
