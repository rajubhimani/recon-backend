import sys
import os
from pathlib import Path
import json
from pyathena import connect
from pyathena.pandas.util import as_pandas
from aws_s3_utils import list_objects, copy_objects
from common import CONF_DICT, LOGGER, DATATYPE_MANAGER
from job_utils import get_bucket_and_path


set_environment = {
    "AWS_ATHENA_S3_STAGING_DIR": "s3://mosaic-recon/temp/dev/",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ATHENA_WORK_GROUP": "athena-backoffice-desktop"
}

for env_var in set_environment:
    os.environ[env_var] = set_environment[env_var]

initial = True


def run(job_conf_path):
    LOGGER.debug('pwd')
    os.system('pwd')
    LOGGER.debug(f'{job_conf_path}')
    path_obj = Path(job_conf_path)
    with open(job_conf_path, "r", encoding="utf-8") as job_reader:
        job_conf = json.load(job_reader)
        # LOGGER.debug(f'{job_conf}')
        common_config = CONF_DICT["common_config"]
        # LOGGER.debug(f'{os.environ}')

        job_description = job_conf["job_description"]
        job_name = job_description["job_name"]
        database_name = common_config["database_name"]
        catalog_name = common_config["catalog_name"]
        table_dict = DATATYPE_MANAGER["table"]
        default_s3_table_datatype = table_dict["default_s3_table_datatype"]
        create_query = table_dict["create_query_with_partition"]
        column_detail = table_dict["column_detail"]
        partition_detail = table_dict["partition_detail"]
        table_properties = table_dict["table_properties"]
        partition_string_query = table_dict["partition_string_query"]
        repair_partition_query = table_dict["repair_partition_query"]
        partition_query = table_dict["partition_query"]
        skip_header = table_properties["skip_header"].format(
            no_of_lines=table_dict["no_of_lines"])
        s3_uri = table_dict["s3_uri"]
        table_reference_string = table_dict["back_quoted_table_reference"]
        catalog_name = common_config["catalog_name"]
        database_name = common_config["database_name"]
        conn = connect()
        cursor = conn.cursor()

        # job_run_date_partition_string = partition_string_query.format(
        #     key="job_run_date", value=run_info["job_run_date"])
        # job_run_date_partitions = partition_query.format(
        #     partition_string=job_run_date_partition_string)

        s1_name = job_name + "___" + job_conf["source1"]
        s2_name = job_name + "___" + job_conf["source2"]

        def create_new_table(table_name, bucket, s3_path):
            data_query = f"select * from {database_name}.{table_name} limit 1"
            table_reference = table_reference_string.format(catalog_name=catalog_name,
                                                            database_name=database_name,
                                                            table_name=table_name)
            cursor.execute(data_query)
            df = as_pandas(cursor)
            column_details = [column_detail.format(column_name=column, column_type=default_s3_table_datatype)
                              for column in df.columns]
            column_details = ", ".join(column_details)
            table_s3_uri = s3_uri.format(bucket=bucket,
                                         s3_path="/".join(s3_path.split("/")[:-1]))
            partition_details = partition_detail.format(
                column_name="job_run_date", column_type=default_s3_table_datatype)
            table_create_query = create_query.format(table_name=table_reference,
                                                     column_details=column_details,
                                                     partition_details=partition_details,
                                                     s3_uri=table_s3_uri,
                                                     table_properties=skip_header)
            drop_query = f"drop table if exists {table_reference}"
            LOGGER.debug(drop_query)
            cursor.execute(drop_query)
            conn.commit()
            LOGGER.debug(table_create_query)
            cursor.execute(table_create_query)
            conn.commit()

        def create_partition(table_name, partition_details):
            table_reference = table_reference_string.format(catalog_name=catalog_name,
                                                            database_name=database_name,
                                                            table_name=table_name)
            final_repair_partition_query = repair_partition_query.format(
                table_reference=table_reference,
                partitions=partition_details
            )
            LOGGER.debug(final_repair_partition_query)
            cursor.execute(final_repair_partition_query)
            conn.commit()
        job_table_query = f"select distinct run_date, run_id, \"$path\" as path from {database_name}.{job_name} order by \"$path\""
        LOGGER.debug(job_table_query)
        cursor.execute(job_table_query)
        data_scanned_in_bytes = cursor.data_scanned_in_bytes
        print("data_scanned_in_bytes -", data_scanned_in_bytes, "bytes", round(data_scanned_in_bytes/1024/1024,
              2), "MB", round(data_scanned_in_bytes/1024/1024/1024, 2), "GB")
        df = as_pandas(cursor)
        df.info()
        file_notexist = []
        partition_set = set()

        for index, row in df.iterrows():
            LOGGER.debug(index)
            LOGGER.debug(row["path"])

            date = row["run_date"].split()[0]
            partition_set.add(date)
            bucket, path = get_bucket_and_path(row["path"])
            run_id = row["run_id"]
            LOGGER.debug(date)
            partition = f"job_run_date={date}"
            job_path = str(path)
            new_job_path = str(Path(path).parent.joinpath(
                partition, job_name+"_"+run_id + ".csv")).replace("s3:/", "s3://")
            # LOGGER.debug(job_path)
            # LOGGER.debug(new_job_path)
            LOGGER.debug(
                f"Copping - s3://{bucket}/{job_path} - s3://{bucket}/{new_job_path}")
            try:
                response = copy_objects(des_bucket=bucket, des_key=new_job_path,
                                        src_bucket=bucket, src_key=job_path)
            except Exception as error:
                file_notexist.append((f"s3://{bucket}/{job_path}", str(error)))
            LOGGER.debug(response)
            if response == 200:
                LOGGER.debug(
                    f"Copied - s3://{bucket}/{job_path} - s3://{bucket}/{new_job_path}")

            s1_s3_path = str(Path(path).parent.parent.joinpath(
                s1_name, s1_name+"_"+run_id + ".csv")).replace("s3:/", "s3://")
            # LOGGER.debug(s1_s3_path)
            new_s1_s3_path = str(Path(path).parent.parent.joinpath(
                s1_name, partition, s1_name+"_"+run_id + ".csv")).replace("s3:/", "s3://")
            # LOGGER.debug(new_s1_s3_path)
            LOGGER.debug(
                f"Copping - s3://{bucket}/{s1_s3_path} - s3://{bucket}/{new_s1_s3_path}")
            try:
                response = copy_objects(des_bucket=bucket, des_key=new_s1_s3_path,
                                        src_bucket=bucket, src_key=s1_s3_path)
            except Exception as error:
                file_notexist.append(
                    (f"s3://{bucket}/{s1_s3_path}", str(error)))
            LOGGER.debug(response)
            if response == 200:
                LOGGER.debug(
                    f"Copied - s3://{bucket}/{s1_s3_path} - s3://{bucket}/{new_s1_s3_path}")

            s2_s3_path = str(Path(path).parent.parent.joinpath(
                s2_name, s2_name+"_"+run_id + ".csv")).replace("s3:/", "s3://")
            # LOGGER.debug(s2_s3_path)
            new_s2_s3_path = str(Path(path).parent.parent.joinpath(
                s2_name, partition, s2_name+"_"+run_id + ".csv")).replace("s3:/", "s3://")
            # LOGGER.debug(new_s2_s3_path)
            LOGGER.debug(
                f"Copping - s3://{bucket}/{s2_s3_path} - s3://{bucket}/{new_s2_s3_path}")
            try:
                response = copy_objects(des_bucket=bucket, des_key=new_s2_s3_path,
                                        src_bucket=bucket, src_key=s2_s3_path)
            except Exception as error:
                file_notexist.append(
                    (f"s3://{bucket}/{s2_s3_path}", str(error)))
            LOGGER.debug(response)

            if response == 200:
                LOGGER.debug(
                    f"Copied - s3://{bucket}/{s2_s3_path} - s3://{bucket}/{new_s2_s3_path}")
            break
            # exit()

        partition_details = ""
        for date_string in partition_set:
            date_partition_string = partition_string_query.format(
                key="job_run_date", value=date_string)
            partition_details += partition_query.format(
                partition_string=date_partition_string)
        create_new_table(table_name=job_name,
                         bucket=bucket, s3_path=job_path)
        create_partition(table_name=job_name,
                         partition_details=partition_details)
        create_new_table(table_name=s1_name,
                         bucket=bucket, s3_path=s1_s3_path)
        create_partition(table_name=s1_name,
                         partition_details=partition_details)
        create_new_table(table_name=s2_name,
                         bucket=bucket, s3_path=s2_s3_path)
        create_partition(table_name=s2_name,
                         partition_details=partition_details)
        print(file_notexist)

        LOGGER.debug("python job_runner.py " +
                     str(path_obj) + " " + str(path_obj.parent))
        # os.system("python job_runner.py " + str(path_obj) + " " + str(path_obj.parent))


if __name__ == "__main__":
    with open("job_list.csv", "r", encoding="utf-8") as file_reader:
        for line in file_reader.readlines():
            run(line.replace("\n",""))
