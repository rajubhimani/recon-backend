""""""
import os
from pathlib import Path
import pandas as pd
from aws_s3_utils import list_objects, download_file, upload_file, delete_objects

tmp_folder_path = Path("tmp")
tmp_folder_path.mkdir(exist_ok=True)
bucket="mosaic-recon"
for s3_file in list_objects(bucket=bucket, prefix="prod/job_info"):
    s3_file_path = s3_file["Key"]
    file_name = s3_file_path.split("/")[-1]
    file_path = tmp_folder_path.joinpath(file_name)
    download_file(bucket, s3_file_path, str(file_path))
    try:
        data = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        delete_objects(bucket, s3_file_path)
    if data.empty:
        delete_objects(bucket, s3_file_path)
        os.remove(file_path)
        continue
    print(data["run_date"].apply(lambda row: row.split(" ")[0]))
    data["job_run_date"] = data["run_date"].apply(lambda row: row.split(" ")[0])
    print(data.columns)
    data.to_csv(file_path,index=False)
    upload_file(bucket, str(file_path), s3_file_path)
    os.remove(file_path)
tmp_folder_path.rmdir()