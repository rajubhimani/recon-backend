import os
import sys
import json
import requests
from common import CONFIG_PATH
from aws_s3_utils import upload_file

conf_dict = json.load(open(CONFIG_PATH, "r"))


def run(lookup_update_config_path):
    if os.path.exists(lookup_update_config_path):
        with open(lookup_update_config_path, "r") as mapping_config_reader:
            mapping_config = json.load(mapping_config_reader)
            lookup_url = conf_dict["lookup_url"]
            recon_bucket = conf_dict["recon_bucket"]
            lookup_details = mapping_config["lookup_details"]
            for lookup_detail in lookup_details:
                lookup_name = lookup_detail["lookup_name"]
                lookup_s3_path = lookup_detail["lookup_s3_path"]
                url = lookup_url.format(lookup_name=lookup_name)
                print("requesting - " + url)
                response = requests.get(url, verify=False)
                if response.status_code != 200:
                    break
                    sys.exit(1)
                else:
                    temp_path = "/tmp/"
                    lookup_file_name = lookup_s3_path.split("/")[-1]
                    temp_file_path = os.path.join(temp_path, lookup_file_name)
                    with open(temp_file_path, "wt") as file_writer:
                        file_writer.write(response.text)
                    print(f"temporary file path - {temp_file_path}")
                    print(f"Uploading to bucket - {recon_bucket} and path - {lookup_s3_path}")
                    status = upload_file(bucket=recon_bucket, file_path=temp_file_path,
                                         s3_file_path=lookup_s3_path)
                    if not status:
                        break
                        sys.exit(1)
    else:
        print(f"lookup_update_config_path - {lookup_update_config_path} not found")
        sys.exit(1)


if __name__ == '__main__':
    run(lookup_update_config_path=sys.argv[1])
