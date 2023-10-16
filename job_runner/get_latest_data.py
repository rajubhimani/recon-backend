import json
import requests
import warnings
from job_runner.common import CONF_DICT

warnings.filterwarnings("ignore")


def get_latest_data(job_name):
    host = CONF_DICT["api_url"]
    url = host + "get_latest_job_time?job_name=" + job_name
    print(url)
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        latest_job_details = json.loads(response.text)
        url = host + "get_job_time?job_name=" + job_name + "&date=" + latest_job_details["date"]
        print(url)
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            latest_time = json.loads(response.text)["job_time_list"]
            for available_time in latest_time:
                print(available_time)
                print(host + "get_job_data?job_name=" + job_name + "&run_id=" + available_time["run_id"] + "&job_run_date=" + available_time["job_run_date"])
                response = requests.get(
                    host + "get_job_data?job_name=" + job_name + "&run_id=" + available_time["run_id"] + "&job_run_date=" + available_time["job_run_date"],
                    verify=False)
                print(response.status_code)
                print("\n")
    else:
        print(response.text)
        print(response.status_code)
