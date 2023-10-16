"""
common Module
"""
import json
import logging
import sys
from pathlib import Path

TESTING = False
if TESTING:
    CONFIG_PATH = "config/config_dev.json"
else:
    from airflow.models import Variable

    CONFIG_PATH = Variable.get("HARTREE_ENV", "../../recon/recon-backend/config/config_dev.json")
print("CONFIG_PATH :-", CONFIG_PATH)
DATATYPE_MANAGER_PATH = Path(CONFIG_PATH).parent.joinpath("datatype_manager.json")
print("DATATYPE_MANAGER_PATH:-", DATATYPE_MANAGER_PATH)
with open(CONFIG_PATH, "r", encoding="utf-8") as conf_reader:
    CONF_DICT = json.load(conf_reader)
with open(DATATYPE_MANAGER_PATH, "r", encoding="utf-8") as datatype_manager_reader:
    DATATYPE_MANAGER = json.load(datatype_manager_reader)
DATATYPE_MAPPER = DATATYPE_MANAGER["datatype_mapper"]
DATATYPE_MAPPER = {key: DATATYPE_MANAGER[DATATYPE_MAPPER[key]] for key in DATATYPE_MAPPER}
DATATYPE_MANAGER["datatype_mapper"] = DATATYPE_MAPPER
LOG_LEVEL = logging.DEBUG
LOGGER = logging.getLogger("job_runner")
LOGGER.setLevel(LOG_LEVEL)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LOG_LEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
