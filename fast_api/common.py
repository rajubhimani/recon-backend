import os
import json
from pathlib import Path

CONFIG_PATH = os.environ.get("HARTREE_ENV", "config/config_dev.json")
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
