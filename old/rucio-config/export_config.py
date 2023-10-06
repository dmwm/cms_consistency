import yaml, sys, getopt, json
from rucio.client.configclient import ConfigClient
from rucio.client.rseclient import RSEClient
from rucio.common.exception import ConfigNotFound

Usage = """
python export_config.py
Exports CE configuration to YAML format, prints it to stdout
"""

CONFIG_SECTION_PREFIX = "consistency_enforcement"

def load_defaults(client):
    # load section from Rucio cfg with RSE defaults
    # post-process the section appropriately and return a dictionary suitable for dumping in YAML format
    try:    defaults = client.get_config(CONFIG_SECTION_PREFIX)
    except: defaults = {}

    for subsection in ("scanner", "dbdump", "missing_action", "dark_action"):
        try:    
            cfg = client.get_config(CONFIG_SECTION_PREFIX + '.' + subsection)
            if subsection == "scanner":
                if "roots" in cfg:
                    cfg["roots"] = sorted(json.loads(cfg["roots"]), key=lambda r: r["path"])
            if cfg:
                defaults[subsection] = cfg
        except ConfigNotFound:
            pass
    return defaults
    
def load_rse(client, rse):
    cfg = None
    try:    
        cfg = client.list_rse_attributes(rse.upper()).get(CONFIG_SECTION_PREFIX)
        if cfg:
            cfg = json.loads(cfg)
            if "roots" in cfg:
                cfg["roots"] = sorted(cfg["roots"], key=lambda r: r["path"])
    except: cfg = None
    return cfg
    
rses = {
        "*":    load_defaults(ConfigClient())
    }

rse_client = RSEClient()
for rse in rse_client.list_rses():
    rse = rse["rse"]
    cfg = load_rse(rse_client, rse)
    if cfg is not None:
        rses[rse] = cfg

out = yaml.dump({"rses":rses})
print(out)

    
