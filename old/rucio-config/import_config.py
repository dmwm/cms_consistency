import yaml, sys, getopt, json
from rucio.client.configclient import ConfigClient
from rucio.client.rseclient import RSEClient
from rucio.common.exception import ConfigNotFound


CONFIG_SECTION_PREFIX = "consistency_enforcement"


Usage = """
python import_ce_config.py [-c] <config.yaml>
    -c -- create RSE if not found
"""


opts, args = getopt.getopt(sys.argv[1:], "c")
if not args:
    print(Usage)
    sys.exit(2)
    
opts = dict(opts)
create_rse = "-c" in opts
    
cc_cfg = yaml.load(open(args[0], "r"), Loader=yaml.SafeLoader)

#
# Defaults
#

cfg_client = ConfigClient()
defaults_in = cc_cfg["rses"]["*"]
    
def set_option(subsection, name, value):
    section = CONFIG_SECTION_PREFIX
    if subsection:  section += "." + subsection
    if isinstance(value, list):
        value = " ".join(value) or "[]"
    cfg_client.set_config_option(section, name, str(value))

def copy_config(config_in, field, subsection, default=None, required=False):
    if field in config_in:
        value = config_in.get(field, default)
        if value is not None:
            set_option(subsection, field, value)
    elif required:
        raise KeyError(f"Rquired field {field} not found")

def clear_section(section):
    section = CONFIG_SECTION_PREFIX if not section else CONFIG_SECTION_PREFIX + "." + section
    try:
        data = cfg_client.get_config(section)
    except ConfigNotFound:
        return
    if data:
        for k in data.keys():
            cfg_client.delete_config_option(section, k)

clear_section("")

set_option("", "npartitions", defaults_in.get("partitions", 10))

clear_section("scanner")
if "scanner" in defaults_in:
    scanner_in = defaults_in["scanner"]
    
    copy_config(scanner_in, "recursion", "scanner")
    copy_config(scanner_in, "nworkers", "scanner")
    copy_config(scanner_in, "timeout", "scanner")
    copy_config(scanner_in, "server_root", "scanner", required=True)
    
    roots_in = scanner_in.get("roots", {})
    set_option("scanner", "roots", json.dumps(roots_in))
    
clear_section("dbdump")
if "dbdump" in defaults_in:
    dbdump_in = defaults_in["dbdump"]
    copy_config(defaults_in, "path_root", "dbdump")

clear_section("missing_action")
if "missing_action" in defaults_in:
    missing_action_in = defaults_in["missing_action"]
    copy_config(missing_action_in, "max_fraction", "missing_action")
    copy_config(missing_action_in, "max_age_last_run", "missing_action")

clear_section("dark_action")
if "dark_action" in defaults_in:
    dark_action_in = defaults_in["dark_action"]
    copy_config(dark_action_in, "max_fraction", "dark_action")
    copy_config(dark_action_in, "max_age_last_run", "dark_action")
    copy_config(dark_action_in, "min_age_first_run", "dark_action")
    copy_config(dark_action_in, "confirmation_window", "dark_action")
    copy_config(dark_action_in, "min_runs", "dark_action")

#
# RSE specifics
#

rse_client = RSEClient()

for rse, rse_cfg in cc_cfg["rses"].items():
    if rse == "*":  continue          # do not try to import defaults
    
    rse = rse.upper()
    try:
        rse_client.get_rse(rse)
    except:
        if create_rse:
            rse_client.add_rse(rse)
            print(f"New RSE {rse} created")
        else:
            print(f"RSE {rse} does not exist - skipping")
            continue
            
    rse_client.add_rse_attribute(rse, CONFIG_SECTION_PREFIX, json.dumps(rse_cfg))
