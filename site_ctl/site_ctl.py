import sys, os, getopt, json, pprint
from rucio.client.rseclient import RSEClient

Usage = """
$ site_ctl set <rse> <parameter> <value>
$ site_ctl reset <rse> <parameter>
$ site_ctl show <rse> (<parameter>|all)
$ site_ctl dump <rse> > <JSON file>
$ site_ctl load <rse> < <JSON file>
$ site_ctl help
"""

Prefix = "CE_cfg"

Params = [Prefix + "." + n for n in 
    [
        "ce_disabled",
        "sever",
        "server_root",
        "timeout",
        "roots",
        "ignore_list",
        "nworkers",
        "max_dark_fraction",
        "max_missing_fraction"
    ]
]

def read_config(rse):
    client = RSEClient()
    params = client.list_rse_attributes(rse)
    config = {name: value for name, value in params.items() if name in Params}
    assert config.get(Prefix + "." + "ce_disabled", False) in (True, False)
    return config

def write_config(rse, config):
    disabled_name = Prefix + "." + "ce_disabled"
    assert config.get(disabled_name, "false") in ("true", "false")
    if disabled_name in config:
        config[disabled_name] = config[disabled_name] and config[disabled_name] != "false"
        print("disabled set to:", config[disabled_name], type(config[disabled_name]))
    #if disabled_name in config:
    #    assert config.get(disabled_name, "false") in ("true", "false")
    #    config = config.copy()
    #    config[disabled_name] = "true" if config.get(disabled_name, "") else "false"
    client = RSEClient()
    existing_config = client.list_rse_attributes(rse)
    for name in existing_config:
        if name in Params and name not in config:
            client.delete_rse_attribute(rse, name)
    for name, value in config.items():
        client.add_rse_attribute(rse, name, str(value))

#
#
#

def do_dump(rse):
    config = read_config(rse)
    print(json.dumps(config, indent=4, sort_keys=True))

def do_show(rse):
    config = read_config(rse)
    pprint.pprint(config)

def do_load(rse):
    rse = argv[0]
    config = json.load(sys.stdin)
    write_config(rse, config)

def do_set(rse, name, value):
    # name and value are external
    config = read_config(rse)
    config[name] = value
    write_config(rse, config)

def do_get(rse, name):
    config = read_config(rse)
    print(name, config.get(name, "-"))

def main():
    if len(sys.argv) < 2:
        print(Usage)
        sys.exit(2)
    cmd, args = sys.argv[1], sys.argv[2:]
    commands = {
        "dump": do_dump,
        "show": do_show,
        "load": do_load,
        "set": do_set,
        "get": do_get
    }
    if cmd not in commands:
        print(Usage)
        sys.exit(2)
    
    commands[cmd](*args)

main()