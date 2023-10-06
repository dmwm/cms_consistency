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

Params = [
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

Lists = [
    "roots",
    "ignore_list"
]

def internal_name(name):
    if not name.startswith(Prefix + '.'):
        name = Prefix + '.' + name
    return name

def external_name(name):
    if name.startswith(Prefix + '.'):
        name = name[len(Prefix) + 1:]
    return name

def internal_to_external(config):
    config = {external_name(name): value for name, value in config.items() if name in Params}
    for name in Lists:
        if name in config:
            config["name"] = config["name"].split('/')
    return config

def external_to_internal(config):
    config = config.copy()
    for name in Lists:
        if name in config:
            config[name] = ','.join(config[name])
    return {internal_name(name): value for name, value in config.items() if name in Params}

def read_config(rse):
    client = RSEClient()
    params = client.list_rse_attributes(rse)
    config = {name: value for name, value in params.items() if name in Params}
    return internal_to_external(config)

def write_config(rse, config):
    config = external_to_internal(config)
    client = RSEClient()
    existing_config = client.list_rse_attributes(rse)
    for name in existing_config:
        if name not in config:
            client.delete_rse_attribute(rse, name)
    for name, value in config.items():
        client.add_rse_attribute(rse, name)
    return config

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
    print(name, config.get(name, ""))

def main():
    cmd, args = sys.argv[1], sys.argv[2:]
    {
        "dump": do_dump,
        "show": do_show,
        "load": do_load,
        "set": do_set,
        "get": do_get
    }[cmd](*args)

main()