import sys, os, getopt, json, pprint
from rucio.client.rseclient import RSEClient

Usage = """
$ site_ctl set <rse> <parameter> <value>
$ site_ctl reset <rse> (<parameter>|-a)
$ site_ctl get <rse> <parameter>
$ site_ctl show <rse> (<parameter>|all)
$ site_ctl dump <rse> > <JSON file>
$ site_ctl load <rse> < <JSON file>
$ site_ctl list
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

def remove_prefix(name):
    assert name.startswith(Prefix + ".")
    return name[len(Prefix) + 1:]
    
def add_prefix(name):
    return Prefix + "." + name

InternalsParams = [add_prefix(name) for name in Params]

def read_config(rse):
    client = RSEClient()
    params = client.list_rse_attributes(rse)
    config = { 
        remove_prefix(name): value 
        for name, value in params.items()
        if name in InternalsParams
    }
    if "ce_disabled" in config:
        value = config["ce_disabled"] in (True, "true")
        config["ce_disabled"] = "true" if value else "false"
    return config

def write_config(rse, config):
    assert config.get("ce_disabled", False) in ("true", "false", True, False)
    #if disabled_name in config:
    #    assert config.get(disabled_name, "false") in ("true", "false")
    #    config = config.copy()
    #    config[disabled_name] = "true" if config.get(disabled_name, "") else "false"
    if "ce_disabled" in config:
        config = config.copy()
        value = config["ce_disabled"] in (True, "true")
        config["ce_disabled"] = "true" if value else "false"
    config = {add_prefix(name): value for name, value in config.items()}

    client = RSEClient()
    existing_config = {
        name: value
        for name, value in client.list_rse_attributes(rse).items()
        if name in InternalsParams
    }
    for name in existing_config:
        if name not in config:
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
    for name, value in sorted(config.items()):
        print("%-30s: %s" % (name, value))

def do_load(rse):
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
    
def do_reset(rse, name):
    if name == "-a":
        config = {}
    else:
        config = read_config(rse)
        if name in config:
            del config[name]
    write_config(rse, config)
        
def do_list():
    for name in Params:
        print(name)

def main():
    if len(sys.argv) < 2:
        print(Usage)
        sys.exit(2)
    opts, args = getopt.getopt(sys.argv[1:], "")
    if len(args) < 2:
        rse, cmd, args = None, args[0], []
    else:
        rse, cmd, args = args[0], args[1], args[2:]
    if cmd == "dump":            do_dump(rse)
    elif cmd == "show":          do_show(rse)
    elif cmd == "load":          do_load(rse)
    elif cmd == "set":           do_set(rse, args[0], args[1])
    elif cmd == "reset":         do_reset(rse, args[0])
    elif cmd == "list":          do_list()
    elif cmd == "get":           do_get(rse, args[0])
    else:
        print(Usage)
        sys.exit(2)

main()
sys.exit(0)