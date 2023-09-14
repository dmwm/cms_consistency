import os, yaml, pprint, json
from rucio.client.rseclient import RSEClient

class MergedCEConfiguration(object):

    CONFIG_PREFIX = "CE_config."
    """
            Disable CE for the site
            Thresholds for CE actions (fraction of dark/missing files to abort the action)
            List of roots to scan
            List of ignored directories
            Scanning concurrency
            Scanning timeout
            Server address
    """

    def __init__(self, rse, config_file, account="root"):
        self.RSEClient = RSEClient(account=account)
        self.RSE = rse
        self.ConfigFromFile = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)
        self.ConfigFromRSE = self.config_from_rse()

    def config_from_rse(self):
        rse_config = self.RSEClient.list_rse_attributes(rse)
        cfg = {}
        if self.CONFIG_PREFIX+"ce_disabled" in rse_config:     
            value = rse_config[self.CONFIG_PREFIX+"ce_disabled"]
            if isinstance(value, str):
                value = value in ("yes", "true", "disabled")
            cfg["ce_disabled"] = value
        if self.CONFIG_PREFIX+"ignore_list" in rse_config: 
            cfg["ignore_list"] = rse_config[self.CONFIG_PREFIX+"ignore_list"].split(",")

        scanner_cfg = {}
        if self.CONFIG_PREFIX+"server" in rse_config:          
            scanner_cfg["server"] = rse_config[self.CONFIG_PREFIX+"server"]
        if self.CONFIG_PREFIX+"server_root" in rse_config:     
            scanner_cfg["server_root"] = rse_config[self.CONFIG_PREFIX+"server_root"]
        if self.CONFIG_PREFIX+"roots" in rse_config:           
            scanner_cfg["roots"] = [{"path":path} for path in rse_config[self.CONFIG_PREFIX+"roots"].split(",")]
        if self.CONFIG_PREFIX+"nworkers" in rse_config:        
            scanner_cfg["nworkers"] = int(rse_config[self.CONFIG_PREFIX+"nworkers"])
        if self.CONFIG_PREFIX+"timeout" in rse_config:         
            scanner_cfg["timeout"] = int(rse_config[self.CONFIG_PREFIX+"timeout"])
        cfg["scanner"] = scanner_cfg

        if self.CONFIG_PREFIX+"max_dark_fraction" in rse_config:       
            cfg["dark_action"] = {"max_fraction":float(rse_config[self.CONFIG_PREFIX+"max_dark_fraction"])}
        if self.CONFIG_PREFIX+"max_missing_fraction" in rse_config:    
            cfg["missing_action"] = {"max_fraction":float(rse_config[self.CONFIG_PREFIX+"max_missing_fraction"])}
        return cfg

    def merge(self, defaults, overrides):
        out = defaults.copy()
        for key, value in overrides.items():
            if isinstance(value, dict):
                out[key] = self.merge(defaults.get(key, {}), value)
            else:
                out[key] = value
        return out

    def merged_config(self):
        rse_config = self.merge(self.ConfigFromFile["rses"].get("*", {}), self.ConfigFromFile["rses"].get(rse, {}))
        #print("merged rse config from file:")
        #pprint.pprint(rse_config)
        out = self.merge(rse_config, self.ConfigFromRSE)
        #print("final merged:", out)
        return out

Usage = """
python merge_config.py merge [-j] <rse> <config file> 
python merge_config.py get [-d <default>] <config file> <path, dot-separated>
"""

if __name__ == "__main__":
    import sys, getopt
    
    if not sys.argv[1:]:
        print(Usage)
        sys.exit(2)
    
    cmd, argv = sys.argv[1], sys.argv[2:]
    
    if cmd == "merge":
        opts, args = getopt.getopt(argv, "j")
        opts = dict(opts)
        rse, config_file = args
        cfg = MergedCEConfiguration(rse, config_file)
        merged = {     # keep format for backward compatibility
            "rses":
                {   "*":        {}, 
                    rse:   cfg.merged_config()
                }
        }
        if "-j" in opts:
            json.dump(merged, sys.stdout, sort_keys=True, indent=4)
        else:
            yaml.dump(merged, sys.stdout)

    elif cmd == "get":
        opts, args = getopt.getopt(argv, "d:")
        opts = dict(opts)
        default = opts.get("-d")
        merged_config_file, path = args
        cfg = yaml.load(open(merged_config_file, "r"), Loader=yaml.SafeLoader)
        path = path.split(".")
        value = cfg
        try:
            while path:
                head = path.pop(0)
                if head:
                    value = value[head]
        except KeyError:
            if default is not None:
                value = default
            else:
                print("Path not fond", file=sys.stderr)
                sys.exit(1)
        print(value)
        
        
    

