import re, os, json, yaml
from configparser import ConfigParser

class DBConfig:

	# class to read relevant parameters from rucio.cfg

    def __init__(self, schema, dburl):
        self.DBURL = dburl
        self.Schema = schema
    
    @staticmethod
    def from_cfg(path):
        cfg = ConfigParser()
        cfg.read(path)
        dbparams = dict(cfg.items("database"))
        return DBConfig(dbparams.get("schema"), dbparams["default"])
        
    @staticmethod
    def from_yaml(path_or_dict):
        if isinstance(path_or_dict, str):
            cfg = yaml.load(open(path_or_dict, "r"), Loader=yaml.SafeLoader)["database"]
        else:
            cfg = path_or_dict

        user = cfg["user"]
        password = cfg["password"]
        schema = cfg["schema"]
        conn_str = None
        if "connstr" in cfg:
            conn_str = cfg["connstr"]
            dburl = "oracle+cx_oracle://%s:%s@%s" % (user, password, conn_str)
        else:
            host = cfg["host"]
            port = cfg["port"]
            service = cfg["service"]
            dburl = "oracle+cx_oracle://%s:%s@%s:%s/?service_name=%s" % (
                                    user, password, host, port, service)
        return DBConfig(schema, dburl)
        
class ConfigBackend(object):
    
    #
    # Helpers
    #

    def section_as_dict(self, parser, section):
        out = {}
        for name, value in parser.items(section):
            try:    value = int(value)
            except: pass
            out[name] = value
        return out

    def roots_as_dict(self, iterable):
        return {r["path"]:r for r in iterable}

    def format_ignore_list(self, lst):
        if not lst: lst = []
        if isinstance(lst, str):
            if " " in lst:
                lst = lst.split()
            else:
                lst = [lst]
        return lst
        
    #
    # To be implemented by the actual backend
    #
            
    def get_config(self, rse="*"):
        # returns configuration for RSE or defaults as dictionary
        raise NotImplementedError()
        
    def get_root_dict(self, rse="*"):
        # returns None if no roots defined for the RSE or dictionary keyed by root path
        raise NotImplementedError()
        
    #
    # Low level methods
    #
    
    def get_scanner(self, rse_name="*"):
        return self.get_config(rse_name).get("scanner", {})

    def get_top(self, rse_name="*"):
        return self.get_config(rse_name)

    def get_dbdump(self, rse_name="*"):
        return self.get_config(rse_name).get("dbdump", {})

    #
    # Methods implementing RSE/common logic
    #
        
    def get_value(self, name, specifc, common, default, required):
        if name in specifc:
            return specifc[name]
        if name in common:
            return common[name]
        if required:
            raise KeyError(f"Required field {name} not found")
        else:
            return default
            
    def scanner_param(self, rse_name, param, default=None, required=False):
        # 1. rses->rse->param
        # 2. rses->*->param
        scanner_rse = self.get_scanner(rse_name)
        scanner_common = self.get_scanner()
        #print("scanner_rse:", scanner_rse)
        return self.get_value(param, scanner_rse, scanner_common, default, required)
        
    def dbdump_param(self, rse_name, param, default=None, required=False):
        # 1. rses->rse->param
        # 2. rses->*->param
        dbdump_rse = self.get_dbdump(rse_name)
        defaults = self.get_dbdump()
        value = self.get_value(param, dbdump_rse, defaults, default, required)
        if param == "ignore": value = self.format_ignore_list(value)
        return value

    def rse_param(self, rse_name, param, default=None, required=False):
        rse_cfg = self.get_top(rse_name)
        defaults = self.get_top()
        return self.get_value(param, rse_cfg, defaults, default, required)

    def root_list(self, rse):
        roots_dict = self.get_root_dict(rse)
        if roots_dict is None:
            roots_dict = self.get_root_dict("*") or {}
        return list(roots_dict.keys())

    def root_param(self, rse, root, param, default=None, required=False):
        roots_dict = self.get_root_dict(rse)
        #print(f"root_param({rse}, {root}, {param}):")
        #print("  specific roots:", roots_dict)
        if roots_dict is None:
            roots_dict = self.get_root_dict("*") or {}
            #print("  common roots:", roots_dict)
        root_config = roots_dict.get(root, {})
        value = self.get_value(param, root_config, {}, default, required)
        #print("  value:", value)
        if param == "ignore": value = self.format_ignore_list(value)
        return value
        
class ConfigYAMLBackend(ConfigBackend):
    
    def __init__(self, cfg):
        # cfg is either YAML file path or a dictionary read from the YAML file
        if isinstance(cfg, str):
            import yaml
            cfg = yaml.load(open(cfg, "r"), Loader=yaml.SafeLoader)
        cfg = cfg.get("rses", {})
        self.Config = cfg
        self.Roots = {}             # {rse -> {root -> root_config}} 
        for rse, data in cfg.items():
            roots = data.get("scanner", {}).get("roots")
            if roots is not None:
                self.Roots[rse] = self.roots_as_dict(roots)
        #print("ConfigYAMLBackend.Config:", self.Config)
        #print("ConfigYAMLBackend.__init__: Roots:", self.Roots)

    def get_config(self, rse="*"):
        cfg = self.Config.get(rse, {})
        #print(f"get_config({rse}): cfg:", cfg)
        return self.Config.get(rse, {})
        
    def get_root_dict(self, rse="*"):
        return self.Roots.get(rse)
        
class ConfigRucioBackend(ConfigBackend):

    CONFIG_SECTION_PREFIX = "consistency_enforcement"

    def __init__(self, account="root"):
        from rucio.client.configclient import ConfigClient
        from rucio.client.rseclient import RSEClient
        from rucio.common.exception import ConfigNotFound

        self.RSEClient = RSEClient(account=account)
        self.ConfigClient = ConfigClient(account=account)
        
        self.Common = None
        self.CommonRoots = None

        self.RSESpecific = {}
        self.RSERoots = {}
        
    def get_config(self, rse="*"):
        if rse == "*":
            if self.Common is None:
                self.Common = self.ConfigClient.get_config(self.CONFIG_SECTION_PREFIX)
                scanner = self.Common["scanner"] = self.ConfigClient.get_config(self.CONFIG_SECTION_PREFIX + ".scanner")
                dbdump = self.Common["dbdump"] = self.ConfigClient.get_config(self.CONFIG_SECTION_PREFIX + ".dbdump")
                
                # parse roots config
                self.CommonRoots = self.roots_as_dict(json.loads(scanner.get("roots", "[]")))
                        
            return self.Common
        else:
            cfg = self.RSESpecific.get(rse)
            if cfg is None:
                cfg = {}
                try:    cfg = self.RSEClient.list_rse_attributes(rse.upper())
                except: pass
                if cfg:
                    cfg = cfg.get(self.CONFIG_SECTION_PREFIX, "{}")
                    cfg = json.loads(cfg)
                self.RSESpecific[rse] = cfg
                roots = cfg.get("scanner", {}).get("roots")
                #print(f"Rucio backend get_config({rse}): roots:", roots)
                if roots is not None:
                    roots = self.roots_as_dict(roots)
                self.RSERoots[rse] = roots
            return cfg
            
    def get_root_dict(self, rse="*"):
        if rse == "*":
            if self.CommonRoots is None:
                self.get_config()
            dct = self.CommonRoots or {}
        else:
            cfg = self.get_config(rse)  # this will fetch self.RSERoots[rse] as dict
            dct = self.RSERoots.get(rse)
        return dct
        
    def get_root(self, root, rse="*"):
        if rse == "*":
            if not root in self.CommonRoots:
                root_cfg = None
                try:    root_cfg = self.ConfigClient.get_config(self.CONFIG_SECTION_PREFIX + ".scanner.root." + root)
                except: pass
                self.CommonRoots[root] = root_cfg
            return self.CommonRoots.get(root) or {}
        else:
            if rse not in self.RSERoots:
                self.get_config(rse)       # this will load self.RSERoots[rse_name], if any
            cfg = (self.RSERoots.get(rse) or {}).get(root)
            #print(f"Rucio backend: get_root({root}, {rse}): cfg:", cfg)
            return cfg
        
class CCConfiguration(object):
    
    def __init__(self, backend, rse):
        self.Backend = backend
        self.RSE = rse

        self.NPartitions = int(backend.rse_param(rse, "npartitions", 10))

        self.Server = backend.scanner_param(rse, "server", required=True)
        self.ServerRoot = backend.scanner_param(rse, "server_root", "/store", required=True)
        self.ScannerTimeout = int(backend.scanner_param(rse, "timeout", 300))
        self.RootList = backend.root_list(rse)
        self.RemovePrefix = backend.scanner_param(rse, "remove_prefix", "/")
        self.AddPrefix = backend.scanner_param(rse, "add_prefix", "/store/")
        self.NWorkers = int(backend.scanner_param(rse, "nworkers", 8))
        self.IncludeSizes = backend.scanner_param(rse, "include_sizes", "yes") == "yes"
        self.RecursionThreshold = int(backend.scanner_param(rse, "recursion", 1))
        self.ServerIsRedirector = backend.scanner_param(rse, "is_redirector", True)

        self.DBDumpPathRoot = backend.dbdump_param(rse, "path_root", "/")
        self.DBDumpIgnoreSubdirs = backend.dbdump_param(rse, "ignore", [])

    def ignore_subdirs(self, root):
        return self.Backend.root_param(self.RSE, root, "ignore", [])

    @staticmethod
    def rse_config(rse, backend_type, *backend_args):
        if backend_type == "rucio":
            backend = ConfigRucioBackend(*backend_args)
        elif backend_type == "yaml":
            backend = ConfigYAMLBackend(*backend_args)
        else:
            raise ValueError(f"Unknown configuration backend type {backend_type}")
        return CCConfiguration(backend, rse)

if __name__ == "__main__":
    import sys, getopt
    opts, args = getopt.getopt(sys.argv[1:], "c:r")
    opts = dict(opts)
    rse, param = args[:2]
    
    if "-c" in opts:
        backend = ConfigYAMLBackend(opts["-c"])
    elif "-r" in opts:
        backend = ConfigRucioBackend()
    
    cfg = CCConfiguration(backend, rse)
    print(getattr(cfg, param))
        
        
    

