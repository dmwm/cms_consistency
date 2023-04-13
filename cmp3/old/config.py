import re, os, json, yaml
from configparser import ConfigParser
from rucio_ce import 

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

    def get_action(self, action, rse_name="*"):
        return self.get_config(rse_name).get(action + "_action", {})

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
    
    def action_param(self, rse_name, action, param, default=None, required=False):
        specific = self.get_action(action, rse_name)
        common = self.get_action(action)
        return self.get_value(param, specific, common, default, required)
    
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
            roots = (data or {}).get("scanner", {}).get("roots")
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
                self.Common["missing_action"] = self.ConfigClient.get_config(self.CONFIG_SECTION_PREFIX + ".missing_action")
                self.Common["dark_action"] = self.ConfigClient.get_config(self.CONFIG_SECTION_PREFIX + ".dark_action")

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


class CEConfiguration(object):
    
    def __init__(self, rse, source, **args):
        if source == "rucio":
            backend = ConfigRucioBackend(**args)
        else:
            backend = ConfigYAMLBackend(source)
        self.Backend = backend
        self.RSE = rse
        self.NPartitions = int(backend.rse_param(rse, "npartitions", 10))
        self.IgnoreList = self.rse_param(rse, "ignore_list", [])
        self.RootList = self.root_list(rse)
        
    def __contains__(self, name):
        try:    _ = self[name]
        except KeyError:    return False
        else:   return True

    def __getattr__(self, name):
        return getattr(self.Backend, name)

    def get(self, name, default=None):
        try:    return self[name]
        except KeyError: return default


class ScannerConfiguration(CEConfiguration):
    
    def __init__(self, rse, source, **source_agrs):
        CEConfiguration.__init__(self, rse, source, **source_agrs)

        self.Server = self.scanner_param(rse, "server", None, required=True)
        self.ServerRoot = self.scanner_param(rse, "server_root", "/")           # prefix up to, but not including /store/
        self.ScannerTimeout = int(self.scanner_param(rse, "timeout", 300))
        self.RemovePrefix = self.scanner_param(rse, "remove_prefix", "")        # to be applied after site root is removed
        self.AddPrefix = self.scanner_param(rse, "add_prefix", "")              # to be applied after site root is removed
        self.NWorkers = int(self.scanner_param(rse, "nworkers", 8))
        self.IncludeSizes = self.scanner_param(rse, "include_sizes", "yes") == "yes"
        self.RecursionThreshold = int(self.scanner_param(rse, "recursion", 1))
        self.ServerIsRedirector = self.scanner_param(rse, "is_redirector", True)

    def ignore_subdirs(self, root):
        return self.root_param(self.RSE, root, "ignore", [])

    def __getitem__(self, name):
        return self.scanner_param(self.RSE, name, required=True)

class DBDumpConfiguration(CEConfiguration):

    def __init__(self, rse, *params, **agrs):
        CEConfiguration.__init__(self, rse, *params, **agrs)
        self.DBDumpPathRoot = self.dbdump_param(rse, "path_root", "/")

    def __getitem__(self, name):
        return self.dbdump_param(self.RSE, name, required=True)

class ActionConfiguration(CEConfiguration):
    
    def __init__(self, rse, source, action, **source_agrs):
        CEConfiguration.__init__(self, rse, source, **source_agrs)
        self.Action = action
        self.AbortThreshold = float(self.action_param(rse, action, "max_fraction", 0.01))
        self.MaxAgeOfLastRun = int(self.action_param(rse, action, "max_age_last_run", 1))               # days

        if action in ("dark", "empty"):
            self.ConfirmationWindow = int(self.action_param(rse, action, "confirmation_window", 35))            # days
            self.MinAgeOfFirstRun = int(self.action_param(rse, action, "min_age_first_run", 25))
            self.MinRuns = int(self.action_param(rse, action, "min_runs", 3))

    def __getitem__(self, name):
        return self.action_param(self.RSE, self.Action, name, required=True)
        
class EmptyActionConfiguration(ActionConfiguration):
    
    def __init__(self, rse, source, **source_agrs):
        ActionConfiguration.__init__(self, rse, source, "empty", **source_agrs)

        self.Server = self.scanner_param(rse, "server", None, required=True)
        self.ServerRoot = self.scanner_param(rse, "server_root", "/store")
        self.ScannerTimeout = int(self.scanner_param(rse, "timeout", 300))
        self.NWorkers = int(self.scanner_param(rse, "nworkers", 8))
        self.ServerIsRedirector = self.scanner_param(rse, "is_redirector", True)
        self.Disabled = self.action_param(rse, "empty", "disabled", False) 

if __name__ == "__main__":
    import sys, getopt
    opts, args = getopt.getopt(sys.argv[1:], "c:r")
    opts = dict(opts)
    rse, param = args[:2]
    
    cfg = CEConfiguration(rse, opts["-c"])
    print(getattr(cfg, param))
        
        
    

