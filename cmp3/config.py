from __future__ import print_function
import yaml, re, os, configparser

class DBConfig:

	# class to read relevant parameters from rucio.cfg

    def __init__(self, schema, dburl):
        self.DBURL = dburl
        self.Schema = schema
    
    @staticmethod
    def from_cfg(path):
        cfg = configparser.ConfigParser()
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
    
        
    

class Config:
        def __init__(self, cfg_file_path):
                cfg = yaml.load(open(cfg_file_path, "r"), Loader=yaml.SafeLoader)
                self.Config = cfg
                self.Defaults = cfg["rses"].get("*", {})
                self.RSEs = cfg["rses"]

        def rsecfg(self, rse_name):
                cfg = {}
                cfg.update(self.RSEs.get("*", {}))
                cfg.update(self.RSEs.get(rse_name, {}))
                return cfg

        def get_by_path(self, *path, default=None):
            c = self.Config
            for p in path[:-1]:
                c1 = c.get(p, None)
                if not c1:
                    return default
                c = c1
            return c.get(path[-1], default)

        def general_param(self, rse_name, param, default=None):
            return self.get_by_path("rses", rse_name, param, 
                    default = self.get_by_path("rses", "*", param, default=default))
                    
        rse_param = general_param

        def scanner_root_config(self, rse_name, root):
            lst = self.scanner_param(rse_name, "roots", self.scanner_param("*", "roots", []))
            for d in lst:
                if d["path"] == root:
                    return d
            return {}
            
        def scanner_param(self, rse_name, param, default=None, root=None):
            #
            # Param lookup order if root is specified:
            # 1. rses->rse->root->param
            # 2. rses->*->root->param
            # 3. rses->rse->param
            # 4. rses->*->param
            #
            # If no root specified:
            # 1. rses->rse->param
            # 2. rses->*->param
            # 
            if root:
                cfg = self.scanner_root_config(rse_name, root)  # that will look in the rse-specific first and then "*"
                value = cfg.get(param)
                if value is None:   value = self.scanner_param(rse_name, param, default=default)
            else:
                default = self.get_by_path("rses", "*", "scanner", param, default=default)
                value = self.get_by_path("rses", rse_name, "scanner", param, default=default)
            return value

        def import_param(self, rse_name, param, default=None):
            default = self.get_by_path("rses", "*", "import", param, default=default)
            value = self.get_by_path("rses", rse_name, "import", param, default=default)
            return value

        def dbdump_param(self, rse_name, param, default=None):
            default = self.get_by_path("rses", "*", "dbdump", param, default=default)
            return self.get_by_path("rses", rse_name, "dbdump", param, default=default)

        def dbdump_root(self, rse_name):
            return self.dbdump_param(rse_name, "path_roots", "/")

        def nparts(self, rse_name):
            return self.general_param(rse_name, "partitions", 10)

        def ignore_lists(self, rse_name):
            lst = self.general_param(rse_name, "ignore_list", [])       # list of absolute paths or regexp patterns, used by scanner and cmp3
            for p in lst:
                try:    p = re.compile("%s(/.*)?$" % (p,))
                except:
                    p = re.compile(p)
                dir_patterns.append(p)
            file_pattenrs = []
            for p in lst:
                p = re.compile("%s/.+" % (p,))
                file_patterns.append(p)
            return dir_patterns, file_pattenrs
            
        def scanner_server_root(self, rse_name):
            return self.scanner_param(rse_name, "server_root")

        def scanner_roots(self, rse_name):
            d = self.scanner_param(rse_name, "roots", [])
            return [x["path"] for x in d]
            
        def scanner_remove_prefix(self, rse_name):
            return self.scanner_param(rse_name, "remove_prefix")

        def scanner_add_prefix(self, rse_name):
            return self.scanner_param(rse_name, "add_prefix")

        def scanner_filter(self, rse_name):
            return self.scanner_param(rse_name, "filter")

        def scanner_rewrite(self, rse_name):
            dct = self.scanner_param(rse_name, "rewrite")
            if not dct:
                return None, None
            return dct["path"], dct["out"]

        def scanner_server(self, rse_name):
            return self.scanner_param(rse_name, "server")

        def scanner_workers(self, rse_name):
            return self.scanner_param(rse_name, "nworkers", default=10)

        def scanner_timeout(self, rse_name):
            return self.scanner_param(rse_name, "timeout", default=60)

        def scanner_recursion_threshold(self, rse_name, root):
            return self.scanner_param(rse_name, "recursion", root=root, default=3)

