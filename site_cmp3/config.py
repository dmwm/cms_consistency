from __future__ import print_function
import yaml, re, os

class DBConfig:

        def __init__(self, cfg):
                self.Host = cfg["host"]
                self.Port = cfg["port"]
                self.Schema = cfg["schema"]
                self.User = cfg["user"]
                self.Password = cfg["password"]
                self.Service = cfg["service"]

        def dburl(self):
                return "oracle+cx_oracle://%s:%s@%s:%s/?service_name=%s" % (
                        self.User, self.Password, self.Host, self.Port, self.Service)


class Config:
	def __init__(self, cfg_file_path):
		cfg = yaml.load(open(cfg_file_path, "r"), Loader=yaml.SafeLoader)
		self.DBConfig = DBConfig(cfg["database"])
		self.DBSchema = self.DBConfig.Schema
		self.DBURL = self.DBConfig.dburl()
		self.RSEs = cfg["rses"]

	def rsecfg(self, rse_name):
		cfg = {}
		cfg.update(self.RSEs.get("*", {}))
		cfg.update(self.RSEs.get(rse_name, {}))
		return cfg

	def lfn_to_path(self, rse_name):
		rules = self.rsecfg(rse_name)["dbdump"]["lfn_to_path"]
		return [ {
			"path":re.compile(r["path"]),
			"out":r["out"].replace("$", "\\")
			} for r in rules
		]

	def path_root(self, rse_name):
		return self.rsecfg(rse_name).get("dbdump",{}).get("path_root")

	def nparts(self, rse_name):
		return self.rsecfg(rse_name).get("partitions", 10)

	def scanner_cfg(self, rse_name):
		return self.rsecfg(rse_name).get("scanner", {})

	def scanner_root(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg.get("root", "/")

	def scanner_remove_prefix(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg.get("remove_prefix", None)

	def scanner_add_prefix(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg.get("add_prefix", None)

	def scanner_server(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg["server"]

	def scanner_workers(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg.get("workers", 5)

	def scanner_timeout(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg.get("timeout", 30)

	def scanner_recursion_threshold(self, rse_name):
                cfg = self.scanner_cfg(rse_name)
		return cfg.get("recursion", 3)

	def dbdump_root(self, rse_name):
                rsecfg = self.rsecfg(rse_name)
		return rsecfg.get("dbdump_root", "/")
		
