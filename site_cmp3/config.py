from __future__ import print_function
import json, re, os

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
		cfg = json.load(open(cfg_file_path, "r"))
		self.DBConfig = DBConfig(cfg["database"])
		self.DBSchema = self.DBConfig.Schema
		self.DBURL = self.DBConfig.dburl()
		self.RSEs = cfg["rses"]

	def lfn_to_pfn(self, rse_name):
		rules = self.RSEs.get(rse_name, self.RSEs.get("*", {}))["lfn_to_pfn"]
		return [ {
			"path":re.compile(r["path"]),
			"out":r["out"].replace("$", "\\")
			} for r in rules
		]

	def rsecfg(self, rse_name):
		cfg = self.RSEs.get(rse_name)
		if cfg is None:
			cfg = self.RSEs.get("*", {})
		return cfg
			

	def xrootd_root(self, rse_name):
                rsecfg = self.rsecfg(rse_name)
		return rsecfg.get("xrootd", {}).get("root", "/")

	def xrootd_remove_prefix(self, rse_name):
                rsecfg = self.rsecfg(rse_name)
		return rsecfg.get("xrootd", {}).get("remove_prefix", None)

	def xrootd_add_prefix(self, rse_name):
                rsecfg = self.rsecfg(rse_name)
		return rsecfg.get("xrootd", {}).get("add_prefix", None)

	def xrootd_server(self, rse_name):
                rsecfg = self.rsecfg(rse_name)
		return rsecfg["xrootd"]["server"]

	def dbdump_root(self, rse_name):
                rsecfg = self.rsecfg(rse_name)
		return rsecfg.get("dbdump_root", "/")
		
