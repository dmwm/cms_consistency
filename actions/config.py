import re, os, json, yaml
from rucio_consistency import CEConfiguration

class ActionConfiguration(object):
    
    def __init__(self, rse, source, action, **source_agrs):
        self.Action = action
        self.Config = CEConfiguration(source)[rse].get(action + "_action", {})

    def __getitem__(self, name):
        return self.Config[name]
        
    def get(self, name, default=None):
        return self.Config.get(name, default)
