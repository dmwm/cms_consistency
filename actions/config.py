import re, os, json, yaml
from rucio_consistency import CEConfiguration

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
