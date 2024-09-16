import json, os, argparse
from typing import Tuple, Dict, List, Optional

from rucio.common.config import config_get
from rucio.core.monitor import MetricManager

PROBES_PREFIX = 'rucio.consistency'
probe_metrics = MetricManager(prefix=PROBES_PREFIX)

def get_prometheus_config() -> Tuple[List, str, Dict]:
    prom_servers = config_get('monitor', 'prometheus_servers', raise_exception=False, default='')
    if prom_servers != '':
        prom_servers = prom_servers.split(',')
    else:
        prom_servers = []
    prom_prefix = config_get('monitor', 'prometheus_prefix', raise_exception=False, default='')
    prom_label_config = config_get('monitor', 'prometheus_labels', raise_exception=False, default=None)
    if prom_label_config:
        try:
            prom_labels = json.loads(prom_label_config)
        except ValueError:
            prom_labels = None
    else:
        prom_labels = None
    return prom_servers, prom_prefix, prom_labels

class PrometheusPusher:
    """
    A context manager to abstract the business of configuring and pushing to prometheus
    """

    def __init__(self, prefix: "Optional[str]" = PROBES_PREFIX, job_name: "Optional[str]" = None):
        self.job_name = job_name
        self.servers, _dummy, self.labels = get_prometheus_config()
        self.prefix = prefix

        self.manager = MetricManager(prefix=self.prefix, push_gateways=self.servers)
        # self.manager = MetricManager(prefix=self.prefix, push_gateways=[os.getenv('PROMETHEUS_PUSHGATEWAY_ENDPOINT')]) # Used for local development

    def __enter__(self) -> "MetricManager":
        """
        Return the Rucio metrics manager
        :return:
        """
        return self.manager

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.manager.push_metrics_to_gw(job=self.job_name, grouping_key=self.labels)

scan_jobs_dict = {
    'site_cmp3': ['dbdump_before', 'scanner', 'dbdump_after', 'cmp3', 'diffs', 'missing_action', 'dark_action', 'empty_action']
}


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Push metrics from a JSON file to prometheus")
    parser.add_argument("filename", type=argparse.FileType('r'), help="Path to the JSON file")
    parser.add_argument("scan_type", choices=list(scan_jobs_dict.keys()), help=f"Scan types. Allowed values: {list(scan_jobs_dict.keys())}")
    args = parser.parse_args()

    # Read the JSON file
    try:
        with args.filename as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        print(f"Error: File not found: {args.filename}")
        data = None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file: {args.filename}")
        data = None
    
    # Work on stats
    if data:
        RSE = data.get('rse', '')

    # Push metrics relative to the scan
    with PrometheusPusher(job_name=args.scan_type) as manager:
        if data:
            for scan_stat_key in data:
                if scan_stat_key not in scan_jobs_dict[args.scan_type]:
                    scan_stat_value = data[scan_stat_key]
                    if isinstance(scan_stat_value, (int, float)):
                        manager.gauge(name=".".join([scan_stat_key, '{rse}']), documentation=f'{scan_stat_key} of {args.scan_type}').labels(rse=RSE).set(scan_stat_value)

    # Push metrics relative to internal scan jobs
    for job_key in scan_jobs_dict[args.scan_type]:
        with PrometheusPusher(job_name=job_key) as manager:
            
            job_dict = None if not data else data.get(job_key, None)
            health_value = 0 if not(job_dict and job_dict.get('status', None) == "done") else 1
            manager.gauge(name=".".join(['health', '{rse}']), documentation=f'Health status of {job_key} (1: Good, 0: Bad)').labels(rse=RSE).set(health_value)
            
            # Metrics are not removed from the pushgateway
            # If a metric is not set when a new job is updated, its value in pushgateway will become NaN
            # For these reasons no other checks are needed

            if job_dict:
                for stat_key in job_dict:
                    
                    stat_value = job_dict[stat_key]

                    if isinstance(stat_value, (int, float)):
                        manager.gauge(name=".".join([stat_key, '{rse}']), documentation=f'{stat_key} of {job_key}').labels(rse=RSE).set(stat_value)

                    if job_key == 'scanner' and stat_key == 'roots':

                        SERVER = job_dict.get('server', '')
                        
                        if type(stat_value) == list:
                            for root_dict in stat_value:
                                
                                ROOT = root_dict.get('root', '')
                                root_failed = 1 if root_dict.get('root_failed', None) else 0
                                manager.gauge(name=".".join([stat_key, 'root_failed', '{rse}', '{server}', '{root}']), documentation=f'Root_failed of {job_key} for given RSE and root (1: False, 0: True)').labels(rse=RSE, server=SERVER, root=ROOT).set(root_failed)

                                for root_stat_key in root_dict:
                                    root_stat_value = root_dict[root_stat_key]
                                    if isinstance(root_stat_value, (int, float)):
                                        manager.gauge(name=".".join([stat_key, root_stat_key, '{rse}', '{server}', '{root}']), documentation=f'{root_stat_key} of {job_key} for given RSE and root').labels(rse=RSE, server=SERVER, root=ROOT).set(root_stat_value)
