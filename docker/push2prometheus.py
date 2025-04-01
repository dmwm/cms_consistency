import json, os, argparse
from typing import Tuple, Dict, List, Optional

from rucio.common.config import config_get
from rucio.core.monitor import MetricManager

PROBES_PREFIX = config_get('monitor', 'prometheus_prefix', raise_exception=False, default='')
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

    def __enter__(self) -> "MetricManager":
        """
        Return the Rucio metrics manager
        :return:
        """
        return self.manager

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.manager.push_metrics_to_gw(job=self.job_name, grouping_key=self.labels)

scan_steps_dict = {
    'site_cmp3': ['dbdump_before', 'scanner', 'dbdump_after', 'cmp3', 'diffs', 'missing_action', 'dark_action', 'empty_action']
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Push metrics from a JSON file to prometheus")
    parser.add_argument("filename", type=argparse.FileType('r'), help="Path to the JSON file")
    parser.add_argument("scan_type", choices=list(scan_steps_dict.keys()), help=f"Scan types. Allowed values: {list(scan_steps_dict.keys())}")
    args = parser.parse_args()

    try:
        with args.filename as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        print(f"Error: File not found: {args.filename}")
        data = None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file: {args.filename}")
        data = None
    
    if data:
        RSE = data.get('rse', '')
        
    with PrometheusPusher(job_name='consistency_check') as manager:
        """
        Metrics are not removed from the pushgateway
        If a metric is not set when a new job is updated, its value in pushgateway will become NaN
        For these reasons no other checks are needed
        """
        
        # Push global metrics
        if data:
            for scan_stat_key in data:
                if scan_stat_key not in scan_steps_dict[args.scan_type]:
                    scan_stat_value = data[scan_stat_key]
                    if isinstance(scan_stat_value, (int, float)):
                        manager.gauge(name=".".join(['global', '{scan_type}', '{rse}', '{stat}']), documentation='Global Consistency Check metrics').labels(scan_type=args.scan_type, rse=RSE, stat=scan_stat_key).set(scan_stat_value)

            # Push metrics relative to internal scan jobs
            for step_key in scan_steps_dict[args.scan_type]:
                step_dict = None if not data else data.get(step_key, None)
                status = 0 
                if step_dict:
                    if step_dict.get('status', None) == "done":
                        status = 1
                    if step_dict.get('status', None) == "failed":
                        status = 2
                    if step_dict.get('status', None) == "aborted":
                        status = 3
                manager.gauge(name=".".join(['jobs', '{scan_type}', '{scan_step}', '{rse}', '{stat}']), documentation='Consistency Check metrics per Job').labels(scan_type=args.scan_type, scan_step=step_key, rse=RSE, stat='status').set(status)

                if step_dict:
                    for stat_key in step_dict:
                        stat_value = step_dict[stat_key]
                        
                        if isinstance(stat_value, (int, float)):
                            manager.gauge(name=".".join(['jobs', '{scan_type}', '{scan_step}', '{rse}', '{stat}']), documentation='Consistency Check metrics per Job').labels(scan_type=args.scan_type, scan_step=step_key, rse=RSE, stat=stat_key).set(stat_value)

                        if step_key == 'scanner' and stat_key == 'roots':
                            SERVER = step_dict.get('server', '')
                            if type(stat_value) == list:
                                for root_dict in stat_value:
                                    ROOT = root_dict.get('root', '')
                                    for root_stat_key in root_dict:
                                        root_stat_value = root_dict[root_stat_key]
                                        if isinstance(root_stat_value, (int, float)):
                                            manager.gauge(name=".".join(['roots', '{scan_type}', '{scan_step}', '{rse}', '{server}', '{root}', '{stat}']), documentation='Consistency Check metrics per root').labels(scan_type=args.scan_type, scan_step=step_key, rse=RSE, server=SERVER, root=ROOT, stat=root_stat_key).set(root_stat_value)
