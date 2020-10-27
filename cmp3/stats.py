
import json, os.path

def write_stats(my_stats, stats_file, stats_key):
    if stats_file:
        if os.path.isfile(stats_file):    
            with open(stats_file, "r") as f:
                stats = json.loads(f.read())
        else:
            stats = {}
        stats[stats_key] = my_stats
        open(stats_file, "w").write(json.dumps(stats))
