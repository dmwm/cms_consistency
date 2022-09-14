from webpie import WPApp, WPHandler, WPStaticHandler
import sys, glob, json, time, os, gzip
from datetime import datetime
from data_source import CCDataSource, StatsCache

Version = "1.11.11"

def display_file_list(lst):
    Indent = "    "
    last_items = []
    out = []
    for path in lst:
        items = [item for item in path.split("/") if item]
        n_common = 0
        for li, i in zip(items, last_items):
            if li == i:
                n_common += 1
            else:
                break
        tail = items[n_common:]
        indent = Indent * n_common
        for i, item in enumerate(tail):
            if i < len(tail)-1:
                item += "/"
            if not indent: item = "/" + item
            out.append(indent + item)
            indent += Indent
        last_items = items
    return out
    

class CEHandler(WPHandler):
    
    DarkSection = "dark_action"
    MissingSection = "missing_action"
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.static = WPStaticHandler(*params, **args)
        self.CCDataSource = CCDataSource(self.App.CCPath, self.App.StatsCache)
        
    def probe(self, request, relpath, **args):
        return self.CCDataSource.status(), "text/plain"
        return "OK" if self.CCDataSource.is_mounted() else ("CE data directory unreachable", 500)

    def index(self, request, relpath, sort="rse", **args):
        #
        # list available RSEs
        #
        data_source = self.CCDataSource

        stats = data_source.latest_stats_per_rse()
        summaries = {rse: data_source.run_summary(stats) for rse, stats in stats.items()}
        for rse, summary in summaries.items():
            summary["rse"] = rse
        summaries = summaries.values()

        if sort == "ce_run":
            summaries = sorted(summaries, key=lambda s: (s.get("start_time") or -1, s["rse"]))
        elif sort == "-ce_run":
            summaries = sorted(summaries, key=lambda s: (s.get("start_time") or -1, s["rse"]), reverse=True)
        else:
            summaries = sorted(summaries, key=lambda s: s["rse"])

        return self.render_to_response("ce_index.html", summaries=summaries, sort_options=True)

    def attention(self, request, relpath, **args):
        data_source = self.CCDataSource

        stats = data_source.latest_stats_per_rse()
        summaries = {rse: data_source.run_summary(stats) for rse, stats in stats.items()}
        for rse, summary in summaries.items():
            summary["rse"] = rse
        now = time.time()
        problems = []
        for summary in summaries.values():

            summary["too_old"] = False
            order = None

            if summary.get("failed") or \
                    summary.get("detection_status") == "failed" or \
                    summary.get("detection_status") == "started" and summary["start_time"] < 3*24*3600:
                order = 1
            elif summary.get("detection_status") == "done":
                for part in ("missing_stats", "dark_stats"):
                    if summary[part].get("action_status") == "failed" \
                                or summary[part].get("action_status") == "aborted" and \
                                        "too many" in summary[part].get("aborted_reason", "").lower() \
                                or summary[part].get("action_status") == "started" and summary["start_time"] < 3*24*3600:
                            order = 2
                            break

            if summary["start_time"] < now - 14*24*3600:          # older than 2 weeks
                order = order or 3
                summary["too_old"] = True

            if order is not None:
                summary["order"] = order
                problems.append(summary)

        if problems:
            problems = sorted(problems, key=lambda s: s["order"])

        return self.render_to_response("ce_index.html", summaries=problems, sort_options=False)
    
    def cache_hit_ratio(self, request, relpath, **args):
        return str(self.App.StatsCache.HitRatio), "text/plain"
        
    def raw_stats(self, request, relpath, rse=None, run=None, **args):
        runs = self.CCDataSource.list_runs(rse)
        raw_stats = mtime = None
        if run:
            raw_stats, mtime = self.CCDataSource.raw_stats(rse, run)
        return self.render_to_response("raw_stats.html", rse=rse, runs=runs, raw_stats=raw_stats, mtime=mtime)

    def show_rse(self, request, relpath, rse=None, **args):
        data_source = self.CCDataSource
        runs = data_source.list_runs(rse)
        runs = sorted(runs, reverse=True)
        
        cc_infos = []
        for run in runs:
            stats, ndark, nmissing, confirmed_dark = data_source.get_stats(rse, run)
            prev_run, missing_old, dark_old = data_source.file_lists_diffs_counts(rse, run)
            summary = data_source.run_summary(stats)
            start_time = summary["start_time"] or 0
            detection_status = summary["detection_status"]
            if detection_status == "failed":
                detection_status = summary["failed"] + " failed"
            running = summary.get("running")
            cc_infos.append((
                run, 
                {
                    "summary":  summary,
                    "start_time":       start_time, 
                    "ndark":ndark, 
                    "nmissing":nmissing, 
                    "detection_status": detection_status, 
                    "running":running,

                    "confirmed_dark":   summary["dark_stats"]["confirmed"], 
                    "acted_dark":       summary["dark_stats"]["acted_on"], 
                    "dark_status":      summary["dark_stats"]["action_status"],
                    "dark_status_reason": summary["dark_stats"].get("aborted_reason", ""),

                    "acted_missing":summary["missing_stats"]["acted_on"], 
                    "missing_status":summary["missing_stats"]["action_status"],
                    "missing_status_reason":    summary["missing_stats"].get("aborted_reason", ""),
                    "start_time_milliseconds":int(start_time*1000),
                    "prev_run":         prev_run,
                    "old_missing":      missing_old,
                    "old_dark":         dark_old
                }
            ))
        return self.render_to_response("ce_rse.html", rse=rse, cc_runs=cc_infos)
        
    def common_paths(self, lst, space="&nbsp;"):
        lst = sorted(lst)
        prev = []
        out = []
        for path in lst:
            parts = path.split("/")
            i = 0
            for j, (x, y) in enumerate(zip(prev, parts)):
                if x == y:
                    i = j
                else:
                    break
            head = "/".join(parts[:i+1])
            tail = "/".join(parts[i+1:])
            out.append(space*len(head)+"/"+tail)
            prev = parts
        return out
        
    def display_file_list(self, lst):
        Indent = "    "
        last_items = []
        out = []
        for path in sorted(lst or []):
            items = [item for item in path.split("/") if item]
            n_common = 0
            for li, i in zip(items, last_items):
                if li == i:
                    n_common += 1
                else:
                    break
            tail = items[n_common:]
            indent = Indent * n_common
            for i, item in enumerate(tail):
                if i < len(tail)-1:
                    item += "/"
                if not indent: item = "/" + item
                out.append(indent + item)
                indent += Indent
            last_items = items
        return out
    
    def check_run(self, stats):
        errors = []
        if not "scanner" in stats or stats["scanner"] is None:
            errors.append("Site scanner statistics missing")
        if not "dbdump_before" in stats or stats["dbdump_before"] is None:
            errors.append("DB dump before site scan statistics missing")
        if not "dbdump_after" in stats or stats["dbdump_after"] is None:
            errors.append("DB dump after site scan statistics missing")
        if not "cmp3" in stats or stats["cmp3"] is None:
            errors.append("Comparison statistics missing")
        if run_info.get("dark") is None:
            errors.append("Dark file list not found")
        if run_info.get("missing") is None:
            errors.append("Missing file list not found")
        return errors
        
    def dark(self, request, relpath, rse=None, run=None, **args):
        lst = self.CCDataSource.get_dark(rse, run)
        return (path+"\n" for path in lst), {
            "Content-Type":"text/plain",
            "Content-Disposition":"attachment"
        }
            
    def missing(self, request, relpath, rse=None, run=None, **args):
        lst = self.CCDataSource.get_missing(rse, run)
        return (path+"\n" for path in lst), {
            "Content-Type":"text/plain",
            "Content-Disposition":"attachment"
        }
    
    LIMIT = 1000
    
    def show_run(self, request, relpath, rse=None, run=None, **args):
        if rse is None:
            self.redirect("./index")
        if run is None:
            self.redirect(f"./show_rse?rse={rse}")
        data_source = self.CCDataSource
        stats, ndark, nmissing, confirmed_dark = data_source.get_stats(rse, run)
        summary = data_source.run_summary(stats)
        errors = []
        if summary["status"] == "failed":
            failed_comp = summary["failed"]
            errors = ["%s failed" % failed_comp]
            failed_stats = stats[failed_comp]
            if failed_stats.get("error"):
                errors.append("error: %s" % (failed_stats["error"],))
                
        stats_parts = [(part, part_name, stats.get(part)) for part, part_name in 
            [
                ("dbdump_before", "DB dump before scan"),
                ("scanner", "Site scanner"),
                ("dbdump_after", "DB dump after scan"),
                ("cmp3", "Comparison"),
                ("cmp2dark", "Dark confirmation"),
                (self.DarkSection, "Dark action"),
                (self.MissingSection, "Missing action")
            ]
            if stats.get(part)
        ]
        for _,_,s in stats_parts:
            if "status" in s:
                status = (s.get("status") or "").lower() or None
                s["status"] = status
        scanner_roots = []
        if "scanner" in stats and "roots" in stats["scanner"]:
            scanner_roots = sorted(stats["scanner"]["roots"], key=lambda x:x["root"])
        
        dark_truncated = (ndark or 0)  > self.LIMIT
        missing_truncated = (nmissing or 0) > self.LIMIT
        
        dark = self.CCDataSource.get_dark(rse, run, self.LIMIT)
        missing = self.CCDataSource.get_missing(rse, run, self.LIMIT)
        
        #
        # retrofit failed directories
        #
        scanner = stats.get("scanner")
        if scanner:
            for r in scanner.get("roots", []):
                failed = r.get("failed_subdirectories") or {}
                if isinstance(failed, list):
                    out = {}
                    for line in failed:
                        error = ""
                        path = line
                        parts = line.split(None, 1)
                        if len(parts) > 1:
                            path, error = parts
                        out[path] = error
                    failed = out
                r["failed_subdirectories"] = failed
        
        prev_run, old_nmissing, old_ndark = data_source.file_lists_diffs_counts(rse, run)
        
        return self.render_to_response("ce_run.html", 
            rse=rse, run=run,
            errors = errors,
            dark_truncated = dark_truncated, 
            missing_truncated=missing_truncated,
            dbdump_before=stats.get("dbdump_before"),
            dbdump_after=stats.get("dbdump_after"),
            scanner=stats.get("scanner"),
            scanner_roots = scanner_roots,
            cmp3=stats.get("cmp3"),
            stats=stats, summary=summary,
            ndark = ndark, nmissing=nmissing,
            old_ndark = old_ndark, old_nmissing = old_nmissing,
            dark = self.display_file_list(dark),
            missing = self.display_file_list(missing),
            stats_parts=stats_parts,
            time_now = time.time()
        )

    def files(self, request, relpath, rse=None, type="*"):
        files = self.CCDataSource.files(rse, type)
        sizes = [os.path.getsize(path) for path in files]
        return [f"{f} {s}\n" for f, s in zip(files, sizes)], "text/plain"
        
    def file(self, request, relpath):
        f = self.CCDataSource.open_file(relpath)
        def read_file(f):
            data = f.read(10240)
            while data:
                yield data
                data = f.read(10240)
        return read_file(f), "text/plain"
        
    def stats(self, request, relpath, rse=None, run=None, **args):
        if not rse or not run:
            return 400, "Missing RSE or run"
        stats, ndark, nmissing, confirmed_dark = self.CCDataSource.get_stats(rse, run)
        return json.dumps(stats, indent=4, sort_keys=True), "text/json"

    def lists_diffs(self, request, relpath, rses=None, **args):
        cc_data_source = self.CCDataSource
        if rses is None:
            rses = set(um_data_source.list_rses()) | set(cc_data_source.list_rses())
        else:
            rses = rses.split(",")
        data = {}      # {rse -> {"old_missing":.., "new_missing":, "old_dark":, "new_dark"}}
        for rse in rses:
            rse_data = dict(
                    nmissing = None,
                    ndark = None,
                    prev_run=None,
                    last_run=None,
                    nmissing_old=None, 
                    ndark_old=None,
            )
            last_stats = cc_data_source.latest_stats_for_rse(rse)
            if last_stats is not None:
                rse_data["last_run"] = last_run = last_stats["run"]
                if last_stats.get("cmp3", {}).get("status") == "done":
                    rse_data["nmissing"] = last_stats["cmp3"]["missing"]
                    rse_data["ndark"] = last_stats["cmp3"]["dark"]
                    rse_data["last_run"] = last_run = last_stats["run"]
                    prev_run, missing_old, dark_old = cc_data_source.file_lists_diffs_counts(rse, last_run)
                    if prev_run is not None:
                        rse_data.update(dict(
                                prev_run=prev_run,
                                nmissing_old=missing_old,
                                ndark_old=dark_old
                        ))
            data[rse] = rse_data
        return json.dumps(data), "text/json"
        
    MAX_HISTORY = 10
        
    def status_history(self, request, relpath, rses=None, **args):
        cc_data_source = self.CCDataSource

        if rses is None:
            rses = set(um_data_source.list_rses()) | set(cc_data_source.list_rses())
        else:
            rses = rses.split(",")
            
        data = {}      # {rse -> (cc_total, um_total, cc_success, um_success)}
        
        for rse in rses:
            if not rse: continue
            cc_summaries = [cc_data_source.run_summary(x) for x in cc_data_source.all_stats_for_rse(rse)]
            
            cc_total = cc_success = 0
            
            cc_total = len(cc_summaries)
            cc_success = len([x for x in cc_summaries if x.get("status") == "done"])
            
            data[rse] = dict(cc_total=cc_total, cc_success=cc_success,
                cc_status_history=[
                    {
                        "cc":       x.get("detection_status"),
                        "missing":  x.get("missing_stats",{}).get("action_status"),
                        "dark":     x.get("dark_stats",{}).get("action_status")
                    }
                    for x in cc_summaries
                ][-self.MAX_HISTORY:],
            )
            
        return json.dumps(data), "text/json"
        
    def ls(self, request, relpath, rse="*", **args):
        lst = self.CCDataSource.ls(rse)
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"] or "", d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
