"""Manual scrape section: filters, subfilters, run, status, download."""
from __future__ import annotations

import json
import os
import threading
from datetime import date, datetime
from typing import Optional

from flask import Blueprint, jsonify, request, send_file

import project_paths as _project_paths

from webapp.config import DATA_SCRAPED_DIR, LOGS_APP_DIR, MANUAL_SCRAPE_LOG_FILE, URLS_JSON
from webapp.services.json_store import (
    load_filter_cache,
    load_manual_credentials,
    save_filter_cache,
)
from webapp.services.path_utils import (
    get_user_downloads_dir,
    safe_manual_output_name,
    unique_path_in_dir,
)
from webapp.state import manual_scrape_status

manual_scrape_bp = Blueprint("manual_scrape", __name__, url_prefix="")


def _log_manual(msg: str, institute: Optional[str] = None) -> None:
    """Append to logs/app/manual_scrape.log and logs/runs/<today>/manual_<institute>.log."""
    try:
        os.makedirs(LOGS_APP_DIR, exist_ok=True)
        line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - " + msg + "\n"
        with open(MANUAL_SCRAPE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        today_str = date.today().strftime("%d-%m-%y")
        inst = (institute or "").strip()
        fname = (
            _project_paths.safe_run_log_filename(inst, "manual")
            if inst
            else "manual_job.log"
        )
        _project_paths.append_logs_runs_line(today_str, fname, msg)
    except OSError:
        pass


def _cache_key(institute, source):
    return f"{str(institute or '').strip().lower()}|{str(source or '').strip().lower()}"


_DEFAULT_PUBLISHER_URLS = ["https://publisher.nopaperforms.com/lead/details"]


@manual_scrape_bp.route("/api/manual-scrape/urls")
def api_manual_scrape_urls():
    """Publisher base URLs from data/reference/urls.json for manual scrape dropdown."""
    try:
        with open(URLS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            urls = [str(u).strip() for u in data if str(u).strip()]
        else:
            urls = []
        if not urls:
            urls = list(_DEFAULT_PUBLISHER_URLS)
        return jsonify({"urls": sorted(set(urls), key=lambda x: x.lower())})
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return jsonify({"urls": list(_DEFAULT_PUBLISHER_URLS)})


@manual_scrape_bp.route("/api/manual-scrape/filters", methods=["POST"])
def api_manual_scrape_filters():
    data = request.get_json(silent=True) or {}
    institute = (data.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    creds = load_manual_credentials()
    cred_key = (data.get("credentials") or "central").strip().lower()
    if cred_key not in creds:
        return jsonify({"error": f"Unknown credentials: {cred_key}"}), 400
    params = {
        "login_url": (data.get("url") or "https://publisher.nopaperforms.com/lead/details").strip(),
        "email": creds[cred_key]["email"],
        "password": creds[cred_key]["password"],
        "institute": institute,
        "source": (data.get("source") or "Collegedunia").strip(),
    }
    try:
        from scrapers.script_scraper import SUBFILTER_CONFIG, fetch_advanced_filters

        key = _cache_key(params["institute"], params["source"])
        cache = load_filter_cache()
        cached = cache.get(key) if isinstance(cache, dict) else None
        if isinstance(cached, dict) and isinstance(cached.get("filters"), list) and cached.get("filters"):
            filters = cached.get("filters")
        else:
            filters = fetch_advanced_filters(params)
            cache = load_filter_cache()
            cache[key] = {
                "filters": filters or [],
                "subfilter_options": (cache.get(key) or {}).get("subfilter_options", {}),
                "institute": params["institute"],
                "source": params["source"],
                "updatedAt": datetime.now().isoformat(timespec="seconds"),
            }
            save_filter_cache(cache)
        subfilter_filter_ids = list(SUBFILTER_CONFIG.keys())
        return jsonify({"filters": filters, "subfilterFilterIds": subfilter_filter_ids})
    except Exception as e:
        _log_manual(f"Load filters failed ({institute}): {e}", institute=institute)
        return jsonify({"error": str(e)[:500]}), 500


@manual_scrape_bp.route("/api/manual-scrape/subfilter-options", methods=["POST"])
def api_manual_scrape_subfilter_options():
    data = request.get_json(silent=True) or {}
    institute = (data.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    creds = load_manual_credentials()
    cred_key = (data.get("credentials") or "central").strip().lower()
    if cred_key not in creds:
        return jsonify({"error": f"Unknown credentials: {cred_key}"}), 400
    filter_ids = data.get("filterIds") or data.get("filter_ids") or []
    if not filter_ids:
        return jsonify({"options": {}})
    params = {
        "login_url": (data.get("url") or "https://publisher.nopaperforms.com/lead/details").strip(),
        "email": creds[cred_key]["email"],
        "password": creds[cred_key]["password"],
        "institute": institute,
        "source": (data.get("source") or "Collegedunia").strip(),
    }
    try:
        from scrapers.script_scraper import fetch_subfilter_options

        key = _cache_key(params["institute"], params["source"])
        cache = load_filter_cache()
        cached_sf = (cache.get(key) or {}).get("subfilter_options", {})
        missing = [fid for fid in filter_ids if not cached_sf.get(fid)]
        if not missing:
            options = {fid: cached_sf.get(fid, []) for fid in filter_ids}
            return jsonify({"options": options})

        fetched = fetch_subfilter_options(params, missing)
        merged = {fid: cached_sf.get(fid, []) for fid in filter_ids}
        for fid in missing:
            merged[fid] = fetched.get(fid, [])

        if fetched:
            cache = load_filter_cache()
            entry = cache.get(key) if isinstance(cache.get(key), dict) else {}
            sf = entry.get("subfilter_options", {}) if isinstance(entry, dict) else {}
            if not isinstance(sf, dict):
                sf = {}
            sf.update(fetched)
            cache[key] = {
                "filters": entry.get("filters", []),
                "subfilter_options": sf,
                "institute": params["institute"],
                "source": params["source"],
                "updatedAt": datetime.now().isoformat(timespec="seconds"),
            }
            save_filter_cache(cache)
        options = merged
        return jsonify({"options": options})
    except Exception as e:
        _log_manual(f"Load subfilter options failed ({institute}): {e}", institute=institute)
        return jsonify({"error": str(e)[:500], "options": {}}), 500


@manual_scrape_bp.route("/api/manual-scrape/run", methods=["POST"])
def api_manual_scrape_run():
    if manual_scrape_status.get("running"):
        return jsonify({"error": "Manual scrape already running"}), 409
    data = request.get_json(silent=True) or {}
    institute = (data.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    creds = load_manual_credentials()
    cred_key = (data.get("credentials") or "central").strip().lower()
    if cred_key not in creds:
        return jsonify({"error": f"Unknown credentials: {cred_key}"}), 400
    from_date = (data.get("from_date") or "").strip()
    to_date = (data.get("to_date") or "").strip()
    do_screenshot = data.get("screenshot") is True
    if do_screenshot:
        if not from_date or not to_date:
            return jsonify({"error": "From date and To date are required for screenshot"}), 400
    else:
        if not from_date or not to_date:
            return jsonify({"error": "From date and To date are required (DD-MM-YYYY)"}), 400

    today_str = date.today().strftime("%d-%m-%y")
    downloads_dir = get_user_downloads_dir()
    # CSV and screenshots go to the user's Downloads folder (not DATA_Scraped).
    out_dir = downloads_dir
    try:
        os.makedirs(out_dir, exist_ok=True)
    except OSError:
        pass
    if do_screenshot:
        ts = datetime.now().strftime("%H%M%S")
        shot_fname = safe_manual_output_name(institute) + "_" + today_str + "_" + ts + ".png"
        screenshot_path = unique_path_in_dir(downloads_dir, shot_fname)
    else:
        screenshot_path = None
    csv_fname = safe_manual_output_name(institute) + "_" + today_str + ".csv"
    csv_full_path = unique_path_in_dir(downloads_dir, csv_fname)
    fname = os.path.basename(csv_full_path)
    params = {
        "login_url": (data.get("url") or "https://publisher.nopaperforms.com/lead/details").strip(),
        "email": creds[cred_key]["email"],
        "password": creds[cred_key]["password"],
        "institute": institute,
        "source": (data.get("source") or "Collegedunia").strip(),
        "from_date": from_date,
        "to_date": to_date,
        "instance": (data.get("instance") or "All").strip(),
        "rows_per_page": str(data.get("rows") or "5000"),
        "order": (data.get("order") or "Ascending").strip(),
        "filename": fname,
        "output_dir": out_dir,
        "advanced_filter_ids": data.get("advanced_filter_ids") or [],
        "subfilter_options": data.get("subfilter_options") or {},
    }
    if do_screenshot:
        params["screenshot_mode"] = True
        params["screenshot_path"] = screenshot_path

    def run():
        manual_scrape_status["running"] = True
        manual_scrape_status["status"] = "Starting..."
        manual_scrape_status["error"] = None
        manual_scrape_status["output_path"] = ""
        manual_scrape_status["output_path_relative"] = ""
        manual_scrape_status["output_download_file"] = ""
        _log_manual(
            f"Run started: institute={institute!r}, {from_date}–{to_date}, "
            f"screenshot={do_screenshot}, downloads_dir={downloads_dir}",
            institute=institute,
        )
        try:
            from scrapers.script_scraper import (
                ManualScrapeLeadsLimitExceeded,
                run_headless,
                was_headless_stopped_by_user,
            )

            def on_status(msg):
                manual_scrape_status["status"] = msg

            out = run_headless(params, status_callback=on_status)
            if was_headless_stopped_by_user():
                manual_scrape_status["error"] = "Stopped by user."
                manual_scrape_status["status"] = "Stopped by user."
                _log_manual("Run stopped by user.", institute=institute)
            elif out:
                manual_scrape_status["output_path"] = out
                manual_scrape_status["output_path_relative"] = ""
                try:
                    out_abs = os.path.abspath(out)
                    dd = os.path.abspath(get_user_downloads_dir())
                    if os.path.normcase(os.path.dirname(out_abs)) == os.path.normcase(dd):
                        manual_scrape_status["output_download_file"] = os.path.basename(out)
                    else:
                        manual_scrape_status["output_download_file"] = ""
                except Exception:
                    manual_scrape_status["output_download_file"] = os.path.basename(out)
                _log_manual(f"Run finished OK: {out}", institute=institute)
            else:
                _log_manual("Run finished with no output path (check UI status).", institute=institute)
        except ManualScrapeLeadsLimitExceeded as e:
            msg = str(e)
            manual_scrape_status["error"] = msg[:500]
            manual_scrape_status["status"] = msg[:300]
            _log_manual(f"Run aborted (manual scrape lead limit): {msg}", institute=institute)
        except Exception as e:
            manual_scrape_status["error"] = str(e)[:500]
            _log_manual(f"Run failed: {e}", institute=institute)
        finally:
            manual_scrape_status["running"] = False
            if not manual_scrape_status.get("error"):
                manual_scrape_status["status"] = "Done"

    threading.Thread(target=run, daemon=False).start()
    return jsonify({"ok": True, "message": "Manual scrape started"})


@manual_scrape_bp.route("/api/manual-scrape/status")
def api_manual_scrape_status():
    return jsonify(dict(manual_scrape_status))


@manual_scrape_bp.route("/api/manual-scrape/download")
def api_manual_scrape_download():
    """Serve a file from the user's Downloads folder (basename only) or legacy path under DATA_Scraped."""
    file_only = (request.args.get("file") or "").strip()
    if file_only:
        if ".." in file_only or "/" in file_only or "\\" in file_only:
            return jsonify({"error": "Invalid file name"}), 400
        d = os.path.abspath(get_user_downloads_dir())
        full = os.path.abspath(os.path.join(d, file_only))
        if os.path.normcase(os.path.dirname(full)) != os.path.normcase(d):
            return jsonify({"error": "Invalid path"}), 400
        if not os.path.isfile(full):
            return jsonify({"error": "File not found"}), 404
        return send_file(full, as_attachment=True, download_name=os.path.basename(full))

    rel = (request.args.get("path") or "").strip().replace("\\", "/")
    if not rel or ".." in rel or rel.startswith("/"):
        return jsonify({"error": "Invalid path"}), 400
    full = os.path.normpath(os.path.join(DATA_SCRAPED_DIR, rel))
    if not full.startswith(os.path.normpath(DATA_SCRAPED_DIR)) or not os.path.isfile(full):
        return jsonify({"error": "File not found"}), 404
    return send_file(full, as_attachment=True, download_name=os.path.basename(full))
