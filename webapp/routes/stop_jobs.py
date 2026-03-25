"""Cooperative stop for long-running jobs (batch scrape, manual, upload, feedback-ready)."""

from flask import Blueprint, jsonify

stop_jobs_bp = Blueprint("stop_jobs", __name__, url_prefix="")


def _signal_scrape_stop() -> None:
    from scrapers.scraper_runner import request_scrape_stop

    request_scrape_stop()


def _signal_manual_stop() -> None:
    from scrapers.script_scraper import request_web_manual_stop

    request_web_manual_stop()


def _signal_upload_stop() -> None:
    from scrapers.upload_runner import request_upload_stop

    request_upload_stop()


def _signal_feedback_ready_stop() -> None:
    from scrapers.feedback_ready_runner import request_feedback_ready_stop

    request_feedback_ready_stop()


_STOP_MESSAGES = {
    "scrape": "Stop requested for batch / dashboard scrape. Exits after the current university.",
    "manual": "Stop requested for manual scrape. Exits after the current page (no CSV if stopped early).",
    "upload": "Stop requested for upload. Exits after the current file.",
    "feedback_ready": "Stop requested for feedback-ready. Exits after the current file.",
}


def _api_stop_one(job: str, signal_fn) -> tuple:
    try:
        signal_fn()
        return True, _STOP_MESSAGES.get(job, "Stop requested.")
    except Exception:
        return False, "Could not signal stop (see server logs)."


@stop_jobs_bp.route("/api/stop/scrape", methods=["POST"])
def api_stop_scrape():
    ok, msg = _api_stop_one("scrape", _signal_scrape_stop)
    return jsonify({"ok": ok, "message": msg, "job": "scrape"})


@stop_jobs_bp.route("/api/stop/manual", methods=["POST"])
def api_stop_manual():
    ok, msg = _api_stop_one("manual", _signal_manual_stop)
    return jsonify({"ok": ok, "message": msg, "job": "manual"})


@stop_jobs_bp.route("/api/stop/upload", methods=["POST"])
def api_stop_upload():
    ok, msg = _api_stop_one("upload", _signal_upload_stop)
    return jsonify({"ok": ok, "message": msg, "job": "upload"})


@stop_jobs_bp.route("/api/stop/feedback-ready", methods=["POST"])
def api_stop_feedback_ready():
    ok, msg = _api_stop_one("feedback_ready", _signal_feedback_ready_stop)
    return jsonify({"ok": ok, "message": msg, "job": "feedback_ready"})


@stop_jobs_bp.route("/api/stop-all-scrapers", methods=["POST"])
def api_stop_all_scrapers():
    """
    Request stop for:
    - Jobs batch scrape / dashboard retry (after current university)
    - Manual scrape headless (between pages / before saving CSV)
    - Upload to Drive (between files)
    - Feedback uploader ready (between files)
    """
    signaled = []

    ok, _ = _api_stop_one("scrape", _signal_scrape_stop)
    if ok:
        signaled.append("batch_scrape")

    ok, _ = _api_stop_one("manual", _signal_manual_stop)
    if ok:
        signaled.append("manual_scrape")

    ok, _ = _api_stop_one("upload", _signal_upload_stop)
    if ok:
        signaled.append("upload")

    ok, _ = _api_stop_one("feedback_ready", _signal_feedback_ready_stop)
    if ok:
        signaled.append("feedback_ready")

    return jsonify(
        {
            "ok": True,
            "message": "Stop requested for all job types. Each exits after the current university, page, or file when possible.",
            "signaled": signaled,
        }
    )
