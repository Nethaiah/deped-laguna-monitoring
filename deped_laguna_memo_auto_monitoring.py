"""
DepEd Laguna Memorandum Auto-Monitoring + Supabase Uploader (Cloud-Driven, memory-only)
- Streams files from SharePoint (memory-only) with robust URL conversion
- Uploads them to Supabase Storage -> documents/docs_from_watcher/
- Inserts metadata into `notifications` table
- Uses Supabase storage listing as forward-only baseline (no local state)
- Writes structured JSONL logs to monitor_log.jsonl
"""

import argparse

import json

import logging
import mimetypes
import os
import re
import time
from datetime import datetime, timezone
import dotenv

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from supabase import Client, create_client
from urllib3.util import Retry

# ---------------- CONFIG ----------------
BASE_URL_TEMPLATE = "https://depedlaguna.com/division-memorandum-{year}/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
LOG_JSONL = "monitor_log.jsonl"
TIMEOUT = 30
DEFAULT_INTERVAL = 30
DEFAULT_WATCH_DURATION = 180

# Supabase (kept in-file per your request)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_BUCKET = "documents"
SUPABASE_FOLDER = "docs_from_watcher"
# ----------------------------------------

# create supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()


def log_jsonl(level: str, event: str, **kwargs):
    entry = {
        "timestamp": now_iso(),
        "level": level,
        "event": event,
    }
    entry.update(kwargs)
    line = json.dumps(entry, ensure_ascii=False)
    # append to JSONL file
    try:
        with open(LOG_JSONL, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as e:
        # fallback to basic logging if JSONL fails
        logging.basicConfig(level=logging.INFO)
        logging.error(f"Failed to write JSONL log: {e}")
    # also print for convenience
    if level == "ERROR":
        logging.error(entry)
    else:
        logging.info(entry)


class MemoMonitor:
    def __init__(
        self, interval=DEFAULT_INTERVAL, watch_duration=DEFAULT_WATCH_DURATION
    ):
        self.interval = interval
        self.watch_duration = watch_duration
        self.session = self._create_session()
        self._cached_latest_number = None

    def _create_session(self):
        session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(HEADERS)
        return session

    # ---------------- Supabase baseline ----------------
    def get_latest_uploaded_from_supabase(self):
        """
        Lists files in SUPABASE_FOLDER and infers the highest memo number from filenames.
        Returns integer 0 if none found or on error.
        """
        try:
            resp = supabase.storage.from_(SUPABASE_BUCKET).list(
                SUPABASE_FOLDER
            )  # may raise or return list
            files = resp or []
            if not isinstance(files, list):
                # sometimes client returns dict with 'data' key
                files = resp.get("data") if isinstance(resp, dict) else []
            if not files:
                log_jsonl(
                    "INFO",
                    "supabase_baseline_empty",
                    message="No files in Supabase folder",
                )
                return 0
            numbers = []
            for f in files:
                # f may be dict with 'name' or string depending on SDK version
                fname = f.get("name") if isinstance(f, dict) else (f or "")
                match = re.search(r"No[_\-]?(\d+)", fname)
                if match:
                    try:
                        numbers.append(int(match.group(1)))
                    except:
                        continue
            latest = max(numbers) if numbers else 0
            log_jsonl(
                "INFO", "supabase_baseline", latest=latest, files_count=len(files)
            )
            return latest
        except Exception as e:
            log_jsonl("ERROR", "supabase_baseline_error", message=str(e))
            return 0

    # ---------------- scraping ----------------
    def _get_page(self, url):
        r = self.session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r

    def fetch_memos_for_year(self, year):
        """
        Returns list of entries with keys: memo_number, title, date, sharepoint_url
        Only memos with number > cached_latest_number (forward-only) will be returned.
        """
        url = BASE_URL_TEMPLATE.format(year=year)
        log_jsonl("INFO", "check_year_start", year=year, url=url)
        try:
            r = self._get_page(url)
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                log_jsonl("WARNING", "no_table_found", year=year)
                return []
            rows = table.find_all("tr")[1:]
            if self._cached_latest_number is None:
                self._cached_latest_number = self.get_latest_uploaded_from_supabase()
            new_entries = []
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue
                memo_number = cols[0].get_text(strip=True)
                title_cell = cols[1]
                date = cols[2].get_text(strip=True)
                link = title_cell.find("a")
                if not link or not link.get("href"):
                    continue
                title = link.get_text(strip=True)
                sharepoint_url = link["href"]
                match = re.search(r"No\.?\s*(\d+)", memo_number)
                num = int(match.group(1)) if match else None
                if num and num <= self._cached_latest_number:
                    continue
                new_entries.append(
                    {
                        "memo_number": memo_number,
                        "title": title,
                        "date": date,
                        "sharepoint_url": sharepoint_url,
                    }
                )
            log_jsonl(
                "INFO", "fetch_year_complete", year=year, new_count=len(new_entries)
            )
            return new_entries
        except Exception as e:
            log_jsonl("ERROR", "fetch_year_failed", year=year, error=str(e))
            return []

    # ---------------- SharePoint URL conversion ----------------
    def convert_sharepoint_to_download_url(self, sharepoint_url):
        """
        Convert SharePoint sharing link to direct download URLs.
        Returns list of candidate URLs to try.
        """
        download_urls = []
        
        try:
            # Strategy 1: Convert :b:/g/personal/ format to direct download
            if ":b:/g/personal/" in sharepoint_url:
                parts = sharepoint_url.split("/personal/")
                if len(parts) >= 2:
                    after_personal = parts[1]
                    components = after_personal.split("/")
                    if len(components) >= 2:
                        user_part = components[0]
                        file_id = components[1].split("?")[0]
                        base = "https://depedph-my.sharepoint.com"
                        download_url = f"{base}/personal/{user_part}/_layouts/15/download.aspx?share={file_id}"
                        download_urls.append(download_url)
            
            # Strategy 2: Add ?download=1 parameter
            if "?" in sharepoint_url:
                download_urls.append(sharepoint_url + "&download=1")
            else:
                download_urls.append(sharepoint_url + "?download=1")
            
            # Strategy 3: Transform :b:/ to regular path
            if ":b:/" in sharepoint_url:
                transformed = sharepoint_url.replace(":b:/", "/").replace("/g/", "/")
                download_urls.append(transformed)
            
            # Strategy 4: Original URL as fallback
            download_urls.append(sharepoint_url)
            
        except Exception as e:
            log_jsonl("WARNING", "url_conversion_error", error=str(e))
            download_urls.append(sharepoint_url)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in download_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls

    # ---------------- download (memory-only) with retry strategies ----------------
    def download_bytes(self, sharepoint_url, memo_number):
        """
        Downloads the resource bytes from sharepoint_url using multiple strategies.
        Returns (bytes, mime_type) or (None, None) on failure.
        """
        download_urls = self.convert_sharepoint_to_download_url(sharepoint_url)
        log_jsonl("INFO", "attempting_download", memo_number=memo_number, strategies=len(download_urls))
        
        last_error = None
        for attempt, download_url in enumerate(download_urls, 1):
            try:
                log_jsonl("DEBUG", "download_attempt", attempt=attempt, url=download_url[:100])
                
                r = self.session.get(download_url, timeout=TIMEOUT, allow_redirects=True)
                
                # Check content type and file magic
                content_type = (r.headers.get("content-type") or "").lower()
                content = r.content or b""
                
                # Validate it's actually a PDF
                is_pdf = ("application/pdf" in content_type) or (
                    len(content) >= 4 and content[:4] == b"%PDF"
                )
                
                if is_pdf:
                    # Additional validation - check file size
                    if len(content) < 1000:
                        # Suspiciously small, verify PDF magic bytes
                        if b"%PDF" not in content[:100]:
                            log_jsonl(
                                "WARNING",
                                "small_invalid_pdf",
                                attempt=attempt,
                                size=len(content)
                            )
                            last_error = f"Invalid PDF (size: {len(content)} bytes)"
                            continue
                    
                    mime = "application/pdf"
                    log_jsonl(
                        "INFO",
                        "download_success",
                        memo_number=memo_number,
                        attempt=attempt,
                        size=len(content)
                    )
                    return content, mime
                else:
                    log_jsonl(
                        "WARNING",
                        "not_pdf_response",
                        attempt=attempt,
                        content_type=content_type,
                        content_start=content[:50].hex()
                    )
                    last_error = f"Non-PDF response: {content_type}"
                    
            except Exception as e:
                log_jsonl("WARNING", "download_attempt_failed", attempt=attempt, error=str(e))
                last_error = str(e)
                continue
        
        # All attempts failed
        log_jsonl(
            "ERROR",
            "download_failed_all_attempts",
            memo_number=memo_number,
            last_error=last_error,
            original_url=sharepoint_url
        )
        return None, None

    # ---------------- upload to supabase ----------------
    def upload_bytes_to_supabase(
        self, file_bytes: bytes, filename: str, mime_type: str
    ):
        """
        Upload bytes to SUPABASE_FOLDER/filename and return public URL (or None).
        """
        try:
            path = f"{SUPABASE_FOLDER}/{filename}"
            # Upload bytes with content-type metadata
            result = supabase.storage.from_(SUPABASE_BUCKET).upload(
                path, file_bytes, {"content-type": mime_type}
            )
            # Handle variations of return type / errors
            if isinstance(result, dict) and result.get("error"):
                log_jsonl(
                    "ERROR",
                    "upload_error",
                    filename=filename,
                    error=result.get("error"),
                )
                return None
            if hasattr(result, "error") and getattr(result, "error", None):
                log_jsonl(
                    "ERROR",
                    "upload_error_attr",
                    filename=filename,
                    error=str(getattr(result, "error")),
                )
                return None
            # get public url
            public = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(path)
            public_url = None
            if isinstance(public, dict):
                public_url = (
                    public.get("publicUrl")
                    or public.get("public_url")
                    or public.get("publicURL")
                )
            else:
                public_url = public
            log_jsonl(
                "INFO", "upload_success", filename=filename, public_url=public_url
            )
            return public_url
        except Exception as e:
            log_jsonl("ERROR", "upload_exception", filename=filename, error=str(e))
            return None

    # ---------------- notifications insert ----------------
    def insert_notification(self, memo_number: str, title: str, date: str, url: str):
        try:
            resp = (
                supabase.table("notifications")
                .insert(
                    {
                        "memo_number": memo_number,
                        "title": title,
                        "date": date,
                        "url": url,
                        "source": "watcher",
                    }
                )
                .execute()
            )
            # check response for errors depending on client version
            if isinstance(resp, dict) and resp.get("error"):
                log_jsonl(
                    "ERROR",
                    "db_insert_error",
                    memo_number=memo_number,
                    error=resp.get("error"),
                )
                return False
            log_jsonl("INFO", "db_insert_success", memo_number=memo_number, url=url)
            return True
        except Exception as e:
            log_jsonl(
                "ERROR", "db_insert_exception", memo_number=memo_number, error=str(e)
            )
            return False

    def safe_filename_from_memo(self, memo_number: str, title: str):
        # create a safe short filename
        safe_memo = re.sub(r"[^\w\s-]", "", memo_number).strip().replace(" ", "_")
        safe_title = re.sub(r"[^\w\s-]", "", title)[:80].strip().replace(" ", "_")
        filename = f"{safe_memo}_{safe_title}.pdf"
        return filename

    # ---------------- process ----------------
    def process_new_memos(self, entries):
        if not entries:
            log_jsonl("INFO", "no_new_memos")
            return
        for entry in entries:
            memo_number = entry["memo_number"]
            title = entry["title"]
            date = entry.get("date")
            sharepoint_url = entry["sharepoint_url"]
            log_jsonl(
                "INFO",
                "memo_discovered",
                memo_number=memo_number,
                title=title,
                url=sharepoint_url,
            )

            # Download bytes with retry strategies (memory-only)
            file_bytes, mime = self.download_bytes(sharepoint_url, memo_number)
            if not file_bytes:
                # invalid download – skip
                log_jsonl("WARNING", "skip_invalid_download", memo_number=memo_number)
                continue

            # Build filename and upload
            filename = self.safe_filename_from_memo(memo_number, title)
            public_url = self.upload_bytes_to_supabase(
                file_bytes, filename, mime or "application/pdf"
            )
            record_url = public_url or sharepoint_url

            # Insert notification
            inserted = self.insert_notification(memo_number, title, date, record_url)
            if not inserted:
                log_jsonl(
                    "ERROR", "notification_insert_failed", memo_number=memo_number
                )

            # update cached latest if upload succeeded and number parseable
            match = re.search(r"No\.?\s*(\d+)", memo_number)
            if match:
                try:
                    num = int(match.group(1))
                    if num and (
                        self._cached_latest_number is None
                        or num > self._cached_latest_number
                    ):
                        self._cached_latest_number = num
                        log_jsonl(
                            "INFO",
                            "update_cached_latest",
                            latest=self._cached_latest_number,
                        )
                except Exception:
                    pass

            # tiny pause to be polite
            time.sleep(0.5)

    # ---------------- run ----------------
    def run_once(self):
        log_jsonl("INFO", "run_start")
        all_new = []
        current_year = datetime.now().year
        for year in [current_year, current_year + 1]:
            # check if page exists
            try:
                head = self.session.head(
                    BASE_URL_TEMPLATE.format(year=year),
                    timeout=TIMEOUT,
                    allow_redirects=True,
                )
                if head.status_code != 200:
                    continue
            except Exception:
                continue
            new = self.fetch_memos_for_year(year)
            all_new.extend(new)
        if all_new:
            log_jsonl("INFO", "new_memos_total", count=len(all_new))
        self.process_new_memos(all_new)
        log_jsonl("INFO", "run_complete")

    def run_continuous(self):
        log_jsonl("INFO", "continuous_start", interval=self.interval)
        try:
            while True:
                self.run_once()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            log_jsonl("INFO", "stopped_by_user")


def main():
    parser = argparse.ArgumentParser(
        description="DepEd Laguna Memo Auto Monitor (Supabase, memory-only)"
    )
    parser.add_argument(
        "--interval", type=int, default=DEFAULT_INTERVAL, help="Poll interval seconds"
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    monitor = MemoMonitor(interval=args.interval)

    if args.once:
        monitor.run_once()
    else:
        monitor.run_continuous()


if __name__ == "__main__":
    main()