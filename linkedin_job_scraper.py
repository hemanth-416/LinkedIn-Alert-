import os
import smtplib
from email.mime.text import MIMEText
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from flask import Flask, request
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from io import StringIO
from datetime import datetime, timezone
import traceback
import re
import hashlib
from urllib.parse import urlparse, parse_qs

app = Flask(__name__)

# -------------------------
# Tuning knobs
# -------------------------
MAX_PAGES = int(os.getenv("MAX_PAGES", 2))                 # 25 jobs/page; 2 pages ~50 jobs per query
PER_RUN_LOCATIONS = int(os.getenv("PER_RUN_LOCATIONS", 6)) # metros per run to limit egress
TIME_WINDOW = os.getenv("TIME_WINDOW", "r86400")           # r3600=1h, r86400=24h, r604800=7d
ENFORCE_COUNTRY = os.getenv("ENFORCE_COUNTRY", "false").lower() == "true"

# -------------------------
# Target job titles (per category)
# -------------------------
TARGET_TITLES_DATA = [
    "Data Analyst","Data Engineer","DevOps Engineer","Site Reliability Engineer","SRE",
    "Cloud Engineer","AWS DevOps Engineer","Azure DevOps Engineer","Platform Engineer",
    "Infrastructure Engineer","Cloud Operations Engineer","Reliability Engineer",
    "Automation Engineer","Cloud Consultant","Build Engineer","CICD Engineer",
    "Systems Reliability Engineer","Observability Engineer","Kubernetes Engineer",
    "DevSecOps Engineer","Infrastructure Developer","Platform Reliability Engineer",
    "Automation Specialist",
]

TARGET_TITLES_CYBER = [
    "Cybersecurity Engineer","Security Engineer","SOC Analyst","SOC Analyst III","Pentester",
    "GRC Analyst","IAM Analyst","IAM Engineer","IAM Administrator","Cloud Security",
    "Cybersecurity Analyst","Cyber Security SOC Analyst II","Incident Response Analyst",
    "Threat Detection Analyst","SIEM Analyst","Senior Cybersecurity Analyst",
    "Security Monitoring Analyst","Information Security Analyst","Cloud Security Analyst",
    "Azure Security Analyst","Identity & Access Specialist","SailPoint Developer",
    "SailPoint Consultant","Azure IAM Engineer","Cloud IAM Analyst",
    "System Engineer","System Engineer I","System Engineer II","System Engineer III",
]

TARGET_TITLES_ORACLE = [
    "Oracle Developer","OIC Developer","Oracle Cloud Engineer","Oracle Integration Cloud",
    "Oracle Fusion Developer","Oracle HCM","Oracle ERP","OIC Consultant","Oracle Cloud Consultant",
]

# -------------------------
# Email / Sheets config
# -------------------------
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER_CYBER = os.getenv("EMAIL_RECEIVER_CYBER", "")
EMAIL_RECEIVER_DATA  = os.getenv("EMAIL_RECEIVER_DATA", "")
EMAIL_RECEIVER_ORACLE = os.getenv("EMAIL_RECEIVER_ORACLE", "")

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
WORKBOOK_NAME = os.getenv("WORKBOOK_NAME", "LinkedIn Job Tracker")
SHEET_CYBER  = os.getenv("SHEET_CYBER",  "Sheet3")
SHEET_DATA   = os.getenv("SHEET_DATA",   "Sheet4")
SHEET_ORACLE = os.getenv("SHEET_ORACLE", "Sheet5")

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.load(StringIO(GOOGLE_CREDENTIALS))
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
client = gspread.authorize(CREDS)

# -------------------------
# LinkedIn session
# -------------------------
BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

def make_session():
    s = requests.Session()
    retries = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.linkedin.com/jobs/search/",
        "Cache-Control": "no-cache",
    })
    return s

SESSION = make_session()

# -------------------------
# Locations (rotating slice)
# -------------------------
LOCATIONS_US = [
    "New York, NY","San Francisco Bay Area","Austin, TX","Dallas-Fort Worth Metroplex",
    "Chicago, IL","Seattle, WA","Atlanta, GA","Boston, MA","Los Angeles, CA",
    "Washington, DC-Baltimore Area","Denver, CO","Phoenix, AZ","Charlotte, NC",
    "Kansas City Metropolitan Area","Philadelphia, PA","Houston, TX","Orlando, FL",
    "Minneapolis-St. Paul, MN","Pittsburgh, PA","Salt Lake City, UT",
]

def rotating_slice(seq, size, seed=None):
    if not seq:
        return []
    if size >= len(seq):
        return list(seq)
    if seed is None:
        seed = datetime.now(timezone.utc).hour  # timezone-aware
    start = seed % len(seq)
    return (seq[start:] + seq[:start])[:size]

# -------------------------
# Helpers (dedupe, email, sheets)
# -------------------------
HEADER_ROW = ["Job ID","Job URL","Title","Company","Location","Category","Country","Scraped At (UTC)"]

job_id_regexes = [
    re.compile(r"/jobs/view/(\d+)", re.IGNORECASE),          # /jobs/view/1234567890/
    re.compile(r"currentJobId=(\d+)", re.IGNORECASE),        # ?currentJobId=1234567890
    re.compile(r"viewJobId=(\d+)", re.IGNORECASE),           # ?viewJobId=1234567890
]

def extract_job_id(url: str) -> str:
    """Extract a stable LinkedIn job ID from a job URL. Fallback to hash if not found."""
    if not url:
        return ""
    for rx in job_id_regexes:
        m = rx.search(url)
        if m:
            return m.group(1)
    # Try query params
    try:
        qs = parse_qs(urlparse(url).query)
        for key in ("currentJobId", "viewJobId", "jobId"):
            if key in qs and qs[key]:
                return qs[key][0]
    except Exception:
        pass
    # Fallback to hash (still prevents duplicates if same canonical URL recurs)
    return "u_" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

def parse_recipients(emails_str: str):
    return [e.strip() for e in emails_str.split(",") if e.strip()]

def send_email(subject, body, to_emails):
    if not to_emails:
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = ", ".join(to_emails)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, to_emails, msg.as_string())
    except Exception as e:
        # On Railway, SMTP may be blocked. We just log and continue.
        print("❌ Email send failed:", e)
        traceback.print_exc()

def extract_country(location: str):
    loc = (location or "").lower()
    return "United States" if ("united states" in loc or "usa" in loc) else "Other"

def ensure_header(ws):
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if first != HEADER_ROW:
        if first:
            ws.delete_rows(1)
        ws.insert_row(HEADER_ROW, index=1)

def load_sheet(tab_name: str):
    try:
        ws = client.open(WORKBOOK_NAME).worksheet(tab_name)
    except gspread.WorksheetNotFound:
        sh = client.open(WORKBOOK_NAME)
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=len(HEADER_ROW))
    ensure_header(ws)
    return ws

def preload_ids(ws) -> set[str]:
    """Load existing Job IDs from col A (skip header)."""
    try:
        col = ws.col_values(1)
        return set(col[1:]) if col else set()
    except Exception as e:
        print(f"❌ Error loading IDs from {ws.title}: {e}")
        return set()

def preload_all_ids(sheets: list) -> set[str]:
    """Union of IDs across all category sheets to avoid cross-category duplicates."""
    ids = set()
    for ws in sheets:
        ids |= preload_ids(ws)
    return ids

def insert_job(ws, job_id, job_url, title, company, location, category, country):
    """Newest first: insert at row 2 with timestamp and a clickable hyperlink."""
    try:
        ensure_header(ws)
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        hyperlink = f'=HYPERLINK("{job_url}", "Open")'
        ws.insert_row(
            [job_id, hyperlink, title, company, location, category, country, now_utc],
            index=2
        )
    except Exception as e:
        print(f"❌ Error writing to sheet {ws.title}: {e}")
        traceback.print_exc()

def matches_any(title_lower: str, keywords):
    # case-insensitive substring match
    return any(k.lower() in title_lower for k in keywords)

# -------------------------
# Core scraping
# -------------------------
def process_jobs(query_params, keywords, category, expected_country, seen_ids: set[str], recipients: list[str], ws):
    in_run_ids = set()

    for page_idx in range(MAX_PAGES):
        query_params["start"] = page_idx * 25
        try:
            resp = SESSION.get(BASE_URL, params=query_params, timeout=15)
        except requests.RequestException as e:
            print(f"❌ Request error: {e}")
            traceback.print_exc()
            break

        if resp.status_code != 200 or not resp.text.strip():
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.find_all("li")
        if not cards:
            break

        for card in cards:
            link_tag = card.select_one('[class*="_full-link"]')
            title_tag = card.select_one('[class*="_title"]')
            company_tag = card.select_one('[class*="_subtitle"]')
            location_tag = card.select_one('[class*="_location"]')
            if not (link_tag and title_tag and company_tag):
                continue

            raw_url = link_tag["href"].strip()
            # canonicalize URL (drop query params for display, but we still extract ID from raw)
            job_url = raw_url.split("?", 1)[0]
            job_id = extract_job_id(raw_url)
            if not job_id:
                # as a last resort, hash the canonical URL to avoid repeats
                job_id = "h_" + hashlib.sha256(job_url.encode("utf-8")).hexdigest()[:16]

            # dedupe by job_id across runs/categories/locations AND within this run
            if job_id in seen_ids or job_id in in_run_ids:
                continue
            in_run_ids.add(job_id)

            title = title_tag.get_text(strip=True)
            title_lower = title.lower()
            company = company_tag.get_text(strip=True)
            location = location_tag.get_text(strip=True) if location_tag else "Unknown"
            country = extract_country(location)

            if matches_any(title_lower, keywords) and ((not ENFORCE_COUNTRY) or (country == expected_country)):
                # send email (best-effort)
                subject = f"🔔 New {category} Job"
                body = f"{title} at {company} — {location}\n{job_url}"
                send_email(subject, body, recipients)

                # write to sheet (newest on top)
                insert_job(ws, job_id, job_url, title, company, location, category, country)

                # remember we've handled this job id (persisted across all categories)
                seen_ids.add(job_id)
                print(f"✅ Saved {category} job: {title} [{job_id}]")

def run_category(category_name, keywords, recipients_env, ws, seen_ids: set[str]):
    recipients = parse_recipients(recipients_env)
    if not recipients:
        print(f"⚠️ No recipients configured for {category_name}. Emails will be skipped.")

    cleaned_keywords = [k.strip() for k in keywords if k and k.strip()]
    for loc in rotating_slice(LOCATIONS_US, PER_RUN_LOCATIONS):
        q = {
            "keywords": " OR ".join(cleaned_keywords),
            "location": loc,
            "f_TPR": TIME_WINDOW,
            "sortBy": "DD",
        }
        process_jobs(
            query_params=q,
            keywords=cleaned_keywords,
            category=category_name,
            expected_country="United States",
            seen_ids=seen_ids,
            recipients=recipients,
            ws=ws
        )

# -------------------------
# Orchestration
# -------------------------
def check_new_jobs():
    ws_cyber  = load_sheet(SHEET_CYBER)
    ws_data   = load_sheet(SHEET_DATA)
    ws_oracle = load_sheet(SHEET_ORACLE)

    # Build a single global set of job IDs from ALL sheets
    global_seen_ids = preload_all_ids([ws_cyber, ws_data, ws_oracle])

    # Run categories — all share the same global_seen_ids to avoid cross-duplication
    run_category("Cybersecurity", TARGET_TITLES_CYBER, EMAIL_RECEIVER_CYBER, ws_cyber,  global_seen_ids)
    run_category("DevOps",        TARGET_TITLES_DATA,  EMAIL_RECEIVER_DATA,  ws_data,   global_seen_ids)
    run_category("Oracle",        TARGET_TITLES_ORACLE,EMAIL_RECEIVER_ORACLE,ws_oracle, global_seen_ids)

# -------------------------
# Routes (health + run)
# -------------------------
@app.route("/", methods=["GET", "HEAD"])
def health():
    if request.method == "HEAD":
        return "", 200
    return "OK"

@app.route("/run", methods=["POST", "GET"])
def run():
    try:
        check_new_jobs()
        return "✅ Scrape complete"
    except Exception as e:
        traceback.print_exc()
        return f"❌ Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
