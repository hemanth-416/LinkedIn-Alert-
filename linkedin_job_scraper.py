import os
import smtplib
from email.mime.text import MIMEText
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from io import StringIO
from datetime import datetime, timezone  # <-- timezone-aware UTC

app = Flask(__name__)

# -------------------------
# Tuning knobs to control egress
# -------------------------
MAX_PAGES = int(os.getenv("MAX_PAGES", 2))                  # pages per query (0, 25, ...)
PER_RUN_LOCATIONS = int(os.getenv("PER_RUN_LOCATIONS", 6))  # metros per run
TIME_WINDOW = os.getenv("TIME_WINDOW", "r3600")             # r3600=1h, r86400=24h, r604800=7d
ENFORCE_COUNTRY = os.getenv("ENFORCE_COUNTRY", "false").lower() == "true"

# -------------------------
# Target job titles
# -------------------------
TARGET_TITLES_DATA = [
    "Data Analyst", "Data Engineer", "DevOps Engineer", "Site Reliability Engineer", "SRE",
    "Cloud Engineer", "AWS DevOps Engineer", "Azure DevOps Engineer", "Platform Engineer",
    "Infrastructure Engineer", "Cloud Operations Engineer", "Reliability Engineer",
    "Automation Engineer", "Cloud Consultant", "Build Engineer", "CICD Engineer",
    "Systems Reliability Engineer", "Observability Engineer", "Kubernetes Engineer",
    "DevSecOps Engineer", "Infrastructure Developer", "Platform Reliability Engineer",
    "Automation Specialist"
]

TARGET_TITLES_CYBER = [
    "Cybersecurity Engineer", "Security Engineer", "SOC Analyst", "SOC Analyst III", "Pentester", "GRC Analyst",
    "IAM Analyst", "IAM Engineer", "IAM Administrator", "Cloud Security", "Cybersecurity Analyst",
    "Cyber Security SOC Analyst II", "Incident Response Analyst", "Threat Detection Analyst", "SIEM Analyst",
    "Senior Cybersecurity Analyst", "Security Monitoring Analyst", "Information Security Analyst",
    "Cloud Security Analyst", "Azure Security Analyst", "Identity & Access Specialist", "SailPoint Developer",
    "SailPoint Consultant", "Azure IAM Engineer", "Cloud IAM Analyst", "System Engineer",
    "System Engineer I", "System Engineer II", "System Engineer III"
]

TARGET_TITLES_ORACLE = [
    "Oracle Developer", "OIC Developer", "Oracle Cloud Engineer", "Oracle Integration Cloud",
    "Oracle Fusion Developer", "Oracle HCM", "Oracle ERP", "OIC Consultant", "Oracle Cloud Consultant"
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

SHEET_CYBER  = os.getenv("SHEET_CYBER",  "Sheet3")
SHEET_DATA   = os.getenv("SHEET_DATA",   "Sheet4")
SHEET_ORACLE = os.getenv("SHEET_ORACLE", "Sheet5")

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.load(StringIO(GOOGLE_CREDENTIALS))
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
client = gspread.authorize(CREDS)
WORKBOOK_NAME = os.getenv("WORKBOOK_NAME", "LinkedIn Job Tracker")

# -------------------------
# LinkedIn config
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
# Locations — rotate a slice per run
# -------------------------
LOCATIONS_US = [
    "New York, NY","San Francisco Bay Area","Austin, TX","Dallas-Fort Worth Metroplex",
    "Chicago, IL","Seattle, WA","Atlanta, GA","Boston, MA","Los Angeles, CA",
    "Washington, DC-Baltimore Area","Denver, CO","Phoenix, AZ","Charlotte, NC",
    "Kansas City Metropolitan Area","Philadelphia, PA","Houston, TX","Orlando, FL",
    "Minneapolis-St. Paul, MN","Pittsburgh, PA","Salt Lake City, UT"
]

def rotating_slice(seq, size, seed=None):
    """Return a deterministic slice of `seq` of length `size` rotating over time."""
    if size >= len(seq):
        return list(seq)
    if seed is None:
        # rotate by UTC hour so each hour you scan a different chunk (timezone-aware)
        seed = datetime.now(timezone.utc).hour
    start = seed % len(seq)
    out = seq[start:] + seq[:start]
    return out[:size]

# -------------------------
# Helpers
# -------------------------
HEADER_ROW = ["Job URL", "Title", "Company", "Location", "Category", "Country"]

def parse_recipients(emails_str: str):
    return [e.strip() for e in emails_str.split(",") if e.strip()]

def send_email(subject, body, to_emails):
    if not to_emails:
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(to_emails)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, to_emails, msg.as_string())

def extract_country(location: str):
    loc = (location or "").lower()
    if "united states" in loc or "usa" in loc:
        return "United States"
    return "Other"

def ensure_header(ws):
    """Make sure the first row is the expected header before inserting data at row 2."""
    try:
        first_row = ws.row_values(1)
    except Exception:
        first_row = []
    if first_row != HEADER_ROW:
        if first_row:  # replace wrong header
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

def preload_urls(ws):
    try:
        col = ws.col_values(1)
        # skip header row (row 1)
        return set(col[1:]) if col else set()
    except Exception as e:
        print(f"❌ Error loading URLs from {ws.title}: {e}")
        return set()

def mark_job_as_sent(ws, job_url, title, company, location, category, country):
    """Insert newest job at row 2 (right under header)."""
    try:
        ensure_header(ws)
        ws.insert_row([job_url, title, company, location, category, country], index=2)
    except Exception as e:
        print(f"❌ Error writing to sheet {ws.title}: {e}")

def matches_any(title_lower: str, keywords):
    return any(k.lower() in title_lower for k in keywords)

def process_jobs(query_params, keywords, category, expected_country, sent_urls, recipients, ws):
    seen_jobs = set()
    for page_idx in range(MAX_PAGES):
        query_params["start"] = page_idx * 25
        try:
            resp = SESSION.get(BASE_URL, params=query_params, timeout=15)
        except requests.RequestException as e:
            print(f"❌ Request error: {e}")
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

            job_url = link_tag['href'].strip().split('?')[0]
            title = title_tag.get_text(strip=True)
            title_lower = title.lower()
            company = company_tag.get_text(strip=True)
            location = location_tag.get_text(strip=True) if location_tag else "Unknown"
            country = extract_country(location)

            dedup_key = f"{title_lower}::{company.lower()}"
            if dedup_key in seen_jobs or job_url in sent_urls:
                continue
            seen_jobs.add(dedup_key)

            country_ok = (not ENFORCE_COUNTRY) or (country == expected_country)
            if matches_any(title_lower, keywords) and country_ok:
                email_body = f"{title} at {company} — {location}\n{job_url}"
                subject = f"🔔 New {category} Job"
                send_email(subject, email_body, recipients)
                mark_job_as_sent(ws, job_url, title, company, location, category, country)
                sent_urls.add(job_url)
                print(f"✅ Sent {category} job: {title}")

def run_category(category_name, keywords, recipients_env, sheet_name):
    ws = load_sheet(sheet_name)
    sent_urls = preload_urls(ws)
    recipients = parse_recipients(recipients_env)
    if not recipients:
        print(f"⚠️ No recipients configured for {category_name}. Set EMAIL_RECEIVER_* env.")

    # Normalize/trim keywords for cleaner queries
    cleaned_keywords = [k.strip() for k in keywords if k and k.strip()]

    locations = rotating_slice(LOCATIONS_US, PER_RUN_LOCATIONS)
    for loc in locations:
        q = {
            "keywords": " OR ".join(cleaned_keywords),
            "location": loc,
            "f_TPR": TIME_WINDOW,
            "sortBy": "DD"
        }
        process_jobs(
            query_params=q,
            keywords=cleaned_keywords,
            category=category_name,
            expected_country="United States",
            sent_urls=sent_urls,
            recipients=recipients,
            ws=ws
        )

# -------------------------
# Orchestration
# -------------------------
def check_new_jobs():
    run_category("Cybersecurity", TARGET_TITLES_CYBER, EMAIL_RECEIVER_CYBER, SHEET_CYBER)
    run_category("DevOps",        TARGET_TITLES_DATA,  EMAIL_RECEIVER_DATA,  SHEET_DATA)
    run_category("Oracle",        TARGET_TITLES_ORACLE,EMAIL_RECEIVER_ORACLE,SHEET_ORACLE)

@app.route("/")
def ping():
    check_new_jobs()
    return "✅ Checked Cybersecurity, DevOps, and Oracle jobs (newest first)."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
