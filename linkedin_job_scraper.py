import os
import smtplib
import time
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup
from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from io import StringIO

app = Flask(__name__)

# -------------------------
# Job title categories
# -------------------------
TARGET_TITLES_DATA = [
    "Data Analyst", "devops engineer", "site reliability engineer", "sre", "cloud engineer",
    "aws devops engineer", "azure devops engineer", "platform engineer",
    "infrastructure engineer", "cloud operations engineer", "reliability engineer",
    "automation engineer", "cloud consultant", "build engineer", "cicd engineer",
    "systems reliability engineer", "observability engineer", "kubernetes engineer",
    "devsecops engineer", "infrastructure developer", "platform reliability engineer",
    "automation specialist"
]

TARGET_TITLES_CYBER = [
    "Cybersecurity Engineer", "Security Engineer", "SOC Analyst", "SOC Analyst III",
    "Pentester", "GRC Analyst", "IAM Analyst", "IAM Engineer", "IAM Administrator",
    "Cloud Security", "Cybersecurity Analyst", "Cyber Security SOC Analyst II",
    "incident response analyst", "threat detection analyst", "SIEM analyst",
    "Senior Cybersecurity Analyst", "security monitoring analyst", "Information Security Analyst",
    "Cloud Security Analyst", "Azure Security Analyst", "Identity & Access Specialist",
    "SailPoint Developer", "SailPoint Consultant", "Azure IAM Engineer", "Cloud IAM Analyst",
    "System Engineer", "System Engineer I", "System Engineer II", "System Engineer III", "Data Analyst"
]

TARGET_TITLES_ORACLE = [
    "Oracle Developer", "OIC Developer", "Oracle Cloud Engineer", "Oracle Integration Cloud",
    "Oracle Fusion Developer", "Oracle HCM", "Oracle ERP", "OIC Consultant", "Oracle Cloud Consultant"
]

# -------------------------
# Config (environment-driven)
# -------------------------
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

EMAIL_RECEIVER_CYBER = os.getenv("EMAIL_RECEIVER_CYBER", "")
EMAIL_RECEIVER_DATA = os.getenv("EMAIL_RECEIVER_DATA", "")
EMAIL_RECEIVER_ORACLE = os.getenv("EMAIL_RECEIVER_ORACLE", "")

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

SHEET_CYBER = os.getenv("SHEET_CYBER", "Sheet3")
SHEET_DATA = os.getenv("SHEET_DATA", "Sheet4")
SHEET_ORACLE = os.getenv("SHEET_ORACLE", "Sheet5")

WORKBOOK_NAME = os.getenv("WORKBOOK_NAME", "LinkedIn Job Tracker")

BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
HEADERS = {"User-Agent": "Mozilla/5.0"}

LOCATIONS_US = [
    "New York, NY", "San Francisco Bay Area", "Austin, TX", "Dallas-Fort Worth Metroplex",
    "Chicago, IL", "Seattle, WA", "Atlanta, GA", "Boston, MA", "Los Angeles, CA",
    "Washington, DC-Baltimore Area", "Denver, CO", "Phoenix, AZ", "Charlotte, NC",
    "Kansas City Metropolitan Area", "Philadelphia, PA", "Houston, TX", "Orlando, FL",
    "Minneapolis-St. Paul, MN", "Pittsburgh, PA", "Salt Lake City, UT"
]

# -------------------------
# Google Sheets setup
# -------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.load(StringIO(GOOGLE_CREDENTIALS))
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
client = gspread.authorize(CREDS)

# -------------------------
# Helpers
# -------------------------
def parse_recipients(emails_str: str):
    return [e.strip() for e in emails_str.split(",") if e.strip()]

def send_email(subject, body, to_emails, retries=3):
    if not to_emails:
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(to_emails)
    
    for attempt in range(retries):
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 587, timeout=20) as server:
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_SENDER, to_emails, msg.as_string())
            return
        except Exception as e:
            print(f"❌ Email send failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2 * (attempt + 1))  # backoff
    print("❌ Giving up on sending email after retries.")

def extract_country(location):
    loc = (location or "").lower()
    if "united states" in loc or "usa" in loc:
        return "United States"
    return "Other"

def load_sheet(tab_name: str):
    try:
        return client.open(WORKBOOK_NAME).worksheet(tab_name)
    except gspread.WorksheetNotFound:
        sh = client.open(WORKBOOK_NAME)
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=6)
        ws.append_row(["Job URL", "Title", "Company", "Location", "Category", "Country"])
        return ws

def preload_urls(ws):
    try:
        return set(ws.col_values(1))
    except Exception as e:
        print(f"❌ Error loading URLs from {ws.title}: {e}")
        return set()

def mark_job_as_sent(ws, job_url, title, company, location, category, country):
    try:
        # Insert at top (row 2) to keep newest jobs at the top
        ws.insert_row([job_url, title, company, location, category, country], index=2)
    except Exception as e:
        print(f"❌ Error writing to sheet {ws.title}: {e}")

def matches_any(title_lower: str, keywords):
    return any(k.lower() in title_lower for k in keywords)

def process_jobs(query_params, keywords, expected_category, expected_country, sent_urls, recipients, ws):
    seen_jobs = set()
    for start in range(0, 100, 25):
        query_params["start"] = start
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=query_params, timeout=20)
        except requests.RequestException as e:
            print(f"❌ Request error: {e}")
            break

        if response.status_code != 200 or not response.text.strip():
            break

        soup = BeautifulSoup(response.text, "html.parser")
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

            if matches_any(title_lower, keywords) and country == expected_country:
                email_body = f"{title} at {company} — {location}\n{job_url}"
                subject = f"🔔 New {expected_category} Job 🔔"
                send_email(subject, email_body, recipients)
                mark_job_as_sent(ws, job_url, title, company, location, expected_category, country)
                sent_urls.add(job_url)
                print(f"✅ Sent {expected_category} job: {title}")

def run_category(category_name, keywords, recipients_env, sheet_name):
    ws = load_sheet(sheet_name)
    sent_urls = preload_urls(ws)
    recipients = parse_recipients(recipients_env)

    for loc in LOCATIONS_US:
        q = {
            "keywords": " OR ".join(keywords),
            "location": loc,
            "f_TPR": "r3600",
            "sortBy": "DD"
        }
        process_jobs(
            query_params=q,
            keywords=keywords,
            expected_category=category_name,
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
    run_category("Data-DevOps",   TARGET_TITLES_DATA,  EMAIL_RECEIVER_DATA,  SHEET_DATA)
    run_category("Oracle",        TARGET_TITLES_ORACLE,EMAIL_RECEIVER_ORACLE,SHEET_ORACLE)

@app.route("/")
def ping():
    check_new_jobs()
    return "✅ Checked Cybersecurity, DevOps, and Oracle jobs across major U.S. metros."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
