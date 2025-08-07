import os
import smtplib
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup
from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from io import StringIO

app = Flask(__name__)

# Cybersecurity job titles
TARGET_TITLES_CYBER = [
    "Cybersecurity Engineer", "Security Engineer", "SOC Analyst", "SOC Analyst III", "Pentester", "GRC Analyst",
    "Cloud Security", "Cybersecurity Analyst", "Cyber Security SOC Analyst II", "incident response analyst",
    "threat detection analyst", "SIEM analyst", "splunk analyst", "QRadar analyst", "sentinel analyst",
    "senior cybersecurity analyst", "security monitoring analyst", "information security analyst", "EDR analyst",
    "cloud security analyst", "Azure security analyst", "AWS security analyst", "IAM Analyst", "IAM Engineer",
    "IAM Administrator", "Identity & Access Specialist", "Privileged Access Management Engineer", "SailPoint Developer",
    "SailPoint Consultant", "Okta Administrator", "Access Control Analyst", "Azure IAM Engineer", "Cloud IAM Analyst",
    "System Engineer", "System Engineer I", "System Engineer II", "System Engineer II", "System Engineer III", "Data Analyst"
]

# Email configuration
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER_CYBER = os.getenv("EMAIL_RECEIVER_CYBER")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

# Google Sheets setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.load(StringIO(GOOGLE_CREDENTIALS))
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
client = gspread.authorize(CREDS)
sheet = client.open("LinkedIn Job Tracker").worksheet("Sheet3")

# LinkedIn config
BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def send_email(subject, body, to_email):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = to_email
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)

def job_already_sent(job_url, sent_urls):
    return job_url in sent_urls

def mark_job_as_sent(job_url, title, company, location, category, country):
    try:
        sheet.append_row([job_url, title, company, location, category, country])
    except Exception as e:
        print(f"❌ Error writing to sheet: {e}")

def extract_country(location):
    location_lower = location.lower()
    if "united states" in location_lower or "usa" in location_lower:
        return "United States"
    else:
        return "Other"

def process_jobs(query_params, expected_category, expected_country, sent_urls):
    seen_jobs = set()

    for start in range(0, 100, 25):
        query_params["start"] = start
        response = requests.get(BASE_URL, headers=HEADERS, params=query_params)
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

            if link_tag and title_tag and company_tag:
                job_url = link_tag['href'].strip().split('?')[0]
                title = title_tag.get_text(strip=True)
                title_lower = title.lower()
                company = company_tag.get_text(strip=True)
                location = location_tag.get_text(strip=True) if location_tag else "Unknown"
                country = extract_country(location)
                dedup_key = f"{title_lower}::{company.lower()}"

                if dedup_key in seen_jobs or job_already_sent(job_url, sent_urls):
                    continue
                seen_jobs.add(dedup_key)

                email_body = f"{title} at {company} — {location}\n{job_url}"

                if expected_category == "Cybersecurity" and any(t.lower() in title_lower for t in TARGET_TITLES_CYBER) and country == expected_country:
                    send_email("🚨🚨🛡 New Cybersecurity Job! 🛡🚨🚨", email_body, EMAIL_RECEIVER_CYBER)
                    mark_job_as_sent(job_url, title, company, location, "Cybersecurity", country)
                    print("✅ Sent Cybersecurity job (United States):", title)

def check_new_jobs():
    try:
        sent_urls = set(sheet.col_values(1))  # Load once
    except Exception as e:
        print(f"❌ Error loading sent job URLs: {e}")
        sent_urls = set()

    locations = [
        "New York, NY", "San Francisco Bay Area", "Austin, TX", "Dallas-Fort Worth Metroplex",
        "Chicago, IL", "Seattle, WA", "Atlanta, GA", "Boston, MA", "Los Angeles, CA",
        "Washington, DC-Baltimore Area", "Denver, CO", "Phoenix, AZ", "Charlotte, NC",
        "Kansas City Metropolitan Area", "Philadelphia, PA", "Houston, TX", "Orlando, FL",
        "Minneapolis-St. Paul, MN", "Pittsburgh, PA", "Salt Lake City, UT"
    ]

    for loc in locations:
        cyber_query = {
            "keywords": " OR ".join(TARGET_TITLES_CYBER),
            "location": loc,
            "f_TPR": "r3600",
            "sortBy": "DD"
        }
        process_jobs(cyber_query, "Cybersecurity", "United States", sent_urls)

@app.route("/")
def ping():
    check_new_jobs()
    return "✅ Checked for Cybersecurity (United States) jobs."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
