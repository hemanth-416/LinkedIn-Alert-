import os
import json
import logging
import smtplib
from email.mime.text import MIMEText

import requests
import gspread
from bs4 import BeautifulSoup
from flask import Flask, request, render_template_string, jsonify
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# CONFIG
# =========================
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:5000")
JOB_LOCATION = os.getenv("JOB_LOCATION", "United States")

BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
HEADERS = {"User-Agent": "Mozilla/5.0"}

if not GOOGLE_CREDENTIALS:
    raise ValueError("Missing GOOGLE_CREDENTIALS environment variable")

if not EMAIL_SENDER:
    raise ValueError("Missing EMAIL_SENDER environment variable")

if not EMAIL_PASSWORD:
    raise ValueError("Missing EMAIL_PASSWORD environment variable")

# =========================
# GOOGLE SHEETS SETUP
# =========================
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

try:
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    client = gspread.authorize(creds)

    spreadsheet = client.open("LinkedIn Job Tracker")
    job_sheet = spreadsheet.worksheet("Sheet2")
    user_sheet = spreadsheet.worksheet("Sheet6")

    logger.info("Connected to Google Sheets successfully")
except Exception as e:
    logger.exception("Failed to initialize Google Sheets")
    raise RuntimeError(f"Google Sheets setup failed: {e}") from e

# =========================
# HELPERS
# =========================
def normalize_email(email: str) -> str:
    return email.strip().lower()


def normalize_titles(titles_str: str) -> list[str]:
    return [t.strip().lower() for t in titles_str.split(",") if t.strip()]


# =========================
# USERS
# =========================
def load_users():
    try:
        rows = user_sheet.get_all_values()
        users = []

        for row in rows:
            if len(row) >= 2:
                email = normalize_email(row[0])
                titles = normalize_titles(row[1])

                if email and titles:
                    users.append({
                        "email": email,
                        "titles": titles
                    })

        return users
    except Exception:
        logger.exception("Error loading users from Google Sheet")
        return []


def save_user(email, titles):
    try:
        email = normalize_email(email)
        new_titles = set(normalize_titles(titles))

        if not email or not new_titles:
            logger.warning("Skipping save_user due to empty email or titles")
            return False

        rows = user_sheet.get_all_values()

        for idx, row in enumerate(rows, start=1):
            if len(row) >= 1 and normalize_email(row[0]) == email:
                existing_titles = set(normalize_titles(row[1])) if len(row) > 1 else set()
                merged_titles = sorted(existing_titles.union(new_titles))
                updated_titles = ",".join(merged_titles)

                user_sheet.update_cell(idx, 2, updated_titles)
                logger.info(f"Updated user: {email}")
                return True

        user_sheet.append_row([email, ",".join(sorted(new_titles))])
        logger.info(f"New user added: {email}")
        return True

    except Exception:
        logger.exception(f"Error saving user {email}")
        return False


# =========================
# EMAIL
# =========================
def send_email(subject, body, to_email):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = to_email

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        logger.info(f"Email sent to {to_email}")
    except Exception:
        logger.exception(f"Failed to send email to {to_email}")


# =========================
# JOB DEDUP
# =========================
def job_already_sent(job_url):
    try:
        sent_urls = job_sheet.col_values(1)
        return job_url in sent_urls
    except Exception:
        logger.exception("Error checking sent jobs")
        return False


def mark_job_as_sent(job_url, title, company, location):
    try:
        job_sheet.append_row([job_url, title, company, location])
        logger.info(f"Marked job as sent: {job_url}")
    except Exception:
        logger.exception(f"Error marking job as sent: {job_url}")


# =========================
# PROCESS JOBS
# =========================
def process_jobs():
    logger.info("Starting job processing")

    users = load_users()
    if not users:
        logger.info("No users found")
        return {"status": "no_users", "processed": 0, "emailed": 0}

    all_titles = set()
    for user in users:
        for title in user["titles"]:
            all_titles.add(title)

    if not all_titles:
        logger.info("No titles found")
        return {"status": "no_titles", "processed": 0, "emailed": 0}

    keywords = " OR ".join(sorted(all_titles))

    query_params = {
        "keywords": keywords,
        "location": JOB_LOCATION,
        "f_TPR": "r3600",
        "sortBy": "DD",
    }

    logger.info(f"Searching LinkedIn jobs with keywords: {keywords} in {JOB_LOCATION}")

    try:
        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params=query_params,
            timeout=20
        )
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("Failed to fetch jobs from LinkedIn")
        return {"status": "fetch_failed", "processed": 0, "emailed": 0}

    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.find_all("li")

    processed_count = 0
    emailed_count = 0

    for card in cards:
        try:
            link_tag = card.select_one('[class*="_full-link"]')
            title_tag = card.select_one('[class*="_title"]')
            company_tag = card.select_one('[class*="_subtitle"]')
            location_tag = card.select_one('[class*="_metadata"]')

            if not (link_tag and title_tag and company_tag):
                continue

            raw_url = link_tag.get("href", "").strip()
            if not raw_url:
                continue

            job_url = raw_url.split("?")[0]

            if job_already_sent(job_url):
                continue

            title = title_tag.get_text(strip=True).lower()
            company = company_tag.get_text(strip=True)
            location = location_tag.get_text(strip=True) if location_tag else JOB_LOCATION

            matched_users = []
            for user in users:
                if any(t in title for t in user["titles"]):
                    matched_users.append(user["email"])

            if not matched_users:
                continue

            body = (
                f"Job Title: {title}\n"
                f"Company: {company}\n"
                f"Location: {location}\n"
                f"Link: {job_url}"
            )

            for email in matched_users:
                send_email("🚨 Job Alert", body, email)
                emailed_count += 1

            mark_job_as_sent(job_url, title, company, location)
            processed_count += 1

        except Exception:
            logger.exception("Error processing a job card")
            continue

    logger.info(
        f"Job processing complete. Jobs matched: {processed_count}, Emails sent: {emailed_count}"
    )

    return {
        "status": "success",
        "processed": processed_count,
        "emailed": emailed_count,
    }


# =========================
# REGISTER PAGE
# =========================
@app.route("/register")
def register():
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Alerts</title>
<style>
body {
  margin: 0;
  font-family: 'Segoe UI', sans-serif;
  background: linear-gradient(135deg, #667eea, #764ba2);
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100vh;
}
.card {
  background: white;
  padding: 30px;
  border-radius: 15px;
  width: 90%;
  max-width: 420px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.2);
  text-align: center;
}
h2 { color: #333; }
input, textarea {
  width: 100%;
  padding: 12px;
  margin-top: 10px;
  margin-bottom: 15px;
  border-radius: 10px;
  border: 1px solid #ccc;
  box-sizing: border-box;
}
button {
  width: 100%;
  padding: 14px;
  border: none;
  border-radius: 10px;
  background: linear-gradient(135deg, #667eea, #764ba2);
  color: white;
  font-size: 16px;
  cursor: pointer;
}
button:hover {
  opacity: 0.95;
}
</style>
</head>
<body>
<div class="card">
  <h2>🚀 Job Alerts</h2>
  <form action="/subscribe" method="post">
    <input type="email" name="email" placeholder="Enter your email" required>
    <textarea name="titles" placeholder="java developer, spring boot developer, backend engineer" required></textarea>
    <button type="submit">Subscribe</button>
  </form>
</div>
</body>
</html>
""")


# =========================
# SUBSCRIBE
# =========================
@app.route("/subscribe", methods=["POST"])
def subscribe():
    email = request.form.get("email", "").strip()
    titles = request.form.get("titles", "").strip()

    if not email or not titles:
        return "Missing email or job titles", 400

    saved = save_user(email, titles)
    if not saved:
        return "Unable to save subscription", 500

    return """
    <h2>✅ Subscription successful!</h2>
    <p>You will start receiving job alerts soon.</p>
    <p><a href="/register">Go back</a></p>
    """


# =========================
# HEALTH CHECK
# =========================
@app.route("/")
def home():
    return "App is running"


# =========================
# MANUAL JOB RUN
# =========================
@app.route("/run-jobs")
def run_jobs():
    result = process_jobs()
    return jsonify(result)


# =========================
# RUN
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
