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
JOB_LOCATION = os.getenv("JOB_LOCATION", "United States")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "LinkedIn Job Tracker")
JOB_SHEET_NAME = os.getenv("JOB_SHEET_NAME", "Sheet7")
USER_SHEET_NAME = os.getenv("USER_SHEET_NAME", "Sheet8")

BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
HEADERS = {"User-Agent": "Mozilla/5.0"}

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

if not GOOGLE_CREDENTIALS:
    raise ValueError("Missing GOOGLE_CREDENTIALS environment variable")

if not EMAIL_SENDER:
    raise ValueError("Missing EMAIL_SENDER environment variable")

if not EMAIL_PASSWORD:
    raise ValueError("Missing EMAIL_PASSWORD environment variable")


# =========================
# GOOGLE SHEETS HELPERS
# =========================
def get_gspread_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    return gspread.authorize(creds)


def get_sheets():
    client = get_gspread_client()
    spreadsheet = client.open(SPREADSHEET_NAME)
    job_sheet = spreadsheet.worksheet(JOB_SHEET_NAME)
    user_sheet = spreadsheet.worksheet(USER_SHEET_NAME)
    return job_sheet, user_sheet


# =========================
# NORMALIZERS
# =========================
def normalize_email(email: str) -> str:
    return email.strip().lower()


def normalize_titles(titles_str: str) -> list[str]:
    return [t.strip().lower() for t in titles_str.split(",") if t.strip()]


# =========================
# USERS
# =========================
def load_users_from_sheet(user_sheet):
    rows = user_sheet.get_all_values()
    users = []

    for row in rows:
        if len(row) < 2:
            continue

        email = normalize_email(row[0])
        titles_raw = row[1].strip()

        # Skip header row
        if email == "email" or titles_raw.lower() == "titles":
            continue

        titles = normalize_titles(titles_raw)

        if email and titles:
            users.append({
                "email": email,
                "titles": titles
            })

    logger.info(f"Loaded {len(users)} users from sheet")
    return users


def save_user(email, titles):
    try:
        _, user_sheet = get_sheets()

        email = normalize_email(email)
        new_titles = set(normalize_titles(titles))

        if not email or not new_titles:
            logger.warning("Skipping save_user because email or titles are empty")
            return False

        rows = user_sheet.get_all_values()

        for idx, row in enumerate(rows, start=1):
            if len(row) >= 1 and normalize_email(row[0]) == email:
                existing_titles = set(
                    t.strip().lower() for t in row[1].split(",")
                ) if len(row) > 1 else set()

                merged = existing_titles.union(new_titles)
                updated_titles = ",".join(sorted(merged))

                user_sheet.update_cell(idx, 2, updated_titles)
                logger.info(f"Updated existing user: {email}")
                return True

        user_sheet.append_row([email, ",".join(sorted(new_titles))])
        logger.info(f"Added new user: {email}")
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

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        logger.info(f"Email sent to {to_email}")
        return True

    except Exception:
        logger.exception(f"Failed to send email to {to_email}")
        return False


# =========================
# PROCESS JOBS
# =========================
def process_jobs():
    logger.info("Starting job processing")

    try:
        job_sheet, user_sheet = get_sheets()
    except Exception:
        logger.exception("Failed to connect to Google Sheets")
        return {
            "status": "sheet_connection_failed",
            "users_loaded": 0,
            "titles_found": 0,
            "cards_found": 0,
            "matched_jobs": 0,
            "jobs_saved": 0,
            "emails_sent": 0,
            "skipped_duplicates": 0,
            "skipped_missing_fields": 0,
        }

    try:
        users = load_users_from_sheet(user_sheet)
    except Exception:
        logger.exception("Error loading users")
        return {
            "status": "user_load_failed",
            "users_loaded": 0,
            "titles_found": 0,
            "cards_found": 0,
            "matched_jobs": 0,
            "jobs_saved": 0,
            "emails_sent": 0,
            "skipped_duplicates": 0,
            "skipped_missing_fields": 0,
        }

    if not users:
        logger.info("No users found in user sheet")
        return {
            "status": "no_users",
            "users_loaded": 0,
            "titles_found": 0,
            "cards_found": 0,
            "matched_jobs": 0,
            "jobs_saved": 0,
            "emails_sent": 0,
            "skipped_duplicates": 0,
            "skipped_missing_fields": 0,
        }

    all_titles = set()
    for user in users:
        for title in user["titles"]:
            all_titles.add(title)

    if not all_titles:
        logger.info("No titles found in user sheet")
        return {
            "status": "no_titles",
            "users_loaded": len(users),
            "titles_found": 0,
            "cards_found": 0,
            "matched_jobs": 0,
            "jobs_saved": 0,
            "emails_sent": 0,
            "skipped_duplicates": 0,
            "skipped_missing_fields": 0,
        }

    keywords = " OR ".join(sorted(all_titles))
    logger.info(f"Searching LinkedIn with keywords: {keywords}")

    query_params = {
        "keywords": keywords,
        "location": JOB_LOCATION,
        "f_TPR": "r3600",
        "sortBy": "DD",
    }

    try:
        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params=query_params,
            timeout=20
        )
        logger.info(f"LinkedIn response status: {response.status_code}")
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("LinkedIn request failed")
        return {
            "status": "fetch_failed",
            "users_loaded": len(users),
            "titles_found": len(all_titles),
            "cards_found": 0,
            "matched_jobs": 0,
            "jobs_saved": 0,
            "emails_sent": 0,
            "skipped_duplicates": 0,
            "skipped_missing_fields": 0,
        }

    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.find_all("li")
    logger.info(f"Found {len(cards)} LinkedIn cards")

    # Read sent URLs once to avoid Google Sheets quota errors
    try:
        sent_urls = set(job_sheet.col_values(1))
    except Exception:
        logger.exception("Failed to load existing sent job URLs")
        sent_urls = set()

    jobs_saved = 0
    emails_sent = 0
    matched_jobs = 0
    skipped_duplicates = 0
    skipped_missing_fields = 0

    for card in cards:
        try:
            link_tag = card.select_one('[class*="_full-link"]')
            title_tag = card.select_one('[class*="_title"]')
            company_tag = card.select_one('[class*="_subtitle"]')
            location_tag = card.select_one('[class*="_metadata"]')

            if not (link_tag and title_tag and company_tag):
                skipped_missing_fields += 1
                continue

            raw_url = link_tag.get("href", "").strip()
            if not raw_url:
                skipped_missing_fields += 1
                continue

            job_url = raw_url.split("?")[0]

            if job_url in sent_urls:
                skipped_duplicates += 1
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

            matched_jobs += 1

            body = (
                f"Job Title: {title}\n"
                f"Company: {company}\n"
                f"Location: {location}\n"
                f"Link: {job_url}"
            )

            # Save first so scraping result is preserved even if email is slow/fails
            try:
                job_sheet.append_row([job_url, title, company, location])
                sent_urls.add(job_url)
                jobs_saved += 1
                logger.info(f"Saved job to sheet: {job_url}")
            except Exception:
                logger.exception(f"Error saving job {job_url}")
                continue

            logger.info(f"Matched users for job {job_url}: {matched_users}")

            for email in matched_users:
                if send_email("🚨 Job Alert", body, email):
                    emails_sent += 1

        except Exception:
            logger.exception("Error processing a job card")
            continue

    result = {
        "status": "success",
        "users_loaded": len(users),
        "titles_found": len(all_titles),
        "cards_found": len(cards),
        "matched_jobs": matched_jobs,
        "jobs_saved": jobs_saved,
        "emails_sent": emails_sent,
        "skipped_duplicates": skipped_duplicates,
        "skipped_missing_fields": skipped_missing_fields,
    }

    logger.info(f"Job processing result: {result}")
    return result


# =========================
# UI
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
    <p>Your email and job titles were saved.</p>
    <p>You will start receiving alerts when matching jobs are found.</p>
    <p><a href="/register">Go back</a></p>
    """


# =========================
# HEALTH CHECK
# =========================
@app.route("/")
def home():
    return "App is running"


# =========================
# RUN JOBS MANUALLY
# =========================
@app.route("/run-jobs")
def run_jobs():
    result = process_jobs()
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
