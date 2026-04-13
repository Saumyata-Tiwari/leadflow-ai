import os
import json
import csv
import io
import re
import socket
import requests
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
from openai import OpenAI
from contextlib import asynccontextmanager
from datetime import date

load_dotenv()

def get_db():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "leadflow_ai")
    )

def init_db():
    db = get_db()
    cursor = db.cursor()

    # ── Core tables ──────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            target_role VARCHAR(255),
            target_location VARCHAR(255),
            company_size_min INT DEFAULT 50,
            company_size_max INT DEFAULT 10000,
            min_salary INT DEFAULT 80000,
            excluded_industries TEXT,
            exclude_intern TINYINT(1) DEFAULT 1,
            exclude_remote TINYINT(1) DEFAULT 1,
            exclude_apprentice TINYINT(1) DEFAULT 1,
            job_date_filter VARCHAR(20) DEFAULT '24h',
            notes TEXT,
            status VARCHAR(50) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            campaign_id INT,
            name VARCHAR(255) NOT NULL,
            industry VARCHAR(255),
            location VARCHAR(255),
            size_range VARCHAR(100),
            website VARCHAR(255),
            job_title VARCHAR(255),
            salary_range VARCHAR(100),
            job_posted VARCHAR(100),
            source VARCHAR(50) DEFAULT 'manual',
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            campaign_id INT,
            company_id INT,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            full_name VARCHAR(255),
            title VARCHAR(255),
            company VARCHAR(255),
            industry VARCHAR(255),
            location VARCHAR(255),
            email VARCHAR(255),
            email_verified TINYINT(1) DEFAULT 0,
            email_source VARCHAR(50) DEFAULT 'unknown',
            confidence_score INT DEFAULT 0,
            linkedin_url VARCHAR(500),
            target_role VARCHAR(255),
            status ENUM('new','emailed','responded','soft_rejection','rejected','unsubscribed','no_response') DEFAULT 'new',
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS emails (
            id INT AUTO_INCREMENT PRIMARY KEY,
            lead_id INT,
            subject TEXT,
            body TEXT,
            email_type VARCHAR(50),
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── NEW: Blacklist table ──────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_blacklist (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            reason VARCHAR(100) DEFAULT 'unsubscribed',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── NEW: Daily email rate limit tracker ──────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_daily_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            campaign_id INT,
            send_date DATE NOT NULL,
            emails_sent INT DEFAULT 0,
            UNIQUE KEY uniq_camp_date (campaign_id, send_date)
        )
    """)

    # ── Safe ALTER TABLE migrations ───────────────────────────────
    migrations = [
        "ALTER TABLE campaigns ADD COLUMN min_salary INT DEFAULT 80000",
        "ALTER TABLE companies ADD COLUMN job_title VARCHAR(255)",
        "ALTER TABLE companies ADD COLUMN salary_range VARCHAR(100)",
        "ALTER TABLE companies ADD COLUMN job_posted VARCHAR(100)",
        "ALTER TABLE companies ADD COLUMN source VARCHAR(50) DEFAULT 'manual'",
        "ALTER TABLE leads ADD COLUMN company_id INT DEFAULT NULL",
        "ALTER TABLE leads ADD COLUMN email_verified TINYINT(1) DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN campaign_id INT DEFAULT NULL",
        # NEW columns
        "ALTER TABLE leads ADD COLUMN email_source VARCHAR(50) DEFAULT 'unknown'",
        "ALTER TABLE leads ADD COLUMN confidence_score INT DEFAULT 0",
        # Expand status ENUM to include unsubscribed + no_response
        "ALTER TABLE leads MODIFY COLUMN status ENUM('new','emailed','responded','soft_rejection','rejected','unsubscribed','no_response') DEFAULT 'new'",
    ]
    for sql in migrations:
        try:
            cursor.execute(sql)
            db.commit()
        except:
            pass

    cursor.close()
    db.close()

@asynccontextmanager
async def lifespan(app):
    init_db()
    yield

app = FastAPI(title="LeadFlow AI", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY")
)

# ── Confidence Score Rules ────────────────────────────────────────
# apollo verified  → 90–95
# prospeo verified → 85–90
# snov             → 75–85
# generated pattern→ max 60 (NEVER auto-send)
CONFIDENCE = {
    "apollo_verified": 92,
    "apollo_unverified": 70,
    "prospeo_linkedin": 88,
    "prospeo_name": 85,
    "snov": 78,
    "generated": 50,   # NEVER auto-send
    "manual": 60,
    "unknown": 0,
}
SEND_THRESHOLD = 70   # confidence_score must be >= this to allow sending

# ── Models ────────────────────────────────────────────────────────

class CampaignCreate(BaseModel):
    name: str
    target_role: str
    target_location: str
    company_size_min: int = 50
    company_size_max: int = 10000
    min_salary: int = 80000
    excluded_industries: Optional[str] = "Staffing,Recruiting,Talent,Consultancy,IT Consultancy,Human Resources Services,Technology Information and Media,University,Non-profit,NGO"
    exclude_intern: bool = True
    exclude_remote: bool = True
    exclude_apprentice: bool = True
    job_date_filter: str = "3days"
    notes: Optional[str] = None

class CompanyCreate(BaseModel):
    campaign_id: int
    name: str
    industry: str = ""
    location: str = ""
    size_range: str = ""
    website: str = ""
    job_title: str = ""
    salary_range: str = ""
    job_posted: str = ""
    source: str = "manual"
    notes: str = ""

class CompanyBulkAdd(BaseModel):
    campaign_id: int
    companies: List[CompanyCreate]

class LeadCreate(BaseModel):
    first_name: str
    last_name: str
    title: str
    company: str
    industry: str = ""
    location: str = ""
    email: str = ""
    linkedin_url: str = ""
    target_role: str = ""
    campaign_id: Optional[int] = None
    company_id: Optional[int] = None
    notes: Optional[str] = None

class EmailRequest(BaseModel):
    lead_id: int
    email_type: str = "cold"

class StatusUpdate(BaseModel):
    status: str

class EmailVerifyRequest(BaseModel):
    linkedin_url: str = ""
    first_name: str = ""
    last_name: str = ""
    company_domain: str = ""

# ── Blacklist Helpers ─────────────────────────────────────────────

def is_blacklisted(email: str) -> bool:
    """Returns True if email is on the blacklist — NEVER contact."""
    if not email:
        return False
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT id FROM email_blacklist WHERE email=%s", (email.lower().strip(),))
    result = cursor.fetchone()
    cursor.close()
    db.close()
    return result is not None

def add_to_blacklist(email: str, reason: str = "unsubscribed"):
    """Add email to permanent blacklist."""
    if not email:
        return
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute(
            "INSERT IGNORE INTO email_blacklist (email, reason) VALUES (%s, %s)",
            (email.lower().strip(), reason)
        )
        db.commit()
    except:
        pass
    cursor.close()
    db.close()

# ── Rate Limit Helpers ────────────────────────────────────────────

DAILY_EMAIL_LIMIT = 40   # max emails per campaign per day

def get_emails_sent_today(campaign_id: int) -> int:
    """How many emails sent today for this campaign."""
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT emails_sent FROM email_daily_log WHERE campaign_id=%s AND send_date=%s",
        (campaign_id, date.today())
    )
    row = cursor.fetchone()
    cursor.close()
    db.close()
    return row[0] if row else 0

def increment_email_count(campaign_id: int):
    """Increment today's email send count for campaign."""
    db = get_db()
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO email_daily_log (campaign_id, send_date, emails_sent)
        VALUES (%s, %s, 1)
        ON DUPLICATE KEY UPDATE emails_sent = emails_sent + 1
    """, (campaign_id, date.today()))
    db.commit()
    cursor.close()
    db.close()

def check_rate_limit(campaign_id: int) -> dict:
    """Returns {allowed: bool, sent: int, limit: int, remaining: int}"""
    sent = get_emails_sent_today(campaign_id)
    allowed = sent < DAILY_EMAIL_LIMIT
    return {
        "allowed": allowed,
        "sent": sent,
        "limit": DAILY_EMAIL_LIMIT,
        "remaining": max(0, DAILY_EMAIL_LIMIT - sent)
    }

# ── Email Safety Gate ─────────────────────────────────────────────

def can_send_email(lead: dict) -> dict:
    """
    Returns {safe: bool, reason: str}
    Checks: blacklist, confidence_score, email_verified, rate limit
    """
    email = lead.get("email", "")
    campaign_id = lead.get("campaign_id")
    status = lead.get("status", "")

    # 1. Must have email
    if not email:
        return {"safe": False, "reason": "No email address found"}

    # 2. Never contact blacklisted / rejected / unsubscribed
    if status in ("rejected", "unsubscribed"):
        return {"safe": False, "reason": f"Lead is permanently {status} — cannot contact"}

    # 3. Blacklist check
    if is_blacklisted(email):
        return {"safe": False, "reason": "Email is on the permanent blacklist (unsubscribed)"}

    # 4. Confidence score gate
    score = lead.get("confidence_score", 0)
    if score < SEND_THRESHOLD:
        return {
            "safe": False,
            "reason": f"Confidence score {score} is below threshold {SEND_THRESHOLD}. Verify email first."
        }

    # 5. Rate limit check (only if campaign_id known)
    if campaign_id:
        rl = check_rate_limit(campaign_id)
        if not rl["allowed"]:
            return {
                "safe": False,
                "reason": f"Daily limit reached ({rl['sent']}/{rl['limit']} emails sent today). Resets tomorrow."
            }

    return {"safe": True, "reason": "OK"}

# ── AI Helpers ────────────────────────────────────────────────────

def get_contact_titles(target_role: str):
    prompt = f"""You are an expert in organizational hierarchy and B2B recruitment outreach.
A US recruitment firm is placing candidates for: "{target_role}"
Return ONLY a JSON array of job titles (max 8) who would make hiring decisions for this role.
Always include CEO, President, and HR/Talent titles.
Include 2-3 senior titles specific to this role's department.
Return ONLY the JSON array, no explanation."""
    try:
        response = client.chat.completions.create(
            model="openrouter/auto",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content.strip()
        if "```" in content:
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        return json.loads(content.strip())
    except:
        return ["CEO", "President", "COO", "VP of HR", "Director of HR",
                "Head of People", "Talent Acquisition Manager", "Recruiting Manager"]

def get_ai_salary(target_role: str):
    prompt = f"""A US recruitment firm is placing candidates for: "{target_role}"
What is the minimum annual salary (in USD, as a number only)?
Return ONLY a number like 85000. Nothing else."""
    try:
        response = client.chat.completions.create(
            model="openrouter/auto",
            messages=[{"role": "user", "content": prompt}]
        )
        salary = int(''.join(filter(str.isdigit, response.choices[0].message.content.strip())))
        return salary if salary > 0 else 80000
    except:
        return 80000

# ── JSearch API ───────────────────────────────────────────────────

def search_jobs_jsearch(role, location, date_posted="week", min_salary=0,
                         excluded_industries="", exclude_intern=True,
                         exclude_remote=True, exclude_apprentice=True, page=1):
    api_key = os.getenv("RAPIDAPI_KEY", "")
    if not api_key:
        return {"error": "RapidAPI key not configured", "jobs": []}

    date_map = {"24h": "today", "3days": "3days", "week": "week", "month": "month"}
    excluded = [i.strip().lower() for i in excluded_industries.split(",") if i.strip()]
    loc_lower = location.lower()
    country = "gb" if any(x in loc_lower for x in ["uk","united kingdom","london","manchester","birmingham"]) else "us"

    try:
        response = requests.get(
            "https://jsearch.p.rapidapi.com/search",
            headers={"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"},
            params={"query": f"{role} {location}", "page": str(page), "num_pages": "5",
                    "date_posted": date_map.get(date_posted, "week"), "country": country},
            timeout=20
        )
        data = response.json()
        if not data.get("data"):
            return {"error": "No results found", "jobs": [], "total": 0}

        jobs = []
        seen_companies = set()
        exclude_keywords = ["staffing","recruiting","recruiter","talent","consulting",
                           "consultancy","consultant","it consulting","human resources",
                           "hr services","university","college","school","non-profit",
                           "nonprofit","ngo","charity","foundation"] + excluded

        for job in data["data"]:
            employer = job.get("employer_name", "").strip()
            if not employer or employer in seen_companies:
                continue

            emp_type = (job.get("employer_company_type") or "").lower()
            job_title_raw = (job.get("job_title") or "").lower()
            employer_lower = employer.lower()
            skip = False

            for kw in exclude_keywords:
                if kw and (kw in emp_type or kw in employer_lower):
                    skip = True; break

            if exclude_intern and ("intern" in job_title_raw or "internship" in job_title_raw):
                skip = True
            if exclude_apprentice and ("apprentice" in job_title_raw):
                skip = True
            if exclude_remote and job.get("job_is_remote"):
                if (job.get("job_min_salary") or 0) < 80000:
                    skip = True
            if skip:
                continue

            min_sal = job.get("job_min_salary") or 0
            max_sal = job.get("job_max_salary") or 0
            if (job.get("job_salary_period") or "").lower() == "hourly":
                min_sal = min_sal * 2080; max_sal = max_sal * 2080

            if min_salary > 0 and min_sal > 0 and min_sal < min_salary:
                continue

            salary_display = f"${min_sal:,.0f} - ${max_sal:,.0f}/yr" if min_sal and max_sal else \
                           f"${min_sal:,.0f}+/yr" if min_sal else "Not disclosed"

            posted = job.get("job_posted_at_datetime_utc", "")
            if posted:
                from datetime import datetime, timezone
                try:
                    posted_dt = datetime.fromisoformat(posted.replace("Z", "+00:00"))
                    days_ago = (datetime.now(timezone.utc) - posted_dt).days
                    posted_str = "Today" if days_ago == 0 else f"{days_ago} days ago"
                except:
                    posted_str = "Recently"
            else:
                posted_str = "Recently"

            seen_companies.add(employer)
            jobs.append({
                "employer_name": employer,
                "job_title": job.get("job_title", role),
                "employer_logo": job.get("employer_logo", ""),
                "location": f"{job.get('job_city','')}, {job.get('job_state','')}".strip(", "),
                "salary_display": salary_display,
                "min_salary": min_sal, "max_salary": max_sal,
                "job_posted": posted_str,
                "is_remote": job.get("job_is_remote", False),
                "apply_link": job.get("job_apply_link", ""),
                "employer_website": job.get("employer_website", ""),
                "company_type": job.get("employer_company_type", ""),
            })

        return {"jobs": jobs, "total": len(jobs), "error": None}
    except Exception as e:
        return {"error": str(e), "jobs": [], "total": 0}

def get_salary_data(role: str, location: str = "United States"):
    api_key = os.getenv("RAPIDAPI_KEY", "")
    if not api_key: return None
    try:
        response = requests.get(
            "https://jsearch.p.rapidapi.com/estimated-salary",
            headers={"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"},
            params={"job_title": role, "location": location, "location_type": "ANY", "years_of_experience": "ALL"},
            timeout=10
        )
        data = response.json()
        if data.get("data") and data["data"]:
            return int(data["data"][0].get("median_salary", 0)) or None
    except:
        pass
    return None

# ── Clearbit ──────────────────────────────────────────────────────

def get_domain_clearbit(company_name: str) -> str:
    try:
        r = requests.get(
            "https://autocomplete.clearbit.com/v1/companies/suggest",
            params={"query": company_name}, timeout=8
        )
        results = r.json()
        if results:
            name_lower = company_name.lower()
            for result in results[:3]:
                domain = result.get("domain", "")
                result_name = result.get("name", "").lower()
                if domain and (name_lower in result_name or result_name in name_lower or
                               name_lower.split()[0] in result_name):
                    return domain
            if results[0].get("domain"):
                return results[0]["domain"]
    except:
        pass
    return ""

def get_domain_variations(company_name: str, website: str = "") -> list:
    if website:
        domain = website.replace("https://","").replace("http://","").replace("www.","").split("/")[0].strip()
        if domain and "." in domain:
            return [domain]
    clearbit = get_domain_clearbit(company_name)
    if clearbit:
        return [clearbit]
    name = company_name.lower()
    v1 = re.sub(r'[^a-z0-9]', '', name)
    v2 = re.sub(r'[^a-z0-9 ]', '', name).strip().replace(" ", "-")
    first = re.sub(r'[^a-z0-9]', '', name.split()[0]) if name.split() else ""
    domains = []
    if v1: domains.append(f"{v1}.com")
    if v2 and v2 != v1: domains.append(f"{v2}.com")
    if first and first != v1: domains.append(f"{first}.com")
    return list(dict.fromkeys(domains))

# ── Prospeo ───────────────────────────────────────────────────────

def find_email_prospeo(linkedin_url="", first_name="", last_name="", company_domain=""):
    api_key = os.getenv("PROSPEO_API_KEY", "")
    if not api_key:
        return {"found": False, "email": "", "confidence_score": 0, "email_source": "unknown"}
    headers = {"Content-Type": "application/json", "X-KEY": api_key}

    if linkedin_url:
        try:
            r = requests.post("https://api.prospeo.io/linkedin-email-finder",
                              headers=headers, json={"url": linkedin_url}, timeout=15)
            d = r.json()
            if d.get("ok") and d.get("response", {}).get("email"):
                return {
                    "found": True,
                    "email": d["response"]["email"],
                    "confidence": d["response"].get("email_confidence_score", 0),
                    "verified": True,
                    "method": "linkedin",
                    "email_source": "prospeo_linkedin",
                    "confidence_score": CONFIDENCE["prospeo_linkedin"],
                    "message": "Email found via LinkedIn"
                }
        except Exception as e:
            print(f"Prospeo LinkedIn error: {e}")

    if first_name and last_name and company_domain:
        try:
            r = requests.post("https://api.prospeo.io/email-finder",
                              headers=headers,
                              json={"first_name": first_name, "last_name": last_name,
                                    "company": company_domain}, timeout=15)
            d = r.json()
            if d.get("ok") and d.get("response", {}).get("email"):
                return {
                    "found": True,
                    "email": d["response"]["email"],
                    "confidence": d["response"].get("email_confidence_score", 0),
                    "verified": True,
                    "method": "name_domain",
                    "email_source": "prospeo_name",
                    "confidence_score": CONFIDENCE["prospeo_name"],
                    "message": "Email found via name+domain"
                }
        except Exception as e:
            print(f"Prospeo name error: {e}")

    return {"found": False, "email": "", "confidence_score": 0, "email_source": "unknown",
            "message": "Email not found via Prospeo."}

# ── Snov.io ───────────────────────────────────────────────────────

def find_email_by_name_snov(first_name: str, last_name: str, domain: str) -> dict:
    client_id = os.getenv("SNOV_CLIENT_ID", "")
    client_secret = os.getenv("SNOV_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        return {"found": False, "email": "", "confidence_score": 0}
    try:
        token_r = requests.post(
            "https://api.snov.io/v1/oauth/access_token",
            data={"grant_type": "client_credentials",
                  "client_id": client_id, "client_secret": client_secret},
            timeout=10
        )
        token = token_r.json().get("access_token")
        if not token:
            return {"found": False, "email": "", "confidence_score": 0}
        r = requests.post(
            "https://api.snov.io/v1/get-emails-by-name",
            data={"access_token": token, "first_name": first_name,
                  "last_name": last_name, "domain": domain, "limit": 5, "type": "personal"},
            timeout=15
        )
        emails = r.json().get("emails", [])
        if emails:
            return {
                "found": True,
                "email": emails[0].get("email", ""),
                "email_source": "snov",
                "confidence_score": CONFIDENCE["snov"],
                "message": "Found via Snov.io"
            }
    except Exception as e:
        print(f"Snov error: {e}")
    return {"found": False, "email": "", "confidence_score": 0}

def enrich_with_email(first: str, last: str, linkedin_url: str, domain: str) -> dict:
    """
    Waterfall: Prospeo LinkedIn → Prospeo name → Snov
    Returns {email, email_source, confidence_score}
    NEVER guesses / generates fake emails.
    """
    # Tier 1: Prospeo via LinkedIn
    if linkedin_url:
        r = find_email_prospeo(linkedin_url=linkedin_url)
        if r.get("found"):
            return {"email": r["email"], "email_source": r["email_source"],
                    "confidence_score": r["confidence_score"]}

    # Tier 2: Prospeo via name + domain
    if first and domain:
        r = find_email_prospeo(first_name=first, last_name=last, company_domain=domain)
        if r.get("found"):
            return {"email": r["email"], "email_source": r["email_source"],
                    "confidence_score": r["confidence_score"]}

    # Tier 3: Snov.io
    if first and domain:
        r = find_email_by_name_snov(first, last, domain)
        if r.get("found"):
            return {"email": r["email"], "email_source": "snov",
                    "confidence_score": CONFIDENCE["snov"]}

    # All failed — do NOT guess email
    return {"email": "", "email_source": "unknown", "confidence_score": 0}

# ── Apollo ────────────────────────────────────────────────────────

def get_apollo_top_people(company_name: str, company_domain: str, titles: list) -> list:
    api_key = os.getenv("APOLLO_API_KEY", "")
    if not api_key: return []
    headers = {"Content-Type": "application/json", "Cache-Control": "no-cache", "X-Api-Key": api_key}
    try:
        org_r = requests.get(
            "https://api.apollo.io/api/v1/organizations/enrich",
            headers=headers, params={"domain": company_domain}, timeout=15
        )
        org_data = org_r.json()
        org_id = org_data.get("organization", {}).get("id", "")
        print(f"Apollo org [{org_r.status_code}]: {org_id} for {company_domain}")
        if not org_id: return []

        top_r = requests.post(
            "https://api.apollo.io/api/v1/mixed_people/organization_top_people",
            headers=headers,
            json={"organization_id": org_id, "person_titles": titles[:10], "page": 1, "per_page": 25},
            timeout=15
        )
        raw = top_r.json().get("people", [])
        print(f"Apollo top people [{top_r.status_code}]: {len(raw)} results")

        if not raw:
            top_r2 = requests.post(
                "https://api.apollo.io/api/v1/mixed_people/organization_top_people",
                headers=headers,
                json={"organization_id": org_id, "page": 1, "per_page": 25},
                timeout=15
            )
            raw = top_r2.json().get("people", [])

        people = []
        seen = set()
        for p in raw:
            name = p.get("name", "")
            if not name or name in seen: continue
            seen.add(name)
            email = p.get("email", "")
            if email and "?" in email: email = ""

            # Assign confidence score based on whether Apollo gave us an email
            email_source = "apollo_verified" if email else "apollo_unverified"
            confidence = CONFIDENCE[email_source]

            name_parts = name.strip().split()
            people.append({
                "first_name": name_parts[0] if name_parts else "",
                "last_name": " ".join(name_parts[1:]) if len(name_parts) > 1 else "",
                "full_name": name,
                "title": p.get("title", ""),
                "linkedin_url": p.get("linkedin_url", ""),
                "email": email,
                "email_source": email_source,
                "confidence_score": confidence,
                "location": f"{p.get('city','')}, {p.get('state','')}".strip(", "),
                "source": "apollo"
            })
        return people[:15]
    except Exception as e:
        print(f"Apollo error: {e}")
        return []

def enrich_email_apollo(first_name: str, last_name: str,
                         company_domain: str, linkedin_url: str = "") -> dict:
    """Apollo people/match — returns {email, email_source, confidence_score}"""
    api_key = os.getenv("APOLLO_API_KEY", "")
    if not api_key: return {"email": "", "email_source": "unknown", "confidence_score": 0}
    try:
        payload = {"first_name": first_name, "last_name": last_name,
                   "organization_domain": company_domain}
        if linkedin_url: payload["linkedin_url"] = linkedin_url
        r = requests.post(
            "https://api.apollo.io/api/v1/people/match",
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache", "X-Api-Key": api_key},
            json=payload, timeout=15
        )
        email = r.json().get("person", {}).get("email", "")
        if email and "?" not in email:
            return {"email": email, "email_source": "apollo_verified",
                    "confidence_score": CONFIDENCE["apollo_verified"]}
    except Exception as e:
        print(f"Apollo match error: {e}")
    return {"email": "", "email_source": "unknown", "confidence_score": 0}

# ── LinkedIn Scraper (RapidAPI Fallback) ──────────────────────────

def search_people_linkedin_scraper(company_name: str, title: str) -> list:
    api_key = os.getenv("RAPIDAPI_KEY", "")
    if not api_key: return []
    try:
        keyword = f'"{title} at {company_name}"'
        r = requests.get(
            "https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/search/people",
            headers={"x-rapidapi-key": api_key,
                     "x-rapidapi-host": "fresh-linkedin-scraper-api.p.rapidapi.com"},
            params={"keyword": keyword, "page": "1"}, timeout=15
        )
        data = r.json()
        if not data.get("success") or not data.get("data"): return []

        people = []
        for p in data["data"]:
            person_title = p.get("title", "")
            company_words = [w.lower() for w in company_name.split() if len(w) > 2]
            company_match = any(w in person_title.lower() for w in company_words)
            title_words = [w for w in title.lower().replace("chief","").replace("officer","").split() if len(w) > 3]
            title_match = any(w in person_title.lower() for w in title_words) if title_words else False
            if not (company_match and (title_match or title.lower()[:4] in person_title.lower()[:20])):
                continue
            full_name = p.get("full_name", "")
            if not full_name: continue
            name_parts = full_name.strip().split()
            people.append({
                "first_name": name_parts[0] if name_parts else "",
                "last_name": " ".join(name_parts[1:]) if len(name_parts) > 1 else "",
                "full_name": full_name,
                "title": person_title,
                "linkedin_url": p.get("url", ""),
                "email": "",
                "email_source": "unknown",
                "confidence_score": 0,
                "location": p.get("location", ""),
                "avatar": p.get("avatar", [{}])[0].get("url", "") if p.get("avatar") else "",
                "source": "linkedin_scraper"
            })
        return people[:3]
    except Exception as e:
        print(f"LinkedIn scraper error: {e}")
        return []

# ── Email Pattern Guesser (LOW CONFIDENCE — NEVER AUTO-SEND) ──────

def guess_email_patterns(first_name: str, last_name: str, domain: str) -> list:
    f = re.sub(r"[^a-z]", "", first_name.lower())
    l = re.sub(r"[^a-z]", "", last_name.lower())
    f1 = f[0] if f else ""
    return [
        f"{f}.{l}@{domain}", f"{f}{l}@{domain}", f"{f1}{l}@{domain}",
        f"{f1}.{l}@{domain}", f"{f}_{l}@{domain}", f"{f}@{domain}",
    ]

# ── Routes ────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

# Campaigns
@app.post("/campaigns")
async def create_campaign(campaign: CampaignCreate):
    db = get_db(); cursor = db.cursor()
    cursor.execute("""INSERT INTO campaigns (name,target_role,target_location,company_size_min,
        company_size_max,min_salary,excluded_industries,exclude_intern,exclude_remote,
        exclude_apprentice,job_date_filter,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (campaign.name,campaign.target_role,campaign.target_location,campaign.company_size_min,
         campaign.company_size_max,campaign.min_salary,campaign.excluded_industries,
         campaign.exclude_intern,campaign.exclude_remote,campaign.exclude_apprentice,
         campaign.job_date_filter,campaign.notes))
    db.commit(); cid = cursor.lastrowid; cursor.close(); db.close()
    return {"message": "Campaign created!", "id": cid}

@app.get("/campaigns")
async def get_campaigns():
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("""SELECT c.*,COUNT(DISTINCT comp.id) as total_companies,
        COUNT(DISTINCT l.id) as total_leads,
        SUM(CASE WHEN l.status='emailed' THEN 1 ELSE 0 END) as emailed,
        SUM(CASE WHEN l.status='responded' THEN 1 ELSE 0 END) as responded
        FROM campaigns c LEFT JOIN companies comp ON comp.campaign_id=c.id
        LEFT JOIN leads l ON l.campaign_id=c.id GROUP BY c.id ORDER BY c.created_at DESC""")
    result = cursor.fetchall(); cursor.close(); db.close()
    return result

@app.get("/campaigns/{campaign_id}")
async def get_campaign(campaign_id: int):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
    c = cursor.fetchone(); cursor.close(); db.close()
    if not c: raise HTTPException(404, "Campaign not found")
    return c

@app.put("/campaigns/{campaign_id}")
async def update_campaign(campaign_id: int, campaign: CampaignCreate):
    db = get_db(); cursor = db.cursor()
    cursor.execute("""UPDATE campaigns SET name=%s,target_role=%s,target_location=%s,
        company_size_min=%s,company_size_max=%s,min_salary=%s,excluded_industries=%s,
        exclude_intern=%s,exclude_remote=%s,exclude_apprentice=%s,job_date_filter=%s,
        notes=%s WHERE id=%s""",
        (campaign.name,campaign.target_role,campaign.target_location,campaign.company_size_min,
         campaign.company_size_max,campaign.min_salary,campaign.excluded_industries,
         campaign.exclude_intern,campaign.exclude_remote,campaign.exclude_apprentice,
         campaign.job_date_filter,campaign.notes,campaign_id))
    db.commit(); cursor.close(); db.close()
    return {"message": "Campaign updated!"}

@app.delete("/campaigns/{campaign_id}")
async def delete_campaign(campaign_id: int):
    db = get_db(); cursor = db.cursor()
    cursor.execute("DELETE FROM campaigns WHERE id=%s", (campaign_id,))
    db.commit(); cursor.close(); db.close()
    return {"message": "Campaign deleted"}

# Job Search
@app.get("/search-jobs/{campaign_id}")
async def search_jobs(campaign_id: int, page: int = 1):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
    c = cursor.fetchone(); cursor.close(); db.close()
    if not c: raise HTTPException(404, "Campaign not found")
    return search_jobs_jsearch(c["target_role"], c["target_location"],
        c.get("job_date_filter","week"), c.get("min_salary",0),
        c.get("excluded_industries",""), bool(c.get("exclude_intern",True)),
        bool(c.get("exclude_remote",True)), bool(c.get("exclude_apprentice",True)), page)

@app.get("/salary-suggestion")
async def salary_suggestion(role: str, location: str = "United States"):
    sal = get_salary_data(role, location)
    if sal:
        min_sal = int(sal * 0.8)
        return {"suggested_salary": min_sal, "median_salary": sal, "source": "JSearch",
                "message": f"Market median ${sal:,}/yr. Suggested min: ${min_sal:,}/yr"}
    ai = get_ai_salary(role)
    return {"suggested_salary": ai, "median_salary": ai, "source": "AI",
            "message": f"AI suggested: ${ai:,}/yr"}

# Companies
@app.post("/companies")
async def create_company(company: CompanyCreate):
    db = get_db(); cursor = db.cursor()
    cursor.execute("""INSERT INTO companies (campaign_id,name,industry,location,size_range,
        website,job_title,salary_range,job_posted,source,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (company.campaign_id,company.name,company.industry,company.location,company.size_range,
         company.website,company.job_title,company.salary_range,company.job_posted,company.source,company.notes))
    db.commit(); cid = cursor.lastrowid; cursor.close(); db.close()
    return {"message": "Company added!", "id": cid}

@app.post("/companies/bulk")
async def bulk_add_companies(data: CompanyBulkAdd):
    db = get_db(); cursor = db.cursor(); added = 0
    for co in data.companies:
        cursor.execute("""INSERT INTO companies (campaign_id,name,industry,location,size_range,
            website,job_title,salary_range,job_posted,source,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (data.campaign_id,co.name,co.industry,co.location,co.size_range,
             co.website,co.job_title,co.salary_range,co.job_posted,co.source,co.notes))
        added += 1
    db.commit(); cursor.close(); db.close()
    return {"message": f"{added} companies added!", "added": added}

@app.get("/companies")
async def get_companies(campaign_id: Optional[int] = None):
    db = get_db(); cursor = db.cursor(dictionary=True)
    if campaign_id:
        cursor.execute("""SELECT comp.*,COUNT(l.id) as total_contacts,
            SUM(CASE WHEN l.status='responded' THEN 1 ELSE 0 END) as responded,
            SUM(CASE WHEN l.status='emailed' THEN 1 ELSE 0 END) as emailed
            FROM companies comp LEFT JOIN leads l ON l.company_id=comp.id
            WHERE comp.campaign_id=%s GROUP BY comp.id ORDER BY comp.created_at DESC""", (campaign_id,))
    else:
        cursor.execute("""SELECT comp.*,c.name as campaign_name,COUNT(l.id) as total_contacts
            FROM companies comp LEFT JOIN campaigns c ON c.id=comp.campaign_id
            LEFT JOIN leads l ON l.company_id=comp.id GROUP BY comp.id ORDER BY comp.created_at DESC""")
    result = cursor.fetchall(); cursor.close(); db.close()
    return result

@app.get("/lookup-domain")
async def lookup_domain(company_name: str, website: str = ""):
    if website:
        domain = website.replace("https://","").replace("http://","").replace("www.","").split("/")[0].strip()
        if domain and "." in domain:
            return {"domain": domain, "source": "website", "confidence": "high"}
    cb = get_domain_clearbit(company_name)
    if cb: return {"domain": cb, "source": "clearbit", "confidence": "high"}
    clean = re.sub(r'[^a-z0-9]', '', company_name.lower())
    return {"domain": f"{clean}.com", "source": "guess", "confidence": "low"}

@app.get("/find-email-by-name")
async def find_email_by_name(first_name: str, last_name: str, domain: str):
    r = find_email_by_name_snov(first_name, last_name, domain)
    if r["found"]: return r
    p = find_email_prospeo(first_name=first_name, last_name=last_name, company_domain=domain)
    if p["found"]: return p
    return {"found": False, "email": "", "confidence_score": 0,
            "message": "Email not found. Enter manually."}

# Auto-Find Contacts
@app.post("/companies/{company_id}/auto-find-contacts")
async def auto_find_contacts(company_id: int, domain_override: str = ""):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM companies WHERE id=%s", (company_id,))
    company = cursor.fetchone()
    if not company: raise HTTPException(404, "Company not found")
    cursor.execute("SELECT * FROM campaigns WHERE id=%s", (company["campaign_id"],))
    campaign = cursor.fetchone()
    target_role = campaign["target_role"] if campaign else ""
    cursor.close(); db.close()

    decision_titles = get_contact_titles(target_role) if target_role else [
        "CEO","President","VP of HR","Head of People","Talent Acquisition Manager","CTO","CFO"]

    domain = domain_override or (get_domain_variations(company["name"], company.get("website","")) or [""])[0]
    company_name = company["name"]

    # Tier 1: Apollo
    all_people = get_apollo_top_people(company_name, domain, decision_titles)

    # Tier 2: LinkedIn Scraper fallback
    if not all_people:
        seen_names = set()
        for title in decision_titles[:5]:
            for p in search_people_linkedin_scraper(company_name, title):
                if p["full_name"] not in seen_names:
                    seen_names.add(p["full_name"])
                    all_people.append(p)

    # Tier 3: Manual search links — graceful fallback (NOT "enter data manually")
    if not all_people:
        rows = []
        for title in decision_titles[:6]:
            q1 = requests.utils.quote(f'"{title}" "{company_name}"')
            q2 = requests.utils.quote(f'intitle:"{title}" "{company_name}"')
            rows.append({
                "title": title,
                "linkedin_url": f"https://www.linkedin.com/search/results/people/?keywords={q1}&origin=GLOBAL_SEARCH_HEADER",
                "google_url": f"https://www.google.com/search?q=site:linkedin.com/in+{q2}"
            })
        return {"success": False, "manual_mode": True, "contacts_added": 0,
                "company_name": company_name, "domain": domain,
                "decision_titles": decision_titles[:6], "rows": rows,
                "message": "No automated results. Use search links below."}

    # Save contacts + enrich emails through waterfall
    db = get_db(); cursor = db.cursor()
    added = 0; skipped = 0; enriched = []

    for person in all_people:
        cursor.execute("SELECT id FROM leads WHERE full_name=%s AND company_id=%s",
                       (person["full_name"], company_id))
        if cursor.fetchone():
            skipped += 1; enriched.append({**person, "email": "exists", "saved": False}); continue

        email = person.get("email", "")
        email_source = person.get("email_source", "unknown")
        confidence_score = person.get("confidence_score", 0)

        # Enrich email if not already found by Apollo
        if not email:
            enriched_data = enrich_email_apollo(
                person["first_name"], person["last_name"],
                domain, person.get("linkedin_url","")
            )
            if not enriched_data["email"]:
                enriched_data = enrich_with_email(
                    person["first_name"], person["last_name"],
                    person.get("linkedin_url",""), domain
                )
            email = enriched_data["email"]
            email_source = enriched_data["email_source"]
            confidence_score = enriched_data["confidence_score"]

        cursor.execute("""INSERT INTO leads
            (first_name,last_name,full_name,title,company,industry,location,
             email,email_verified,email_source,confidence_score,
             linkedin_url,target_role,campaign_id,company_id,notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (person["first_name"],person["last_name"],person["full_name"],person["title"],
             company_name,company.get("industry",""),
             person.get("location","") or company.get("location",""),
             email, 1 if email else 0,
             email_source, confidence_score,
             person.get("linkedin_url",""), target_role,
             company["campaign_id"],company_id,"Auto-found via Apollo/LinkedIn"))
        added += 1
        enriched.append({**person, "email": email, "email_source": email_source,
                         "confidence_score": confidence_score, "saved": True})

    db.commit(); cursor.close(); db.close()
    return {"success": True, "contacts_added": added, "contacts_skipped": skipped,
            "total_found": len(all_people), "people": enriched,
            "message": f"Found {len(all_people)} decision makers at {company_name}, saved {added}"}

@app.delete("/companies/{company_id}")
async def delete_company(company_id: int):
    db = get_db(); cursor = db.cursor()
    cursor.execute("DELETE FROM leads WHERE company_id=%s", (company_id,))
    cursor.execute("DELETE FROM companies WHERE id=%s", (company_id,))
    db.commit(); cursor.close(); db.close()
    return {"message": "Company deleted"}

# Leads
@app.post("/leads")
async def create_lead(lead: LeadCreate):
    db = get_db(); cursor = db.cursor()
    # Manual leads get confidence 60 (manual entry) — not auto-sendable
    cursor.execute("""INSERT INTO leads
        (first_name,last_name,full_name,title,company,industry,location,
         email,email_source,confidence_score,linkedin_url,target_role,campaign_id,company_id,notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (lead.first_name,lead.last_name,f"{lead.first_name} {lead.last_name}",
         lead.title,lead.company,lead.industry,lead.location,lead.email,
         "manual", CONFIDENCE["manual"],
         lead.linkedin_url,lead.target_role,lead.campaign_id,lead.company_id,lead.notes))
    db.commit(); lid = cursor.lastrowid; cursor.close(); db.close()
    return {"message": "Lead added!", "id": lid}

@app.get("/leads/stats")
async def get_stats():
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("""SELECT COUNT(*) as total_leads,
        SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as new_leads,
        SUM(CASE WHEN status='emailed' THEN 1 ELSE 0 END) as emailed,
        SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END) as responded,
        SUM(CASE WHEN status='soft_rejection' THEN 1 ELSE 0 END) as soft_rejection,
        SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as rejected,
        SUM(CASE WHEN status='unsubscribed' THEN 1 ELSE 0 END) as unsubscribed,
        SUM(CASE WHEN status='no_response' THEN 1 ELSE 0 END) as no_response
        FROM leads""")
    r = cursor.fetchone(); cursor.close(); db.close()
    return r

@app.get("/leads")
async def get_leads(campaign_id: Optional[int] = None, company_id: Optional[int] = None):
    db = get_db(); cursor = db.cursor(dictionary=True)
    q = """SELECT l.*,c.name as campaign_name FROM leads l
        LEFT JOIN campaigns c ON c.id=l.campaign_id
        LEFT JOIN companies comp ON comp.id=l.company_id WHERE 1=1"""
    params = []
    if campaign_id: q += " AND l.campaign_id=%s"; params.append(campaign_id)
    if company_id: q += " AND l.company_id=%s"; params.append(company_id)
    q += " ORDER BY l.created_at DESC"
    cursor.execute(q, params); result = cursor.fetchall(); cursor.close(); db.close()
    return result

@app.get("/leads/{lead_id}")
async def get_lead(lead_id: int):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s", (lead_id,))
    l = cursor.fetchone(); cursor.close(); db.close()
    if not l: raise HTTPException(404, "Lead not found")
    return l

@app.put("/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, body: StatusUpdate):
    new_status = body.status

    # Load lead for safety checks
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s", (lead_id,))
    lead = cursor.fetchone()
    cursor.close(); db.close()

    if not lead:
        raise HTTPException(404, "Lead not found")

    # Safety: never change status of permanently blacklisted leads
    if lead.get("status") in ("rejected", "unsubscribed") and new_status not in ("rejected", "unsubscribed"):
        raise HTTPException(400, f"Lead is permanently {lead['status']} — status cannot be changed")

    # If marking as unsubscribed → add to blacklist
    if new_status == "unsubscribed" and lead.get("email"):
        add_to_blacklist(lead["email"], reason="unsubscribed")

    db = get_db(); cursor = db.cursor()
    cursor.execute("UPDATE leads SET status=%s WHERE id=%s", (new_status, lead_id))
    db.commit(); cursor.close(); db.close()
    return {"message": f"Status: {new_status}"}

@app.put("/leads/{lead_id}")
async def update_lead(lead_id: int, body: dict):
    db = get_db(); cursor = db.cursor()
    allowed = ['first_name','last_name','title','email','linkedin_url','location','target_role','notes']
    fields = [f"{f}=%s" for f in allowed if f in body]
    values = [body[f] for f in allowed if f in body]
    if not fields: raise HTTPException(400, "No fields to update")
    if 'first_name' in body or 'last_name' in body:
        fields.append("full_name=%s")
        values.append(f"{body.get('first_name','')} {body.get('last_name','')}".strip())
    # If email updated manually, reset confidence to manual level
    if 'email' in body:
        fields.append("email_source=%s")
        values.append("manual")
        fields.append("confidence_score=%s")
        values.append(CONFIDENCE["manual"])
        fields.append("email_verified=%s")
        values.append(0)
    values.append(lead_id)
    cursor.execute(f"UPDATE leads SET {', '.join(fields)} WHERE id=%s", values)
    db.commit(); cursor.close(); db.close()
    return {"message": "Lead updated"}

@app.delete("/leads/{lead_id}")
async def delete_lead(lead_id: int):
    db = get_db(); cursor = db.cursor()
    cursor.execute("DELETE FROM emails WHERE lead_id=%s", (lead_id,))
    cursor.execute("DELETE FROM leads WHERE id=%s", (lead_id,))
    db.commit(); cursor.close(); db.close()
    return {"message": "Lead deleted"}

# Email Verification
@app.post("/verify-email")
async def verify_email(request: EmailVerifyRequest):
    return find_email_prospeo(request.linkedin_url, request.first_name,
                               request.last_name, request.company_domain)

@app.post("/leads/{lead_id}/verify-email")
async def verify_and_save_email(lead_id: int):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s", (lead_id,))
    lead = cursor.fetchone()
    if not lead: raise HTTPException(404, "Lead not found")

    result = find_email_prospeo(lead.get("linkedin_url",""), lead.get("first_name",""), lead.get("last_name",""))
    if result["found"]:
        email_source = result.get("email_source", "prospeo_linkedin")
        confidence = result.get("confidence_score", CONFIDENCE["prospeo_linkedin"])
        cursor.execute("""UPDATE leads SET email=%s,email_verified=1,
            email_source=%s,confidence_score=%s WHERE id=%s""",
            (result["email"], email_source, confidence, lead_id))
        db.commit()

    cursor.close(); db.close()
    return result

# Email Safety Check endpoint (use before sending)
@app.get("/leads/{lead_id}/can-send")
async def can_send_check(lead_id: int):
    """Check if this lead is safe to email."""
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s", (lead_id,))
    lead = cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404, "Lead not found")
    result = can_send_email(lead)
    result["confidence_score"] = lead.get("confidence_score", 0)
    result["email_source"] = lead.get("email_source", "unknown")
    result["threshold"] = SEND_THRESHOLD
    return result

# Rate limit status
@app.get("/campaigns/{campaign_id}/rate-limit")
async def get_rate_limit(campaign_id: int):
    return check_rate_limit(campaign_id)

# Email Generation (with safety gate)
@app.post("/generate-email")
async def generate_email(request: EmailRequest):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s", (request.lead_id,))
    lead = cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404, "Lead not found")

    # ── EMAIL SAFETY GATE ─────────────────────────────────────────
    if request.email_type in ("cold", "followup1", "followup2"):
        safety = can_send_email(lead)
        if not safety["safe"]:
            raise HTTPException(400, f"Email blocked: {safety['reason']}")

    prompts = {
        "cold": f"""Write a short professional B2B cold email for a US/UK recruitment firm.
Lead: {lead['first_name']} {lead['last_name']}, {lead['title']} at {lead['company']}
Industry: {lead['industry']} | Location: {lead['location']} | Hiring: {lead['target_role']}
Rules: Max 75 words body. Mention company. Focus on pre-vetted US candidates. Soft CTA 15-min call. Industry-tailored.
Return ONLY:
Subject: [subject]
Body: [body]""",
        "followup1": f"""Write Day 3 follow-up for recruitment firm.
Lead: {lead['first_name']} at {lead['company']} | Industry: {lead['industry']}
Rules: Max 50 words. Reference previous email. Soft CTA only.
Return ONLY: Subject: [subject]\nBody: [body]""",
        "followup2": f"""Write Day 7 final follow-up. Lead: {lead['first_name']} at {lead['company']}
Rules: Max 40 words. Brief, respectful, leave door open.
Return ONLY: Subject: [subject]\nBody: [body]""",
        "soft_rejection": f"""Write warm soft rejection response. Lead: {lead['first_name']} at {lead['company']}
Rules: Max 40 words. Warm, leave door open.
Return ONLY: Subject: [subject]\nBody: [body]""",
        "rejection": f"""Write graceful rejection response. Lead: {lead['first_name']} at {lead['company']}
Rules: Max 30 words. Professional, leave door open for referrals.
Return ONLY: Subject: [subject]\nBody: [body]"""
    }

    if request.email_type not in prompts:
        raise HTTPException(400, "Invalid email type")

    response = client.chat.completions.create(
        model="openrouter/auto",
        messages=[{"role": "user", "content": prompts[request.email_type]}]
    )
    content = response.choices[0].message.content.strip()
    lines = content.split("\n")
    subject = ""; body_lines = []; body_started = False
    for line in lines:
        if line.startswith("Subject:"): subject = line.replace("Subject:","").strip()
        elif line.startswith("Body:"): body_started = True
        elif body_started: body_lines.append(line)
    body = "\n".join(body_lines).strip()

    db = get_db(); cursor = db.cursor()
    cursor.execute("INSERT INTO emails (lead_id,subject,body,email_type) VALUES (%s,%s,%s,%s)",
                   (request.lead_id, subject, body, request.email_type))

    if request.email_type == "cold":
        cursor.execute("UPDATE leads SET status='emailed' WHERE id=%s", (request.lead_id,))
        # Increment daily rate limit counter
        if lead.get("campaign_id"):
            increment_email_count(lead["campaign_id"])

    db.commit(); cursor.close(); db.close()
    return {"subject": subject, "body": body, "email_type": request.email_type,
            "confidence_score": lead.get("confidence_score", 0),
            "email_source": lead.get("email_source", "unknown")}

# Blacklist
@app.post("/blacklist")
async def add_blacklist(email: str, reason: str = "unsubscribed"):
    add_to_blacklist(email, reason)
    return {"message": f"{email} added to blacklist"}

@app.get("/blacklist")
async def get_blacklist():
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM email_blacklist ORDER BY added_at DESC")
    result = cursor.fetchall(); cursor.close(); db.close()
    return result

@app.delete("/blacklist/{email}")
async def remove_from_blacklist(email: str):
    db = get_db(); cursor = db.cursor()
    cursor.execute("DELETE FROM email_blacklist WHERE email=%s", (email,))
    db.commit(); cursor.close(); db.close()
    return {"message": f"{email} removed from blacklist"}

# Role Hierarchy
@app.get("/role-hierarchy/{target_role}")
async def get_role_hierarchy(target_role: str):
    return {"target_role": target_role, "contact_titles": get_contact_titles(target_role)}

# Global Company Search
@app.get("/search-companies")
async def search_companies_global(q: str = ""):
    if not q or len(q) < 2: return []
    results = []; seen = set()
    db = get_db(); cursor = db.cursor(dictionary=True)
    like_q = f"%{q}%"
    cursor.execute("""SELECT c.id,c.name,c.industry,c.location,c.website,c.campaign_id,
        c.salary_range,camp.name as campaign_name,camp.target_role,COUNT(l.id) as total_contacts
        FROM companies c LEFT JOIN campaigns camp ON c.campaign_id=camp.id
        LEFT JOIN leads l ON l.company_id=c.id
        WHERE c.name LIKE %s OR c.industry LIKE %s OR c.location LIKE %s
        GROUP BY c.id ORDER BY c.name ASC LIMIT 10""", (like_q,like_q,like_q))
    for co in cursor.fetchall():
        seen.add(co["name"].lower())
        results.append({**co, "saved": True,
                        "logo": f"https://logo.clearbit.com/{co.get('website','')}" if co.get('website') else ""})
    cursor.close(); db.close()

    try:
        r = requests.get("https://autocomplete.clearbit.com/v1/companies/suggest",
                         params={"query": q}, timeout=8)
        for co in r.json()[:8]:
            name = co.get("name","")
            if not name or name.lower() in seen: continue
            seen.add(name.lower())
            domain = co.get("domain","")
            results.append({"id": None, "name": name, "domain": domain, "industry": "",
                            "location": "", "website": domain, "campaign_id": None,
                            "campaign_name": None, "total_contacts": 0, "saved": False,
                            "logo": co.get("logo","")})
    except: pass
    return results[:15]

# Analytics
@app.get("/analytics")
async def get_analytics():
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("""SELECT
        COUNT(*) as total_leads,
        SUM(CASE WHEN status='emailed' THEN 1 ELSE 0 END) as total_sent,
        SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END) as total_replied,
        SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END) as positive_responses,
        SUM(CASE WHEN status='soft_rejection' THEN 1 ELSE 0 END) as soft_rejections,
        SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as hard_rejections,
        SUM(CASE WHEN status='unsubscribed' THEN 1 ELSE 0 END) as unsubscribes,
        SUM(CASE WHEN status='no_response' THEN 1 ELSE 0 END) as no_response,
        SUM(CASE WHEN email_verified=1 THEN 1 ELSE 0 END) as verified_emails,
        AVG(confidence_score) as avg_confidence
        FROM leads""")
    stats = cursor.fetchone()
    sent = stats.get("total_sent") or 0
    replied = stats.get("total_replied") or 0
    stats["response_rate"] = round((replied / sent * 100), 1) if sent > 0 else 0
    cursor.close(); db.close()
    return stats

# CSV Export
@app.get("/leads/export/csv")
async def export_csv(campaign_id: Optional[int] = None, verified_only: bool = False):
    db = get_db(); cursor = db.cursor(dictionary=True)
    q = """SELECT c.name as campaign,comp.name as company,comp.salary_range,
        l.first_name,l.last_name,l.title,l.target_role,l.linkedin_url,l.email,
        l.email_source,l.confidence_score,
        CASE WHEN l.email_verified=1 THEN 'Verified' ELSE 'Unverified' END as email_status,
        l.location,l.status,l.created_at FROM leads l
        LEFT JOIN campaigns c ON c.id=l.campaign_id
        LEFT JOIN companies comp ON comp.id=l.company_id WHERE 1=1"""
    params = []
    if campaign_id: q += " AND l.campaign_id=%s"; params.append(campaign_id)
    if verified_only: q += " AND l.email_verified=1"
    q += " ORDER BY l.created_at DESC"
    cursor.execute(q, params); leads = cursor.fetchall(); cursor.close(); db.close()
    output = io.StringIO()
    if leads:
        writer = csv.DictWriter(output, fieldnames=leads[0].keys())
        writer.writeheader()
        for lead in leads:
            if lead.get("created_at"): lead["created_at"] = str(lead["created_at"])
            writer.writerow(lead)
    output.seek(0)
    filename = f"leadflow_export_{date.today()}.csv"
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"})

# Debug endpoints
@app.get("/debug-apollo")
async def debug_apollo(domain: str = "allstate.com", company: str = "Allstate"):
    api_key = os.getenv("APOLLO_API_KEY","")
    if not api_key: return {"error": "APOLLO_API_KEY not set"}
    headers = {"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key":api_key}
    result = {"domain": domain}
    r1 = requests.get("https://api.apollo.io/api/v1/organizations/enrich",
                       headers=headers, params={"domain": domain}, timeout=15)
    d1 = r1.json(); org = d1.get("organization",{}); org_id = org.get("id","")
    result["step1_org_enrich"] = {"status": r1.status_code, "org_id": org_id,
                                   "org_name": org.get("name",""), "error": d1.get("error","")}
    if not org_id: return result
    r2 = requests.post("https://api.apollo.io/api/v1/mixed_people/organization_top_people",
                        headers=headers, json={"organization_id": org_id,"page":1,"per_page":10}, timeout=15)
    d2 = r2.json(); raw = d2.get("people",[])
    result["step2_top_people"] = {"status": r2.status_code, "count": len(raw),
                                   "error": d2.get("error",""),
                                   "sample": [{"name":p.get("name"),"title":p.get("title")} for p in raw[:5]]}

    # Also test people/search (Basic plan)
    r3 = requests.post("https://api.apollo.io/api/v1/mixed_people/search",
                        headers=headers,
                        json={"q_organization_domains": domain, "page": 1, "per_page": 5,
                              "person_titles": ["CEO","VP of HR","President"]},
                        timeout=15)
    d3 = r3.json(); raw3 = d3.get("people",[])
    result["step3_people_search_basic"] = {
        "status": r3.status_code, "count": len(raw3),
        "error": d3.get("error",""),
        "sample": [{"name":p.get("name"),"title":p.get("title"),"email":p.get("email","")} for p in raw3[:5]]
    }
    return result

@app.get("/guess-emails")
async def guess_emails_endpoint(first_name: str, last_name: str, domain: str):
    patterns = guess_email_patterns(first_name, last_name, domain)
    snov = find_email_by_name_snov(first_name, last_name, domain)
    return {"patterns": patterns,
            "warning": "These are guesses — confidence_score=50, DO NOT auto-send",
            "confirmed": snov.get("email","") if snov.get("found") else ""}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)