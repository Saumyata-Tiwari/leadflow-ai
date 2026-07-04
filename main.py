import os
import json
import csv
import io
import re
import socket
import requests
import smtplib
import hashlib
import secrets
import imaplib
import email as email_lib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import mysql.connector
from fastapi import FastAPI, HTTPException, Cookie, Response, File, UploadFile, Form, BackgroundTasks, Body, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
from openai import OpenAI
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta

load_dotenv()
def get_db():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "leadflow_ai")
    )
def send_via_resend(to_email: str, subject: str, body: str, from_name: str = "LeadFlow AI") -> dict:
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        return {"success": False, "error": "RESEND_API_KEY not configured"}
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"from": f"{from_name} <onboarding@resend.dev>", "to": [to_email], "subject": subject, "text": body},
            timeout=15
        )
        data = r.json()
        if r.status_code in (200, 201) and data.get("id"):
            print(f"[Resend] ✓ Sent to {to_email}")
            return {"success": True, "message_id": data["id"]}
        print(f"[Resend] Error: {data}")
        return {"success": False, "error": str(data)}
    except Exception as e:
        print(f"[Resend] Exception: {e}")
        return {"success": False, "error": str(e)}
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "leadflow_ai")
    )


    # Add new tracking columns if they don't exist
    db=get_db(); c=db.cursor()
    new_cols = [
        "ALTER TABLE leads ADD COLUMN open_count INT DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN click_count INT DEFAULT 0", 
        "ALTER TABLE leads ADD COLUMN last_opened_at TIMESTAMP NULL",
        "ALTER TABLE leads ADD COLUMN last_clicked_at TIMESTAMP NULL",
        "ALTER TABLE leads ADD COLUMN email_opened TINYINT(1) DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN email_clicked TINYINT(1) DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN email_bounced TINYINT(1) DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN ab_variant VARCHAR(1) DEFAULT NULL",
        "ALTER TABLE campaigns ADD COLUMN soft_rejection_days INT DEFAULT 30",
        "ALTER TABLE campaigns ADD COLUMN hard_rejection_days INT DEFAULT 90",
        "CREATE TABLE IF NOT EXISTS warmup_settings (id INT AUTO_INCREMENT PRIMARY KEY, warmup_enabled TINYINT(1) DEFAULT 0, daily_limit INT DEFAULT 50, warmup_start_date DATE, warmup_week INT DEFAULT 0)",
    ]
    for sql in new_cols:
        try: c.execute(sql); db.commit()
        except: pass
    c.close(); db.close()

def init_db():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(80) UNIQUE NOT NULL,
        email VARCHAR(120) UNIQUE NOT NULL,
        password_hash VARCHAR(64) NOT NULL,
        session_token VARCHAR(64),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS campaigns (
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
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS companies (
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
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS leads (
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
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS emails (
        id INT AUTO_INCREMENT PRIMARY KEY,
        lead_id INT,
        subject TEXT,
        body TEXT,
        email_type VARCHAR(50),
        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS email_blacklist (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        reason VARCHAR(100) DEFAULT 'unsubscribed',
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS email_daily_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        campaign_id INT,
        send_date DATE NOT NULL,
        emails_sent INT DEFAULT 0,
        UNIQUE KEY uniq_camp_date (campaign_id, send_date)
    )""")
    try:
        cursor.execute("ALTER TABLE user_email_settings ADD COLUMN daily_send_limit INT DEFAULT 20")
        cursor.execute("ALTER TABLE user_email_settings ADD COLUMN calendly_url VARCHAR(255) DEFAULT NULL")
        cursor.execute("ALTER TABLE user_email_settings ADD COLUMN base_url VARCHAR(255) DEFAULT 'http://localhost:8000'")
        db.commit()
    except: pass
    cursor.execute("""CREATE TABLE IF NOT EXISTS user_email_settings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL UNIQUE,
        gmail_user VARCHAR(120),
        gmail_app_password VARCHAR(120),
        imap_enabled TINYINT(1) DEFAULT 1,
        scan_frequency INT DEFAULT 30,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )""")
    migrations = [
        "ALTER TABLE campaigns ADD COLUMN min_salary INT DEFAULT 80000",
        "ALTER TABLE companies ADD COLUMN job_title VARCHAR(255)",
        "ALTER TABLE companies ADD COLUMN salary_range VARCHAR(100)",
        "ALTER TABLE companies ADD COLUMN job_posted VARCHAR(100)",
        "ALTER TABLE companies ADD COLUMN source VARCHAR(50) DEFAULT 'manual'",
        "ALTER TABLE leads ADD COLUMN company_id INT DEFAULT NULL",
        "ALTER TABLE leads ADD COLUMN email_verified TINYINT(1) DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN campaign_id INT DEFAULT NULL",
        "ALTER TABLE leads ADD COLUMN email_source VARCHAR(50) DEFAULT 'unknown'",
        "ALTER TABLE leads ADD COLUMN confidence_score INT DEFAULT 0",
        "ALTER TABLE leads MODIFY COLUMN status ENUM('new','emailed','responded','soft_rejection','rejected','unsubscribed','no_response') DEFAULT 'new'",
        "ALTER TABLE campaigns ADD COLUMN soft_rejection_days INT DEFAULT 30",
        "ALTER TABLE campaigns ADD COLUMN hard_rejection_days INT DEFAULT 90",
        "ALTER TABLE leads ADD COLUMN cooldown_until DATE DEFAULT NULL",
        "ALTER TABLE leads ADD COLUMN emailed_at TIMESTAMP DEFAULT NULL",
        "ALTER TABLE leads ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    ]
    for sql in migrations:
        try:
            cursor.execute(sql)
            db.commit()
        except:
            pass
    cursor.close()
    db.close()

def send_scheduled_emails():
    """Background thread — checks every 60s for due scheduled emails and sends via Resend"""
    while True:
        try:
            db=get_db(); c=db.cursor(dictionary=True)
            now=datetime.now()
            c.execute("""SELECT se.*,l.full_name,l.email as lead_email,l.company
                         FROM scheduled_emails se LEFT JOIN leads l ON l.id=se.lead_id
                         WHERE se.status='pending' AND se.send_at <= %s""",(now,))
            due=c.fetchall(); c.close(); db.close()
            for email in due:
                try:
                    to_email=email.get("lead_email","")
                    if not to_email: continue
                    result=send_via_resend(to_email,email.get("subject",""),email.get("body",""))
                    db2=get_db(); c2=db2.cursor()
                    if result["success"]:
                        c2.execute("UPDATE scheduled_emails SET status='sent',sent_at=NOW() WHERE id=%s",(email["id"],))
                        c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW() WHERE id=%s",(email["lead_id"],))
                        print(f"[Scheduler] ✓ Sent to {to_email}")
                    else:
                        c2.execute("UPDATE scheduled_emails SET status='failed' WHERE id=%s",(email["id"],))
                        print(f"[Scheduler] Failed: {result['error']}")
                    db2.commit(); c2.close(); db2.close()
                except Exception as e: print(f"[Scheduler] Error: {e}")
        except Exception as e: print(f"[Scheduler] Loop error: {e}")
        time.sleep(60)

@asynccontextmanager
async def lifespan(app):
    init_db()
    t=threading.Thread(target=send_scheduled_emails,daemon=True)
    t.start()
    print("[Scheduler] Background email sender started")
    yield

app = FastAPI(title="LeadFlow AI", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── AUTH ──────────────────────────────────────────────────────────
def hash_password(p): return __import__('hashlib').sha256(p.encode()).hexdigest()
def generate_token(): return __import__('secrets').token_hex(32)

def get_current_user(session_token: str = Cookie(default=None)):
    if not session_token: return None
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE session_token=%s", (session_token,))
    user = cursor.fetchone(); cursor.close(); db.close()
    return user

class AuthRequest(BaseModel):
    username: str
    email: Optional[str] = None
    password: str

@app.post("/auth/signup")
async def signup(request: AuthRequest, response: Response):
    if not request.email: raise HTTPException(400, "Email required")
    if len(request.password) < 6: raise HTTPException(400, "Password min 6 chars")
    db = get_db(); cursor = db.cursor()
    try:
        token = generate_token()
        cursor.execute("INSERT INTO users (username,email,password_hash,session_token) VALUES (%s,%s,%s,%s)",
            (request.username.strip(), request.email.strip().lower(), hash_password(request.password), token))
        db.commit()
        response.set_cookie("session_token", token, httponly=True, max_age=86400*30)
        return {"success": True, "username": request.username, "token": token}
    except mysql.connector.IntegrityError as e:
        raise HTTPException(400, "Username taken" if "username" in str(e) else "Email already registered")
    finally:
        cursor.close(); db.close()

@app.post("/auth/login")
async def login(request: AuthRequest, response: Response):
    db = get_db(); cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE (username=%s OR email=%s) AND password_hash=%s",
        (request.username, request.username.lower(), hash_password(request.password)))
    user = cursor.fetchone(); cursor.close(); db.close()
    if not user: raise HTTPException(401, "Invalid username or password")
    token = generate_token()
    db2 = get_db(); c2 = db2.cursor()
    c2.execute("UPDATE users SET session_token=%s WHERE id=%s", (token, user["id"]))
    db2.commit(); c2.close(); db2.close()
    response.set_cookie("session_token", token, httponly=True, max_age=86400*30)
    return {"success": True, "username": user["username"], "token": token}

@app.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("session_token")
    return {"success": True}

@app.get("/auth/me")
async def get_me(session_token: str = Cookie(default=None)):
    user = get_current_user(session_token)
    if not user: raise HTTPException(401, "Not logged in")
    return {"username": user["username"], "email": user["email"]}

client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=os.getenv("GROQ_API_KEY"))

CONFIDENCE = {"apollo_verified":92,"apollo_unverified":70,"prospeo_linkedin":88,"prospeo_name":85,"snov":78,"generated":50,"manual":60,"unknown":0}
SEND_THRESHOLD = 70

class CampaignCreate(BaseModel):
    name: str; target_role: str; target_location: str
    company_size_min: int = 50; company_size_max: int = 10000; min_salary: int = 80000
    excluded_industries: Optional[str] = "Staffing,Recruiting,Talent,Consultancy,IT Consultancy,Human Resources Services,Technology Information and Media,University,Non-profit,NGO"
    exclude_intern: bool = True; exclude_remote: bool = True; exclude_apprentice: bool = True
    job_date_filter: str = "3days"; notes: Optional[str] = None

class CompanyCreate(BaseModel):
    campaign_id: int; name: str; industry: str = ""; location: str = ""; size_range: str = ""
    website: str = ""; job_title: str = ""; salary_range: str = ""; job_posted: str = ""
    source: str = "manual"; notes: str = ""

class CompanyBulkAdd(BaseModel):
    campaign_id: int; companies: List[CompanyCreate]

class LeadCreate(BaseModel):
    first_name: str; last_name: str; title: str; company: str
    industry: str = ""; location: str = ""; email: str = ""; linkedin_url: str = ""
    target_role: str = ""; campaign_id: Optional[int] = None; company_id: Optional[int] = None; notes: Optional[str] = None

class EmailRequest(BaseModel):
    lead_id: int; email_type: str; scenario: Optional[str] = None

class StatusUpdate(BaseModel):
    status: str

class EmailVerifyRequest(BaseModel):
    linkedin_url: str = ""; first_name: str = ""; last_name: str = ""; company_domain: str = ""

class EmailSettingsRequest(BaseModel):
    gmail_user: str; gmail_app_password: str; imap_enabled: bool = True; scan_frequency: int = 30
    calendly_url: Optional[str] = None
    base_url: Optional[str] = "http://localhost:8000"
    daily_send_limit: Optional[int] = 20

class BulkEmailRequest(BaseModel):
    campaign_id: int; email_type: str; scenario: Optional[str] = None

class CooldownSettings(BaseModel):
    soft_rejection_days: int = 30; hard_rejection_days: int = 90

class SendEmailRequest(BaseModel):
    lead_id: int; subject: str; body: str

def is_blacklisted(email: str) -> bool:
    if not email: return False
    db = get_db(); cursor = db.cursor()
    cursor.execute("SELECT id FROM email_blacklist WHERE email=%s", (email.lower().strip(),))
    r = cursor.fetchone(); cursor.close(); db.close()
    return r is not None

def add_to_blacklist(email: str, reason: str = "unsubscribed"):
    if not email: return
    db = get_db(); cursor = db.cursor()
    try: cursor.execute("INSERT IGNORE INTO email_blacklist (email,reason) VALUES (%s,%s)", (email.lower().strip(), reason)); db.commit()
    except: pass
    cursor.close(); db.close()

DAILY_EMAIL_LIMIT = 40

def get_emails_sent_today(campaign_id):
    db = get_db(); cursor = db.cursor()
    cursor.execute("SELECT emails_sent FROM email_daily_log WHERE campaign_id=%s AND send_date=%s", (campaign_id, date.today()))
    row = cursor.fetchone(); cursor.close(); db.close()
    return row[0] if row else 0

def increment_email_count(campaign_id):
    db = get_db(); cursor = db.cursor()
    cursor.execute("INSERT INTO email_daily_log (campaign_id,send_date,emails_sent) VALUES (%s,%s,1) ON DUPLICATE KEY UPDATE emails_sent=emails_sent+1", (campaign_id, date.today()))
    db.commit(); cursor.close(); db.close()

def check_rate_limit(campaign_id):
    sent = get_emails_sent_today(campaign_id)
    return {"allowed": sent < DAILY_EMAIL_LIMIT, "sent": sent, "limit": DAILY_EMAIL_LIMIT, "remaining": max(0, DAILY_EMAIL_LIMIT - sent)}

def can_send_email(lead):
    email = lead.get("email",""); campaign_id = lead.get("campaign_id"); status = lead.get("status","")
    if not email: return {"safe": False, "reason": "No email address found"}
    if status in ("rejected","unsubscribed"): return {"safe": False, "reason": f"Lead is permanently {status}"}
    if is_blacklisted(email): return {"safe": False, "reason": "Email is blacklisted"}
    score = lead.get("confidence_score", 0)
    if score < SEND_THRESHOLD: return {"safe": False, "reason": f"Confidence score {score} below threshold {SEND_THRESHOLD}"}
    if campaign_id:
        rl = check_rate_limit(campaign_id)
        if not rl["allowed"]: return {"safe": False, "reason": f"Daily limit reached ({rl['sent']}/{rl['limit']})"}
    return {"safe": True, "reason": "OK"}

def get_user_gmail(session_token: str):
    user = get_current_user(session_token)
    if user:
        db = get_db(); cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM user_email_settings WHERE user_id=%s", (user["id"],))
        s = cursor.fetchone(); cursor.close(); db.close()
        if s and s.get("gmail_user") and s.get("gmail_app_password"):
            return s["gmail_user"], s["gmail_app_password"]
    return os.getenv("GMAIL_USER",""), os.getenv("GMAIL_APP_PASSWORD","")


def get_contact_titles(target_role: str):
    prompt = f"""You are a Senior B2B Lead Generation Strategist specializing in US recruitment outreach.
A US recruitment firm wants to place candidates for the role: "{target_role}"
Return the EXACT job titles of people who would APPROVE HIRING for this role.
RULES:
1. NEVER include the target role itself or same-level peers.
2. Include 3-4 titles ABOVE the target role in the reporting chain.
3. Include 3 HR titles at appropriate seniority:
   VP/Director level → "CHRO","VP of Human Resources","VP of Talent Acquisition"
   Manager/Senior → "Director of HR","Director of Talent Acquisition","Talent Acquisition Manager"
   Junior → "HR Manager","Talent Acquisition Manager","Recruiter"
4. Return EXACTLY 6-7 titles total.
Examples:
- "VP of Sales" → ["CEO","President","Chief Revenue Officer","Chief Sales Officer","CHRO","Chief People Officer","VP of Talent Acquisition"]
- "Financial Analyst" → ["CFO","VP of Finance","Director of Finance","Finance Manager","HR Manager","Talent Acquisition Manager","Director of Recruitment"]
- "Software Engineer" → ["CTO","VP of Engineering","Director of Engineering","Engineering Manager","HR Manager","Technical Recruiting Manager","Director of Recruitment"]
Return ONLY a JSON array. No explanation."""
    try:
        response = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=[{"role":"user","content":prompt}])
        content = response.choices[0].message.content.strip()
        if "```" in content:
            content = content.split("```")[1]
            if content.startswith("json"): content = content[4:]
        titles = json.loads(content.strip())
        if isinstance(titles, list) and len(titles) >= 3: return titles[:8]
        raise ValueError("bad")
    except:
        role_lower = target_role.lower()
        is_vp = any(w in role_lower for w in ["vp","vice president","chief","cto","cfo","cmo","coo","ceo","president","director"])
        is_mgr = any(w in role_lower for w in ["manager","lead","senior","head of"])
        hr = ["CHRO","Chief People Officer","VP of Human Resources","VP of Talent Acquisition","Director of Recruitment"] if is_vp else \
             ["Director of HR","Director of Talent Acquisition","Director of Recruitment","Talent Acquisition Manager"] if is_mgr else \
             ["HR Manager","Talent Acquisition Manager","Director of Recruitment","Recruiting Manager"]
        if any(w in role_lower for w in ["engineer","developer","software","tech","data","ai","ml"]): return ["CTO","VP of Engineering","Director of Engineering"]+hr
        if any(w in role_lower for w in ["finance","financial","analyst","accounting"]): return ["CFO","VP of Finance","Director of Finance"]+hr
        if any(w in role_lower for w in ["vp of sales","chief revenue","cro"]): return ["CEO","President","Chief Revenue Officer"]+hr
        if any(w in role_lower for w in ["sales","revenue","account executive"]): return ["VP of Sales","Chief Revenue Officer","Director of Sales"]+hr
        if any(w in role_lower for w in ["marketing","brand","growth","content"]): return ["CMO","VP of Marketing","Director of Marketing"]+hr
        if any(w in role_lower for w in ["hr","human resources","people operations"]): return ["CEO","COO","Chief People Officer","VP of People"]+hr[:2]
        if any(w in role_lower for w in ["talent","recruiter","recruiting"]): return ["CHRO","VP of Human Resources","Director of HR","VP of Talent Acquisition","Head of People"]
        if any(w in role_lower for w in ["operations","ops","supply chain"]): return ["COO","VP of Operations","Director of Operations"]+hr
        if any(w in role_lower for w in ["product","product manager"]): return ["CPO","VP of Product","Director of Product","CTO"]+hr
        return ["CEO","COO","VP of HR","Director of HR","Talent Acquisition Manager","Head of People"]

def get_ai_salary(target_role: str):
    try:
        r = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=[{"role":"user","content":f'Minimum annual salary USD for "{target_role}" at a US company. Return ONLY a number like 85000.'}])
        return int(''.join(filter(str.isdigit, r.choices[0].message.content.strip()))) or 80000
    except: return 80000

def search_jobs_jsearch(role, location, date_posted="week", min_salary=0, excluded_industries="", exclude_intern=True, exclude_remote=True, exclude_apprentice=True, page=1):
    api_key = os.getenv("RAPIDAPI_KEY","")
    if not api_key: return {"error":"RapidAPI key not configured","jobs":[]}
    date_map = {"24h":"today","3days":"3days","week":"week","month":"month"}
    excluded = [i.strip().lower() for i in excluded_industries.split(",") if i.strip()]
    country = "gb" if any(x in location.lower() for x in ["uk","united kingdom","london","manchester"]) else "us"
    try:
        response = requests.get("https://jsearch.p.rapidapi.com/search",
            headers={"X-RapidAPI-Key":api_key,"X-RapidAPI-Host":"jsearch.p.rapidapi.com"},
            params={"query":f"{role} {location}","page":str(page),"num_pages":"5","date_posted":date_map.get(date_posted,"week"),"country":country},
            timeout=20)
        data = response.json()
        if not data.get("data"): return {"error":"No results","jobs":[],"total":0}
        jobs=[]; seen=set()
        excl_kw=["staffing","recruiting","recruiter","talent","consulting","consultancy","consultant","it consulting","human resources","hr services","university","college","school","non-profit","nonprofit","ngo","charity","foundation"]+excluded
        for job in data["data"]:
            employer = job.get("employer_name","").strip()
            if not employer or employer in seen: continue
            emp_type=(job.get("employer_company_type") or "").lower()
            job_title_raw=(job.get("job_title") or "").lower()
            emp_lower=employer.lower()
            skip=any(kw and (kw in emp_type or kw in emp_lower) for kw in excl_kw)
            if exclude_intern and ("intern" in job_title_raw or "internship" in job_title_raw): skip=True
            if exclude_apprentice and "apprentice" in job_title_raw: skip=True
            if exclude_remote and job.get("job_is_remote") and (job.get("job_min_salary") or 0)<80000: skip=True
            if skip: continue
            min_sal=job.get("job_min_salary") or 0; max_sal=job.get("job_max_salary") or 0
            if (job.get("job_salary_period") or "").lower()=="hourly": min_sal*=2080; max_sal*=2080
            if min_salary>0 and min_sal>0 and min_sal<min_salary: continue
            salary_display=f"${min_sal:,.0f} - ${max_sal:,.0f}/yr" if min_sal and max_sal else f"${min_sal:,.0f}+/yr" if min_sal else "Not disclosed"
            posted=job.get("job_posted_at_datetime_utc","")
            try:
                from datetime import timezone
                posted_dt=datetime.fromisoformat(posted.replace("Z","+00:00"))
                days_ago=(datetime.now(timezone.utc)-posted_dt).days
                posted_str="Today" if days_ago==0 else f"{days_ago} days ago"
            except: posted_str="Recently"
            seen.add(employer)
            jobs.append({"employer_name":employer,"job_title":job.get("job_title",role),"employer_logo":job.get("employer_logo",""),
                "location":f"{job.get('job_city','')}, {job.get('job_state','')}".strip(", "),
                "salary_display":salary_display,"min_salary":min_sal,"max_salary":max_sal,"job_posted":posted_str,
                "is_remote":job.get("job_is_remote",False),"apply_link":job.get("job_apply_link",""),
                "employer_website":job.get("employer_website",""),"company_type":job.get("employer_company_type","")})
        return {"jobs":jobs,"total":len(jobs),"error":None}
    except Exception as e: return {"error":str(e),"jobs":[],"total":0}

def search_companies_apollo(role,location="United States",min_employees=50,max_employees=10000,excluded_industries="",page=1):
    api_key=os.getenv("APOLLO_API_KEY","")
    if not api_key: return {"error":"Apollo not configured","jobs":[]}
    headers={"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key":api_key}
    excluded=[i.strip().lower() for i in excluded_industries.split(",") if i.strip()]
    excl_kw=["staffing","recruiting","recruiter","talent","consulting","consultancy","human resources","university","college","non-profit","nonprofit","ngo"]+excluded
    try:
        r=requests.post("https://api.apollo.io/api/v1/mixed_companies/search",headers=headers,
            json={"page":page,"per_page":25,"organization_locations":[location],"organization_num_employees_ranges":[f"{min_employees},{max_employees}"]},timeout=20)
        if not r.text or r.status_code!=200: return {"error":f"Apollo HTTP {r.status_code}","jobs":[],"total":0}
        data=r.json(); orgs=data.get("organizations",[]); total=data.get("pagination",{}).get("total_entries",0)
        if not orgs: return {"error":"No companies found","jobs":[],"total":0}
        jobs=[]; seen=set()
        for org in orgs:
            name=org.get("name","").strip()
            if not name or name in seen: continue
            if any(kw and kw in name.lower() for kw in excl_kw): continue
            seen.add(name); domain=org.get("primary_domain",""); revenue=org.get("organization_revenue_printed","")
            jobs.append({"employer_name":name,"job_title":f"Active company — hiring {role}","employer_logo":org.get("logo_url",""),
                "location":location,"salary_display":f"Revenue: ${revenue}" if revenue else "Not disclosed",
                "min_salary":0,"max_salary":0,"job_posted":"Active company","is_remote":False,
                "apply_link":org.get("website_url",""),"employer_website":org.get("website_url",""),
                "company_type":"","domain":domain,"phone":org.get("sanitized_phone","") or "",
                "linkedin_url":org.get("linkedin_url",""),"source":"apollo_org"})
        return {"jobs":jobs,"total":total,"error":None,"source":"apollo"}
    except Exception as e: return {"error":str(e),"jobs":[],"total":0}

def search_jobs_adzuna(role,location,min_salary=0,excluded_industries="",exclude_intern=True,exclude_remote=True,page=1):
    app_id=os.getenv("ADZUNA_APP_ID",""); api_key=os.getenv("ADZUNA_API_KEY","")
    if not app_id or not api_key: return {"error":"Adzuna not configured","jobs":[]}
    country="gb" if any(x in location.lower() for x in ["uk","united kingdom","london","manchester"]) else "us"
    excluded=[i.strip().lower() for i in excluded_industries.split(",") if i.strip()]
    excl_kw=["staffing","recruiting","recruiter","talent","consulting","consultancy","human resources","university","college","non-profit","nonprofit","ngo"]+excluded
    try:
        loc_raw=location.split(",")[0].strip()
        loc_clean="" if loc_raw.lower() in ["united states","us","usa","united kingdom","uk","gb"] else loc_raw
        if not loc_clean: loc_clean="New York"
        r=requests.get(f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}",
            params={"app_id":app_id,"app_key":api_key,"results_per_page":20,"page":page,"what":role,"where":loc_clean,"distance":100},timeout=20)
        if not r.text or r.status_code!=200: return {"error":f"Adzuna HTTP {r.status_code}","jobs":[],"total":0}
        results=r.json().get("results",[])
        if not results: return {"error":"No results from Adzuna","jobs":[],"total":0}
        jobs=[]; seen=set()
        for job in results:
            company=job.get("company",{}).get("display_name","").strip()
            if not company or company in seen: continue
            if any(kw and kw in company.lower() for kw in excl_kw): continue
            job_title_raw=job.get("title","").lower()
            if exclude_intern and ("intern" in job_title_raw or "internship" in job_title_raw): continue
            sal_min=job.get("salary_min",0) or 0; sal_max=job.get("salary_max",0) or 0
            salary_display=f"${sal_min:,.0f} - ${sal_max:,.0f}/yr" if sal_min and sal_max else f"${sal_min:,.0f}+/yr" if sal_min else "Not disclosed"
            created=job.get("created","")
            try:
                from datetime import timezone
                created_dt=datetime.fromisoformat(created.replace("Z","+00:00"))
                days_ago=(datetime.now(timezone.utc)-created_dt).days
                posted_str="Today" if days_ago==0 else f"{days_ago} days ago"
            except: posted_str="Recently"
            seen.add(company)
            jobs.append({"employer_name":company,"job_title":job.get("title",role),"employer_logo":"",
                "location":job.get("location",{}).get("display_name",location),"salary_display":salary_display,
                "min_salary":sal_min,"max_salary":sal_max,"job_posted":posted_str,"is_remote":False,
                "apply_link":job.get("redirect_url",""),"employer_website":"",
                "company_type":job.get("category",{}).get("label",""),"source":"adzuna"})
        return {"jobs":jobs,"total":len(jobs),"error":None,"source":"adzuna"}
    except Exception as e: return {"error":str(e),"jobs":[],"total":0}

def get_salary_data(role,location="United States"):
    api_key=os.getenv("RAPIDAPI_KEY","")
    if not api_key: return None
    try:
        r=requests.get("https://jsearch.p.rapidapi.com/estimated-salary",
            headers={"X-RapidAPI-Key":api_key,"X-RapidAPI-Host":"jsearch.p.rapidapi.com"},
            params={"job_title":role,"location":location,"location_type":"ANY","years_of_experience":"ALL"},timeout=10)
        data=r.json()
        if data.get("data") and data["data"]: return int(data["data"][0].get("median_salary",0)) or None
    except: pass
    return None

def get_domain_clearbit(company_name):
    try:
        r=requests.get("https://autocomplete.clearbit.com/v1/companies/suggest",params={"query":company_name},timeout=8)
        results=r.json()
        if results:
            name_lower=company_name.lower()
            for result in results[:3]:
                domain=result.get("domain",""); result_name=result.get("name","").lower()
                if domain and (name_lower in result_name or result_name in name_lower or name_lower.split()[0] in result_name):
                    return domain
            if results[0].get("domain"): return results[0]["domain"]
    except: pass
    return ""

def get_domain_variations(company_name,website=""):
    if website:
        domain=website.replace("https://","").replace("http://","").replace("www.","").split("/")[0].strip()
        if domain and "." in domain: return [domain]
    clearbit=get_domain_clearbit(company_name)
    if clearbit: return [clearbit]
    name=company_name.lower()
    v1=re.sub(r'[^a-z0-9]','',name); v2=re.sub(r'[^a-z0-9 ]','',name).strip().replace(" ","-")
    first=re.sub(r'[^a-z0-9]','',name.split()[0]) if name.split() else ""
    domains=[]
    if v1: domains.append(f"{v1}.com")
    if v2 and v2!=v1: domains.append(f"{v2}.com")
    if first and first!=v1: domains.append(f"{first}.com")
    return list(dict.fromkeys(domains))

def find_email_prospeo(linkedin_url="",first_name="",last_name="",company_domain=""):
    api_key=os.getenv("PROSPEO_API_KEY","")
    if not api_key: return {"found":False,"email":"","confidence_score":0,"email_source":"unknown"}
    headers={"Content-Type":"application/json","X-KEY":api_key}
    if linkedin_url:
        try:
            r=requests.post("https://api.prospeo.io/linkedin-email-finder",headers=headers,json={"url":linkedin_url},timeout=15)
            d=r.json()
            if d.get("ok") and d.get("response",{}).get("email"):
                return {"found":True,"email":d["response"]["email"],"verified":True,"method":"linkedin",
                        "email_source":"prospeo_linkedin","confidence_score":CONFIDENCE["prospeo_linkedin"],"message":"Email found via LinkedIn"}
        except Exception as e: print(f"Prospeo LinkedIn error: {e}")
    if first_name and last_name and company_domain:
        try:
            r=requests.post("https://api.prospeo.io/email-finder",headers=headers,
                json={"first_name":first_name,"last_name":last_name,"company":company_domain},timeout=15)
            d=r.json()
            if d.get("ok") and d.get("response",{}).get("email"):
                return {"found":True,"email":d["response"]["email"],"verified":True,"method":"name_domain",
                        "email_source":"prospeo_name","confidence_score":CONFIDENCE["prospeo_name"],"message":"Email found via name+domain"}
        except Exception as e: print(f"Prospeo name error: {e}")
    return {"found":False,"email":"","confidence_score":0,"email_source":"unknown","message":"Not found via Prospeo"}

def find_email_by_name_snov(first_name,last_name,domain):
    cid=os.getenv("SNOV_CLIENT_ID",""); cs=os.getenv("SNOV_CLIENT_SECRET","")
    if not cid or not cs: return {"found":False,"email":"","confidence_score":0}
    try:
        token=requests.post("https://api.snov.io/v1/oauth/access_token",
            data={"grant_type":"client_credentials","client_id":cid,"client_secret":cs},timeout=10).json().get("access_token")
        if not token: return {"found":False,"email":"","confidence_score":0}
        emails=requests.post("https://api.snov.io/v1/get-emails-by-name",
            data={"access_token":token,"first_name":first_name,"last_name":last_name,"domain":domain,"limit":5,"type":"personal"},timeout=15).json().get("emails",[])
        if emails: return {"found":True,"email":emails[0].get("email",""),"email_source":"snov","confidence_score":CONFIDENCE["snov"]}
    except Exception as e: print(f"Snov error: {e}")
    return {"found":False,"email":"","confidence_score":0}

def enrich_with_email(first,last,linkedin_url,domain):
    if linkedin_url:
        r=find_email_prospeo(linkedin_url=linkedin_url)
        if r.get("found"): return {"email":r["email"],"email_source":r["email_source"],"confidence_score":r["confidence_score"]}
    if first and domain:
        r=find_email_prospeo(first_name=first,last_name=last,company_domain=domain)
        if r.get("found"): return {"email":r["email"],"email_source":r["email_source"],"confidence_score":r["confidence_score"]}
    if first and domain:
        r=find_email_by_name_snov(first,last,domain)
        if r.get("found"): return {"email":r["email"],"email_source":"snov","confidence_score":CONFIDENCE["snov"]}
    return {"email":"","email_source":"unknown","confidence_score":0}

def get_apollo_top_people(company_name,company_domain,titles):
    api_key=os.getenv("APOLLO_API_KEY","")
    if not api_key: return []
    headers={"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key":api_key}
    try:
        # Use people/search — works with Apollo Basic plan
        payload = {
            "per_page": 10,
            "page": 1,
            "person_titles": titles[:10] if titles else ["CEO","CTO","VP","Director","Manager","Recruiter","President","Founder"],
        }
        if company_domain:
            payload["q_organization_domains"] = company_domain
        else:
            payload["q_organization_name"] = company_name
        r=requests.post("https://api.apollo.io/api/v1/people/search",headers=headers,json=payload,timeout=15)
        print(f"[Apollo] people/search status: {r.status_code}")
        data = r.json()
        raw = data.get("people",[])
        print(f"[Apollo] Found {len(raw)} people for {company_name}")
        
        # Fallback to mixed_people if people/search returns nothing
        if not raw:
            fallback_payload = {"page":1,"per_page":10}
            if company_domain:
                fallback_payload["q_organization_domains_list"] = [company_domain]
            else:
                fallback_payload["q_organization_name"] = company_name
            if titles:
                fallback_payload["person_titles"] = titles[:10]
            r2=requests.post("https://api.apollo.io/api/v1/mixed_people/api_search",headers=headers,
                json=fallback_payload,timeout=15)
            raw=r2.json().get("people",[])
            print(f"[Apollo] Fallback mixed_people found {len(raw)} for {company_name}")
        if not raw: return []
        # Filter by relevant title keywords
        RELEVANT_KW = ["talent","recruit","hr","human resource","people","staffing","hiring",
                       "acquisition","vp","vice president","director","chief","head","president",
                       "founder","ceo","coo","cto","partner","managing","executive","workforce"]
        people=[]; seen=set()
        for p in raw:
            first=p.get("first_name","")
            if not first or first in seen: continue
            p_title = p.get("title","").lower()
            # Apply title filter only when titles were provided
            if titles and not any(kw in p_title for kw in RELEVANT_KW):
                print(f"[Apollo] Skipping {first} ({p.get('title','')}) — not relevant")
                continue
            seen.add(first)
            people.append({"first_name":first,"last_name":"","full_name":first,
                "title":p.get("title","").split(",")[0].strip(),"linkedin_url":p.get("linkedin_url","") or "",
                "email":"","email_source":"unknown","confidence_score":0,"location":"",
                "apollo_id":p.get("id",""),"has_email":p.get("has_email",False),"source":"apollo"})
        return people[:10]
    except Exception as e: print(f"Apollo api_search error: {e}"); return []

def enrich_email_apollo(first_name,last_name,company_domain,linkedin_url="",apollo_id=""):
    api_key=os.getenv("APOLLO_API_KEY","")
    if not api_key: return {"email":"","email_source":"unknown","confidence_score":0}
    try:
        payload={"id":apollo_id,"reveal_personal_emails":True} if apollo_id else {"first_name":first_name,"last_name":last_name,"organization_domain":company_domain}
        if linkedin_url and not apollo_id: payload["linkedin_url"]=linkedin_url
        r=requests.post("https://api.apollo.io/api/v1/people/match",
            headers={"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key":api_key},json=payload,timeout=15)
        person=r.json().get("person",{}); email=person.get("email","")
        full_name=f"{person.get('first_name','')} {person.get('last_name','')}".strip()
        if email and "?" not in email:
            return {"email":email,"email_source":"apollo_verified","confidence_score":CONFIDENCE["apollo_verified"],
                    "full_name":full_name,"first_name":person.get("first_name",""),"last_name":person.get("last_name","")}
    except Exception as e: print(f"Apollo match error: {e}")
    return {"email":"","email_source":"unknown","confidence_score":0}

def search_people_linkedin_scraper(company_name,title):
    api_key=os.getenv("RAPIDAPI_KEY","")
    if not api_key: return []
    try:
        r=requests.get("https://fresh-linkedin-scraper-api.p.rapidapi.com/api/v1/search/people",
            headers={"x-rapidapi-key":api_key,"x-rapidapi-host":"fresh-linkedin-scraper-api.p.rapidapi.com"},
            params={"keyword":f'"{title} at {company_name}"',"page":"1"},timeout=15)
        data=r.json()
        if not data.get("success") or not data.get("data"): return []
        people=[]
        for p in data["data"]:
            pt=p.get("title",""); comp_words=[w.lower() for w in company_name.split() if len(w)>2]
            if not any(w in pt.lower() for w in comp_words): continue
            full_name=p.get("full_name","")
            if not full_name: continue
            parts=full_name.strip().split()
            people.append({"first_name":parts[0] if parts else "","last_name":" ".join(parts[1:]) if len(parts)>1 else "",
                "full_name":full_name,"title":pt,"linkedin_url":p.get("url",""),"email":"",
                "email_source":"unknown","confidence_score":0,"location":p.get("location",""),
                "avatar":p.get("avatar",[{}])[0].get("url","") if p.get("avatar") else "","source":"linkedin_scraper"})
        return people[:3]
    except Exception as e: print(f"LinkedIn scraper error: {e}"); return []

def search_domain_hunter(domain,titles=[]):
    api_key=os.getenv("HUNTER_API_KEY","")
    if not api_key: return []
    try:
        r=requests.get("https://api.hunter.io/v2/domain-search",
            params={"domain":domain,"api_key":api_key,"limit":10,"department":"human_resources,executive"},timeout=15)
        data=r.json()
        if data.get("errors"):
            r2=requests.get("https://api.hunter.io/v2/domain-search",params={"domain":domain,"api_key":api_key,"limit":10},timeout=15)
            data=r2.json()
            if data.get("errors"): return []
        emails=data.get("data",{}).get("emails",[])
        if not emails: return []
        generic={"chief","officer","vp","vice","president","director","manager","head","senior","lead","executive","associate","the","of","and","for","a","an","in","at","to","by","on","or","is","general"}
        domain_kw=set(); exact_titles=set()
        for t in titles:
            t_lower=t.lower().strip()
            if len(t_lower)<=5 and t_lower.isalpha(): exact_titles.add(t_lower)
            for word in t_lower.split():
                clean=word.strip(".,()-")
                if clean and clean not in generic and len(clean)>=3: domain_kw.add(clean)
        alias={"revenue":["sales","commercial"],"human":["hr","people","workforce"],"resources":["hr","people"],
               "talent":["recruiting","recruitment","staffing","hiring"],"acquisition":["recruiting","recruitment"],"people":["hr","human resources"]}
        expanded=set(domain_kw)
        for kw in domain_kw:
            if kw in alias: expanded.update(alias[kw])
        domain_kw=expanded
        people=[]; seen=set()
        for e in emails:
            fn_raw = e.get('first_name','') or ''
            ln_raw = e.get('last_name','') or ''
            # Filter out literal "None" strings from Hunter API
            fn_clean = fn_raw if fn_raw.lower() not in ('none','null','') else ''
            ln_clean = ln_raw if ln_raw.lower() not in ('none','null','') else ''
            full_name = f"{fn_clean} {ln_clean}".strip()
            # If no real name, derive from email prefix (e.g. humanresources@company.com -> HR Contact)
            if not full_name:
                email_val = e.get("value","") or ""
                prefix = email_val.split("@")[0] if "@" in email_val else ""
                # Map common prefixes to readable names
                prefix_map = {
                    "hr": "HR Contact", "humanresources": "HR Contact",
                    "talent": "Talent Contact", "recruiting": "Recruiting Contact",
                    "contact": "General Contact", "info": "Info Contact",
                    "accounts": "Accounts Contact", "productivity": "Operations Contact",
                    "careers": "Careers Contact", "jobs": "Jobs Contact",
                }
                full_name = prefix_map.get(prefix.lower().replace(".","").replace("-","").replace("_",""), prefix.replace("."," ").replace("-"," ").replace("_"," ").title() + " Contact" if prefix else "")
            if not full_name or full_name in seen: continue
            pt=(e.get("position") or "").lower().strip()
            hc=e.get("confidence",0); score=90 if hc>=90 else 82 if hc>=75 else 72
            if not pt:
                # No title — save anyway if confidence is high (generic/department emails)
                if hc >= 75:
                    seen.add(full_name)
                    people.append({"first_name":e.get("first_name",""),"last_name":e.get("last_name",""),
                        "full_name":full_name,"title":"Contact",
                        "email":e.get("value",""),"email_source":"hunter","confidence_score":score,
                        "linkedin_url":e.get("linkedin","") or "","phone":e.get("phone_number","") or "",
                        "location":"","source":"hunter"})
                continue
            tw=set(pt.replace("-"," ").split())
            match=any(ex in tw for ex in exact_titles) or any(kw in pt for kw in domain_kw)
            if not match: continue
            seen.add(full_name)
            people.append({"first_name":e.get("first_name",""),"last_name":e.get("last_name",""),"full_name":full_name,
                "title":e.get("position",""),"email":e.get("value",""),"email_source":"hunter","confidence_score":score,
                "linkedin_url":e.get("linkedin","") or "","phone":e.get("phone_number","") or "","location":"","source":"hunter"})
        print(f"Hunter found {len(people)} at {domain}")
        return people[:10]
    except Exception as e: print(f"Hunter error: {e}"); return []

def guess_email_patterns(first_name,last_name,domain):
    f=re.sub(r"[^a-z]","",first_name.lower()); l=re.sub(r"[^a-z]","",last_name.lower()); f1=f[0] if f else ""
    return [f"{f}.{l}@{domain}",f"{f}{l}@{domain}",f"{f1}{l}@{domain}",f"{f1}.{l}@{domain}",f"{f}_{l}@{domain}",f"{f}@{domain}"]


# ── Routes ────────────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
async def serve_login():
    with open("templates/login.html", encoding="utf-8") as f: return HTMLResponse(f.read())

@app.get("/", response_class=HTMLResponse)
async def home(session_token: str = Cookie(default=None)):
    user = get_current_user(session_token)
    if not user: return RedirectResponse(url="/login", status_code=302)
    with open("templates/index.html", encoding="utf-8") as f: return HTMLResponse(f.read())

@app.post("/campaigns")
async def create_campaign(campaign: CampaignCreate):
    db = get_db(); cursor = db.cursor()
    cursor.execute("""INSERT INTO campaigns (name,target_role,target_location,company_size_min,company_size_max,min_salary,excluded_industries,exclude_intern,exclude_remote,exclude_apprentice,job_date_filter,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (campaign.name,campaign.target_role,campaign.target_location,campaign.company_size_min,campaign.company_size_max,campaign.min_salary,campaign.excluded_industries,campaign.exclude_intern,campaign.exclude_remote,campaign.exclude_apprentice,campaign.job_date_filter,campaign.notes))
    db.commit(); cid=cursor.lastrowid; cursor.close(); db.close()
    return {"message":"Campaign created!","id":cid}

@app.get("/campaigns")
async def get_campaigns():
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("""SELECT c.*,COUNT(DISTINCT comp.id) as total_companies,COUNT(DISTINCT l.id) as total_leads,
        SUM(CASE WHEN l.status='emailed' THEN 1 ELSE 0 END) as emailed,SUM(CASE WHEN l.status='responded' THEN 1 ELSE 0 END) as responded
        FROM campaigns c LEFT JOIN companies comp ON comp.campaign_id=c.id LEFT JOIN leads l ON l.campaign_id=c.id GROUP BY c.id ORDER BY c.created_at DESC""")
    result=cursor.fetchall(); cursor.close(); db.close(); return result

@app.get("/campaigns/{campaign_id}")
async def get_campaign(campaign_id: int):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM campaigns WHERE id=%s",(campaign_id,))
    c=cursor.fetchone(); cursor.close(); db.close()
    if not c: raise HTTPException(404,"Campaign not found")
    return c

@app.put("/campaigns/{campaign_id}")
async def update_campaign(campaign_id: int, campaign: CampaignCreate):
    db=get_db(); cursor=db.cursor()
    cursor.execute("""UPDATE campaigns SET name=%s,target_role=%s,target_location=%s,company_size_min=%s,company_size_max=%s,min_salary=%s,excluded_industries=%s,exclude_intern=%s,exclude_remote=%s,exclude_apprentice=%s,job_date_filter=%s,notes=%s WHERE id=%s""",
        (campaign.name,campaign.target_role,campaign.target_location,campaign.company_size_min,campaign.company_size_max,campaign.min_salary,campaign.excluded_industries,campaign.exclude_intern,campaign.exclude_remote,campaign.exclude_apprentice,campaign.job_date_filter,campaign.notes,campaign_id))
    db.commit(); cursor.close(); db.close(); return {"message":"Campaign updated!"}

@app.delete("/campaigns/{campaign_id}")
async def delete_campaign(campaign_id: int):
    db=get_db(); cursor=db.cursor()
    cursor.execute("DELETE FROM campaigns WHERE id=%s",(campaign_id,))
    db.commit(); cursor.close(); db.close(); return {"message":"Campaign deleted"}

@app.get("/search-jobs/{campaign_id}")
async def search_jobs(campaign_id: int, page: int = 1):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM campaigns WHERE id=%s",(campaign_id,))
    c=cursor.fetchone(); cursor.close(); db.close()
    if not c: raise HTTPException(404,"Campaign not found")
    # Apollo first
    print("[Search] Trying Apollo company search first...")
    result = search_companies_apollo(c["target_role"], c.get("target_location","United States"), page=page)
    if result.get("jobs"):
        print(f"[Search] Apollo returned {len(result['jobs'])} companies")
        return result
    # JSearch fallback
    print("[Search] Apollo empty — falling back to JSearch...")
    result=search_jobs_jsearch(c["target_role"],c["target_location"],c.get("job_date_filter","week"),c.get("min_salary",0),c.get("excluded_industries",""),bool(c.get("exclude_intern",True)),bool(c.get("exclude_remote",True)),bool(c.get("exclude_apprentice",True)),page)
    if not result.get("jobs"):
        try:
            ar=search_companies_apollo(c["target_role"],c["target_location"],c.get("company_size_min",50),c.get("company_size_max",10000),c.get("excluded_industries",""),page)
            if ar.get("jobs"): ar["note"]="Companies from Apollo database (274K+ US companies)"; return ar
        except Exception as e: print(f"Apollo failed: {e}")
        try:
            az=search_jobs_adzuna(c["target_role"],c["target_location"],c.get("min_salary",0),c.get("excluded_industries",""),bool(c.get("exclude_intern",True)),bool(c.get("exclude_remote",True)),page)
            if az.get("jobs"): az["note"]="Results from Adzuna"; return az
        except Exception as e: print(f"Adzuna failed: {e}")
        return {"jobs":[],"total":0,"error":None,"message":"No results found. Try adding companies manually via global search."}
    return result

@app.get("/salary-suggestion")
async def salary_suggestion(role: str, location: str = "United States"):
    sal=get_salary_data(role,location)
    if sal: return {"suggested_salary":int(sal*0.8),"median_salary":sal,"source":"JSearch","message":f"Market median ${sal:,}/yr. Suggested min: ${int(sal*0.8):,}/yr"}
    ai=get_ai_salary(role)
    return {"suggested_salary":ai,"median_salary":ai,"source":"AI","message":f"AI suggested: ${ai:,}/yr"}

@app.post("/companies")
async def create_company(company: CompanyCreate, background_tasks: BackgroundTasks):
    db=get_db(); cursor=db.cursor()
    cursor.execute("INSERT INTO companies (campaign_id,name,industry,location,size_range,website,job_title,salary_range,job_posted,source,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (company.campaign_id,company.name,company.industry,company.location,company.size_range,company.website,company.job_title,company.salary_range,company.job_posted,company.source,company.notes))
    db.commit(); cid=cursor.lastrowid; cursor.close(); db.close()
    background_tasks.add_task(autofind_contacts_for_companies, [cid], company.campaign_id)
    return {"message":"Company added! Finding contacts in background...","id":cid}

@app.post("/companies/bulk")
async def bulk_add_companies(data: CompanyBulkAdd, background_tasks: BackgroundTasks):
    db=get_db(); cursor=db.cursor(); added=0; saved_ids=[]
    for co in data.companies:
        cursor.execute(
            "INSERT INTO companies (campaign_id,name,industry,location,size_range,website,job_title,salary_range,job_posted,source,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (data.campaign_id,co.name,co.industry,co.location,co.size_range,co.website,
             co.job_title,co.salary_range,co.job_posted,co.source,co.notes))
        saved_ids.append(cursor.lastrowid)
        added+=1
    db.commit(); cursor.close(); db.close()
    # Queue contact-finding as FastAPI background task
    background_tasks.add_task(autofind_contacts_for_companies, saved_ids, data.campaign_id)
    return {"message":f"{added} companies added! Finding contacts in background...","added":added}

def autofind_contacts_for_companies(company_ids: list, campaign_id: int):
    """
    FastAPI BackgroundTask — runs after response is returned.
    For each saved company: get domain → Hunter → Apollo → LinkedIn fallback → save contacts.
    """
    import time
    print(f"[AutoFind] Starting for {len(company_ids)} companies")
    
    # Get campaign target role
    try:
        db=get_db(); c=db.cursor(dictionary=True)
        c.execute("SELECT target_role FROM campaigns WHERE id=%s", (campaign_id,))
        camp=c.fetchone(); c.close(); db.close()
        target_role = camp["target_role"] if camp else "Recruiter"
    except Exception as e:
        print(f"[AutoFind] Campaign fetch error: {e}"); target_role="Recruiter"

    decision_titles = get_contact_titles(target_role) if target_role else [
        "CEO","President","Managing Director","VP of Talent","Head of Talent","Director of Recruiting",
        "VP of HR","Head of HR","Chief People Officer","Talent Acquisition Manager","Recruiting Manager"]

    for cid in company_ids:
        try:
            # Fetch company
            db=get_db(); c=db.cursor(dictionary=True)
            c.execute("SELECT * FROM companies WHERE id=%s", (cid,))
            company=c.fetchone(); c.close(); db.close()
            if not company: continue

            cname = company["name"]
            website = company.get("website","") or ""
            print(f"[AutoFind] Processing: {cname}")

            # Get domain
            domain = ""
            try:
                cb = get_domain_clearbit(cname)
                if cb:
                    raw = cb.replace("https://","").replace("http://","").replace("www.","").split("/")[0]
                    parts = raw.split(".")
                    domain = ".".join(parts[-2:]) if len(parts)>2 else raw
            except: pass

            if not domain and website:
                try:
                    raw = website.replace("https://","").replace("http://","").replace("www.","").split("/")[0]
                    parts = raw.split(".")
                    domain = ".".join(parts[-2:]) if len(parts)>2 else raw
                except: pass

            if not domain:
                # Try domain from company name
                variations = get_domain_variations(cname, website)
                if variations:
                    domain = variations[0].replace("https://","").replace("http://","").replace("www.","").split("/")[0]

            print(f"[AutoFind] {cname} → domain: {domain or 'not found'}")

            all_people = []

            # Source 1: Apollo (best coverage for decision makers)
            try:
                apollo_people = get_apollo_top_people(cname, domain, decision_titles)
                if apollo_people:
                    all_people.extend(apollo_people)
                    print(f"[AutoFind] Apollo found {len(apollo_people)} for {cname}")
            except Exception as e:
                print(f"[AutoFind] Apollo error: {e}")

            # Source 2: Hunter (good for verified emails)
            if domain:
                try:
                    hunter_people = search_domain_hunter(domain, decision_titles)
                    if hunter_people:
                        # Add only new people not already found
                        existing_names = {p.get("full_name","").lower() for p in all_people}
                        new_hunter = [p for p in hunter_people if p.get("full_name","").lower() not in existing_names]
                        all_people.extend(new_hunter)
                        print(f"[AutoFind] Hunter added {len(new_hunter)} for {cname}")
                except Exception as e:
                    print(f"[AutoFind] Hunter error: {e}")

            # Source 3: LinkedIn scraper fallback
            if not all_people:
                try:
                    seen_names=set()
                    for title in decision_titles[:4]:
                        for p in search_people_linkedin_scraper(cname, title):
                            if p.get("full_name","") not in seen_names:
                                seen_names.add(p["full_name"]); all_people.append(p)
                    if all_people: print(f"[AutoFind] LinkedIn found {len(all_people)} for {cname}")
                except Exception as e:
                    print(f"[AutoFind] LinkedIn error: {e}")

            # Source 4: Snov domain search — finds emails by company domain
            if not all_people and domain:
                try:
                    cid=os.getenv("SNOV_CLIENT_ID",""); cs=os.getenv("SNOV_CLIENT_SECRET","")
                    if cid and cs:
                        token=requests.post("https://api.snov.io/v1/oauth/access_token",
                            data={"grant_type":"client_credentials","client_id":cid,"client_secret":cs},timeout=10).json().get("access_token")
                        if token:
                            result=requests.post("https://api.snov.io/v2/domain-search",
                                data={"access_token":token,"domain":domain,"type":"personal","limit":10},timeout=15).json()
                            emails_found = result.get("emails",[])
                            for e in emails_found:
                                full_name = f"{e.get('firstName','')} {e.get('lastName','')}".strip()
                                title = e.get("currentJob",[{}])[0].get("title","") if e.get("currentJob") else ""
                                email = e.get("email","")
                                if full_name and email:
                                    all_people.append({
                                        "full_name": full_name,
                                        "title": title,
                                        "email": email,
                                        "confidence_score": 78,
                                        "source": "snov"
                                    })
                            if emails_found: print(f"[AutoFind] Snov found {len(emails_found)} for {cname}")
                except Exception as e:
                    print(f"[AutoFind] Snov error: {e}")

            if not all_people:
                print(f"[AutoFind] No contacts found for {cname}"); time.sleep(0.2); continue

            # Save contacts to DB
            db=get_db(); c=db.cursor(); saved=0
            for person in all_people:
                try:
                    full_name = person.get("full_name","").strip()
                    if not full_name or full_name.lower() in ("none none","none",""):
                        continue
                    # Skip duplicates
                    c.execute("SELECT id FROM leads WHERE full_name=%s AND company_id=%s",
                              (full_name, cid))
                    if c.fetchone(): continue

                    # Try to enrich email if missing
                    email = person.get("email","") or ""
                    email_source = person.get("email_source","unknown")
                    confidence_score = person.get("confidence_score",0)

                    if not email and domain:
                        try:
                            ed = enrich_email_apollo(
                                person.get("first_name",""), person.get("last_name",""),
                                domain, person.get("linkedin_url",""), person.get("apollo_id",""))
                            if ed.get("email"):
                                email=ed["email"]; email_source=ed["email_source"]
                                confidence_score=ed["confidence_score"]
                        except: pass

                    # Skip contacts with no email — must have email to save
                    person_title = person.get("title","").lower()
                    if not email:
                        print(f"[AutoFind] Skipping {full_name} — no email found")
                        continue

                    # Title relevance check — keyword based, not exact match
                    RELEVANT_KEYWORDS = [
                        "talent","recruit","hr","human resource","people","staffing",
                        "hiring","acquisition","workforce","personnel","vp","vice president",
                        "director","chief","head of","president","founder","ceo","coo","cto",
                        "partner","managing","principal","executive"
                    ]
                    title_match = any(kw in person_title for kw in RELEVANT_KEYWORDS)
                    if not title_match:
                        print(f"[AutoFind] Skipping {full_name} ({person.get('title','')}) — title not relevant")
                        continue

                    # Auto-delete rule: skip contacts with email but low confidence
                    if email and confidence_score < 70:
                        print(f"[AutoFind] Skipping {full_name} — low confidence ({confidence_score})")
                        continue

                    c.execute("""INSERT INTO leads
                        (first_name,last_name,full_name,title,company,industry,location,
                         email,email_verified,email_source,confidence_score,
                         linkedin_url,target_role,campaign_id,company_id,notes)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (person.get("first_name",""), person.get("last_name",""), full_name,
                         person.get("title","Contact"), cname,
                         company.get("industry",""), person.get("location","") or company.get("location",""),
                         email, 1 if email else 0, email_source, confidence_score,
                         person.get("linkedin_url",""), target_role,
                         campaign_id, cid, "Auto-found on company save"))
                    saved+=1
                except Exception as e:
                    print(f"[AutoFind] Save error for {full_name}: {e}")

            db.commit(); c.close(); db.close()
            print(f"[AutoFind] {cname}: saved {saved} contacts")
            time.sleep(0.5)

        except Exception as e:
            print(f"[AutoFind] Error processing company {cid}: {e}")

    print(f"[AutoFind] Done — processed {len(company_ids)} companies")


@app.get("/companies")
async def get_companies(campaign_id: Optional[int] = None):
    db=get_db(); cursor=db.cursor(dictionary=True)
    if campaign_id:
        cursor.execute("""SELECT comp.*,COUNT(l.id) as total_contacts,SUM(CASE WHEN l.status='responded' THEN 1 ELSE 0 END) as responded,SUM(CASE WHEN l.status='emailed' THEN 1 ELSE 0 END) as emailed
            FROM companies comp LEFT JOIN leads l ON l.company_id=comp.id WHERE comp.campaign_id=%s GROUP BY comp.id ORDER BY comp.created_at DESC""",(campaign_id,))
    else:
        cursor.execute("""SELECT comp.*,c.name as campaign_name,COUNT(l.id) as total_contacts FROM companies comp LEFT JOIN campaigns c ON c.id=comp.campaign_id LEFT JOIN leads l ON l.company_id=comp.id GROUP BY comp.id ORDER BY comp.created_at DESC""")
    result=cursor.fetchall(); cursor.close(); db.close(); return result

@app.get("/lookup-domain")
async def lookup_domain(company_name: str, website: str = ""):
    if website:
        domain=website.replace("https://","").replace("http://","").replace("www.","").split("/")[0].strip()
        if domain and "." in domain: return {"domain":domain,"source":"website","confidence":"high"}
    cb=get_domain_clearbit(company_name)
    if cb: return {"domain":cb,"source":"clearbit","confidence":"high"}
    clean=re.sub(r'[^a-z0-9]','',company_name.lower())
    return {"domain":f"{clean}.com","source":"guess","confidence":"low"}

@app.get("/find-email-by-name")
async def find_email_by_name(first_name: str, last_name: str, domain: str):
    r=find_email_by_name_snov(first_name,last_name,domain)
    if r["found"]: return r
    p=find_email_prospeo(first_name=first_name,last_name=last_name,company_domain=domain)
    if p["found"]: return p
    return {"found":False,"email":"","confidence_score":0,"message":"Email not found. Enter manually."}

@app.post("/companies/{company_id}/auto-find-contacts")
async def auto_find_contacts(company_id: int, domain_override: str = ""):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM companies WHERE id=%s",(company_id,))
    company=cursor.fetchone()
    if not company: raise HTTPException(404,"Company not found")
    cursor.execute("SELECT * FROM campaigns WHERE id=%s",(company["campaign_id"],))
    campaign=cursor.fetchone()
    target_role=campaign["target_role"] if campaign else ""
    cursor.close(); db.close()
    decision_titles=get_contact_titles(target_role) if target_role else ["CEO","President","VP of HR","Head of People","Talent Acquisition Manager","CTO","CFO"]
    if domain_override: raw_domain=domain_override
    else:
        cb_domain=get_domain_clearbit(company["name"])
        if cb_domain: raw_domain=cb_domain
        else: raw_domain=(get_domain_variations(company["name"],company.get("website","")) or [""])[0]
    if raw_domain:
        parts=raw_domain.replace("https://","").replace("http://","").replace("www.","").split("/")[0].split(".")
        domain=".".join(parts[-2:]) if len(parts)>2 else raw_domain
    else: domain=""
    print(f"Domain resolved: {company['name']} → {domain}")
    company_name=company["name"]
    # Apollo first (paid plan — best coverage)
    all_people=get_apollo_top_people(company_name,domain,decision_titles)
    # Hunter second
    if domain:
        hunter_people=search_domain_hunter(domain,decision_titles)
        existing_names={p.get("full_name","").lower() for p in all_people}
        for p in hunter_people:
            if p.get("full_name","").lower() not in existing_names:
                all_people.append(p)
    # LinkedIn scraper fallback
    if not all_people:
        seen_names=set()
        for title in decision_titles[:5]:
            for p in search_people_linkedin_scraper(company_name,title):
                if p["full_name"] not in seen_names: seen_names.add(p["full_name"]); all_people.append(p)
    # Try LinkedIn scraper for every title before giving up
    if not all_people:
        seen_names=set()
        for title in decision_titles[:6]:
            try:
                for p in search_people_linkedin_scraper(company_name, title):
                    if p.get("full_name","") not in seen_names:
                        seen_names.add(p["full_name"]); all_people.append(p)
            except: pass
        if all_people: print(f"[FindDM] LinkedIn scraper found {len(all_people)} for {company_name}")

    # Try Snov domain search as last resort
    if not all_people and domain:
        try:
            cid_s=os.getenv("SNOV_CLIENT_ID",""); cs_s=os.getenv("SNOV_CLIENT_SECRET","")
            if cid_s and cs_s:
                token=requests.post("https://api.snov.io/v1/oauth/access_token",
                    data={"grant_type":"client_credentials","client_id":cid_s,"client_secret":cs_s},timeout=10).json().get("access_token")
                if token:
                    result=requests.post("https://api.snov.io/v2/domain-search",
                        data={"access_token":token,"domain":domain,"type":"personal","limit":10},timeout=15).json()
                    for e in result.get("emails",[]):
                        full_name=f"{e.get('firstName','')} {e.get('lastName','')}".strip()
                        if full_name:
                            all_people.append({
                                "first_name":e.get("firstName",""),"last_name":e.get("lastName",""),
                                "full_name":full_name,"title":e.get("currentJob",[{}])[0].get("title","") if e.get("currentJob") else "",
                                "email":e.get("email",""),"confidence_score":78,"source":"snov","linkedin_url":""
                            })
                    if all_people: print(f"[FindDM] Snov found {len(all_people)} for {company_name}")
        except Exception as e: print(f"[FindDM] Snov error: {e}")

    if not all_people:
        rows=[]
        for title in decision_titles[:6]:
            q1=requests.utils.quote(f'"{title}" "{company_name}"'); q2=requests.utils.quote(f'intitle:"{title}" "{company_name}"')
            rows.append({"title":title,"linkedin_url":f"https://www.linkedin.com/search/results/people/?keywords={q1}&origin=GLOBAL_SEARCH_HEADER","google_url":f"https://www.google.com/search?q=site:linkedin.com/in+{q2}"})
        return {"success":False,"manual_mode":True,"contacts_added":0,"company_name":company_name,"domain":domain,"decision_titles":decision_titles[:6],"rows":rows,"message":"No automated results. Use search links below."}
    db=get_db(); cursor=db.cursor(); added=0; skipped=0; enriched=[]
    for person in all_people:
        cursor.execute("SELECT id FROM leads WHERE full_name=%s AND company_id=%s",(person["full_name"],company_id))
        if cursor.fetchone(): skipped+=1; enriched.append({**person,"email":"exists","saved":False}); continue
        email=person.get("email",""); email_source=person.get("email_source","unknown"); confidence_score=person.get("confidence_score",0)
        if not email:
            ed=enrich_email_apollo(person["first_name"],person["last_name"],domain,person.get("linkedin_url",""),person.get("apollo_id",""))
            if not ed["email"]: ed=enrich_with_email(person["first_name"],person["last_name"],person.get("linkedin_url",""),domain)
            email=ed["email"]; email_source=ed["email_source"]; confidence_score=ed["confidence_score"]
            if ed.get("full_name") and ed.get("first_name"):
                person["first_name"]=ed["first_name"]; person["last_name"]=ed.get("last_name",""); person["full_name"]=ed["full_name"]
        # Skip contacts with no email — don't save unverified/empty
        if not email:
            print(f"[FindDM] Skipping {person.get('full_name','')} — no email found")
            continue
        # Auto-quality filter: skip if confidence too low
        if confidence_score < 70:
            print(f"[FindDM] Skipping {person.get('full_name','')} — low confidence ({confidence_score})")
            continue
        company_industry=company.get("industry","") or ""; company_size=company.get("size_range","") or ""
        notes_str=f"Auto-found | Industry: {company_industry} | Size: {company_size}" if company_industry or company_size else "Auto-found via Hunter/Apollo/LinkedIn"
        cursor.execute("""INSERT INTO leads (first_name,last_name,full_name,title,company,industry,location,email,email_verified,email_source,confidence_score,linkedin_url,target_role,campaign_id,company_id,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (person["first_name"],person["last_name"],person["full_name"],person["title"],company_name,company_industry,person.get("location","") or company.get("location",""),email,1 if email else 0,email_source,confidence_score,person.get("linkedin_url",""),target_role,company["campaign_id"],company_id,notes_str))
        added+=1; enriched.append({**person,"email":email,"email_source":email_source,"confidence_score":confidence_score,"saved":True})
    db.commit(); cursor.close(); db.close()
    return {"success":True,"contacts_added":added,"contacts_skipped":skipped,"total_found":len(all_people),"people":enriched,"message":f"Found {len(all_people)} decision makers at {company_name}, saved {added}"}

@app.delete("/companies/{company_id}")
async def delete_company(company_id: int):
    db=get_db(); cursor=db.cursor()
    cursor.execute("DELETE FROM leads WHERE company_id=%s",(company_id,))
    cursor.execute("DELETE FROM companies WHERE id=%s",(company_id,))
    db.commit(); cursor.close(); db.close(); return {"message":"Company deleted"}

@app.post("/leads")
async def create_lead(lead: LeadCreate):
    db=get_db(); cursor=db.cursor()
    cursor.execute("INSERT INTO leads (first_name,last_name,full_name,title,company,industry,location,email,email_source,confidence_score,linkedin_url,target_role,campaign_id,company_id,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (lead.first_name,lead.last_name,f"{lead.first_name} {lead.last_name}",lead.title,lead.company,lead.industry,lead.location,lead.email,"manual",CONFIDENCE["manual"],lead.linkedin_url,lead.target_role,lead.campaign_id,lead.company_id,lead.notes))
    db.commit(); lid=cursor.lastrowid; cursor.close(); db.close(); return {"message":"Lead added!","id":lid}

@app.get("/leads/stats")
async def get_stats():
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("""SELECT COUNT(*) as total_leads,COALESCE(SUM(CASE WHEN status='new' THEN 1 ELSE 0 END),0) as new_leads,COALESCE(SUM(CASE WHEN status='emailed' THEN 1 ELSE 0 END),0) as emailed,COALESCE(SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END),0) as responded,COALESCE(SUM(CASE WHEN status='soft_rejection' THEN 1 ELSE 0 END),0) as soft_rejection,COALESCE(SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END),0) as rejected,COALESCE(SUM(CASE WHEN status='unsubscribed' THEN 1 ELSE 0 END),0) as unsubscribed,COALESCE(SUM(CASE WHEN status='no_response' THEN 1 ELSE 0 END),0) as no_response FROM leads""")
    r=cursor.fetchone(); cursor.close(); db.close(); return r


@app.post("/leads/cleanup-low-confidence")
async def cleanup_low_confidence(campaign_id: int = 0):
    """Delete saved leads that have email but confidence < 70"""
    db=get_db(); cursor=db.cursor()
    if campaign_id:
        cursor.execute(
            "DELETE FROM leads WHERE confidence_score < 70 AND email != '' AND email IS NOT NULL AND campaign_id=%s",
            (campaign_id,))
    else:
        cursor.execute(
            "DELETE FROM leads WHERE confidence_score < 70 AND email != '' AND email IS NOT NULL")
    deleted = cursor.rowcount
    db.commit(); cursor.close(); db.close()
    return {"success": True, "deleted": deleted, "message": f"Removed {deleted} low-confidence leads"}


@app.get("/campaigns/{campaign_id}/leads-by-role")
async def get_leads_by_role(campaign_id: int):
    """Returns unique titles saved in a campaign's leads"""
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT l.title, COUNT(*) as count
        FROM leads l
        WHERE l.campaign_id=%s AND l.title IS NOT NULL AND l.title != ''
        GROUP BY l.title
        ORDER BY count DESC
    """, (campaign_id,))
    roles = cursor.fetchall()
    cursor.close(); db.close()
    return {"roles": roles, "total_roles": len(roles)}

@app.post("/campaigns/{campaign_id}/bulk-send-by-role")
async def bulk_send_by_role(campaign_id: int, request: BulkEmailRequest, background_tasks: BackgroundTasks):
    """Bulk send email to leads of a specific role/title in a campaign"""
    if not request.title_filter:
        raise HTTPException(400, "title_filter is required for role-based bulk send")
    
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("""
        SELECT l.* FROM leads l
        WHERE l.campaign_id=%s 
        AND l.title=%s
        AND l.email IS NOT NULL AND l.email != ''
        AND l.confidence_score >= 70
        AND l.status NOT IN ('rejected','unsubscribed')
    """, (campaign_id, request.title_filter))
    leads = cursor.fetchall(); cursor.close(); db.close()

    if not leads:
        return {"success": False, "message": f"No eligible leads found with title: {request.title_filter}"}

    # Get Gmail credentials
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_email_settings LIMIT 1")
    user_email = cursor.fetchone(); cursor.close(); db.close()
    gmail_user = (user_email["gmail_user"] if user_email else None) or os.getenv("GMAIL_USER","")
    gmail_pass = (user_email["gmail_app_password"] if user_email else None) or os.getenv("GMAIL_APP_PASSWORD","")
    if not gmail_user or not gmail_pass:
        return {"success": False, "message": "Gmail not configured. Go to Email Settings first."}

    sent=0; failed=0; results=[]
    sc = request.scenario.strip() if request.scenario else ""
    for lead in leads:
        try:
            if sc:
                prompt = f"Expert cold email writer.\nLead: {lead['first_name']} {lead.get('last_name','')}, {lead['title']} at {lead['company']}\nUser intent: {sc}\nMax 75 words. One CTA.\nReturn ONLY:\nSubject: [subject]\nBody: [body]"
            else:
                prompt = f"Write short cold email to {lead['title']}.\nLead: {lead['first_name']} {lead.get('last_name','')}, {lead['title']} at {lead['company']}\nMax 75 words. Clear CTA.\nReturn ONLY:\nSubject: [subject]\nBody: [body]"
            ai_r = client.chat.completions.create(model="llama-3.3-70b-versatile",max_tokens=500,messages=[{"role":"user","content":prompt}])
            raw = ai_r.choices[0].message.content.strip()
            subject = raw.split("Subject:",1)[1].split("\n")[0].strip() if "Subject:" in raw else f"Quick note — {lead['company']}"
            body = raw.split("Body:",1)[1].strip() if "Body:" in raw else raw
            body = body + get_email_footer(lead["id"])
            result = send_via_resend(lead["email"], subject, body)
            if result["success"]:
                db2=get_db(); c2=db2.cursor()
                c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW() WHERE id=%s",(lead["id"],))
                db2.commit(); c2.close(); db2.close()
                sent+=1
                results.append({"name":lead.get("full_name",""),"email":lead["email"],"title":lead["title"],"status":"sent"})
            else:
                failed+=1
                results.append({"name":lead.get("full_name",""),"email":lead["email"],"title":lead["title"],"status":"failed","error":result["error"]})
            time.sleep(1)
        except Exception as e:
            failed+=1
            results.append({"name":lead.get("full_name",""),"email":lead["email"],"title":lead["title"],"status":"failed","error":str(e)})
    return {"success":True,"message":f"Sent {sent} emails to {request.title_filter}s — {failed} failed","sent":sent,"failed":failed,"role":request.title_filter,"results":results}


@app.get("/unsubscribe")
async def unsubscribe_lead(token: str, response: Response):
    """One-click unsubscribe — marks lead as unsubscribed"""
    try:
        import base64
        lead_id = int(base64.urlsafe_b64decode(token + "==").decode())
        db=get_db(); cursor=db.cursor()
        cursor.execute("UPDATE leads SET status='unsubscribed' WHERE id=%s", (lead_id,))
        db.commit(); cursor.close(); db.close()
        return HTMLResponse(content="""
        <html><body style="font-family:Arial,sans-serif;text-align:center;padding:60px;background:#f5f5f5">
        <div style="background:white;border-radius:12px;padding:40px;max-width:400px;margin:0 auto;box-shadow:0 2px 12px rgba(0,0,0,0.1)">
        <div style="font-size:48px;margin-bottom:16px">✓</div>
        <h2 style="color:#2E8B57;margin-bottom:12px">Unsubscribed successfully</h2>
        <p style="color:#666;font-size:14px">You will not receive any more emails from this sender.</p>
        </div></body></html>
        """)
    except Exception as e:
        return HTMLResponse(content="<html><body>Invalid unsubscribe link</body></html>", status_code=400)

def get_unsubscribe_link(lead_id: int, base_url: str = "http://localhost:8000") -> str:
    """Generate unsubscribe token for a lead"""
    import base64
    token = base64.urlsafe_b64encode(str(lead_id).encode()).decode().rstrip("=")
    return f"{base_url}/unsubscribe?token={token}"

def get_email_footer(lead_id: int) -> str:
    """Standard CAN-SPAM compliant footer"""
    unsub_link = get_unsubscribe_link(lead_id)
    return f"""

---
This email was sent to you as part of a professional outreach. If you'd like to stop receiving emails, click here to unsubscribe: {unsub_link}"""


@app.post("/check-spam-score")
async def check_spam_score(request: dict = Body(...)):
    """Check email content for spam triggers before sending"""
    subject = request.get("subject","")
    body = request.get("body","")
    
    issues = []
    score = 10  # Start at 10, deduct for issues
    
    # Check subject line
    if subject.isupper() and len(subject) > 3:
        issues.append({"field":"subject","issue":"ALL CAPS subject line","severity":"high"})
        score -= 2
    
    spam_words = ["free","guarantee","urgent","act now","limited time","click here",
                  "make money","earn money","winner","congratulations","no obligation",
                  "risk free","best price","lowest price","amazing","incredible",
                  "don't miss","once in a lifetime","order now","buy now"]
    
    found_spam = [w for w in spam_words if w.lower() in (subject+body).lower()]
    if found_spam:
        issues.append({"field":"body","issue":f"Spam trigger words: {', '.join(found_spam[:3])}","severity":"medium"})
        score -= len(found_spam) * 0.5
    
    # Check for excessive exclamation marks
    excl_count = (subject+body).count("!")
    if excl_count > 3:
        issues.append({"field":"body","issue":f"{excl_count} exclamation marks — reduce to 1-2","severity":"medium"})
        score -= 1
    
    # Check for missing personalization
    if lead_name_tokens := [t for t in ["{first_name}","Hi,","Hello,","Dear"] if t in body]:
        pass
    elif not any(p in body for p in ["Hi ","Hello ","Dear "]):
        issues.append({"field":"body","issue":"No personalization detected — add lead name","severity":"low"})
        score -= 0.5
    
    # Check length
    word_count = len(body.split())
    if word_count > 150:
        issues.append({"field":"body","issue":f"Email is {word_count} words — keep under 100 for cold email","severity":"low"})
        score -= 0.5
    
    # Check unsubscribe
    if "unsubscribe" not in body.lower():
        issues.append({"field":"body","issue":"No unsubscribe link — CAN-SPAM violation risk","severity":"high"})
        score -= 1.5
    
    score = max(0, min(10, round(score, 1)))
    
    return {
        "score": score,
        "grade": "Excellent" if score >= 8 else "Good" if score >= 6 else "Needs work" if score >= 4 else "High spam risk",
        "color": "green" if score >= 8 else "orange" if score >= 6 else "red",
        "issues": issues,
        "word_count": word_count,
        "is_safe": score >= 6
    }


@app.get("/warmup-settings")
async def get_warmup_settings():
    """Get current warm-up settings"""
    db=get_db(); cursor=db.cursor(dictionary=True)
    # Store in a simple settings table or env
    cursor.execute("SELECT * FROM user_email_settings LIMIT 1")
    s=cursor.fetchone(); cursor.close(); db.close()
    if s:
        daily_limit = s.get("daily_send_limit", 20)
    else:
        daily_limit = 20
    # Count emails sent today
    db=get_db(); cursor=db.cursor()
    cursor.execute("SELECT COUNT(*) FROM leads WHERE DATE(emailed_at)=CURDATE() AND emailed_at IS NOT NULL")
    sent_today = cursor.fetchone()[0]; cursor.close(); db.close()
    return {"daily_limit": daily_limit, "sent_today": sent_today, "remaining": max(0, daily_limit - sent_today)}

@app.post("/warmup-settings")
async def save_warmup_settings(limit: int = Body(..., embed=True)):
    """Update daily send limit"""
    db=get_db(); cursor=db.cursor()
    cursor.execute("UPDATE user_email_settings SET daily_send_limit=%s", (limit,))
    if cursor.rowcount == 0:
        cursor.execute("INSERT INTO user_email_settings (daily_send_limit) VALUES (%s)", (limit,))
    db.commit(); cursor.close(); db.close()
    return {"success": True, "daily_limit": limit}

@app.get("/leads")
async def get_leads(campaign_id: Optional[int] = None, company_id: Optional[int] = None):
    db=get_db(); cursor=db.cursor(dictionary=True)
    q="SELECT l.*,c.name as campaign_name FROM leads l LEFT JOIN campaigns c ON c.id=l.campaign_id LEFT JOIN companies comp ON comp.id=l.company_id WHERE 1=1"
    params=[]
    if campaign_id: q+=" AND l.campaign_id=%s"; params.append(campaign_id)
    if company_id: q+=" AND l.company_id=%s"; params.append(company_id)
    q+=" ORDER BY l.created_at DESC"
    cursor.execute(q,params); result=cursor.fetchall(); cursor.close(); db.close(); return result

@app.get("/leads/{lead_id}")
async def get_lead(lead_id: int):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s",(lead_id,))
    l=cursor.fetchone(); cursor.close(); db.close()
    if not l: raise HTTPException(404,"Lead not found")
    return l

@app.put("/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, body: StatusUpdate):
    new_status=body.status
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT l.*,c.soft_rejection_days,c.hard_rejection_days FROM leads l LEFT JOIN campaigns c ON c.id=l.campaign_id WHERE l.id=%s",(lead_id,))
    lead=cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404,"Lead not found")
    if lead.get("status") in ("rejected","unsubscribed") and new_status not in ("rejected","unsubscribed"):
        raise HTTPException(400,f"Lead is permanently {lead['status']}")
    if new_status=="unsubscribed" and lead.get("email"): add_to_blacklist(lead["email"],"unsubscribed")
    cooldown_until=None
    if new_status=="soft_rejection": cooldown_until=(date.today()+timedelta(days=int(lead.get("soft_rejection_days") or 30))).strftime("%Y-%m-%d")
    elif new_status=="rejected": cooldown_until=(date.today()+timedelta(days=int(lead.get("hard_rejection_days") or 90))).strftime("%Y-%m-%d")
    db=get_db(); cursor=db.cursor()
    if cooldown_until: cursor.execute("UPDATE leads SET status=%s,cooldown_until=%s WHERE id=%s",(new_status,cooldown_until,lead_id))
    else: cursor.execute("UPDATE leads SET status=%s,cooldown_until=NULL WHERE id=%s",(new_status,lead_id))
    db.commit(); cursor.close(); db.close()
    return {"message":f"Status: {new_status}","cooldown_until":cooldown_until}

@app.put("/leads/{lead_id}")
async def update_lead(lead_id: int, body: dict):
    db=get_db(); cursor=db.cursor()
    allowed=['first_name','last_name','title','email','linkedin_url','location','target_role','notes']
    fields=[f"{f}=%s" for f in allowed if f in body]; values=[body[f] for f in allowed if f in body]
    if not fields: raise HTTPException(400,"No fields to update")
    if 'first_name' in body or 'last_name' in body:
        fields.append("full_name=%s"); values.append(f"{body.get('first_name','')} {body.get('last_name','')}".strip())
    if 'email' in body:
        fields.append("email_source=%s"); values.append("manual")
        fields.append("confidence_score=%s"); values.append(CONFIDENCE["manual"])
        fields.append("email_verified=%s"); values.append(0)
    values.append(lead_id)
    cursor.execute(f"UPDATE leads SET {', '.join(fields)} WHERE id=%s",values)
    db.commit(); cursor.close(); db.close(); return {"message":"Lead updated"}

@app.delete("/leads/{lead_id}")
async def delete_lead(lead_id: int):
    db=get_db(); cursor=db.cursor()
    cursor.execute("DELETE FROM emails WHERE lead_id=%s",(lead_id,))
    cursor.execute("DELETE FROM leads WHERE id=%s",(lead_id,))
    db.commit(); cursor.close(); db.close(); return {"message":"Lead deleted"}

@app.post("/verify-email")
async def verify_email(request: EmailVerifyRequest):
    return find_email_prospeo(request.linkedin_url,request.first_name,request.last_name,request.company_domain)

@app.post("/leads/{lead_id}/verify-email")
async def verify_and_save_email(lead_id: int):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s",(lead_id,))
    lead=cursor.fetchone()
    if not lead: raise HTTPException(404,"Lead not found")
    result=find_email_prospeo(lead.get("linkedin_url",""),lead.get("first_name",""),lead.get("last_name",""))
    if result["found"]:
        cursor.execute("UPDATE leads SET email=%s,email_verified=1,email_source=%s,confidence_score=%s WHERE id=%s",
            (result["email"],result.get("email_source","prospeo_linkedin"),result.get("confidence_score",CONFIDENCE["prospeo_linkedin"]),lead_id))
        db.commit()
    cursor.close(); db.close(); return result

@app.get("/leads/{lead_id}/can-send")
async def can_send_check(lead_id: int):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s",(lead_id,))
    lead=cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404,"Lead not found")
    result=can_send_email(lead)
    result["confidence_score"]=lead.get("confidence_score",0); result["email_source"]=lead.get("email_source","unknown"); result["threshold"]=SEND_THRESHOLD
    return result

@app.get("/campaigns/{campaign_id}/rate-limit")
async def get_rate_limit(campaign_id: int): return check_rate_limit(campaign_id)

@app.post("/generate-email")
async def generate_email(request: EmailRequest):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s",(request.lead_id,))
    lead=cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404,"Lead not found")
    if request.email_type in ("cold","followup1","followup2"):
        safety=can_send_email(lead)
        if not safety["safe"]: raise HTTPException(400,f"Email blocked: {safety['reason']}")

    sc = request.scenario.strip() if request.scenario else ""
    fn = lead["first_name"]; ln = lead.get("last_name","")
    title = lead["title"]; company = lead["company"]
    industry = lead.get("industry",""); location = lead.get("location","")
    target = lead.get("target_role","")

    if sc:
        # Smart universal prompt — AI figures out intent from context
        base_info = f"{fn} {ln}, {title} at {company} | Industry: {industry} | Location: {location}"
        if request.email_type == "cold":
            prompt = (
                "You are an expert cold email writer. Write a short professional cold email.\n"
                f"Lead: {base_info}\n"
                f"User intent / context: {sc}\n\n"
                "Read the intent and write the most appropriate cold email:\n"
                "- Recruitment/candidates: pitch pre-vetted candidates, soft 15-min CTA\n"
                "- Product/SaaS/demo: introduce product naturally, ask for demo time\n"
                "- Freelance/portfolio: introduce skills, ask to discuss projects\n"
                "- Student/internship: professional intro, express interest, ask for call\n"
                "- Any other: professional outreach matching the user goal\n"
                "Max 75 words body. Personalized to company. One clear CTA.\n"
                "Return ONLY:\nSubject: [subject]\nBody: [body]"
            )
        elif request.email_type == "followup1":
            prompt = (
                f"Write a short Day 3 follow-up email.\nLead: {base_info}\n"
                f"User intent: {sc}\n"
                "Rules: Max 50 words. Reference previous outreach. New value angle. Soft CTA.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
        elif request.email_type == "followup2":
            prompt = (
                f"Write a short Day 7 final follow-up.\nLead: {fn} at {company}\n"
                f"Context: {sc}\n"
                "Rules: Max 40 words. Brief, respectful, leave door open.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
        elif request.email_type == "soft_rejection":
            prompt = (
                f"Write a warm soft rejection response.\nLead: {fn} at {company}\n"
                "Rules: Max 40 words. Warm, leave door open.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
        else:
            prompt = (
                f"Write a graceful hard rejection response.\nLead: {fn} at {company}\n"
                "Rules: Max 30 words. Professional, leave door open for referrals.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
    else:
        # Default recruitment prompts (no context provided)
        if request.email_type == "cold":
            prompt = (
                "Write a short professional B2B cold email for a US/UK recruitment firm.\n"
                f"Lead: {fn} {ln}, {title} at {company}\n"
                f"Industry: {industry} | Location: {location} | Hiring: {target}\n"
                "Rules: Max 75 words body. Mention company. Focus on pre-vetted candidates. Soft CTA 15-min call. Industry-tailored.\n"
                "Return ONLY:\nSubject: [subject]\nBody: [body]"
            )
        elif request.email_type == "followup1":
            prompt = (
                f"Write Day 3 follow-up for recruitment firm.\nLead: {fn} at {company} | Industry: {industry}\n"
                "Rules: Max 50 words. Reference previous email. Soft CTA only.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
        elif request.email_type == "followup2":
            prompt = (
                f"Write Day 7 final follow-up. Lead: {fn} at {company}\n"
                "Rules: Max 40 words. Brief, respectful, leave door open.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
        elif request.email_type == "soft_rejection":
            prompt = (
                f"Write warm soft rejection response. Lead: {fn} at {company}\n"
                "Rules: Max 40 words. Warm, leave door open.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )
        else:
            prompt = (
                f"Write graceful rejection response. Lead: {fn} at {company}\n"
                "Rules: Max 30 words. Professional, leave door open for referrals.\n"
                "Return ONLY: Subject: [subject]\nBody: [body]"
            )

    if not prompt: raise HTTPException(400,"Invalid email type")
    response=client.chat.completions.create(model="llama-3.3-70b-versatile",messages=[{"role":"user","content":prompt}])
    raw=response.choices[0].message.content.strip()
    subject=""; body=""
    if "Subject:" in raw:
        subject=raw.split("Subject:",1)[1].split("\n")[0].strip()
    if "Body:" in raw: body=raw.split("Body:",1)[1].strip()
    elif subject and "\n" in raw:
        lines=raw.split("\n"); body_lines=[]; past=False
        for line in lines:
            if line.startswith("Subject:"): past=True; continue
            if past and line.strip(): body_lines.append(line)
        body="\n".join(body_lines).strip()
    if not body: body=raw
    # Append Calendly link + CAN-SPAM footer
    try:
        db_s=get_db(); c_s=db_s.cursor(dictionary=True)
        c_s.execute("SELECT calendly_url FROM user_email_settings WHERE calendly_url IS NOT NULL AND calendly_url!='' LIMIT 1")
        es=c_s.fetchone(); c_s.close(); db_s.close()
        cal_url=es.get("calendly_url","") if es else ""
        print(f"[Calendly] URL from DB: {repr(cal_url)}")
    except Exception as e: cal_url=""; print(f"[Calendly] Error: {e}")
    if cal_url and cal_url.strip():
        body=body+f"\n\nBook a 15-min call here: {cal_url.strip()}"
        print(f"[Calendly] Appended to email")
    body=body+get_email_footer(request.lead_id)
    db=get_db(); cursor=db.cursor()
    cursor.execute("INSERT INTO emails (lead_id,subject,body,email_type) VALUES (%s,%s,%s,%s)",(request.lead_id,subject,body,request.email_type))
    # NOTE: Status updated to 'emailed' only when email is actually SENT, not generated
    db.commit(); cursor.close(); db.close()
    return {"subject":subject,"body":body,"email_type":request.email_type,
            "confidence_score":lead.get("confidence_score",0),"email_source":lead.get("email_source","unknown")}


@app.post("/blacklist")
async def add_blacklist(email: str, reason: str = "unsubscribed"):
    add_to_blacklist(email,reason); return {"message":f"{email} added to blacklist"}

@app.get("/blacklist")
async def get_blacklist():
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM email_blacklist ORDER BY added_at DESC")
    result=cursor.fetchall(); cursor.close(); db.close(); return result

@app.delete("/blacklist/{email}")
async def remove_from_blacklist(email: str):
    db=get_db(); cursor=db.cursor()
    cursor.execute("DELETE FROM email_blacklist WHERE email=%s",(email,))
    db.commit(); cursor.close(); db.close(); return {"message":f"{email} removed"}

@app.get("/role-hierarchy/{target_role}")
async def get_role_hierarchy(target_role: str):
    return {"target_role":target_role,"contact_titles":get_contact_titles(target_role)}

@app.get("/search-companies")
async def search_companies_global(q: str = ""):
    if not q or len(q)<2: return []
    results=[]; seen=set()
    db=get_db(); cursor=db.cursor(dictionary=True); like_q=f"%{q}%"
    cursor.execute("""SELECT c.id,c.name,c.industry,c.location,c.website,c.campaign_id,c.salary_range,camp.name as campaign_name,camp.target_role,COUNT(l.id) as total_contacts
        FROM companies c LEFT JOIN campaigns camp ON c.campaign_id=camp.id LEFT JOIN leads l ON l.company_id=c.id
        WHERE c.name LIKE %s OR c.industry LIKE %s OR c.location LIKE %s GROUP BY c.id ORDER BY c.name ASC LIMIT 10""",(like_q,like_q,like_q))
    for co in cursor.fetchall():
        seen.add(co["name"].lower()); results.append({**co,"saved":True,"logo":f"https://logo.clearbit.com/{co.get('website','')}" if co.get('website') else ""})
    cursor.close(); db.close()
    try:
        r=requests.get("https://autocomplete.clearbit.com/v1/companies/suggest",params={"query":q},timeout=8)
        for co in r.json()[:8]:
            name=co.get("name","")
            if not name or name.lower() in seen: continue
            seen.add(name.lower()); domain=co.get("domain","")
            results.append({"id":None,"name":name,"domain":domain,"industry":"","location":"","website":domain,"campaign_id":None,"campaign_name":None,"total_contacts":0,"saved":False,"logo":co.get("logo","")})
    except: pass
    return results[:15]

@app.get("/analytics")
async def get_analytics():
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("""SELECT COUNT(*) as total_leads,SUM(CASE WHEN status='emailed' THEN 1 ELSE 0 END) as total_sent,SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END) as total_replied,SUM(CASE WHEN status='soft_rejection' THEN 1 ELSE 0 END) as soft_rejections,SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as hard_rejections,SUM(CASE WHEN email_verified=1 THEN 1 ELSE 0 END) as verified_emails,AVG(confidence_score) as avg_confidence FROM leads""")
    stats=cursor.fetchone()
    sent=stats.get("total_sent") or 0; replied=stats.get("total_replied") or 0
    stats["response_rate"]=round((replied/sent*100),1) if sent>0 else 0
    cursor.close(); db.close(); return stats

@app.get("/leads/export/csv")
async def export_csv(campaign_id: Optional[int] = None, verified_only: bool = False):
    db=get_db(); cursor=db.cursor(dictionary=True)
    q="""SELECT c.name as campaign,comp.name as company,comp.salary_range,l.first_name,l.last_name,l.title,l.target_role,l.linkedin_url,l.email,l.email_source,l.confidence_score,CASE WHEN l.email_verified=1 THEN 'Verified' ELSE 'Unverified' END as email_status,l.location,l.status,l.cooldown_until,l.emailed_at,l.created_at FROM leads l LEFT JOIN campaigns c ON c.id=l.campaign_id LEFT JOIN companies comp ON comp.id=l.company_id WHERE 1=1"""
    params=[]
    if campaign_id: q+=" AND l.campaign_id=%s"; params.append(campaign_id)
    if verified_only: q+=" AND l.email_verified=1"
    q+=" ORDER BY l.created_at DESC"
    cursor.execute(q,params); leads=cursor.fetchall(); cursor.close(); db.close()
    output=io.StringIO()
    if leads:
        writer=csv.DictWriter(output,fieldnames=leads[0].keys()); writer.writeheader()
        for lead in leads:
            for k in ['created_at','emailed_at']:
                if lead.get(k): lead[k]=str(lead[k])
            writer.writerow(lead)
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]),media_type="text/csv",
        headers={"Content-Disposition":f"attachment; filename=leadflow_export_{date.today()}.csv"})

@app.get("/debug-apollo")
async def debug_apollo(domain: str = "salesforce.com", company: str = "Salesforce"):
    api_key=os.getenv("APOLLO_API_KEY","")
    if not api_key: return {"error":"APOLLO_API_KEY not set"}
    headers={"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key":api_key}
    result={"domain":domain,"api_key_preview":api_key[:8]+"..."}
    r1=requests.post("https://api.apollo.io/api/v1/mixed_people/api_search",headers=headers,
        json={"q_organization_domains_list":[domain],"person_titles":["VP of Sales","CRO","VP of HR","CEO"],"page":1,"per_page":10},timeout=15)
    try:
        d1=r1.json(); raw=d1.get("people",[])
        result["step1_api_search"]={"status":r1.status_code,"count":len(raw),"error":d1.get("error",""),
            "sample":[{"first_name":p.get("first_name"),"title":p.get("title","").split(",")[0],"has_email":p.get("has_email"),"apollo_id":p.get("id","")} for p in raw[:5]]}
    except: result["step1_api_search"]={"failed":True,"status":r1.status_code}; return result
    if raw:
        r2=requests.post("https://api.apollo.io/api/v1/people/match",headers=headers,json={"id":raw[0].get("id"),"reveal_personal_emails":True},timeout=15)
        try:
            d2=r2.json(); person=d2.get("person",{})
            result["step2_people_match"]={"status":r2.status_code,"email":person.get("email",""),"name":f"{person.get('first_name','')} {person.get('last_name','')}","title":person.get("title","")}
        except: result["step2_people_match"]={"failed":True}
    return result

@app.get("/debug-hunter")
async def debug_hunter(domain: str = "allstate.com"):
    api_key=os.getenv("HUNTER_API_KEY","")
    if not api_key: return {"error":"HUNTER_API_KEY not set"}
    try:
        r=requests.get("https://api.hunter.io/v2/domain-search",params={"domain":domain,"api_key":api_key,"limit":10},timeout=15)
        data=r.json(); emails=data.get("data",{}).get("emails",[])
        return {"domain":domain,"status":r.status_code,"total_found":len(emails),
            "sample":[{"name":f"{e.get('first_name','')} {e.get('last_name','')}".strip(),"title":e.get("position",""),"email":e.get("value",""),"confidence":e.get("confidence",0)} for e in emails[:5]],
            "errors":data.get("errors",[])}
    except Exception as e: return {"error":str(e)}

@app.get("/guess-emails")
async def guess_emails_endpoint(first_name: str, last_name: str, domain: str):
    patterns=guess_email_patterns(first_name,last_name,domain)
    snov=find_email_by_name_snov(first_name,last_name,domain)
    return {"patterns":patterns,"warning":"These are guesses — DO NOT auto-send","confirmed":snov.get("email","") if snov.get("found") else ""}

@app.get("/companies/{company_id}/find-person")
async def find_specific_person(company_id: int, query: str = ""):
    if not query or len(query)<2: return {"found":False,"people":[],"message":"Enter a name, title, or email"}
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM companies WHERE id=%s",(company_id,))
    company=cursor.fetchone(); cursor.close(); db.close()
    if not company: raise HTTPException(404,"Company not found")
    cb_domain=get_domain_clearbit(company["name"])
    raw_domain=cb_domain if cb_domain else (get_domain_variations(company["name"],company.get("website","")) or [""])[0]
    if raw_domain:
        parts=raw_domain.replace("https://","").replace("http://","").replace("www.","").split("/")[0].split(".")
        domain=".".join(parts[-2:]) if len(parts)>2 else raw_domain
    else: domain=""
    api_key=os.getenv("APOLLO_API_KEY","")
    headers={"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key":api_key}
    people=[]; query_clean=query.strip()
    if "@" in query_clean:
        return {"found":False,"people":[],"query_type":"email","message":"Add this email manually using the Add Contact button","prefill":{"email":query_clean,"domain":domain}}
    title_words=["ceo","coo","cfo","cto","cmo","chro","cro","vp","vice","president","director","manager","chief","head","officer","talent","hr","recruiter","acquisition","partner","founder"]
    looks_like_title=any(w in query_clean.lower() for w in title_words) or len(query_clean.split())<=3
    if api_key and domain:
        try:
            payload={"q_organization_domains_list":[domain],"person_titles":[query_clean],"page":1,"per_page":5} if looks_like_title else {"q_organization_domains_list":[domain],"q_keywords":query_clean,"page":1,"per_page":5}
            r=requests.post("https://api.apollo.io/api/v1/mixed_people/api_search",headers=headers,json=payload,timeout=15)
            for p in r.json().get("people",[]):
                first=p.get("first_name","")
                if not first: continue
                people.append({"first_name":first,"last_name":"","full_name":first,"title":p.get("title","").split(",")[0].strip(),"linkedin_url":p.get("linkedin_url","") or "","apollo_id":p.get("id",""),"has_email":p.get("has_email",False),"location":"","source":"apollo"})
        except Exception as e: print(f"Find person error: {e}")
    if not people and looks_like_title and domain:
        for p in search_domain_hunter(domain,[query_clean]): people.append(p)
    if not people:
        return {"found":False,"people":[],"query":query_clean,"message":f"No results for '{query_clean}' at {company['name']}. Try adding manually.","prefill":{"title":query_clean if looks_like_title else "","domain":domain}}
    return {"found":True,"people":people[:5],"query":query_clean,"domain":domain,"company_name":company["name"],"message":f"Found {len(people)} result(s)"}


# ── SEND EMAIL ────────────────────────────────────────────────────
@app.post("/leads/{lead_id}/send-email")
async def send_email(lead_id: int, request: SendEmailRequest, session_token: str = Cookie(default=None)):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s",(lead_id,))
    lead=cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404,"Lead not found")
    if not lead.get("email"): raise HTTPException(400,"Lead has no email address")
    result=send_via_resend(lead["email"],request.subject,request.body)
    if result["success"]:
        db2=get_db(); c2=db2.cursor()
        c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW() WHERE id=%s",(lead_id,)); db2.commit(); c2.close(); db2.close()
        return {"success":True,"message":f"Email sent to {lead['email']}","to":lead["email"]}
    raise HTTPException(500,f"Failed to send: {result['error']}")

# ══════════════════════════════════════════════════════════════════
# NEW FEATURES
# ══════════════════════════════════════════════════════════════════

@app.get("/user/email-settings")
async def get_email_settings(session_token: str = Cookie(default=None)):
    user=get_current_user(session_token)
    if not user: raise HTTPException(401,"Not logged in")
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_email_settings WHERE user_id=%s",(user["id"],))
    settings=cursor.fetchone(); cursor.close(); db.close()
    if not settings: return {"gmail_user":user.get("email",""),"gmail_app_password":"","imap_enabled":True,"scan_frequency":30,"configured":False}
    settings["configured"]=bool(settings.get("gmail_app_password"))
    settings["gmail_app_password"]="••••••••" if settings.get("gmail_app_password") else ""
    return settings

@app.post("/user/email-settings")
async def save_email_settings(request: EmailSettingsRequest, session_token: str = Cookie(default=None)):
    user=get_current_user(session_token)
    if not user: raise HTTPException(401,"Not logged in")
    resend_key=os.getenv("RESEND_API_KEY","")
    if resend_key:
        try:
            r=requests.get("https://api.resend.com/domains",headers={"Authorization":f"Bearer {resend_key}"},timeout=10)
            if r.status_code not in (200,403): raise Exception(f"Resend returned {r.status_code}")
        except Exception as e: raise HTTPException(400,f"Connection failed: {str(e)}")
    db=get_db(); cursor=db.cursor()
    cursor.execute("""INSERT INTO user_email_settings (user_id,gmail_user,gmail_app_password,imap_enabled,scan_frequency,calendly_url,base_url,daily_send_limit)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE gmail_user=%s,gmail_app_password=%s,imap_enabled=%s,scan_frequency=%s,calendly_url=%s,base_url=%s,daily_send_limit=%s""",
        (user["id"],request.gmail_user,request.gmail_app_password,request.imap_enabled,request.scan_frequency,
         request.calendly_url,request.base_url or "http://localhost:8000",request.daily_send_limit or 20,
         request.gmail_user,request.gmail_app_password,request.imap_enabled,request.scan_frequency,
         request.calendly_url,request.base_url or "http://localhost:8000",request.daily_send_limit or 20))
    db.commit(); cursor.close(); db.close()
    return {"success":True,"message":"Settings saved ✓ (Emails via Resend API)"}

@app.get("/campaigns/{campaign_id}/cooldown")
async def get_cooldown_settings(campaign_id: int):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT soft_rejection_days,hard_rejection_days FROM campaigns WHERE id=%s",(campaign_id,))
    c=cursor.fetchone(); cursor.close(); db.close()
    if not c: raise HTTPException(404,"Not found")
    return {"soft_rejection_days":c.get("soft_rejection_days") or 30,"hard_rejection_days":c.get("hard_rejection_days") or 90}

@app.put("/campaigns/{campaign_id}/cooldown")
async def update_cooldown_settings(campaign_id: int, settings: CooldownSettings):
    db=get_db(); cursor=db.cursor()
    cursor.execute("UPDATE campaigns SET soft_rejection_days=%s,hard_rejection_days=%s WHERE id=%s",(settings.soft_rejection_days,settings.hard_rejection_days,campaign_id))
    db.commit(); cursor.close(); db.close()
    return {"success":True,"message":"Cooldown settings saved"}

@app.post("/campaigns/{campaign_id}/bulk-send")
async def bulk_send_emails(campaign_id: int, request: BulkEmailRequest, session_token: str = Cookie(default=None)):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM campaigns WHERE id=%s",(campaign_id,))
    campaign=cursor.fetchone()
    if not campaign: raise HTTPException(404,"Campaign not found")
    if campaign.get("is_paused"): return {"success":False,"message":"Campaign is paused. Resume it first.","sent":0,"skipped":0}
    today=date.today()
    cursor.execute("""SELECT * FROM leads WHERE campaign_id=%s AND status NOT IN ('rejected','unsubscribed')
        AND email IS NOT NULL AND email != '' AND confidence_score >= %s
        AND (cooldown_until IS NULL OR cooldown_until <= %s)""",(campaign_id,SEND_THRESHOLD,today))
    eligible=cursor.fetchall(); cursor.close(); db.close()
    if not eligible: return {"success":False,"message":"No eligible leads — all in cooldown or missing emails.","sent":0,"skipped":0}
    sent=0; failed=0; results=[]
    try:
        # Resend API used instead of SMTP
        for lead in eligible:
            try:
                sc_bulk=request.scenario.strip() if request.scenario else ""
                if sc_bulk:
                    prompt=(
                        "You are an expert cold email writer. Write a short professional cold email.\n"
                        f"Lead: {lead['first_name']} {lead.get('last_name','')}, {lead['title']} at {lead['company']}\n"
                        f"Industry: {lead.get('industry','')} | Target: {lead.get('target_role','')}\n"
                        f"User intent: {sc_bulk}\n"
                        "Read intent and write most appropriate email (recruitment/product/freelance/student/other).\n"
                        "Max 75 words. Personalized. Single CTA.\n"
                        "Return ONLY:\nSubject: [subject]\nBody: [body]"
                    )
                else:
                    prompt=(
                        "Write a short professional B2B cold email for a US/UK recruitment firm.\n"
                        f"Lead: {lead['first_name']} {lead.get('last_name','')}, {lead['title']} at {lead['company']}\n"
                        f"Industry: {lead.get('industry','')} | Hiring: {lead.get('target_role','')}\n"
                        "Rules: Max 75 words. Mention company. Focus on pre-vetted candidates. Soft CTA 15-min call.\n"
                        "Return ONLY:\nSubject: [subject]\nBody: [body]"
                    )
                ai_r=client.chat.completions.create(model="llama-3.3-70b-versatile",messages=[{"role":"user","content":prompt}])
                raw=ai_r.choices[0].message.content.strip()
                subject=raw.split("Subject:",1)[1].split("\n")[0].strip() if "Subject:" in raw else f"Opportunity — {lead['company']}"
                body=raw.split("Body:",1)[1].strip() if "Body:" in raw else raw
                send_via_resend(lead["email"],subject,body)
                db2=get_db(); c2=db2.cursor()
                c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW() WHERE id=%s",(lead["id"],)); db2.commit(); c2.close(); db2.close()
                sent+=1; results.append({"name":lead.get("full_name",""),"email":lead["email"],"status":"sent"})
                time.sleep(1)
            except Exception as e: failed+=1; results.append({"name":lead.get("full_name",""),"email":lead["email"],"status":"failed"})
        
    except Exception as e: return {"success":False,"message":f"SMTP error: {str(e)}","sent":sent,"failed":failed}
    return {"success":True,"message":f"Done: {sent} sent, {failed} failed","sent":sent,"failed":failed,"results":results}


@app.get("/followup-queue")
async def get_followup_queue(campaign_id: Optional[int] = None):
    today=date.today()
    db=get_db(); cursor=db.cursor(dictionary=True)
    q="""SELECT l.*,c.name as campaign_name FROM leads l LEFT JOIN campaigns c ON c.id=l.campaign_id
        WHERE l.status='emailed' AND l.emailed_at IS NOT NULL AND (l.cooldown_until IS NULL OR l.cooldown_until <= %s)"""
    params=[today]
    if campaign_id: q+=" AND l.campaign_id=%s"; params.append(campaign_id)
    q+=" ORDER BY l.emailed_at ASC"
    cursor.execute(q,params); leads=cursor.fetchall(); cursor.close(); db.close()
    day3_due=[]; day7_due=[]; upcoming=[]
    for lead in leads:
        emailed=lead.get("emailed_at")
        if not emailed: continue
        if hasattr(emailed,'date'): emailed_date=emailed.date()
        else:
            try: emailed_date=datetime.fromisoformat(str(emailed)).date()
            except: continue
        days_since=(today-emailed_date).days; lead["days_since_email"]=days_since
        if days_since>=7: day7_due.append(lead)
        elif days_since>=3: day3_due.append(lead)
        else: lead["days_until_followup"]=3-days_since; upcoming.append(lead)
    return {"day3_due":day3_due,"day7_due":day7_due,"upcoming":upcoming,"total_due":len(day3_due)+len(day7_due)}

@app.post("/followup-queue/bulk-send")
async def bulk_followup_send(request: BulkEmailRequest, session_token: str = Cookie(default=None)):
    queue=await get_followup_queue(request.campaign_id if request.campaign_id else None)
    leads_to_send=queue["day3_due"] if request.email_type=="followup1" else queue["day7_due"] if request.email_type=="followup2" else queue["day3_due"]+queue["day7_due"]
    if not leads_to_send: return {"success":False,"message":"No leads due for this follow-up","sent":0}
    gmail_user,gmail_pass=get_user_gmail(session_token)
    if not gmail_user: raise HTTPException(400,"Gmail not configured")
    sent=0; failed=0
    try:
        # Resend API used instead of SMTP
        for lead in leads_to_send:
            try:
                label="Day 3 follow-up" if request.email_type=="followup1" else "Day 7 final follow-up"
                prompt=f"""Write short professional B2B {label} email for a recruitment firm.
Lead: {lead["first_name"]} at {lead["company"]}
Rules: Max 50 words. Reference previous email. Soft CTA only.
Return ONLY: Subject: [subject]\nBody: [body]"""
                ai_r=client.chat.completions.create(model="llama-3.3-70b-versatile",messages=[{"role":"user","content":prompt}])
                raw=ai_r.choices[0].message.content.strip()
                subject=raw.split("Subject:",1)[1].split("\n")[0].strip() if "Subject:" in raw else "Following up"
                body_text=raw.split("Body:",1)[1].strip() if "Body:" in raw else raw
                msg=MIMEMultipart("alternative"); msg["Subject"]=subject; msg["From"]=gmail_user; msg["To"]=lead["email"]
                msg.attach(MIMEText(body_text,"plain")); pass  # using Resend instead)
                db2=get_db(); c2=db2.cursor()
                c2.execute("UPDATE leads SET emailed_at=NOW() WHERE id=%s",(lead["id"],)); db2.commit(); c2.close(); db2.close()
                sent+=1; time.sleep(1)
            except: failed+=1
        
    except Exception as e: return {"success":False,"message":str(e),"sent":sent}
    return {"success":True,"sent":sent,"failed":failed,"message":f"{sent} follow-ups sent"}

@app.post("/scan-replies")
async def scan_inbox_for_replies(session_token: str = Cookie(default=None)):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT id,email,full_name,company FROM leads WHERE status='emailed' AND email IS NOT NULL AND email != ''")
    emailed_leads=cursor.fetchall(); cursor.close(); db.close()
    if not emailed_leads: return {"detected":[],"message":"No emailed leads to check"}
    lead_by_email={l["email"].lower():l for l in emailed_leads}; detected=[]
    try:
        mail=imaplib.IMAP4_SSL("imap.gmail.com"); mail.login(gmail_user,gmail_pass); mail.select("inbox")
        since_date=(date.today()-timedelta(days=30)).strftime("%d-%b-%Y")
        _,nums=mail.search(None,f'(SINCE "{since_date}")')
        for num in (nums[0].split() or [])[-100:]:
            try:
                _,msg_data=mail.fetch(num,"(RFC822)")
                msg=email_lib.message_from_bytes(msg_data[0][1])
                from_hdr=msg.get("From","").lower()
                for lead_email,lead in lead_by_email.items():
                    if lead_email in from_hdr:
                        db2=get_db(); c2=db2.cursor()
                        c2.execute("UPDATE leads SET status='responded' WHERE id=%s AND status='emailed'",(lead["id"],))
                        if c2.rowcount>0:
                            db2.commit()
                            detected.append({"lead_id":lead["id"],"name":lead["full_name"],"company":lead["company"],"email":lead_email})
                        c2.close(); db2.close(); break
            except: continue
        mail.logout()
    except Exception as e: return {"detected":detected,"message":f"Scan error: {str(e)}","error":True}
    return {"detected":detected,"message":f"Scan complete. {len(detected)} new replies auto-detected."}

@app.post("/leads/{lead_id}/send-with-attachment")
async def send_with_attachment(
    lead_id: int,
    subject: str = Form(...),
    body: str = Form(...),
    session_token: str = Cookie(default=None),
    attachment: Optional[UploadFile] = File(default=None)
):
    db=get_db(); cursor=db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM leads WHERE id=%s",(lead_id,))
    lead=cursor.fetchone(); cursor.close(); db.close()
    if not lead: raise HTTPException(404,"Lead not found")
    if not lead.get("email"): raise HTTPException(400,"Lead has no email")
    gmail_user,gmail_pass=get_user_gmail(session_token)
    if not gmail_user: raise HTTPException(400,"Gmail not configured")
    try:
        msg=MIMEMultipart(); msg["Subject"]=subject; msg["From"]=gmail_user; msg["To"]=lead["email"]
        msg.attach(MIMEText(body,"plain"))
        if attachment:
            file_content=await attachment.read()
            part=MIMEBase("application","octet-stream"); part.set_payload(file_content)
            encoders.encode_base64(part); part.add_header("Content-Disposition",f"attachment; filename={attachment.filename}")
            msg.attach(part)
        send_via_resend(lead["email"],subject,body)
        db2=get_db(); c2=db2.cursor()
        c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW() WHERE id=%s",(lead_id,)); db2.commit(); c2.close(); db2.close()
        return {"success":True,"to":lead["email"],"attachment":attachment.filename if attachment else None}
    except Exception as e: raise HTTPException(500,str(e))

# ── TRACKING PIXEL & CLICK TRACKING ─────────────────────────
@app.get("/track/open/{lead_id}/{token}")
async def track_open(lead_id: int, token: str):
    from fastapi.responses import Response as FR
    import base64
    try:
        db=get_db(); c=db.cursor()
        c.execute("UPDATE leads SET open_count=COALESCE(open_count,0)+1,last_opened_at=NOW(),email_opened=1 WHERE id=%s",(lead_id,))
        db.commit(); c.close(); db.close()
    except: pass
    pixel=base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
    return FR(content=pixel,media_type="image/gif",headers={"Cache-Control":"no-store"})

@app.get("/track/click/{lead_id}/{token}")
async def track_click(lead_id: int, token: str, url: str = ""):
    from fastapi.responses import RedirectResponse
    try:
        db=get_db(); c=db.cursor()
        c.execute("UPDATE leads SET click_count=COALESCE(click_count,0)+1,last_clicked_at=NOW(),email_clicked=1 WHERE id=%s",(lead_id,))
        db.commit(); c.close(); db.close()
    except: pass
    return RedirectResponse(url=url or "https://google.com")

@app.get("/track/stats/{lead_id}")
async def get_track_stats(lead_id: int):
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT open_count,click_count,last_opened_at,last_clicked_at,email_opened,email_clicked FROM leads WHERE id=%s",(lead_id,))
    row=c.fetchone(); c.close(); db.close()
    return row or {}

def get_tracking_pixel(lead_id: int, base_url: str = "http://localhost:8000") -> str:
    import hashlib
    token=hashlib.md5(f"{lead_id}open".encode()).hexdigest()
    return f'<img src="{base_url}/track/open/{lead_id}/{token}" width="1" height="1" style="display:none" alt="">'

def get_unsubscribe_link(lead_id: int, base_url: str = "http://localhost:8000") -> str:
    import hashlib
    token=hashlib.md5(f"{lead_id}leadflow_unsub_2026".encode()).hexdigest()
    return f"{base_url}/unsubscribe/{token}"

def add_email_footer(body: str, lead_id: int, base_url: str = "http://localhost:8000") -> str:
    unsub_url=get_unsubscribe_link(lead_id,base_url)
    pixel=get_tracking_pixel(lead_id,base_url)
    return body + f"\n\n--\nTo unsubscribe: {unsub_url}\n{pixel}"

# ── CHECK BOUNCES ─────────────────────────────────────────────
@app.post("/check-bounces")
async def check_bounces():
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT * FROM user_email_settings LIMIT 1")
    row=c.fetchone(); c.close(); db.close()
    gmail_user=(row["gmail_user"] if row else None) or os.getenv("GMAIL_USER","")
    gmail_pass=(row["gmail_app_password"] if row else None) or os.getenv("GMAIL_APP_PASSWORD","")
    if not gmail_user: return {"success":False,"message":"Gmail not configured"}
    bounced=[]
    try:
        import imaplib,email as eml,re
        mail=imaplib.IMAP4_SSL("imap.gmail.com"); mail.login(gmail_user,gmail_pass); mail.select("inbox")
        for term in [b'FROM "mailer-daemon"',b'SUBJECT "Delivery Status"',b'SUBJECT "Undeliverable"']: 
            try:
                _,msgs=mail.search(None,term)
                if not msgs[0]: continue
                for num in msgs[0].split()[-20:]:
                    _,data=mail.fetch(num,"(RFC822)")
                    msg=eml.message_from_bytes(data[0][1])
                    body_txt="".join(part.get_payload(decode=True).decode(errors="ignore") for part in msg.walk() if part.get_content_type()=="text/plain") if msg.is_multipart() else msg.get_payload(decode=True).decode(errors="ignore")
                    for ea in re.findall(r'[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}',body_txt):
                        if ea.lower()==gmail_user.lower(): continue
                        db2=get_db(); c2=db2.cursor(dictionary=True)
                        c2.execute("SELECT id,full_name FROM leads WHERE email=%s AND status NOT IN ('unsubscribed','rejected')",(ea,))
                        lead=c2.fetchone(); c2.close(); db2.close()
                        if lead:
                            db3=get_db(); c3=db3.cursor()
                            c3.execute("UPDATE leads SET email_bounced=1,status='rejected' WHERE id=%s",(lead["id"],))
                            db3.commit(); c3.close(); db3.close()
                            bounced.append({"name":lead["full_name"],"email":ea})
            except: continue
        mail.logout()
    except Exception as e: return {"success":False,"message":str(e)}
    return {"success":True,"bounced_count":len(bounced),"bounced":bounced,"message":f"{len(bounced)} bounced emails found"}

# ── PREVIEW BULK EMAIL ────────────────────────────────────────
@app.post("/campaigns/{campaign_id}/preview-bulk-email")
async def preview_bulk_email(campaign_id: int, request: BulkEmailRequest):
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT l.* FROM leads l WHERE l.campaign_id=%s AND l.email IS NOT NULL AND l.email!='' AND l.confidence_score>=70 AND l.status NOT IN ('rejected','unsubscribed') LIMIT 1",(campaign_id,))
    lead=c.fetchone(); c.close(); db.close()
    if not lead: return {"success":False,"message":"No eligible leads in this campaign"}
    sc=request.scenario.strip() if request.scenario else ""
    fn=lead["first_name"]; company=lead["company"]; title=lead["title"]
    prompt=(f"You are an expert cold email writer.\nLead: {fn}, {title} at {company}\nUser intent: {sc}\nWrite appropriate cold email. Max 75 words.\nReturn ONLY:\nSubject: [subject]\nBody: [body]" if sc else f"Write professional B2B cold email.\nLead: {fn}, {title} at {company}\nMax 75 words. Soft CTA.\nReturn ONLY:\nSubject: [subject]\nBody: [body]")
    try:
        r=client.chat.completions.create(model="llama-3.3-70b-versatile",messages=[{"role":"user","content":prompt}])
        raw=r.choices[0].message.content.strip()
        subj=raw.split("Subject:",1)[1].split("\n")[0].strip() if "Subject:" in raw else f"Quick note — {company}"
        body=raw.split("Body:",1)[1].strip() if "Body:" in raw else raw
        return {"success":True,"subject":subj,"body":body,"preview_for":{"name":lead.get("full_name",""),"title":title,"company":company}}
    except Exception as e: return {"success":False,"message":str(e)}

# ── MEETING BOOKING LINK ──────────────────────────────────────
@app.get("/meeting-link-settings")
async def get_meeting_link():
    db=get_db(); c=db.cursor(dictionary=True)
    try:
        c.execute("SELECT calendly_url FROM user_email_settings LIMIT 1")
        row=c.fetchone(); c.close(); db.close()
        return {"calendly_url":row.get("calendly_url","") if row else ""}
    except: c.close(); db.close(); return {"calendly_url":""}

@app.post("/meeting-link-settings")
async def save_meeting_link(request: dict):
    url=request.get("calendly_url","").strip()
    db=get_db(); c=db.cursor()
    try:
        c.execute("UPDATE user_email_settings SET calendly_url=%s",(url,))
        if c.rowcount==0: c.execute("INSERT INTO user_email_settings (calendly_url) VALUES (%s)",(url,))
        db.commit()
    except:
        try: c.execute("ALTER TABLE user_email_settings ADD COLUMN calendly_url VARCHAR(500)"); c.execute("UPDATE user_email_settings SET calendly_url=%s",(url,)); db.commit()
        except: pass
    c.close(); db.close()
    return {"success":True,"calendly_url":url}

# ── SCHEDULE EMAIL (TIMEZONE) ─────────────────────────────────
def get_tz_for_location(location: str) -> str:
    if not location: return "America/New_York"
    loc=location.lower()
    if any(x in loc for x in ["california","los angeles","san francisco","seattle","oregon","nevada"]): return "America/Los_Angeles"
    if any(x in loc for x in ["arizona","phoenix"]): return "America/Phoenix"
    if any(x in loc for x in ["colorado","denver","utah","texas","chicago","illinois","minnesota"]): return "America/Chicago"
    if any(x in loc for x in ["uk","london","england"]): return "Europe/London"
    return "America/New_York"

@app.post("/schedule-email")
async def schedule_email_tz(request: dict):
    lead_id=request.get("lead_id"); target_hour=request.get("target_hour",9)
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT location FROM leads WHERE id=%s",(lead_id,))
    lead=c.fetchone(); c.close(); db.close()
    if not lead: return {"success":False,"message":"Lead not found"}
    tz_name=get_tz_for_location(lead.get("location",""))
    from datetime import datetime,timedelta
    try:
        import pytz
        tz=pytz.timezone(tz_name); now=datetime.now(pytz.utc); pnow=now.astimezone(tz)
        target=pnow.replace(hour=target_hour,minute=0,second=0,microsecond=0)
        if pnow.hour>=target_hour: target+=timedelta(days=1)
        delay=int((target.astimezone(pytz.utc)-now).total_seconds())
    except: delay=0
    return {"success":True,"timezone":tz_name,"send_in_seconds":delay,"delay_hours":round(delay/3600,1)}

# ── CSV IMPORT ────────────────────────────────────────────────
@app.post("/campaigns/{campaign_id}/import-csv")
async def import_csv(campaign_id: int, file: UploadFile = File(...)):
    import csv,io
    content=await file.read()
    try: text=content.decode("utf-8")
    except: text=content.decode("latin-1")
    reader=csv.DictReader(io.StringIO(text))
    headers=reader.fieldnames or []
    col={}
    for h in headers:
        hl=h.lower().strip().replace(" ","_").replace("-","_")
        if hl in ("first_name","firstname","first"): col["first_name"]=h
        elif hl in ("last_name","lastname","last","surname"): col["last_name"]=h
        elif hl in ("name","full_name","fullname","contact"): col["full_name"]=h
        elif hl in ("email","email_address","work_email"): col["email"]=h
        elif hl in ("title","job_title","position","role"): col["title"]=h
        elif hl in ("company","company_name","organization"): col["company"]=h
        elif hl in ("linkedin","linkedin_url","profile_url"): col["linkedin_url"]=h
        elif hl in ("location","city","country"): col["location"]=h
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT target_role FROM campaigns WHERE id=%s",(campaign_id,))
    camp=c.fetchone(); c.close(); db.close()
    target_role=camp["target_role"] if camp else ""
    imported=0; skipped=0
    db=get_db(); c=db.cursor()
    for row in reader:
        try:
            fn=row.get(col.get("first_name",""),"").strip(); ln=row.get(col.get("last_name",""),"").strip()
            full=row.get(col.get("full_name",""),"").strip()
            if not fn and full: parts=full.split(" ",1); fn=parts[0]; ln=parts[1] if len(parts)>1 else ""
            if not full: full=f"{fn} {ln}".strip()
            email=row.get(col.get("email",""),"").strip()
            title=row.get(col.get("title",""),"").strip(); company=row.get(col.get("company",""),"").strip()
            li=row.get(col.get("linkedin_url",""),"").strip(); loc=row.get(col.get("location",""),"").strip()
            if not full and not email: skipped+=1; continue
            if email:
                c.execute("SELECT id FROM leads WHERE email=%s AND campaign_id=%s",(email,campaign_id))
                if c.fetchone(): skipped+=1; continue
            c.execute("INSERT INTO leads (first_name,last_name,full_name,title,company,email,linkedin_url,location,target_role,campaign_id,status,email_source,confidence_score,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'new','csv_import',80,'CSV import')",(fn,ln,full,title,company,email,li,loc,target_role,campaign_id))
            imported+=1
        except: skipped+=1
    db.commit(); c.close(); db.close()
    return {"success":True,"imported":imported,"skipped":skipped,"message":f"{imported} contacts imported, {skipped} skipped","column_map":col}

# ── WEBHOOK ───────────────────────────────────────────────────
@app.get("/webhook-settings")
async def get_webhook():
    db=get_db(); c=db.cursor(dictionary=True)
    try:
        c.execute("SELECT webhook_url,webhook_events FROM user_email_settings LIMIT 1")
        row=c.fetchone(); c.close(); db.close()
        return {"webhook_url":row.get("webhook_url","") if row else "","webhook_events":row.get("webhook_events","responded") if row else "responded"}
    except: c.close(); db.close(); return {"webhook_url":"","webhook_events":"responded"}

@app.post("/webhook-settings")
async def save_webhook(request: dict):
    url=request.get("webhook_url","").strip(); events=request.get("webhook_events","responded")
    db=get_db(); c=db.cursor()
    try:
        c.execute("UPDATE user_email_settings SET webhook_url=%s,webhook_events=%s",(url,events))
        if c.rowcount==0: c.execute("INSERT INTO user_email_settings (webhook_url,webhook_events) VALUES (%s,%s)",(url,events))
        db.commit()
    except:
        try: c.execute("ALTER TABLE user_email_settings ADD COLUMN webhook_url VARCHAR(500)"); c.execute("ALTER TABLE user_email_settings ADD COLUMN webhook_events VARCHAR(200) DEFAULT 'responded'"); c.execute("UPDATE user_email_settings SET webhook_url=%s,webhook_events=%s",(url,events)); db.commit()
        except: pass
    c.close(); db.close(); return {"success":True}

async def fire_webhook(lead_data: dict, event: str):
    db=get_db(); c=db.cursor(dictionary=True)
    try:
        c.execute("SELECT webhook_url,webhook_events FROM user_email_settings LIMIT 1")
        row=c.fetchone(); c.close(); db.close()
    except: c.close(); db.close(); return
    if not row or not row.get("webhook_url"): return
    if event not in (row.get("webhook_events","") or "").split(","): return
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5) as hc:
            await hc.post(row["webhook_url"],json={"event":event,"lead_id":lead_data.get("id"),"name":lead_data.get("full_name",""),"email":lead_data.get("email",""),"company":lead_data.get("company",""),"title":lead_data.get("title","")})
    except: pass

# ── A/B TEST BULK SEND ────────────────────────────────────────
@app.post("/campaigns/{campaign_id}/bulk-send-ab")
async def bulk_send_ab(campaign_id: int, request: BulkEmailRequest, background_tasks: BackgroundTasks):
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT l.* FROM leads l WHERE l.campaign_id=%s AND l.email IS NOT NULL AND l.email!='' AND l.confidence_score>=70 AND l.status NOT IN ('rejected','unsubscribed','emailed') ORDER BY RAND()",(campaign_id,))
    all_leads=c.fetchall(); c.close(); db.close()
    if not all_leads: return {"success":False,"message":"No eligible leads"}
    mid=len(all_leads)//2; ga=all_leads[:mid]; gb=all_leads[mid:]
    background_tasks.add_task(_send_ab_bg,ga,gb,request.scenario or "")
    return {"success":True,"message":f"A/B test started — {len(ga)} in A, {len(gb)} in B","group_a":len(ga),"group_b":len(gb)}

def _send_ab_bg(group_a,group_b,scenario):
    import time
    for variant,leads in [("A",group_a),("B",group_b)]:
        for lead in leads:
            try:
                sc=scenario or ""
                variant_note="Direct professional opener" if variant=="A" else "Start with a compelling question"
                prompt=f"Write short cold email.\nLead: {lead['first_name']}, {lead['title']} at {lead['company']}\n{'Intent: '+sc if sc else 'B2B outreach'}\nVariant {variant}: {variant_note}\nMax 75 words. CTA.\nReturn ONLY:\nSubject: [subject]\nBody: [body]"
                r=client.chat.completions.create(model="llama-3.3-70b-versatile",max_tokens=500,messages=[{"role":"user","content":prompt}])
                raw=r.choices[0].message.content.strip()
                subj=raw.split("Subject:",1)[1].split("\n")[0].strip() if "Subject:" in raw else f"Quick note — {lead['company']}"
                body=raw.split("Body:",1)[1].strip() if "Body:" in raw else raw
                body_full=add_email_footer(body,lead["id"])
                result=send_via_resend(lead["email"],subj,body_full)
                if result["success"]:
                    db2=get_db(); c2=db2.cursor()
                    c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW(),ab_variant=%s WHERE id=%s",(variant,lead["id"]))
                    db2.commit(); c2.close(); db2.close()
                time.sleep(1)
            except Exception as e: print(f"AB err: {e}")

# ── BLACKLIST CHECKER ─────────────────────────────────────────
@app.get("/check-blacklist")
async def check_blacklist():
    db=get_db(); c=db.cursor(dictionary=True)
    try: c.execute("SELECT gmail_user FROM user_email_settings LIMIT 1"); row=c.fetchone(); c.close(); db.close()
    except: c.close(); db.close(); row=None
    gmail_user=(row["gmail_user"] if row else None) or os.getenv("GMAIL_USER","")
    if not gmail_user: return {"success":False,"message":"No Gmail configured"}
    domain=gmail_user.split("@")[1] if "@" in gmail_user else "gmail.com"
    import socket
    bls=["zen.spamhaus.org","bl.spamcop.net","dnsbl.sorbs.net","b.barracudacentral.org","dbl.spamhaus.org"]
    clean=0; flagged=[]
    for bl in bls:
        try: socket.gethostbyname(f"{domain}.{bl}"); flagged.append(bl)
        except socket.gaierror: clean+=1
    return {"domain":domain,"clean_on":clean,"blacklisted_on":flagged,"total_checked":len(bls),"status":"clean" if not flagged else "blacklisted","message":f"Clean on {clean}/{len(bls)} blacklists" if not flagged else f"Listed on {len(flagged)}: {', '.join(flagged)}"}

# ── UNIFIED INBOX ─────────────────────────────────────────────
@app.get("/unified-inbox")
async def unified_inbox(campaign_id: int = 0):
    db=get_db(); c=db.cursor(dictionary=True)
    q="SELECT l.*,ca.name as campaign_name FROM leads l LEFT JOIN campaigns ca ON l.campaign_id=ca.id WHERE l.status IN ('responded','soft_rejection')"
    p=[]
    if campaign_id: q+=" AND l.campaign_id=%s"; p.append(campaign_id)
    q+=" ORDER BY l.updated_at DESC LIMIT 100"
    c.execute(q,p); leads=c.fetchall(); c.close(); db.close()
    return {"leads":leads,"total":len(leads)}

@app.get("/inbox-stats")
async def inbox_stats():
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END) as responded,SUM(CASE WHEN email_opened=1 THEN 1 ELSE 0 END) as opened,SUM(CASE WHEN email_clicked=1 THEN 1 ELSE 0 END) as clicked,SUM(CASE WHEN emailed_at>=DATE_SUB(NOW(),INTERVAL 7 DAY) THEN 1 ELSE 0 END) as sent_week,SUM(COALESCE(open_count,0)) as total_opens FROM leads WHERE emailed_at IS NOT NULL")
    row=c.fetchone(); c.close(); db.close()
    return row or {}

# ── HOT COMPANIES / INTENT SIGNALS ───────────────────────────
@app.get("/campaigns/{campaign_id}/hot-companies")
async def hot_companies(campaign_id: int):
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT co.*,(SELECT COUNT(*) FROM leads l WHERE l.company_id=co.id) as contact_count,(SELECT COUNT(*) FROM leads l WHERE l.company_id=co.id AND l.status='responded') as responded_count FROM companies co WHERE co.campaign_id=%s ORDER BY co.created_at DESC",(campaign_id,))
    companies=c.fetchall(); c.close(); db.close()
    for co in companies:
        score=0; signals=[]
        posted=co.get("job_posted","") or ""
        if any(x in posted.lower() for x in ["today","hours","1 day"]): score+=30; signals.append("Posted today")
        elif any(x in posted.lower() for x in ["2 day","3 day"]): score+=15; signals.append("Posted recently")
        if co.get("salary_range") and co["salary_range"]!="Not disclosed": score+=15; signals.append("Salary disclosed")
        if co.get("contact_count",0)>0: score+=20; signals.append(f"{co['contact_count']} contacts saved")
        if co.get("responded_count",0)>0: score+=30; signals.append("Has a response!")
        co["heat_score"]=score; co["signals"]=signals
        co["heat_label"]="Hot" if score>=50 else "Warm" if score>=25 else "Cold"
    companies.sort(key=lambda x:x["heat_score"],reverse=True)
    return {"companies":companies,"hot_count":sum(1 for c in companies if c["heat_score"]>=50)}


# ═══════════════════════════════════════════════════════════════
# MULTI-USER — INVITE TEAM MEMBER
# ═══════════════════════════════════════════════════════════════
@app.post("/auth/invite")
async def invite_user(request: dict):
    """Create a new team member account"""
    email = request.get("email","").strip().lower()
    role = request.get("role","member")
    password = request.get("password","LeadFlow2026!")
    if not email: raise HTTPException(400,"Email required")
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT id FROM users WHERE username=%s",(email,))
    if c.fetchone(): c.close(); db.close(); return {"success":False,"message":"User already exists"}
    hashed = hash_password(password)
    c2=db.cursor()
    c2.execute("INSERT INTO users (username,email,password_hash,role) VALUES (%s,%s,%s,%s)",(email,email,hashed,role))
    db.commit(); c2.close(); c.close(); db.close()
    return {"success":True,"message":f"Account created for {email}","email":email,"role":role}

@app.get("/auth/team")
async def get_team():
    """Get all team members"""
    db=get_db(); c=db.cursor(dictionary=True)
    try:
        c.execute("SELECT id,username,role,created_at FROM users ORDER BY created_at DESC")
        users=c.fetchall(); c.close(); db.close()
        return {"users":users,"total":len(users)}
    except: c.close(); db.close(); return {"users":[],"total":0}


# ═══════════════════════════════════════════════════════════════
# CONDITIONAL SEQUENCES — BEHAVIOUR-BASED FOLLOW-UPS
# ═══════════════════════════════════════════════════════════════
@app.get("/followup-queue/conditional")
async def get_conditional_queue(campaign_id: int = 0):
    """Returns leads segmented by behaviour for conditional sequences"""
    db=get_db(); c=db.cursor(dictionary=True)
    q = """SELECT l.*, ca.name as campaign_name FROM leads l
           LEFT JOIN campaigns ca ON l.campaign_id=ca.id
           WHERE l.status='emailed' AND l.emailed_at IS NOT NULL"""
    params=[]
    if campaign_id: q+=" AND l.campaign_id=%s"; params.append(campaign_id)
    c.execute(q,params); leads=c.fetchall(); c.close(); db.close()
    from datetime import datetime, date
    opened_due=[]; not_opened_due=[]; clicked_due=[]; no_response_due=[]
    for l in leads:
        if not l.get("emailed_at"): continue
        emailed=l["emailed_at"]
        if isinstance(emailed,str): 
            try: emailed=datetime.fromisoformat(emailed)
            except: continue
        days=(datetime.now()-emailed).days
        if days<3: continue
        if l.get("email_clicked"): clicked_due.append(l)
        elif l.get("email_opened") and days>=3: opened_due.append(l)
        elif not l.get("email_opened") and days>=3: not_opened_due.append(l)
        if days>=7 and not l.get("email_clicked") and not l.get("email_opened"): no_response_due.append(l)
    return {
        "opened_due": opened_due,
        "not_opened_due": not_opened_due,
        "clicked_due": clicked_due,
        "no_response_due": no_response_due,
        "total": len(opened_due)+len(not_opened_due)+len(clicked_due)+len(no_response_due)
    }


# ═══════════════════════════════════════════════════════════════
# CRITICAL FIX 1 — EMAIL TEMPLATE LIBRARY
# ═══════════════════════════════════════════════════════════════
@app.get("/email-templates")
async def get_email_templates(session_token: str = Cookie(default=None)):
    db=get_db(); c=db.cursor(dictionary=True)
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS email_templates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            subject TEXT,
            body TEXT,
            email_type VARCHAR(50) DEFAULT 'cold',
            use_count INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        db.commit()
        c.execute("SELECT * FROM email_templates ORDER BY use_count DESC, created_at DESC")
        templates=c.fetchall()
    except Exception as e:
        templates=[]; print(f"Template error: {e}")
    c.close(); db.close()
    return {"templates": templates, "total": len(templates)}

@app.post("/email-templates")
async def save_email_template(request: dict):
    name=request.get("name","").strip()
    subject=request.get("subject","").strip()
    body=request.get("body","").strip()
    email_type=request.get("email_type","cold")
    if not name or not body: raise HTTPException(400,"Name and body required")
    db=get_db(); c=db.cursor()
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS email_templates (
            id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), subject TEXT, body TEXT,
            email_type VARCHAR(50) DEFAULT 'cold', use_count INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("INSERT INTO email_templates (name,subject,body,email_type) VALUES (%s,%s,%s,%s)",
                  (name,subject,body,email_type))
        db.commit(); tid=c.lastrowid
    except Exception as e: c.close(); db.close(); raise HTTPException(500,str(e))
    c.close(); db.close()
    return {"success":True,"id":tid,"message":f"Template '{name}' saved"}

@app.put("/email-templates/{tid}/use")
async def use_template(tid: int):
    db=get_db(); c=db.cursor()
    c.execute("UPDATE email_templates SET use_count=use_count+1 WHERE id=%s",(tid,))
    db.commit(); c.close(); db.close()
    return {"success":True}

@app.delete("/email-templates/{tid}")
async def delete_template(tid: int):
    db=get_db(); c=db.cursor()
    c.execute("DELETE FROM email_templates WHERE id=%s",(tid,))
    db.commit(); c.close(); db.close()
    return {"success":True}

# ═══════════════════════════════════════════════════════════════
# CRITICAL FIX 2 — LEAD ACTIVITY LOG
# ═══════════════════════════════════════════════════════════════
@app.get("/leads/{lead_id}/activity")
async def get_lead_activity(lead_id: int):
    db=get_db(); c=db.cursor(dictionary=True)
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS lead_activity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            lead_id INT NOT NULL,
            activity_type VARCHAR(50) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX(lead_id))""")
        db.commit()
        c.execute("SELECT * FROM lead_activity WHERE lead_id=%s ORDER BY created_at DESC LIMIT 50",(lead_id,))
        activities=c.fetchall()
    except: activities=[]
    c.close(); db.close()
    return {"activities":activities}

@app.post("/leads/{lead_id}/activity")
async def add_lead_activity(lead_id: int, request: dict):
    activity_type=request.get("activity_type","note")
    description=request.get("description","").strip()
    if not description: raise HTTPException(400,"Description required")
    db=get_db(); c=db.cursor()
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS lead_activity (
            id INT AUTO_INCREMENT PRIMARY KEY, lead_id INT NOT NULL,
            activity_type VARCHAR(50), description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX(lead_id))""")
        c.execute("INSERT INTO lead_activity (lead_id,activity_type,description) VALUES (%s,%s,%s)",
                  (lead_id,activity_type,description))
        db.commit()
    except Exception as e: c.close(); db.close(); raise HTTPException(500,str(e))
    c.close(); db.close()
    return {"success":True}

def log_lead_activity(lead_id: int, activity_type: str, description: str):
    """Auto-log activity when emails sent, status changed etc"""
    try:
        db=get_db(); c=db.cursor()
        c.execute("INSERT INTO lead_activity (lead_id,activity_type,description) VALUES (%s,%s,%s)",
                  (lead_id,activity_type,description))
        db.commit(); c.close(); db.close()
    except: pass

# ═══════════════════════════════════════════════════════════════
# CRITICAL FIX 3 — CAMPAIGN PAUSE / RESUME
# ═══════════════════════════════════════════════════════════════
@app.post("/campaigns/{campaign_id}/pause")
async def pause_campaign(campaign_id: int):
    db=get_db(); c=db.cursor()
    try:
        c.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS is_paused TINYINT(1) DEFAULT 0")
        db.commit()
    except: pass
    c.execute("UPDATE campaigns SET is_paused=1 WHERE id=%s",(campaign_id,))
    db.commit(); c.close(); db.close()
    return {"success":True,"message":"Campaign paused — no emails will be sent until resumed"}

@app.post("/campaigns/{campaign_id}/resume")
async def resume_campaign(campaign_id: int):
    db=get_db(); c=db.cursor()
    c.execute("UPDATE campaigns SET is_paused=0 WHERE id=%s",(campaign_id,))
    db.commit(); c.close(); db.close()
    return {"success":True,"message":"Campaign resumed"}

@app.get("/campaigns/{campaign_id}/status")
async def get_campaign_status(campaign_id: int):
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT id,name,is_paused FROM campaigns WHERE id=%s",(campaign_id,))
    row=c.fetchone(); c.close(); db.close()
    return row or {}

# ═══════════════════════════════════════════════════════════════
# CRITICAL FIX 4 — BULK STATUS UPDATE
# ═══════════════════════════════════════════════════════════════
@app.post("/leads/bulk-status-update")
async def bulk_status_update(request: dict):
    lead_ids=request.get("lead_ids",[])
    status=request.get("status","")
    valid_statuses=["new","emailed","responded","soft_rejection","rejected","unsubscribed"]
    if not lead_ids or status not in valid_statuses:
        raise HTTPException(400,"Invalid request")
    db=get_db(); c=db.cursor()
    placeholders=",".join(["%s"]*len(lead_ids))
    c.execute(f"UPDATE leads SET status=%s WHERE id IN ({placeholders})",[status]+lead_ids)
    updated=c.rowcount; db.commit(); c.close(); db.close()
    return {"success":True,"updated":updated,"message":f"{updated} leads updated to {status}"}

# ═══════════════════════════════════════════════════════════════
# CRITICAL FIX 5 — DAILY EMAIL LOG / AUDIT TRAIL
# ═══════════════════════════════════════════════════════════════
@app.get("/email-log")
async def get_email_log(campaign_id: int = 0, days: int = 7):
    db=get_db(); c=db.cursor(dictionary=True)
    q="""SELECT l.full_name,l.email,l.title,l.company,l.status,l.emailed_at,l.open_count,
               l.email_opened,l.email_clicked,ca.name as campaign_name
         FROM leads l LEFT JOIN campaigns ca ON l.campaign_id=ca.id
         WHERE l.emailed_at >= DATE_SUB(NOW(),INTERVAL %s DAY)"""
    params=[days]
    if campaign_id: q+=" AND l.campaign_id=%s"; params.append(campaign_id)
    q+=" ORDER BY l.emailed_at DESC LIMIT 500"
    c.execute(q,params); rows=c.fetchall(); c.close(); db.close()
    return {"emails":rows,"total":len(rows),"days":days}

# ═══════════════════════════════════════════════════════════════
# PARTIAL FIX 1 — BASE URL FOR UNSUBSCRIBE / TRACKING
# ═══════════════════════════════════════════════════════════════
def get_base_url() -> str:
    """Get configured base URL from settings or fall back to env/localhost"""
    try:
        db=get_db(); c=db.cursor(dictionary=True)
        c.execute("SELECT base_url FROM user_email_settings WHERE base_url IS NOT NULL AND base_url != '' LIMIT 1")
        row=c.fetchone(); c.close(); db.close()
        if row and row.get("base_url"):
            return row["base_url"].rstrip("/")
    except: pass
    return os.getenv("BASE_URL","http://localhost:8000")

# ═══════════════════════════════════════════════════════════════
# PARTIAL FIX 2 — TIMEZONE BULK SEND
# ═══════════════════════════════════════════════════════════════
@app.post("/campaigns/{campaign_id}/bulk-send-timezone")
async def bulk_send_with_timezone(campaign_id: int, request: BulkEmailRequest, background_tasks: BackgroundTasks):
    """Schedule emails to send at 9am each prospect's local time"""
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("""SELECT l.* FROM leads l WHERE l.campaign_id=%s
        AND l.email IS NOT NULL AND l.email!='' AND l.confidence_score>=70
        AND l.status NOT IN ('rejected','unsubscribed')""",(campaign_id,))
    leads=c.fetchall(); c.close(); db.close()
    if not leads: return {"success":False,"message":"No eligible leads"}
    background_tasks.add_task(_send_tz_bg, leads, request.scenario or "")
    return {"success":True,"message":f"Scheduling {len(leads)} emails at 9am local time per prospect","total":len(leads)}

def _send_tz_bg(leads, scenario):
    import time, smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT * FROM user_email_settings LIMIT 1")
    row=c.fetchone(); c.close(); db.close()
    gmail_user=(row["gmail_user"] if row else None) or os.getenv("GMAIL_USER","")
    gmail_pass=(row["gmail_app_password"] if row else None) or os.getenv("GMAIL_APP_PASSWORD","")
    if not gmail_user: return
    # Group by timezone
    from datetime import datetime
    import pytz
    tz_groups = {}
    for lead in leads:
        tz=get_tz_for_location(lead.get("location",""))
        if tz not in tz_groups: tz_groups[tz]=[]
        tz_groups[tz].append(lead)
    # For each group calculate delay and sleep
    for tz_name, tz_leads in tz_groups.items():
        try:
            tz=pytz.timezone(tz_name)
            now=datetime.now(pytz.utc)
            pnow=now.astimezone(tz)
            from datetime import timedelta
            target=pnow.replace(hour=9,minute=0,second=0,microsecond=0)
            if pnow.hour>=9: target+=timedelta(days=1)
            delay=int((target.astimezone(pytz.utc)-now).total_seconds())
            if delay > 0: time.sleep(min(delay, 3600))  # Max 1hr wait
        except: pass
        try:
            for lead in tz_leads:
                try:
                    sc=scenario or ""
                    prompt=(f"Expert cold email writer.\nLead: {lead['first_name']} {lead.get('last_name','')}, {lead['title']} at {lead['company']}\nUser intent: {sc}\nMax 75 words. One CTA.\nReturn ONLY:\nSubject: [subject]\nBody: [body]"
                            if sc else
                            f"Write B2B cold email.\nLead: {lead['first_name']} {lead.get('last_name','')}, {lead['title']} at {lead['company']}\nMax 75 words.\nReturn ONLY:\nSubject: [subject]\nBody: [body]")
                    r=client.chat.completions.create(model="llama-3.3-70b-versatile",max_tokens=500,messages=[{"role":"user","content":prompt}])
                    raw=r.choices[0].message.content.strip()
                    subj=raw.split("Subject:",1)[1].split("\n")[0].strip() if "Subject:" in raw else f"Quick note — {lead['company']}"
                    body=raw.split("Body:",1)[1].strip() if "Body:" in raw else raw
                    base_url=get_base_url()
                    body_full=add_email_footer(body,lead["id"],base_url)
                    result=send_via_resend(lead["email"],subj,body_full)
                    if result["success"]:
                        db2=get_db(); c2=db2.cursor()
                        c2.execute("UPDATE leads SET status='emailed',emailed_at=NOW() WHERE id=%s",(lead["id"],))
                        db2.commit(); c2.close(); db2.close()
                    time.sleep(1)
                except Exception as e: print(f"TZ send error: {e}")
        except Exception as e: print(f"TZ group error: {e}")

# ═══════════════════════════════════════════════════════════════
# NICE-TO-HAVE — WHATSAPP (via Twilio if configured)
# ═══════════════════════════════════════════════════════════════
@app.post("/leads/{lead_id}/send-whatsapp")
async def send_whatsapp(lead_id: int, request: dict):
    """Send WhatsApp message via Twilio if configured"""
    twilio_sid=os.getenv("TWILIO_ACCOUNT_SID","")
    twilio_token=os.getenv("TWILIO_AUTH_TOKEN","")
    twilio_wa=os.getenv("TWILIO_WHATSAPP_FROM","")
    if not all([twilio_sid,twilio_token,twilio_wa]):
        return {"success":False,"message":"Twilio not configured. Add TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM to .env"}
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT * FROM leads WHERE id=%s",(lead_id,))
    lead=c.fetchone(); c.close(); db.close()
    if not lead: raise HTTPException(404,"Lead not found")
    phone=lead.get("phone","") or request.get("phone","")
    if not phone: return {"success":False,"message":"No phone number for this lead"}
    message=request.get("message","").strip()
    if not message: return {"success":False,"message":"Message required"}
    try:
        from twilio.rest import Client
        tw=Client(twilio_sid,twilio_token)
        msg=tw.messages.create(body=message,from_=f"whatsapp:{twilio_wa}",to=f"whatsapp:{phone}")
        return {"success":True,"message_sid":msg.sid}
    except Exception as e:
        return {"success":False,"message":str(e)}

# ═══════════════════════════════════════════════════════════════
# NICE-TO-HAVE — CHROME EXTENSION ENDPOINTS
# ═══════════════════════════════════════════════════════════════
@app.post("/extension/add-lead")
async def extension_add_lead(request: dict):
    """Add lead from Chrome extension — accepts LinkedIn profile data"""
    first_name=request.get("first_name","").strip()
    last_name=request.get("last_name","").strip()
    title=request.get("title","").strip()
    company=request.get("company","").strip()
    linkedin_url=request.get("linkedin_url","").strip()
    campaign_id=request.get("campaign_id",0)
    if not first_name or not company: raise HTTPException(400,"Name and company required")
    full_name=f"{first_name} {last_name}".strip()
    db=get_db(); c=db.cursor()
    c.execute("SELECT id FROM leads WHERE linkedin_url=%s",(linkedin_url,)) if linkedin_url else None
    if linkedin_url and c.fetchone():
        c.close(); db.close()
        return {"success":False,"message":"Lead already exists","duplicate":True}
    c.execute("""INSERT INTO leads (first_name,last_name,full_name,title,company,linkedin_url,campaign_id,status,email_source,confidence_score,notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,'new','chrome_extension',0,'Added via Chrome Extension')""",
        (first_name,last_name,full_name,title,company,linkedin_url,campaign_id or None))
    db.commit(); lid=c.lastrowid; c.close(); db.close()
    return {"success":True,"lead_id":lid,"message":f"{full_name} added to LeadFlow"}

@app.get("/extension/campaigns")
async def extension_get_campaigns():
    """Get campaigns list for Chrome extension dropdown"""
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT id,name,target_role FROM campaigns ORDER BY created_at DESC LIMIT 20")
    camps=c.fetchall(); c.close(); db.close()
    return {"campaigns":camps}

# ═══════════════════════════════════════════════════════════════
# NICE-TO-HAVE — VIDEO EMAIL (Loom-style thumbnail)
# ═══════════════════════════════════════════════════════════════
@app.post("/generate-video-email")
async def generate_video_email(request: dict):
    """Generate email with Loom video thumbnail embed"""
    loom_url=request.get("loom_url","").strip()
    lead_id=request.get("lead_id",0)
    if not loom_url: raise HTTPException(400,"Loom URL required")
    # Extract video ID
    import re
    vid_match=re.search(r'loom\.com/share/([a-zA-Z0-9]+)',loom_url)
    if not vid_match: raise HTTPException(400,"Invalid Loom URL")
    vid_id=vid_match.group(1)
    thumbnail_url=f"https://cdn.loom.com/sessions/thumbnails/{vid_id}-with-play.gif"
    # Generate email with video thumbnail
    video_html = f"""
<a href="{loom_url}" target="_blank">
  <img src="{thumbnail_url}" alt="Watch my video message" width="480" style="border-radius:8px;border:2px solid #D4895A">
</a>
<br><br>
👆 Click to watch my 60-second video for you
"""
    return {"success":True,"video_html":video_html,"thumbnail_url":thumbnail_url,
            "loom_url":loom_url,"embed_instructions":"Paste the video_html into your email body"}



@app.delete("/auth/team/{user_id}")
async def remove_team_member(user_id: int, request: Request):
    token=request.cookies.get("session_token","")
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT id FROM users WHERE session_token=%s",(token,))
    me=c.fetchone()
    if not me: c.close(); db.close(); raise HTTPException(401,"Not authenticated")
    if me["id"]==user_id: c.close(); db.close(); return {"success":False,"message":"Cannot delete your own account"}
    c2=db.cursor()
    c2.execute("DELETE FROM users WHERE id=%s",(user_id,))
    db.commit(); c2.close(); c.close(); db.close()
    return {"success":True,"message":"Removed"}

@app.patch("/auth/team/{user_id}/role")
async def change_team_role(user_id: int, request: Request):
    body=await request.json(); new_role=body.get("role","member")
    if new_role not in ("admin","member"): raise HTTPException(400,"Role must be admin or member")
    token=request.cookies.get("session_token","")
    db=get_db(); c=db.cursor(dictionary=True)
    c.execute("SELECT id FROM users WHERE session_token=%s",(token,))
    me=c.fetchone()
    if not me: c.close(); db.close(); raise HTTPException(401,"Not authenticated")
    c2=db.cursor()
    c2.execute("UPDATE users SET role=%s WHERE id=%s",(new_role,user_id))
    db.commit(); c2.close(); c.close(); db.close()
    return {"success":True,"message":f"Role updated to {new_role}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)