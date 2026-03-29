"""
OSG LIVE - Free Fire Esports Tournament Platform Backend
Complete FastAPI backend with MongoDB
"""

from fastapi import FastAPI, HTTPException, Depends, Header, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from passlib.context import CryptContext
from jose import JWTError, jwt
import os
import re
import hashlib
import hmac
import asyncio
import json
from enum import Enum
from decimal import Decimal
from pymongo import MongoClient, DESCENDING, ASCENDING
import uuid
import secrets

# Initialize FastAPI
app = FastAPI(title="OSG LIVE API", version="1.0.0")

# ─── Helper: safely extract user _id as plain string ─────────────────────────
def get_user_id_str(user: dict) -> str:
    raw = user.get("id") or user.get("_id") or ""
    return str(raw).strip()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB
MONGO_URL = os.environ.get("MONGO_URL", "mongodb+srv://Satvik:g7c4jUfCWe8nDk1l@cluster0.oyjqi5w.mongodb.net/?appName=Cluster0")
DB_NAME = os.environ.get("DB_NAME", "Cluster0")
client = MongoClient(MONGO_URL)
db = client[DB_NAME]

# Collections
users_col = db["users"]
teams_col = db["teams"]
tournaments_col = db["tournaments"]
registrations_col = db["registrations"]
matches_col = db["matches"]
match_results_col = db["match_results"]
bans_col = db["bans"]
banned_ips_col = db["banned_ips"]
transactions_col = db["transactions"]
withdrawals_col = db["withdrawals"]
notifications_col = db["notifications"]
otp_codes_col = db["otp_codes"]
login_logs_col = db["login_logs"]
rate_limits_col = db["rate_limits"]

# Create indexes
users_col.create_index("email", unique=True)
users_col.create_index("mobile", unique=True)
users_col.create_index("ffUid", unique=True)
teams_col.create_index("name", unique=True)
banned_ips_col.create_index("ip", unique=True)

# JWT Settings
SECRET_KEY = os.environ.get("JWT_SECRET", "osg-live-super-secret-key-2024")

# SMTP Email Config
SMTP_HOST = os.environ.get("SMTP_HOST", "mail.osglive.in")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "no-reply-otp-verify@osglive.in")
SMTP_PASS = os.environ.get("SMTP_PASS", "_O.FOeyZG-JEiPV7")
SMTP_FROM = os.environ.get("SMTP_FROM", "OSG LIVE <no-reply-otp-verify@osglive.in>")


def send_email(to_email: str, subject: str, html_body: str):
    """Send email via cPanel SMTP"""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_FROM
        msg["To"] = to_email
        msg.attach(MIMEText(html_body, "html"))
        
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, to_email, msg.as_string())
        print(f"[EMAIL] Sent to {to_email}: {subject}")
        return True
    except Exception as e:
        print(f"[EMAIL ERROR] Failed to send to {to_email}: {e}")
        return False
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

# Password hashing - suppress bcrypt version warning
import warnings
warnings.filterwarnings("ignore", ".*error reading bcrypt version.*")
warnings.filterwarnings("ignore", ".*AttributeError.*bcrypt.*")
try:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
except Exception:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# SSE Clients
sse_clients: Dict[str, List[asyncio.Queue]] = {}

# ============== ENUMS ==============
class Role(str, Enum):
    PLAYER = "PLAYER"
    ADMIN = "ADMIN"

class TournamentStatus(str, Enum):
    DRAFT = "DRAFT"
    UPCOMING = "UPCOMING"
    REGISTERING = "REGISTERING"
    LIVE = "LIVE"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    POSTPONED = "POSTPONED"

class MatchStatus(str, Enum):
    PENDING = "PENDING"
    LIVE = "LIVE"
    COMPLETED = "COMPLETED"

class PaymentStatus(str, Enum):
    PENDING = "PENDING"
    PAID = "PAID"
    REFUNDED = "REFUNDED"
    FAILED = "FAILED"

class BanType(str, Enum):
    MATCH_TERMINATION = "MATCH_TERMINATION"
    THREE_DAYS = "THREE_DAYS"
    SEVEN_DAYS = "SEVEN_DAYS"
    THIRTY_DAYS = "THIRTY_DAYS"
    PERMANENT = "PERMANENT"

class WithdrawalStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class MapType(str, Enum):
    BERMUDA = "BERMUDA"
    PURGATORY = "PURGATORY"
    KALAHARI = "KALAHARI"
    ALPHINE = "ALPHINE"
    NEXTERRA = "NEXTERRA"
    SOLARA = "SOLARA"
    IRON_CAGE = "IRON_CAGE"  # Lone Wolf only

# ============== MODELS ==============
class UserRegister(BaseModel):
    fullName: str = Field(..., min_length=2, max_length=100)
    email: EmailStr
    mobile: str = Field(..., pattern=r"^[6-9]\d{9}$")
    password: str = Field(..., min_length=8)
    ffUid: str = Field(..., pattern=r"^\d{9,12}$")
    ign: str = Field(..., min_length=2, max_length=30)
    state: str
    dob: str
    
    @field_validator('password')
    @classmethod
    def validate_password(cls, v):
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one number')
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        return v

class UserLogin(BaseModel):
    identifier: str  # email or mobile
    password: str

class UserUpdate(BaseModel):
    ign: Optional[str] = None
    email: Optional[EmailStr] = None

class PasswordChange(BaseModel):
    currentPassword: str
    newPassword: str

class TeamCreate(BaseModel):
    name: str = Field(..., min_length=3, max_length=30)

class TeamInvite(BaseModel):
    ffUid: str

class TournamentMode(str, Enum):
    BR = "BR"
    CLASH_SQUAD_1v1 = "CS_1v1"
    CLASH_SQUAD_2v2 = "CS_2v2"
    CLASH_SQUAD_4v4 = "CS_4v4"
    LONE_WOLF_1v1 = "LW_1v1"
    LONE_WOLF_2v2 = "LW_2v2"

class TournamentCreate(BaseModel):
    name: str
    map: MapType
    scheduledAt: str
    entryFee: float
    maxTeams: int = 12
    playersPerTeam: int = 4
    prizePool: Dict[str, float]
    perKillPrize: float = 0
    rules: str
    youtubeUrl: Optional[str] = None
    mode: Optional[str] = "BR"
    description: Optional[str] = None

class TournamentUpdate(BaseModel):
    name: Optional[str] = None
    map: Optional[str] = None  # Allow any string including IRON_CAGE
    scheduledAt: Optional[str] = None
    entryFee: Optional[float] = None
    prizePool: Optional[Dict[str, float]] = None
    perKillPrize: Optional[float] = None
    rules: Optional[str] = None
    youtubeUrl: Optional[str] = None
    status: Optional[TournamentStatus] = None
    roomId: Optional[str] = None
    roomPassword: Optional[str] = None
    postponeReason: Optional[str] = None
    postponeNewDate: Optional[str] = None
    cancelReason: Optional[str] = None

class MatchResultEntry(BaseModel):
    teamId: str
    kills: int = 0
    placement: int
    booyah: bool = False

class BanCreate(BaseModel):
    userId: str
    banType: BanType
    reason: str
    evidence: Optional[str] = None
    matchId: Optional[str] = None
    tournamentId: Optional[str] = None
    ipBanned: bool = False

class WithdrawalCreate(BaseModel):
    amount: float = Field(..., ge=50)
    upiId: str = Field(..., pattern=r"^[\w.-]+@[\w]+$")

class WithdrawalProcess(BaseModel):
    status: WithdrawalStatus
    utrNumber: Optional[str] = None
    rejectionReason: Optional[str] = None

class WalletTopupRequest(BaseModel):
    amount: float = Field(..., ge=10, le=10000)
    utrNumber: str = Field(..., min_length=6)
    paymentMethod: str = "UPI"
    screenshotNote: Optional[str] = None

class RedeemCodeRequest(BaseModel):
    code: str = Field(..., min_length=4, max_length=20)

class PaymentVerify(BaseModel):
    orderId: str
    paymentId: str
    signature: str

# ============== HELPERS ==============
def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable dict"""
    if doc is None:
        return None
    doc["id"] = str(doc.pop("_id"))
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, ObjectId):
            doc[key] = str(value)
    return doc

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

async def get_current_user(authorization: str = Header(None), request: Request = None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = users_col.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    # Check for active bans
    return serialize_doc(user)

async def get_admin_user(authorization: str = Header(None)):
    user = await get_current_user(authorization)
    if user.get("role") != "ADMIN":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

# Points calculation
PLACEMENT_POINTS = {
    1: 12, 2: 9, 3: 8, 4: 7, 5: 6,
    6: 5, 7: 4, 8: 3, 9: 2, 10: 1,
    11: 0, 12: 0
}

def calculate_match_points(kills: int, placement: int) -> Dict[str, int]:
    placement_points = PLACEMENT_POINTS.get(placement, 0)
    kill_points = kills * 1
    total_points = placement_points + kill_points
    return {
        "placementPoints": placement_points,
        "killPoints": kill_points,
        "totalPoints": total_points
    }

# Rate limiting
def check_rate_limit(key: str, limit: int = 5, window_minutes: int = 15) -> bool:
    entry = rate_limits_col.find_one({"key": key})
    now = datetime.now(timezone.utc)
    
    if entry:
        try:
            reset_at = entry["resetAt"]
            if reset_at.tzinfo is None:
                reset_at = reset_at.replace(tzinfo=timezone.utc)
            
            if reset_at > now:
                if entry["count"] >= limit:
                    return False
                rate_limits_col.update_one({"key": key}, {"$inc": {"count": 1}})
            else:
                # Window expired - reset count
                rate_limits_col.update_one(
                    {"key": key},
                    {"$set": {"count": 1, "resetAt": now + timedelta(minutes=window_minutes)}}
                )
        except Exception:
            rate_limits_col.update_one(
                {"key": key},
                {"$set": {"count": 1, "resetAt": now + timedelta(minutes=window_minutes)}}
            )
    else:
        rate_limits_col.insert_one({
            "key": key,
            "count": 1,
            "resetAt": now + timedelta(minutes=window_minutes),
            "createdAt": now
        })
    return True

def clear_rate_limit(key: str):
    """Clear rate limit after successful action"""
    rate_limits_col.delete_one({"key": key})

# ============== AUTH ROUTES ==============
@app.post("/api/auth/register")
async def register(user_data: UserRegister, request: Request):
    ip = get_client_ip(request)
    
    # Check banned IP
    if banned_ips_col.find_one({"ip": ip}):
        raise HTTPException(status_code=403, detail="Registration not allowed from this IP")
    
    # Check rate limit
    if not check_rate_limit(f"register:{ip}"):
        raise HTTPException(status_code=429, detail="Too many registration attempts")
    
    # Check duplicates
    if users_col.find_one({"email": user_data.email}):
        raise HTTPException(status_code=400, detail="Email already registered")
    if users_col.find_one({"mobile": user_data.mobile}):
        raise HTTPException(status_code=400, detail="Mobile already registered")
    if users_col.find_one({"ffUid": user_data.ffUid}):
        raise HTTPException(status_code=400, detail="Free Fire UID already registered")
    
    # Check age (must be 13+)
    try:
        dob_str = user_data.dob.replace("Z", "+00:00")
        dob = datetime.fromisoformat(dob_str)
        # Make both timezone-aware or both naive
        if dob.tzinfo is not None:
            now = datetime.now(timezone.utc)
        else:
            now = datetime.now()
        age = (now - dob).days // 365
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date of birth format")
    if age < 13:
        raise HTTPException(status_code=400, detail="Must be at least 13 years old")
    
    # Create user
    user_doc = {
        "fullName": user_data.fullName,
        "email": user_data.email,
        "emailVerified": False,
        "emailVerifyToken": str(uuid.uuid4()),
        "emailVerifyExpiry": datetime.now(timezone.utc) + timedelta(hours=24),
        "mobile": user_data.mobile,
        "mobileVerified": False,
        "passwordHash": get_password_hash(user_data.password),
        "ffUid": user_data.ffUid,
        "ign": user_data.ign,
        "ignUpdatedAt": datetime.now(timezone.utc),
        "state": user_data.state,
        "dob": dob,
        "role": "PLAYER",
        "walletBalance": 0.0,
        "isActive": True,
        "registeredIp": ip,
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc)
    }
    
    result = users_col.insert_one(user_doc)
    user_id = str(result.inserted_id)
    
    # Create notification
    notifications_col.insert_one({
        "userId": user_id,
        "title": "Welcome to OSG LIVE!",
        "message": "Your account has been created. Verify your email to get started.",
        "type": "INFO",
        "isRead": False,
        "createdAt": datetime.now(timezone.utc)
    })
    
    # Mock email send
    print(f"[EMAIL] Verification email sent to {user_data.email}")
    
    token = create_access_token({"sub": user_id, "role": "PLAYER"})
    
    return {
        "message": "Registration successful",
        "token": token,
        "user": {
            "id": user_id,
            "fullName": user_data.fullName,
            "email": user_data.email,
            "mobile": user_data.mobile,
            "ffUid": user_data.ffUid,
            "ign": user_data.ign,
            "role": "PLAYER"
        }
    }

@app.post("/api/auth/login")
async def login(credentials: UserLogin, request: Request):
    ip = get_client_ip(request)
    
    # Check banned IP
    if banned_ips_col.find_one({"ip": ip}):
        raise HTTPException(status_code=403, detail="Access denied from this IP")
    
    # Check rate limit
    if not check_rate_limit(f"login:{ip}", limit=10, window_minutes=15):
        raise HTTPException(status_code=429, detail="Too many login attempts. Please wait 15 minutes.")
    
    # Find user
    user = users_col.find_one({
        "$or": [
            {"email": credentials.identifier},
            {"mobile": credentials.identifier}
        ]
    })
    
    # Log attempt
    login_logs_col.insert_one({
        "userId": str(user["_id"]) if user else None,
        "ip": ip,
        "userAgent": request.headers.get("user-agent", ""),
        "success": False,
        "createdAt": datetime.now(timezone.utc)
    })
    
    if not user or not verify_password(credentials.password, user["passwordHash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Check active ban - allow login but return ban details
    active_ban = bans_col.find_one({
        "userId": str(user["_id"]),
        "isActive": True,
        "banType": {"$ne": "MATCH_TERMINATION"},
        "$or": [
            {"expiresAt": None},
            {"expiresAt": {"$gt": datetime.now(timezone.utc)}}
        ]
    })
    
    # Clear rate limit on successful login
    clear_rate_limit(f"login:{ip}")
    
    # Update login log - find latest failed log and mark success
    latest_log = login_logs_col.find_one(
        {"userId": str(user["_id"]), "ip": ip, "success": False},
        sort=[("createdAt", DESCENDING)]
    )
    if latest_log:
        login_logs_col.update_one(
            {"_id": latest_log["_id"]},
            {"$set": {"success": True}}
        )
    
    # Update user
    users_col.update_one(
        {"_id": user["_id"]},
        {"$set": {"lastLoginIp": ip, "lastLoginAt": datetime.now(timezone.utc)}}
    )
    
    token = create_access_token({"sub": str(user["_id"]), "role": user["role"]})
    
    # Prepare ban info for response
    ban_info = None
    if active_ban:
        expires = active_ban.get("expiresAt")
        if expires and expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        ban_info = {
            "id": str(active_ban["_id"]),
            "banType": active_ban["banType"],
            "reason": active_ban.get("reason", ""),
            "expiresAt": expires.isoformat() if expires else None,
            "isActive": True,
            "appealStatus": active_ban.get("appealStatus"),
            "appealText": active_ban.get("appealText")
        }
    
    return {
        "token": token,
        "isBanned": ban_info is not None,
        "activeBan": ban_info,
        "user": {
            "id": str(user["_id"]),
            "fullName": user["fullName"],
            "email": user["email"],
            "mobile": user["mobile"],
            "ffUid": user["ffUid"],
            "ign": user["ign"],
            "role": user["role"],
            "walletBalance": user.get("walletBalance", 0),
            "emailVerified": user.get("emailVerified", False),
            "mobileVerified": user.get("mobileVerified", False),
            "isActive": user.get("isActive", True)
        }
    }

@app.post("/api/auth/admin/login")
async def admin_login(credentials: UserLogin, request: Request):
    ip = get_client_ip(request)
    
    if not check_rate_limit(f"admin_login:{ip}", limit=10, window_minutes=30):
        raise HTTPException(status_code=429, detail="Too many attempts. Please wait 30 minutes.")
    
    user = users_col.find_one({
        "$or": [{"email": credentials.identifier}, {"mobile": credentials.identifier}],
        "role": "ADMIN"
    })
    
    if not user or not verify_password(credentials.password, user["passwordHash"]):
        raise HTTPException(status_code=401, detail="Invalid admin credentials")
    
    token = create_access_token(
        {"sub": str(user["_id"]), "role": "ADMIN"},
        expires_delta=timedelta(hours=4)
    )
    
    # Clear rate limit on successful login
    clear_rate_limit(f"admin_login:{ip}")
    
    return {
        "token": token,
        "user": {
            "id": str(user["_id"]),
            "fullName": user["fullName"],
            "email": user["email"],
            "role": "ADMIN"
        }
    }

@app.get("/api/auth/me")
async def get_me(user: dict = Depends(get_current_user)):
    # Get team info
    team = None
    team_doc = teams_col.find_one({"members": user["id"]})
    if team_doc:
        team = serialize_doc(team_doc)
        # Get member details
        members = []
        for member_id in team_doc.get("members", []):
            member = users_col.find_one({"_id": ObjectId(member_id)}, {"passwordHash": 0})
            if member:
                members.append({
                    "id": str(member["_id"]),
                    "fullName": member["fullName"],
                    "ign": member["ign"],
                    "ffUid": member["ffUid"]
                })
        team["memberDetails"] = members
    
    # Get active ban (excluding expired ones with timezone fix)
    now = datetime.now(timezone.utc)
    all_bans = list(bans_col.find({"userId": user["id"], "isActive": True}))
    active_ban = None
    for b in all_bans:
        if b.get("banType") == "MATCH_TERMINATION":
            continue
        exp = b.get("expiresAt")
        if exp is None:
            active_ban = b
            break
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp > now:
            active_ban = b
            break
    
    ban_data = None
    if active_ban:
        exp = active_ban.get("expiresAt")
        if exp and exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        ban_data = {
            "id": str(active_ban["_id"]),
            "banType": active_ban["banType"],
            "reason": active_ban.get("reason", ""),
            "expiresAt": exp.isoformat() if exp else None,
            "isActive": True,
            "appealStatus": active_ban.get("appealStatus"),
            "appealText": active_ban.get("appealText"),
            "appealedAt": active_ban.get("appealedAt").isoformat() if active_ban.get("appealedAt") else None
        }
    
    return {
        **user,
        "team": team,
        "activeBan": ban_data,
        "isBanned": ban_data is not None
    }

@app.post("/api/auth/verify-email")
async def verify_email(token: str):
    user = users_col.find_one({
        "emailVerifyToken": token,
        "emailVerifyExpiry": {"$gt": datetime.now(timezone.utc)}
    })
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    
    users_col.update_one(
        {"_id": user["_id"]},
        {
            "$set": {"emailVerified": True},
            "$unset": {"emailVerifyToken": "", "emailVerifyExpiry": ""}
        }
    )
    
    return {"message": "Email verified successfully"}

@app.post("/api/auth/send-otp")
async def send_otp(mobile: str, request: Request):
    ip = get_client_ip(request)
    if not check_rate_limit(f"otp:{ip}"):
        raise HTTPException(status_code=429, detail="Too many OTP requests")
    
    import random as rand_mod
    otp = str(rand_mod.randint(100000, 999999))
    
    otp_codes_col.insert_one({
        "mobile": mobile,
        "code": otp,
        "purpose": "verification",
        "isUsed": False,
        "expiresAt": datetime.now(timezone.utc) + timedelta(minutes=10),
        "createdAt": datetime.now(timezone.utc)
    })
    
    print(f"[OTP] {otp} for {mobile}")
    return {"message": "OTP sent successfully", "otp_debug": otp}

@app.post("/api/auth/send-email-otp")
async def send_email_otp(email: str, request: Request):
    """Send OTP to email for verification"""
    ip = get_client_ip(request)
    if not check_rate_limit(f"email_otp:{ip}", limit=5, window_minutes=15):
        raise HTTPException(status_code=429, detail="Too many OTP requests")
    
    import random as rand_mod
    otp = str(rand_mod.randint(100000, 999999))
    
    otp_codes_col.insert_one({
        "email": email,
        "code": otp,
        "purpose": "email_verification",
        "isUsed": False,
        "expiresAt": datetime.now(timezone.utc) + timedelta(minutes=10),
        "createdAt": datetime.now(timezone.utc)
    })
    
    html = f"""
    <div style="background:#0A0A0A;padding:40px;font-family:Arial">
      <h2 style="color:#FF6B00">OSG LIVE</h2>
      <h3 style="color:white">Email Verification OTP</h3>
      <div style="background:#1A1A1A;border:1px solid #FF6B00;border-radius:8px;padding:20px;text-align:center">
        <p style="color:#A1A1AA">Your OTP code is:</p>
        <div style="font-size:48px;font-weight:bold;color:#FFD700;letter-spacing:8px">{otp}</div>
        <p style="color:#52525B;font-size:12px">Valid for 10 minutes. Do not share this code.</p>
      </div>
      <p style="color:#52525B;font-size:11px;margin-top:20px">OSG LIVE | support@tg.toonverse.icu</p>
    </div>
    """
    
    sent = send_email(email, "OSG LIVE - Email Verification OTP", html)
    
    return {"message": "OTP sent to your email", "sent": sent}

@app.post("/api/auth/verify-email-otp")
async def verify_email_otp_code(email: str, code: str):
    """Verify email OTP"""
    otp_doc = otp_codes_col.find_one({
        "email": email,
        "code": code,
        "purpose": "email_verification",
        "isUsed": False,
        "expiresAt": {"$gt": datetime.now(timezone.utc)}
    })
    
    if not otp_doc:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")
    
    otp_codes_col.update_one({"_id": otp_doc["_id"]}, {"$set": {"isUsed": True}})
    users_col.update_one({"email": email}, {"$set": {"emailVerified": True}})
    
    return {"message": "Email verified successfully"}

@app.post("/api/auth/verify-otp")
async def verify_otp(mobile: str, code: str):
    otp_doc = otp_codes_col.find_one({
        "mobile": mobile,
        "code": code,
        "isUsed": False,
        "expiresAt": {"$gt": datetime.now(timezone.utc)}
    })
    
    if not otp_doc:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")
    
    otp_codes_col.update_one({"_id": otp_doc["_id"]}, {"$set": {"isUsed": True}})
    
    # Mark mobile as verified if user exists
    users_col.update_one(
        {"mobile": mobile},
        {"$set": {"mobileVerified": True}}
    )
    
    return {"message": "Mobile verified successfully"}






@app.post("/api/auth/forgot-password")
async def forgot_password(request: Request):
    body = await request.json()
    email = body.get("email", "").lower().strip()
    ip = get_client_ip(request)

    if not check_rate_limit(f"forgot:{ip}", limit=5, window_minutes=15):
        raise HTTPException(status_code=429, detail="Too many requests. Try again later.")

    user = users_col.find_one({"email": email})
    if user:
        reset_token = secrets.token_urlsafe(32)
        users_col.update_one(
            {"_id": user["_id"]},
            {"$set": {
                "passwordResetToken": reset_token,
                "passwordResetExpiry": datetime.now(timezone.utc) + timedelta(hours=1)
            }}
        )

        frontend_url = os.environ.get("FRONTEND_URL", "https://osglive.in")
        reset_link = f"{frontend_url}/reset-password?token={reset_token}"

        html = f"""
        <div style="background:#0A0A0A;padding:40px;font-family:Arial,sans-serif;max-width:520px;margin:0 auto;border-radius:12px">
          <div style="text-align:center;margin-bottom:24px">
            <h1 style="color:#FF6B00;font-size:28px;margin:0;letter-spacing:2px">OSG LIVE</h1>
            <p style="color:#71717A;font-size:13px;margin:4px 0 0">Free Fire Esports Platform</p>
          </div>
          <h2 style="color:#FFFFFF;font-size:20px;margin:0 0 12px">Reset Your Password</h2>
          <p style="color:#A1A1AA;font-size:14px;line-height:1.6;margin:0 0 24px">
            We received a request to reset the password for your OSG LIVE account.
            Click the button below to set a new password. This link is valid for <strong style="color:#FFFFFF">1 hour</strong>.
          </p>
          <div style="text-align:center;margin:32px 0">
            <a href="{reset_link}"
               style="display:inline-block;background:#FF6B00;color:#FFFFFF;padding:14px 40px;
                      border-radius:8px;text-decoration:none;font-weight:700;font-size:16px;
                      letter-spacing:0.5px">
              Reset Password
            </a>
          </div>
          <p style="color:#52525B;font-size:12px;line-height:1.6;margin:24px 0 0">
            If you didn't request a password reset, you can safely ignore this email.
            Your password won't change until you click the link above and create a new one.
          </p>
          <hr style="border:none;border-top:1px solid #222;margin:24px 0"/>
          <p style="color:#3F3F46;font-size:11px;text-align:center;margin:0">
            OSG LIVE &bull; <a href="mailto:support@tg.toonverse.icu" style="color:#3F3F46">support@tg.toonverse.icu</a>
          </p>
        </div>
        """
        send_email(email, "OSG LIVE - Reset Your Password", html)

    return {"message": "If this email is registered, a password reset link has been sent."}


@app.post("/api/auth/reset-password")
async def reset_password(request: Request):
    body = await request.json()
    token = body.get("token", "")
    new_password = body.get("newPassword", "")

    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    if not re.search(r'[A-Z]', new_password):
        raise HTTPException(status_code=400, detail="Password must contain at least one uppercase letter")
    if not re.search(r'\d', new_password):
        raise HTTPException(status_code=400, detail="Password must contain at least one number")
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', new_password):
        raise HTTPException(status_code=400, detail="Password must contain at least one special character")

    user = users_col.find_one({
        "passwordResetToken": token,
        "passwordResetExpiry": {"$gt": datetime.now(timezone.utc)}
    })

    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reset link. Please request a new one.")

    users_col.update_one(
        {"_id": user["_id"]},
        {
            "$set": {"passwordHash": get_password_hash(new_password), "updatedAt": datetime.now(timezone.utc)},
            "$unset": {"passwordResetToken": "", "passwordResetExpiry": ""}
        }
    )

    notifications_col.insert_one({
        "userId": str(user["_id"]),
        "title": "Password Changed",
        "message": "Your password was successfully reset. If this wasn\'t you, contact support immediately.",
        "type": "INFO",
        "isRead": False,
        "createdAt": datetime.now(timezone.utc)
    })

    return {"message": "Password reset successfully. You can now login with your new password."}

# ============== PLAYER ROUTES ==============
@app.get("/api/player/profile")
async def get_player_profile(user: dict = Depends(get_current_user)):
    # Get stats
    registrations = list(registrations_col.find({"teamId": {"$in": [user["id"]]}}))
    
    # Calculate stats
    total_tournaments = len(registrations)
    total_kills = 0
    total_wins = 0
    total_earnings = 0
    
    for reg in registrations:
        results = list(match_results_col.find({"teamId": reg.get("teamId")}))
        for r in results:
            total_kills += r.get("kills", 0)
            if r.get("placement") == 1:
                total_wins += 1
    
    # Get transactions for earnings
    credits = transactions_col.aggregate([
        {"$match": {"userId": user["id"], "type": {"$in": ["CREDIT", "PRIZE"]}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ])
    credits_list = list(credits)
    total_earnings = credits_list[0]["total"] if credits_list else 0
    
    return {
        **user,
        "stats": {
            "tournamentsPlayed": total_tournaments,
            "totalKills": total_kills,
            "totalWins": total_wins,
            "totalEarnings": total_earnings
        }
    }

@app.put("/api/player/profile")
async def update_player_profile(updates: UserUpdate, user: dict = Depends(get_current_user)):
    update_data = {"updatedAt": datetime.now(timezone.utc)}
    
    if updates.ign:
        # Check 30-day cooldown
        last_update = user.get("ignUpdatedAt")
        if last_update:
            last_dt = datetime.fromisoformat(last_update) if isinstance(last_update, str) else last_update
            # Fix timezone awareness
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - last_dt < timedelta(days=30):
                days_left = 30 - (datetime.now(timezone.utc) - last_dt).days
                raise HTTPException(status_code=400, detail=f"IGN can be changed in {days_left} days")
        
        update_data["ign"] = updates.ign
        update_data["ignUpdatedAt"] = datetime.now(timezone.utc)
    
    if updates.email:
        if users_col.find_one({"email": updates.email, "_id": {"$ne": ObjectId(user["id"])}}):
            raise HTTPException(status_code=400, detail="Email already in use")
        update_data["email"] = updates.email
        update_data["emailVerified"] = False
    
    users_col.update_one({"_id": ObjectId(user["id"])}, {"$set": update_data})
    
    return {"message": "Profile updated"}

@app.post("/api/player/change-password")
async def change_password(data: PasswordChange, user: dict = Depends(get_current_user)):
    user_doc = users_col.find_one({"_id": ObjectId(user["id"])})
    
    if not verify_password(data.currentPassword, user_doc["passwordHash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    
    users_col.update_one(
        {"_id": ObjectId(user["id"])},
        {"$set": {"passwordHash": get_password_hash(data.newPassword)}}
    )
    
    return {"message": "Password changed successfully"}

# ============== WALLET ROUTES ==============
@app.get("/api/player/wallet")
async def get_wallet(user: dict = Depends(get_current_user)):
    transactions = list(transactions_col.find(
        {"userId": user["id"]}
    ).sort("createdAt", DESCENDING).limit(50))
    
    return {
        "balance": user.get("walletBalance", 0),
        "transactions": [serialize_doc(t) for t in transactions]
    }

@app.post("/api/player/withdrawal")
async def create_withdrawal(data: WithdrawalCreate, user: dict = Depends(get_current_user)):
    balance = user.get("walletBalance", 0)
    PLATFORM_FEE = 5.0  # Always ₹5 platform fee
    
    if data.amount < 50:
        raise HTTPException(status_code=400, detail="Minimum withdrawal is ₹50")
    
    total_deduct = data.amount + PLATFORM_FEE
    if total_deduct > balance:
        raise HTTPException(status_code=400, detail=f"Insufficient balance. Need ₹{total_deduct} (₹{data.amount} + ₹{PLATFORM_FEE} fee)")
    
    # Check pending withdrawals
    pending = withdrawals_col.find_one({
        "userId": user["id"],
        "status": {"$in": ["PENDING", "PROCESSING"]}
    })
    if pending:
        raise HTTPException(status_code=400, detail="You have a pending withdrawal request")
    
    # Create withdrawal
    withdrawal_doc = {
        "userId": user["id"],
        "amount": data.amount,
        "upiId": data.upiId,
        "status": "PENDING",
        "requestedAt": datetime.now(timezone.utc)
    }
    result = withdrawals_col.insert_one(withdrawal_doc)
    
    # Deduct from wallet (amount + platform fee)
    new_balance = balance - total_deduct
    users_col.update_one(
        {"_id": ObjectId(user["id"])},
        {"$set": {"walletBalance": new_balance}}
    )
    
    # Create transaction
    transactions_col.insert_one({
        "userId": user["id"],
        "type": "WITHDRAWAL",
        "amount": -data.amount,
        "description": f"Withdrawal ₹{data.amount} to {data.upiId} (₹{PLATFORM_FEE} platform fee deducted)",
        "referenceId": str(result.inserted_id),
        "balanceBefore": balance,
        "balanceAfter": new_balance,
        "createdAt": datetime.now(timezone.utc)
    })
    
    return {"message": "Withdrawal request submitted", "id": str(result.inserted_id)}

@app.get("/api/player/withdrawals")
async def get_withdrawals(user: dict = Depends(get_current_user)):
    withdrawals = list(withdrawals_col.find({"userId": user["id"]}).sort("requestedAt", DESCENDING))
    return [serialize_doc(w) for w in withdrawals]

# ============== WALLET TOPUP ROUTES ==============

@app.get("/api/admin/payment-settings")
async def get_payment_settings_public():
    """Get payment settings - public for wallet page"""
    settings = db["settings"].find_one({"key": "payment_settings"})
    if not settings:
        # Default settings
        return {
            "upiEnabled": True,
            "redeemEnabled": True,
            "razorpayEnabled": False,
            "upiId": "osglive@upi",
            "qrCodeUrl": "",
            "minTopup": 10,
            "maxTopup": 10000
        }
    return {
        "upiEnabled": settings.get("upiEnabled", True),
        "redeemEnabled": settings.get("redeemEnabled", True),
        "razorpayEnabled": settings.get("razorpayEnabled", False),
        "upiId": settings.get("upiId", "osglive@upi"),
        "qrCodeUrl": settings.get("qrCodeUrl", ""),
        "minTopup": settings.get("minTopup", 10),
        "maxTopup": settings.get("maxTopup", 10000)
    }

@app.post("/api/player/wallet/topup-request")
async def request_wallet_topup(data: WalletTopupRequest, user: dict = Depends(get_current_user)):
    """Player submits UTR number for wallet topup verification"""
    # Check for duplicate UTR
    existing = db["topup_requests"].find_one({"utrNumber": data.utrNumber})
    if existing:
        raise HTTPException(status_code=400, detail="This UTR number has already been used")
    
    topup_doc = {
        "userId": user["id"],
        "amount": data.amount,
        "utrNumber": data.utrNumber.strip(),
        "paymentMethod": data.paymentMethod,
        "screenshotNote": data.screenshotNote,
        "status": "PENDING",
        "requestedAt": datetime.now(timezone.utc)
    }
    result = db["topup_requests"].insert_one(topup_doc)
    
    # Notify admin
    admin = users_col.find_one({"role": "ADMIN"})
    if admin:
        notifications_col.insert_one({
            "userId": str(admin["_id"]),
            "title": "💰 Wallet Topup Request",
            "message": f"{user.get('ign')} requested ₹{data.amount} topup (UTR: {data.utrNumber})",
            "type": "INFO",
            "isRead": False,
            "link": "/admin/withdrawals",
            "createdAt": datetime.now(timezone.utc)
        })
    
    return {"message": "Topup request submitted. Admin will verify and credit within 30 minutes.", "id": str(result.inserted_id)}

@app.post("/api/player/wallet/redeem")
async def redeem_wallet_code(data: RedeemCodeRequest, user: dict = Depends(get_current_user)):
    """Redeem a gift/promo code for wallet credit"""
    code = db["redeem_codes"].find_one({
        "code": data.code.upper().strip(),
        "isActive": True,
        "expiresAt": {"$gt": datetime.now(timezone.utc)}
    })
    
    if not code:
        raise HTTPException(status_code=400, detail="Invalid or expired redeem code")
    
    # Check if already used by this user
    if user["id"] in code.get("usedBy", []):
        raise HTTPException(status_code=400, detail="You have already used this code")
    
    # Check max uses
    if code.get("maxUses") and len(code.get("usedBy", [])) >= code["maxUses"]:
        raise HTTPException(status_code=400, detail="This code has reached its maximum uses")
    
    amount = code["amount"]
    balance = user.get("walletBalance", 0)
    new_balance = balance + amount
    
    # Credit wallet
    users_col.update_one({"_id": ObjectId(user["id"])}, {"$set": {"walletBalance": new_balance}})
    
    # Mark as used
    db["redeem_codes"].update_one(
        {"_id": code["_id"]},
        {"$addToSet": {"usedBy": user["id"]}, "$inc": {"useCount": 1}}
    )
    
    # Transaction record
    transactions_col.insert_one({
        "userId": user["id"],
        "type": "CREDIT",
        "amount": amount,
        "description": f"Redeem code: {data.code.upper()}",
        "referenceId": str(code["_id"]),
        "balanceBefore": balance,
        "balanceAfter": new_balance,
        "createdAt": datetime.now(timezone.utc)
    })
    
    notifications_col.insert_one({
        "userId": user["id"],
        "title": "🎁 Code Redeemed!",
        "message": f"₹{amount} credited to your wallet via redeem code",
        "type": "SUCCESS",
        "isRead": False,
        "createdAt": datetime.now(timezone.utc)
    })
    
    return {"message": f"Code redeemed! ₹{amount} added to your wallet", "amount": amount, "newBalance": new_balance}

@app.get("/api/player/wallet/topup-history")
async def get_topup_history(user: dict = Depends(get_current_user)):
    requests = list(db["topup_requests"].find({"userId": user["id"]}).sort("requestedAt", DESCENDING).limit(20))
    return [serialize_doc(r) for r in requests]

# ============== ADMIN TOPUP MANAGEMENT ==============
@app.get("/api/admin/topup-requests")
async def get_topup_requests(status: Optional[str] = None, admin: dict = Depends(get_admin_user)):
    query = {}
    if status:
        query["status"] = status
    requests = list(db["topup_requests"].find(query).sort("requestedAt", DESCENDING))
    result = []
    for req in requests:
        player = users_col.find_one({"_id": ObjectId(req["userId"])}, {"passwordHash": 0})
        result.append({
            **serialize_doc(req),
            "playerIgn": player.get("ign", "Unknown") if player else "Unknown",
            "playerEmail": player.get("email", "") if player else ""
        })
    return result

@app.put("/api/admin/topup-requests/{request_id}")
async def process_topup_request(request_id: str, action: str, note: Optional[str] = None, admin: dict = Depends(get_admin_user)):
    req = db["topup_requests"].find_one({"_id": ObjectId(request_id)})
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    if req["status"] != "PENDING":
        raise HTTPException(status_code=400, detail="Request already processed")
    
    if action == "APPROVED":
        # Credit wallet
        player = users_col.find_one({"_id": ObjectId(req["userId"])})
        if player:
            balance = player.get("walletBalance", 0)
            new_balance = balance + req["amount"]
            users_col.update_one({"_id": ObjectId(req["userId"])}, {"$set": {"walletBalance": new_balance}})
            transactions_col.insert_one({
                "userId": req["userId"],
                "type": "CREDIT",
                "amount": req["amount"],
                "description": f"Wallet topup via UPI (UTR: {req['utrNumber']})",
                "referenceId": request_id,
                "balanceBefore": balance,
                "balanceAfter": new_balance,
                "createdAt": datetime.now(timezone.utc)
            })
            notifications_col.insert_one({
                "userId": req["userId"],
                "title": "✅ Wallet Topup Approved!",
                "message": f"₹{req['amount']} has been added to your wallet",
                "type": "SUCCESS",
                "isRead": False,
                "createdAt": datetime.now(timezone.utc)
            })
        db["topup_requests"].update_one({"_id": ObjectId(request_id)}, {"$set": {"status": "APPROVED", "processedBy": admin["id"], "processedAt": datetime.now(timezone.utc)}})
    else:
        notifications_col.insert_one({
            "userId": req["userId"],
            "title": "❌ Wallet Topup Rejected",
            "message": f"Your topup of ₹{req['amount']} was rejected. {note or 'Please contact support.'}",
            "type": "DANGER",
            "isRead": False,
            "createdAt": datetime.now(timezone.utc)
        })
        db["topup_requests"].update_one({"_id": ObjectId(request_id)}, {"$set": {"status": "REJECTED", "rejectionNote": note, "processedBy": admin["id"], "processedAt": datetime.now(timezone.utc)}})
    
    return {"message": f"Topup request {action.lower()}"}

@app.get("/api/admin/payment-settings-full")
async def get_full_payment_settings(admin: dict = Depends(get_admin_user)):
    settings = db["settings"].find_one({"key": "payment_settings"})
    if not settings:
        return {"upiEnabled": True, "redeemEnabled": True, "razorpayEnabled": False, "upiId": "osglive@upi", "qrCodeUrl": "", "minTopup": 10, "maxTopup": 10000}
    return serialize_doc(settings)

@app.put("/api/admin/payment-settings")
async def update_payment_settings(settings: dict, admin: dict = Depends(get_admin_user)):
    settings["key"] = "payment_settings"
    db["settings"].update_one({"key": "payment_settings"}, {"$set": settings}, upsert=True)
    return {"message": "Payment settings updated"}

@app.post("/api/admin/redeem-codes")
async def create_redeem_code(
    code: str, amount: float, maxUses: int = 1,
    expiryHours: int = 24,
    admin: dict = Depends(get_admin_user)
):
    existing = db["redeem_codes"].find_one({"code": code.upper()})
    if existing:
        raise HTTPException(status_code=400, detail="Code already exists")
    db["redeem_codes"].insert_one({
        "code": code.upper().strip(),
        "amount": amount,
        "maxUses": maxUses,
        "useCount": 0,
        "usedBy": [],
        "isActive": True,
        "expiresAt": datetime.now(timezone.utc) + timedelta(hours=expiryHours),
        "createdBy": admin["id"],
        "createdAt": datetime.now(timezone.utc)
    })
    return {"message": f"Redeem code {code.upper()} created for ₹{amount}"}

@app.get("/api/admin/redeem-codes")
async def list_redeem_codes(admin: dict = Depends(get_admin_user)):
    codes = list(db["redeem_codes"].find().sort("createdAt", DESCENDING))
    return [serialize_doc(c) for c in codes]

@app.delete("/api/admin/redeem-codes/{code_id}")
async def delete_redeem_code(code_id: str, admin: dict = Depends(get_admin_user)):
    db["redeem_codes"].update_one({"_id": ObjectId(code_id)}, {"$set": {"isActive": False}})
    return {"message": "Code deactivated"}

@app.get("/api/teams/invites")
async def get_my_invites(current_user: dict = Depends(get_current_user)):
    user_id_str = str(current_user.get("id") or current_user.get("_id") or "").strip()
    if not user_id_str or user_id_str in ("", "None"):
        raise HTTPException(status_code=401, detail="Not authenticated")

    teams = list(teams_col.find({"pendingInvites": user_id_str}))

    result = []
    for team in teams:
        captain_info = None
        captain_id = str(team.get("captainId", ""))
        if captain_id and captain_id not in ("", "None"):
            try:
                cap = users_col.find_one({"_id": ObjectId(captain_id)})
                if cap:
                    captain_info = {"id": captain_id, "ign": cap.get("ign", "")}
            except Exception:
                pass

        result.append({
            "id":          str(team["_id"]),
            "name":        team.get("name", ""),
            "captainId":   captain_id,
            "captain":     captain_info,
            "memberCount": len(team.get("members", [])),
        })

    return result

@app.post("/api/teams")
async def create_team(data: TeamCreate, user: dict = Depends(get_current_user)):
    # Check if user already in a team
    existing = teams_col.find_one({"members": user["id"]})
    if existing:
        raise HTTPException(status_code=400, detail="You are already in a team")
    
    # Check team name
    if teams_col.find_one({"name": {"$regex": f"^{data.name}$", "$options": "i"}}):
        raise HTTPException(status_code=400, detail="Team name already taken")
    
    team_doc = {
        "name": data.name,
        "captainId": user["id"],
        "members": [user["id"]],
        "pendingInvites": [],
        "isActive": True,
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc)
    }
    
    result = teams_col.insert_one(team_doc)
    
    return {"message": "Team created", "id": str(result.inserted_id)}

@app.get("/api/teams/my")
async def get_my_team(user: dict = Depends(get_current_user)):
    team = teams_col.find_one({"members": user["id"]})
    if not team:
        return None
    
    team_data = serialize_doc(team)
    
    # Get member details
    members = []
    for member_id in team.get("members", []):
        member = users_col.find_one({"_id": ObjectId(member_id)}, {"passwordHash": 0})
        if member:
            members.append({
                "id": str(member["_id"]),
                "fullName": member["fullName"],
                "ign": member["ign"],
                "ffUid": member["ffUid"],
                "isCaptain": str(member["_id"]) == team["captainId"]
            })
    
    team_data["memberDetails"] = members
    return team_data


@app.post("/api/teams/invite")
async def invite_to_team(data: TeamInvite, user: dict = Depends(get_current_user)):
    captain_id_str = str(user.get("id") or user.get("_id") or "").strip()
    if not captain_id_str or captain_id_str in ("", "None"):
        raise HTTPException(status_code=401, detail="User not authenticated")

    team = teams_col.find_one({"captainId": captain_id_str})
    if not team:
        raise HTTPException(status_code=403, detail="Only team captain can invite")

    if len(team.get("members", [])) >= 4:
        raise HTTPException(status_code=400, detail="Team is full (max 4 players)")

    player = users_col.find_one({"ffUid": data.ffUid})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found with this FF UID")

    player_id_str = str(player["_id"])

    if player_id_str == captain_id_str:
        raise HTTPException(status_code=400, detail="You cannot invite yourself")

    if player_id_str in [str(m) for m in team.get("members", [])]:
        raise HTTPException(status_code=400, detail="Player is already in your team")

    if player_id_str in [str(p) for p in team.get("pendingInvites", [])]:
        raise HTTPException(status_code=400, detail="Player already has a pending invite")

    if teams_col.find_one({"members": player_id_str}):
        raise HTTPException(status_code=400, detail="Player is already in another team")

    teams_col.update_one(
        {"_id": team["_id"]},
        {"$addToSet": {"pendingInvites": player_id_str}}
    )

    try:
        notifications_col.insert_one({
            "userId":    player_id_str,
            "title":     "Team Invitation",
            "message":   f"You've been invited to join team '{team.get('name', '')}'",
            "type":      "INFO",
            "isRead":    False,
            "link":      "/dashboard/team",
            "createdAt": datetime.now(timezone.utc)
        })
    except Exception:
        pass

    return {"message": f"Invitation sent to {player.get('ign', player_id_str)}"}
    

# ─── POST /api/teams/accept/{team_id} ────────────────────────────────────────
@app.post("/api/teams/accept/{team_id}")
async def accept_invite(team_id: str, current_user: dict = Depends(get_current_user)):
    if not team_id or team_id in ("undefined", "null", ""):
        raise HTTPException(status_code=400, detail="Invalid team ID")

    try:
        team_oid = ObjectId(team_id)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid team ID format: {team_id}")

    user_id_str = str(current_user.get("id") or current_user.get("_id") or "").strip()
    if not user_id_str or user_id_str in ("", "None"):
        raise HTTPException(status_code=401, detail="Not authenticated")

    team = teams_col.find_one({"_id": team_oid})
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    member_ids  = [str(m) for m in team.get("members", [])]
    pending_ids = [str(p) for p in team.get("pendingInvites", [])]

    if user_id_str in member_ids:
        raise HTTPException(status_code=400, detail="Already a member of this team")
    if user_id_str not in pending_ids:
        raise HTTPException(status_code=404, detail="No pending invite found")
    if len(member_ids) >= 4:
        raise HTTPException(status_code=400, detail="Team is already full")

    teams_col.update_one(
        {"_id": team_oid},
        {
            "$addToSet": {"members": user_id_str},
            "$pull":     {"pendingInvites": user_id_str}
        }
    )

    captain_id = str(team.get("captainId", ""))
    if captain_id and captain_id not in ("", "None"):
        try:
            notifications_col.insert_one({
                "userId":    captain_id,
                "title":     "Invite Accepted",
                "message":   f"{current_user.get('ign') or current_user.get('username') or 'A player'} joined '{team.get('name', '')}'",
                "type":      "SUCCESS",
                "isRead":    False,
                "link":      "/dashboard/team",
                "createdAt": datetime.now(timezone.utc)
            })
        except Exception:
            pass

    return {
        "message":  "Successfully joined the team",
        "teamId":   str(team_oid),
        "teamName": team.get("name", "")
    }


@app.delete("/api/teams/member/{member_id}")
async def remove_member(member_id: str, user: dict = Depends(get_current_user)):
    captain_id_str = get_user_id_str(user)
    if not captain_id_str:
        raise HTTPException(status_code=401, detail="User not authenticated")

    # Validate member_id format
    if not member_id or member_id in ("undefined", "null", ""):
        raise HTTPException(status_code=400, detail="Invalid member ID")

    # Find the team where this user is captain
    team = teams_col.find_one({"captainId": captain_id_str})
    if not team:
        raise HTTPException(status_code=403, detail="Only the team captain can remove members")

    # Prevent captain from removing themselves
    if member_id == captain_id_str:
        raise HTTPException(status_code=400, detail="You cannot remove yourself. Use 'leave team' instead")

    # Ensure the target is actually a member
    member_ids = [str(m) for m in team.get("members", [])]
    if member_id not in member_ids:
        raise HTTPException(status_code=404, detail="This player is not a member of your team")

    teams_col.update_one(
        {"_id": team["_id"]},
        {"$pull": {"members": member_id}}
    )

    # Optional: notify the removed player
    try:
        notifications_col.insert_one({
            "userId":    member_id,
            "title":     "Removed from Team",
            "message":   f"You have been removed from team '{team.get('name', '')}'",
            "type":      "WARNING",
            "isRead":    False,
            "link":      "/dashboard/team",
            "createdAt": datetime.now(timezone.utc)
        })
    except Exception:
        pass

    return {"message": "Member removed successfully"}

# ============== TOURNAMENT ROUTES ==============
@app.get("/api/tournaments")
async def list_tournaments(
    status: Optional[str] = None,
    map: Optional[str] = None,
    minFee: Optional[str] = None,
    maxFee: Optional[str] = None,
    mode: Optional[str] = None
):
    query = {}
    
    # Always exclude DRAFT; handle "undefined" from frontend
    clean_status = status if status and status not in ("undefined", "null", "all", "ALL", "") else None
    clean_map = map if map and map not in ("undefined", "null", "all", "") else None
    clean_mode = mode if mode and mode not in ("undefined", "null", "all", "") else None
    
    if clean_status:
        if clean_status == "DRAFT":
            return []
        query["status"] = clean_status
    else:
        query["status"] = {"$nin": ["DRAFT"]}
    
    if clean_map:
        query["map"] = clean_map
    
    if clean_mode:
        query["mode"] = clean_mode
    
    # Parse fees - ignore "undefined" strings from frontend
    try:
        min_fee = float(minFee) if minFee and minFee not in ("undefined", "null", "") else None
        max_fee = float(maxFee) if maxFee and maxFee not in ("undefined", "null", "") else None
    except (ValueError, TypeError):
        min_fee = max_fee = None
    
    if min_fee is not None or max_fee is not None:
        query["entryFee"] = {}
        if min_fee is not None:
            query["entryFee"]["$gte"] = min_fee
        if max_fee is not None:
            query["entryFee"]["$lte"] = max_fee
    
    # Exclude cup qualifier sub-tournaments from main listing
    # (cup qualifiers have cupId set — show them only via cup page)
    # Actually show them all for now
    tournaments = list(tournaments_col.find(query).sort([("status", ASCENDING), ("scheduledAt", ASCENDING)]))
    return [serialize_doc(t) for t in tournaments]

@app.get("/api/tournaments/featured")
async def get_featured_tournaments():
    tournaments = list(tournaments_col.find({
        "status": {"$in": ["UPCOMING", "REGISTERING", "LIVE"]}
    }).sort("scheduledAt", ASCENDING).limit(6))
    return [serialize_doc(t) for t in tournaments]

@app.get("/api/tournaments/{tournament_id}")
async def get_tournament(tournament_id: str):
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    return serialize_doc(tournament)

@app.get("/api/tournaments/{tournament_id}/teams")
async def get_tournament_teams(tournament_id: str):
    registrations = list(registrations_col.find({
        "tournamentId": tournament_id,
        "paymentStatus": "PAID"
    }).sort("slotNumber", ASCENDING))
    
    teams = []
    for reg in registrations:
        team = teams_col.find_one({"_id": ObjectId(reg["teamId"])})
        if team:
            teams.append({
                "slotNumber": reg["slotNumber"],
                "teamName": team["name"],
                "teamId": str(team["_id"])
            })
    
    return teams

@app.get("/api/tournaments/{tournament_id}/standings")
async def get_tournament_standings(tournament_id: str):
    # Get all matches for tournament
    matches = list(matches_col.find({"tournamentId": tournament_id}).sort("matchNumber", ASCENDING))
    
    # Get all results
    team_standings = {}
    
    for match in matches:
        results = list(match_results_col.find({"matchId": str(match["_id"])}))
        for result in results:
            team_id = result["teamId"]
            if team_id not in team_standings:
                team = teams_col.find_one({"_id": ObjectId(team_id)})
                team_standings[team_id] = {
                    "teamId": team_id,
                    "teamName": team["name"] if team else "Unknown",
                    "matches": {},
                    "totalPlacementPts": 0,
                    "totalKills": 0,
                    "totalKillPts": 0,
                    "totalPoints": 0
                }
            
            team_standings[team_id]["matches"][f"M{match['matchNumber']}"] = {
                "placement": result["placement"],
                "kills": result["kills"],
                "placementPoints": result["placementPoints"],
                "killPoints": result["killPoints"],
                "totalPoints": result["totalPoints"],
                "booyah": result.get("booyah", False)
            }
            team_standings[team_id]["totalPlacementPts"] += result["placementPoints"]
            team_standings[team_id]["totalKills"] += result["kills"]
            team_standings[team_id]["totalKillPts"] += result["killPoints"]
            team_standings[team_id]["totalPoints"] += result["totalPoints"]
    
    # Sort by total points, then kills
    standings = sorted(
        team_standings.values(),
        key=lambda x: (-x["totalPoints"], -x["totalKills"])
    )
    
    # Add rank
    for i, team in enumerate(standings):
        team["rank"] = i + 1
    
    return standings

@app.get("/api/tournaments/{tournament_id}/room")
async def get_room_details(tournament_id: str, user: dict = Depends(get_current_user)):
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    # Check if user's team is registered
    team = teams_col.find_one({"members": user["id"]})
    if not team:
        raise HTTPException(status_code=403, detail="Not in a team")
    
    reg = registrations_col.find_one({
        "tournamentId": tournament_id,
        "teamId": str(team["_id"]),
        "paymentStatus": "PAID"
    })
    
    if not reg:
        raise HTTPException(status_code=403, detail="Team not registered for this tournament")
    
    # Check if room is released
    if not tournament.get("roomReleasedAt"):
        return {"released": False, "message": "Room details not yet released"}
    
    return {
        "released": True,
        "roomId": tournament.get("roomId"),
        "roomPassword": tournament.get("roomPassword"),
        "releasedAt": tournament.get("roomReleasedAt")
    }

# ============== REGISTRATION ROUTES ==============
@app.post("/api/registrations/check-eligibility")
async def check_eligibility(tournament_id: str, user: dict = Depends(get_current_user)):
    issues = []
    
    # Check verification requirements from platform settings
    p_settings = db["platform_settings"].find_one({"key": "main"}) or {}
    if p_settings.get("emailVerifyRequired", False) and not user.get("emailVerified"):
        issues.append({"code": "EMAIL_NOT_VERIFIED", "message": "Email verification required"})
    if p_settings.get("mobileVerifyRequired", False) and not user.get("mobileVerified"):
        issues.append({"code": "MOBILE_NOT_VERIFIED", "message": "Mobile verification required"})
    
    # ✅ Fetch tournament FIRST before using it
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    
    # Check tournament status
    if not tournament:
        issues.append({"code": "TOURNAMENT_NOT_FOUND", "message": "Tournament not found"})
    elif tournament["status"] != "REGISTERING":
        issues.append({"code": "REGISTRATION_CLOSED", "message": "Registration not open"})
    elif tournament["filledSlots"] >= tournament["maxTeams"]:
        issues.append({"code": "TOURNAMENT_FULL", "message": "Tournament is full"})
    
    # Get mode info
    t_mode = tournament.get("mode", "BR") if tournament else "BR"
    players_needed = tournament.get("playersPerTeam", 4) if tournament else 4
    
    solo_modes = ["CS_1v1", "LW_1v1"]
    duo_modes = ["CS_2v2", "LW_2v2"]
    
    # Check team based on mode
    team = teams_col.find_one({"members": user["id"]})
    
    if t_mode in solo_modes:
        pass  # Solo — no team needed
    elif t_mode in duo_modes:
        if not team:
            issues.append({"code": "NO_TEAM", "message": "Need a duo team for this mode"})
        elif len(team.get("members", [])) < 2:
            issues.append({"code": "TEAM_INCOMPLETE", "message": "Team needs 2 members for duo mode"})
    else:
        # BR and CS_4v4 — full squad of 4
        if not team:
            issues.append({"code": "NO_TEAM", "message": "Not in a team"})
        elif len(team.get("members", [])) < players_needed:
            issues.append({"code": "TEAM_INCOMPLETE", "message": f"Team needs {players_needed} members"})
    
    # Check active ban
    active_ban = bans_col.find_one({
        "userId": user["id"],
        "isActive": True,
        "banType": {"$ne": "MATCH_TERMINATION"},
        "$or": [
            {"expiresAt": None},
            {"expiresAt": {"$gt": datetime.now(timezone.utc)}}
        ]
    })
    if active_ban:
        issues.append({"code": "BANNED", "message": f"Account banned: {active_ban['reason']}"})
    
    # Check existing registration
    if team:
        existing = registrations_col.find_one({
            "tournamentId": tournament_id,
            "teamId": str(team["_id"])
        })
        if existing:
            issues.append({"code": "ALREADY_REGISTERED", "message": "Team already registered"})
    
    return {
        "eligible": len(issues) == 0,
        "issues": issues,
        "team": serialize_doc(team) if team else None
    }

@app.post("/api/registrations/create")
async def create_registration(tournament_id: str, payment_method: str, user: dict = Depends(get_current_user)):
    # Re-check eligibility
    eligibility = await check_eligibility(tournament_id, user)
    if not eligibility["eligible"]:
        raise HTTPException(status_code=400, detail=eligibility["issues"][0]["message"])
    
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    team = teams_col.find_one({"members": user["id"]})
    
    # Get next slot
    next_slot = tournament["filledSlots"] + 1
    
    # Create registration
    reg_doc = {
        "tournamentId": tournament_id,
        "teamId": str(team["_id"]),
        "slotNumber": next_slot,
        "paymentStatus": "PENDING",
        "paymentMethod": payment_method,
        "amountPaid": tournament["entryFee"],
        "registeredAt": datetime.now(timezone.utc)
    }
    
    if payment_method == "wallet":
        # Entry fee split equally among team members
        t_mode = tournament.get("mode", "BR")
        solo_modes = ["CS_1v1", "LW_1v1"]
        duo_modes = ["CS_2v2", "LW_2v2"]
        
        if t_mode in solo_modes:
            players_count = 1
        elif t_mode in duo_modes:
            players_count = 2
        else:
            players_count = 4
        
        per_player_fee = round(tournament["entryFee"] / players_count, 2)
        
        # Deduct from each team member
        current_team = teams_col.find_one({"members": user["id"]}) if t_mode not in solo_modes else None
        members_to_charge = [user["id"]] if t_mode in solo_modes else (current_team.get("members", [user["id"]]) if current_team else [user["id"]])
        
        # Check all members have enough balance
        for member_id in members_to_charge:
            member = users_col.find_one({"_id": ObjectId(member_id)})
            if member and member.get("walletBalance", 0) < per_player_fee:
                raise HTTPException(status_code=400, detail=f"Insufficient balance for {member.get('ign','member')} (needs ₹{per_player_fee})")
        
        # Charge each member
        for member_id in members_to_charge:
            member = users_col.find_one({"_id": ObjectId(member_id)})
            if member:
                new_bal = member.get("walletBalance", 0) - per_player_fee
                users_col.update_one({"_id": ObjectId(member_id)}, {"$set": {"walletBalance": new_bal}})
                transactions_col.insert_one({
                    "userId": member_id,
                    "type": "DEBIT",
                    "amount": -per_player_fee,
                    "description": f"Tournament: {tournament['name']} (₹{per_player_fee}/player)",
                    "tournamentId": tournament_id,
                    "balanceBefore": member.get("walletBalance", 0),
                    "balanceAfter": new_bal,
                    "createdAt": datetime.now(timezone.utc)
                })
        
        # Use user's balance for reg_doc tracking
        new_balance = user.get("walletBalance", 0) - per_player_fee
        users_col.update_one({"_id": ObjectId(user["id"])}, {"$set": {"walletBalance": new_balance}})
        
        # Dummy transaction (already recorded per member above, skip duplicate)
        if not current_team or user["id"] == members_to_charge[0]:
            pass  # already recorded above
        
        # Set amount paid on registration = full entry fee
        reg_doc["amountPaid"] = tournament["entryFee"]
        
        # Create transaction — kept for backward compat
        transactions_col.insert_one({
            "userId": user["id"],
            "type": "DEBIT",
            "amount": -tournament["entryFee"],
            "description": f"Tournament registration: {tournament['name']} (₹{per_player_fee}×{players_count})",
            "tournamentId": tournament_id,
            "balanceBefore": user["walletBalance"],
            "balanceAfter": new_balance,
            "createdAt": datetime.now(timezone.utc)
        })
        
        reg_doc["paymentStatus"] = "PAID"
        reg_doc["confirmedAt"] = datetime.now(timezone.utc)
    
    result = registrations_col.insert_one(reg_doc)
    
    # Update tournament slots
    if reg_doc["paymentStatus"] == "PAID":
        tournaments_col.update_one(
            {"_id": ObjectId(tournament_id)},
            {"$inc": {"filledSlots": 1}}
        )
        
        # Send confirmation notification
        notifications_col.insert_one({
            "userId": user["id"],
            "title": "Registration Confirmed!",
            "message": f"Slot #{next_slot} confirmed for {tournament['name']}",
            "type": "SUCCESS",
            "isRead": False,
            "link": f"/tournaments/{tournament_id}",
            "createdAt": datetime.now(timezone.utc)
        })
        
        # Notify all team members
        for member_id in team.get("members", []):
            if member_id != user["id"]:
                notifications_col.insert_one({
                    "userId": member_id,
                    "title": "Team Registered!",
                    "message": f"Your team registered for {tournament['name']} - Slot #{next_slot}",
                    "type": "SUCCESS",
                    "isRead": False,
                    "createdAt": datetime.now(timezone.utc)
                })
        
        print(f"[EMAIL] Tournament confirmation sent to team members")
    
    return {
        "registrationId": str(result.inserted_id),
        "slotNumber": next_slot,
        "paymentStatus": reg_doc["paymentStatus"],
        "amountPaid": tournament["entryFee"]
    }

@app.get("/api/player/tournaments")
async def get_player_tournaments(user: dict = Depends(get_current_user)):
    team = teams_col.find_one({"members": user["id"]})
    if not team:
        return []
    
    registrations = list(registrations_col.find({"teamId": str(team["_id"])}))
    
    result = []
    for reg in registrations:
        tournament = tournaments_col.find_one({"_id": ObjectId(reg["tournamentId"])})
        if tournament:
            result.append({
                "registration": serialize_doc(reg),
                "tournament": serialize_doc(tournament)
            })
    
    return result

# ============== PAYMENT ROUTES ==============
@app.post("/api/payments/razorpay/create-order")
async def create_razorpay_order(tournament_id: str, user: dict = Depends(get_current_user)):
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    team = teams_col.find_one({"members": user["id"]})
    if not team:
        raise HTTPException(status_code=400, detail="Not in a team")
    
    # Mock Razorpay order (in production, use razorpay SDK)
    order_id = f"order_OSG{datetime.now().strftime('%Y%m%d%H%M%S')}{str(uuid.uuid4())[:8]}"
    
    # Store order reference
    registrations_col.update_one(
        {"tournamentId": tournament_id, "teamId": str(team["_id"])},
        {"$set": {"razorpayOrderId": order_id}},
        upsert=False
    )
    
    return {
        "orderId": order_id,
        "amount": int(tournament["entryFee"] * 100),
        "currency": "INR",
        "keyId": os.environ.get("RAZORPAY_KEY_ID", "rzp_test_demo")
    }

@app.post("/api/payments/razorpay/verify")
async def verify_razorpay_payment(data: PaymentVerify, user: dict = Depends(get_current_user)):
    # In production, verify signature with Razorpay
    # For demo, we'll simulate success
    
    reg = registrations_col.find_one({"razorpayOrderId": data.orderId})
    if not reg:
        raise HTTPException(status_code=404, detail="Registration not found")
    
    tournament = tournaments_col.find_one({"_id": ObjectId(reg["tournamentId"])})
    
    # Update registration
    registrations_col.update_one(
        {"_id": reg["_id"]},
        {
            "$set": {
                "paymentStatus": "PAID",
                "razorpayPaymentId": data.paymentId,
                "razorpaySignature": data.signature,
                "confirmedAt": datetime.now(timezone.utc)
            }
        }
    )
    
    # Update tournament slots
    tournaments_col.update_one(
        {"_id": ObjectId(reg["tournamentId"])},
        {"$inc": {"filledSlots": 1}}
    )
    
    return {"message": "Payment verified", "slotNumber": reg["slotNumber"]}

@app.post("/api/webhooks/razorpay")
async def razorpay_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("x-razorpay-signature", "")
    
    if not body:
        return {"status": "ok"}
    
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {"status": "ok"}
    event = data.get("event")
    
    if event == "payment.captured":
        payment = data["payload"]["payment"]["entity"]
        order_id = payment.get("order_id")
        
        reg = registrations_col.find_one({"razorpayOrderId": order_id})
        if reg:
            registrations_col.update_one(
                {"_id": reg["_id"]},
                {"$set": {"paymentStatus": "PAID", "confirmedAt": datetime.now(timezone.utc)}}
            )
            tournaments_col.update_one(
                {"_id": ObjectId(reg["tournamentId"])},
                {"$inc": {"filledSlots": 1}}
            )
    
    return {"status": "ok"}

# ============== LEADERBOARD ROUTES ==============
@app.get("/api/leaderboard")
async def get_leaderboard(period: str = "all"):
    # Filter by period
    match_filter = {}
    if period == "weekly":
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        # Get match IDs from last week
        recent_matches = list(matches_col.find(
            {"playedAt": {"$gte": week_ago}},
            {"_id": 1}
        ))
        match_ids = [str(m["_id"]) for m in recent_matches]
        if match_ids:
            match_filter = {"matchId": {"$in": match_ids}}
        else:
            return []
    elif period == "monthly":
        month_ago = datetime.now(timezone.utc) - timedelta(days=30)
        recent_matches = list(matches_col.find(
            {"playedAt": {"$gte": month_ago}},
            {"_id": 1}
        ))
        match_ids = [str(m["_id"]) for m in recent_matches]
        if match_ids:
            match_filter = {"matchId": {"$in": match_ids}}
        else:
            return []

    pipeline = [
        {"$match": match_filter} if match_filter else {"$match": {}},
        {"$group": {
            "_id": "$teamId",
            "totalKills": {"$sum": "$kills"},
            "totalWins": {"$sum": {"$cond": [{"$eq": ["$placement", 1]}, 1, 0]}},
            "totalPlacementPts": {"$sum": "$placementPoints"},
            "totalKillPts": {"$sum": "$killPoints"},
            "totalPoints": {"$sum": "$totalPoints"},
            "matchesPlayed": {"$sum": 1}
        }},
        {"$sort": {"totalPoints": -1, "totalKills": -1}},
        {"$limit": 100}
    ]

    results = list(match_results_col.aggregate(pipeline))

    leaderboard = []
    for i, entry in enumerate(results):
        if not entry["_id"]:
            continue
        try:
            team = teams_col.find_one({"_id": ObjectId(entry["_id"])})
        except Exception:
            continue
        if team:
            # Safe ObjectId conversion
            captain = None
            try:
                captain = users_col.find_one({"_id": ObjectId(team["captainId"])})
            except Exception:
                pass
            
            # Get total earnings
            total_earnings = 0
            try:
                earnings = transactions_col.aggregate([
                    {"$match": {"userId": str(team.get("captainId", "")), "type": "PRIZE"}},
                    {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
                ])
                earnings_list = list(earnings)
                total_earnings = earnings_list[0]["total"] if earnings_list else 0
            except Exception:
                pass

            leaderboard.append({
                "rank": i + 1,
                "teamName": team["name"],
                "captainIgn": captain["ign"] if captain else team.get("name", "Unknown"),
                "totalKills": entry["totalKills"],
                "totalWins": entry["totalWins"],
                "totalPlacementPts": entry["totalPlacementPts"],
                "totalKillPts": entry["totalKillPts"],
                "totalPoints": entry["totalPoints"],
                "matchesPlayed": entry["matchesPlayed"],
                "totalEarnings": total_earnings
            })

    return leaderboard

@app.get("/api/leaderboard/tournament/{tournament_id}")
async def get_tournament_leaderboard(tournament_id: str):
    """Public tournament standings"""
    standings = await get_tournament_standings(tournament_id)
    return standings

# ============== NOTIFICATIONS ==============
@app.get("/api/notifications")
async def get_notifications(user: dict = Depends(get_current_user)):
    notifications = list(notifications_col.find(
        {"userId": user["id"]}
    ).sort("createdAt", DESCENDING).limit(20))
    
    return [serialize_doc(n) for n in notifications]

@app.put("/api/notifications/{notification_id}/read")
async def mark_notification_read(notification_id: str, user: dict = Depends(get_current_user)):
    notifications_col.update_one(
        {"_id": ObjectId(notification_id), "userId": user["id"]},
        {"$set": {"isRead": True}}
    )
    return {"message": "Marked as read"}

@app.put("/api/notifications/read-all")
async def mark_all_read(user: dict = Depends(get_current_user)):
    notifications_col.update_many(
        {"userId": user["id"]},
        {"$set": {"isRead": True}}
    )
    return {"message": "All marked as read"}

# ============== BAN ROUTES (Player) ==============
@app.get("/api/player/bans")
async def get_player_bans(user: dict = Depends(get_current_user)):
    bans = list(bans_col.find({"userId": user["id"]}).sort("createdAt", DESCENDING))
    return [serialize_doc(b) for b in bans]

@app.post("/api/player/bans/{ban_id}/appeal")
async def appeal_ban(ban_id: str, appeal_text: str, user: dict = Depends(get_current_user)):
    ban = bans_col.find_one({"_id": ObjectId(ban_id), "userId": user["id"]})
    if not ban:
        raise HTTPException(status_code=404, detail="Ban not found")
    
    # Allow appeals for all ban types except PERMANENT
    if ban["banType"] == "PERMANENT":
        raise HTTPException(status_code=400, detail="Permanent bans cannot be appealed")
    
    if ban.get("appealStatus") == "PENDING":
        raise HTTPException(status_code=400, detail="Appeal already submitted and pending review")
    
    if ban.get("appealStatus") == "REJECTED":
        raise HTTPException(status_code=400, detail="Your previous appeal was rejected")
    
    if not ban.get("isActive"):
        raise HTTPException(status_code=400, detail="This ban is no longer active")
    
    if not appeal_text or len(appeal_text.strip()) < 10:
        raise HTTPException(status_code=400, detail="Appeal text must be at least 10 characters")
    
    bans_col.update_one(
        {"_id": ObjectId(ban_id)},
        {
            "$set": {
                "appealText": appeal_text.strip(),
                "appealStatus": "PENDING",
                "appealedAt": datetime.now(timezone.utc)
            }
        }
    )
    
    # Notify admin
    admin = users_col.find_one({"role": "ADMIN"})
    if admin:
        notifications_col.insert_one({
            "userId": str(admin["_id"]),
            "title": "Ban Appeal Received",
            "message": f"Player {user.get('ign', 'Unknown')} has appealed their {ban['banType']} ban",
            "type": "INFO",
            "isRead": False,
            "link": f"/admin/players/{user['id']}",
            "createdAt": datetime.now(timezone.utc)
        })
    
    return {"message": "Appeal submitted successfully. Admin will review within 24 hours."}

# ============== ADMIN ROUTES ==============
@app.get("/api/admin/dashboard")
async def admin_dashboard(admin: dict = Depends(get_admin_user)):
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Stats
    active_tournaments = tournaments_col.count_documents({"status": {"$in": ["UPCOMING", "REGISTERING", "LIVE"]}})
    today_registrations = registrations_col.count_documents({
        "registeredAt": {"$gte": today_start},
        "paymentStatus": "PAID"
    })
    
    # Today's revenue
    today_regs = list(registrations_col.find({
        "registeredAt": {"$gte": today_start},
        "paymentStatus": "PAID"
    }))
    today_revenue = sum(r.get("amountPaid", 0) for r in today_regs)
    
    pending_withdrawals = withdrawals_col.count_documents({"status": "PENDING"})
    
    # Recent registrations
    recent = list(registrations_col.find({"paymentStatus": "PAID"}).sort("confirmedAt", DESCENDING).limit(10))
    recent_data = []
    for reg in recent:
        team = teams_col.find_one({"_id": ObjectId(reg["teamId"])})
        tournament = tournaments_col.find_one({"_id": ObjectId(reg["tournamentId"])})
        recent_data.append({
            "teamName": team["name"] if team else "Unknown",
            "tournamentName": tournament["name"] if tournament else "Unknown",
            "slotNumber": reg["slotNumber"],
            "amount": reg["amountPaid"],
            "confirmedAt": reg.get("confirmedAt")
        })
    
    return {
        "stats": {
            "activeTournaments": active_tournaments,
            "todayRegistrations": today_registrations,
            "todayRevenue": today_revenue,
            "pendingWithdrawals": pending_withdrawals
        },
        "recentRegistrations": recent_data
    }


@app.put("/api/admin/tournaments/{tournament_id}/edit")
async def edit_tournament_details(tournament_id: str, data: dict, admin: dict = Depends(get_admin_user)):
    """Full edit of any tournament field"""
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    allowed = ["name","map","maps","scheduledAt","entryFee","maxTeams","prizePool",
               "perKillPrize","rules","youtubeUrl","status","roomId","roomPassword",
               "description","bannerUrl","matchCount"]
    update = {k: v for k, v in data.items() if k in allowed}
    if "scheduledAt" in update and isinstance(update["scheduledAt"], str):
        try:
            update["scheduledAt"] = datetime.fromisoformat(update["scheduledAt"].replace("Z","")).replace(tzinfo=timezone.utc)
        except: pass
    update["updatedAt"] = datetime.now(timezone.utc)
    tournaments_col.update_one({"_id": ObjectId(tournament_id)}, {"$set": update})
    return {"message": "Tournament updated"}

@app.get("/api/admin/cups")
async def get_all_cups(admin: dict = Depends(get_admin_user)):
    cups = list(db["cups"].find().sort("createdAt", DESCENDING))
    result = []
    for cup in cups:
        qualifiers = list(tournaments_col.find({"cupId": str(cup["_id"]), "tournamentType": "QUALIFIER"}))
        finals = tournaments_col.find_one({"cupId": str(cup["_id"]), "tournamentType": "FINALS"})
        result.append({
            **serialize_doc(cup),
            "qualifierCount": len(qualifiers),
            "qualifiers": [{"id": str(q["_id"]), "name": q["name"], "status": q["status"], "qualifierNumber": q.get("qualifierNumber")} for q in qualifiers],
            "finals": {"id": str(finals["_id"]), "name": finals["name"], "status": finals["status"]} if finals else None
        })
    return result

@app.post("/api/admin/cup-tournaments")
async def create_cup_tournament(data: dict, admin: dict = Depends(get_admin_user)):
    """Create Cup with qualifiers auto-generated"""
    import random as rand_mod
    cup_name = data.get("name","OSG Cup")
    num_q = int(data.get("numQualifiers",2))
    teams_per_q = int(data.get("teamsPerQualifier",12))
    maps_per_q = data.get("mapsPerQualifier",["BERMUDA","PURGATORY","KALAHARI"])
    finals_teams = int(data.get("finalsTeams",6))
    entry_fee = float(data.get("entryFee",50))
    prize_pool = data.get("prizePool",{"1":1000,"2":500,"3":300})
    per_kill = float(data.get("perKillPrize",5))
    rules = data.get("rules","Standard OSG Cup rules.")
    scheduled_str = data.get("scheduledAt")
    scheduled_at = datetime.now(timezone.utc) + timedelta(days=3)
    if scheduled_str:
        try: scheduled_at = datetime.fromisoformat(scheduled_str.replace("Z","")).replace(tzinfo=timezone.utc)
        except: pass

    cup = db["cups"].insert_one({
        "name": cup_name, "status": "UPCOMING",
        "numQualifiers": num_q, "teamsPerQualifier": teams_per_q,
        "mapsPerQualifier": maps_per_q, "finalsTeams": finals_teams,
        "finalsGenerated": False, "entryFee": entry_fee,
        "prizePool": prize_pool, "perKillPrize": per_kill,
        "rules": rules, "createdBy": admin["id"],
        "createdAt": datetime.now(timezone.utc),
    })
    cup_id = str(cup.inserted_id)
    created = []
    for q in range(1, num_q+1):
        t = tournaments_col.insert_one({
            "name": f"{cup_name} — Qualifier {q}",
            "cupId": cup_id, "cupName": cup_name,
            "tournamentType": "QUALIFIER", "qualifierNumber": q,
            "map": maps_per_q[0] if maps_per_q else "BERMUDA",
            "maps": maps_per_q, "matchCount": len(maps_per_q),
            "scheduledAt": scheduled_at + timedelta(days=(q-1)*2),
            "entryFee": entry_fee, "maxTeams": teams_per_q,
            "playersPerTeam": 4, "prizePool": {},
            "perKillPrize": per_kill, "rules": rules,
            "status": "UPCOMING", "totalSlots": teams_per_q,
            "filledSlots": 0,
            "finalsTeamsCount": max(1, finals_teams // num_q),
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        })
        t_id = str(t.inserted_id)
        for mi, map_name in enumerate(maps_per_q):
            matches_col.insert_one({"tournamentId": t_id, "matchNumber": mi+1, "mapName": map_name, "status": "PENDING", "createdAt": datetime.now(timezone.utc)})
        created.append({"id": t_id, "name": f"Qualifier {q}"})
    return {"message": f"Cup created with {num_q} qualifiers", "cupId": cup_id, "tournaments": created}

@app.post("/api/admin/cups/{cup_id}/generate-finals")
async def generate_cup_finals(cup_id: str, admin: dict = Depends(get_admin_user)):
    """Auto-generate finals from qualifier standings"""
    cup = db["cups"].find_one({"_id": ObjectId(cup_id)})
    if not cup:
        raise HTTPException(status_code=404, detail="Cup not found")
    qualifiers = list(tournaments_col.find({"cupId": cup_id, "tournamentType": "QUALIFIER"}))
    if not qualifiers:
        raise HTTPException(status_code=400, detail="No qualifiers found")
    per_q = max(1, cup["finalsTeams"] // len(qualifiers))
    advancing = []
    for q in qualifiers:
        standings = await get_tournament_standings(str(q["_id"]))
        for team in standings[:per_q]:
            advancing.append({"teamId": team["teamId"], "teamName": team["teamName"], "fromQualifier": q.get("qualifierNumber",1), "qualifierPoints": team["totalPoints"]})
    if len(advancing) < 2:
        raise HTTPException(status_code=400, detail="Not enough teams qualified. Enter match results first.")
    finals_maps = cup.get("mapsPerQualifier",["BERMUDA","PURGATORY","KALAHARI","ALPHINE","NEXTERRA","SOLARA"])[:6]
    finals = tournaments_col.insert_one({
        "name": f"{cup['name']} — Finals", "cupId": cup_id, "cupName": cup["name"],
        "tournamentType": "FINALS", "map": finals_maps[0], "maps": finals_maps,
        "matchCount": len(finals_maps),
        "scheduledAt": datetime.now(timezone.utc) + timedelta(days=7),
        "entryFee": 0, "maxTeams": len(advancing), "playersPerTeam": 4,
        "prizePool": cup.get("prizePool",{"1":1000}),
        "perKillPrize": cup.get("perKillPrize",5),
        "rules": cup.get("rules",""), "status": "UPCOMING",
        "totalSlots": len(advancing), "filledSlots": len(advancing),
        "advancingTeams": advancing,
        "createdAt": datetime.now(timezone.utc), "updatedAt": datetime.now(timezone.utc)
    })
    finals_id = str(finals.inserted_id)
    for mi, map_name in enumerate(finals_maps):
        matches_col.insert_one({"tournamentId": finals_id, "matchNumber": mi+1, "mapName": map_name, "status": "PENDING", "createdAt": datetime.now(timezone.utc)})
    for si, team_info in enumerate(advancing):
        registrations_col.update_one(
            {"tournamentId": finals_id, "teamId": team_info["teamId"]},
            {"$set": {"tournamentId": finals_id, "teamId": team_info["teamId"], "slotNumber": si+1, "paymentStatus": "PAID", "amountPaid": 0, "fromQualifier": team_info["fromQualifier"], "registeredAt": datetime.now(timezone.utc), "confirmedAt": datetime.now(timezone.utc)}},
            upsert=True
        )
    db["cups"].update_one({"_id": ObjectId(cup_id)}, {"$set": {"finalsGenerated": True, "finalsId": finals_id}})
    return {"message": f"Finals created with {len(advancing)} teams", "finalsId": finals_id, "advancingTeams": advancing}

@app.post("/api/admin/tournaments")
async def create_tournament(data: TournamentCreate, admin: dict = Depends(get_admin_user)):
    # Determine players per team and mode constraints
    mode = data.mode or "BR"
    players_map = {
        "BR": 4, "CS_4v4": 4, "CS_2v2": 2, "CS_1v1": 1, "LW_1v1": 1, "LW_2v2": 2
    }
    players_per_team = players_map.get(mode, data.playersPerTeam)
    
    # Override map for Lone Wolf (Iron Cage only)
    tournament_map = data.map
    if mode in ["LW_1v1", "LW_2v2"]:
        tournament_map = "IRON_CAGE"
    
    # Cap max teams based on mode
    max_teams_cap = {
        "BR": 48,   # supports large tournaments
        "CS_4v4": 16, "CS_2v2": 16, "CS_1v1": 48,  # CS bracket style
        "LW_1v1": 48, "LW_2v2": 16
    }
    max_teams = min(data.maxTeams, max_teams_cap.get(mode, 48))
    
    tournament_doc = {
        "name": data.name,
        "map": data.map,
        "scheduledAt": datetime.fromisoformat(data.scheduledAt.replace("Z", "+00:00")),
        "entryFee": data.entryFee,
        "maxTeams": max_teams,
        "playersPerTeam": players_per_team,
        "prizePool": data.prizePool,
        "perKillPrize": data.perKillPrize,
        "rules": data.rules,
        "youtubeUrl": data.youtubeUrl,
        "mode": mode,
        "description": data.description,
        "status": "DRAFT",
        "totalSlots": max_teams,
        "filledSlots": 0,
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc)
    }
    
    result = tournaments_col.insert_one(tournament_doc)
    
    # Create matches - use maps list if provided, otherwise single map
    maps_list = getattr(data, "maps", None) or [tournament_map]
    if not maps_list:
        maps_list = [tournament_map]
    
    # For multi-match tournaments, create one match per map
    num_matches = max(len(maps_list), 1)
    
    for i in range(1, num_matches + 1):
        map_for_match = maps_list[i - 1] if i <= len(maps_list) else tournament_map
        matches_col.insert_one({
            "tournamentId": str(result.inserted_id),
            "matchNumber": i,
            "mapName": map_for_match,
            "status": "PENDING",
            "createdAt": datetime.now(timezone.utc)
        })
    
    return {"message": "Tournament created", "id": str(result.inserted_id)}

@app.put("/api/admin/tournaments/{tournament_id}")
async def update_tournament(tournament_id: str, data: TournamentUpdate, admin: dict = Depends(get_admin_user)):
    update_data = {"updatedAt": datetime.now(timezone.utc)}
    
    for field, value in data.model_dump(exclude_unset=True).items():
        if value is not None:
            if field == "scheduledAt":
                update_data[field] = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif field == "postponeNewDate" and value:
                update_data[field] = datetime.fromisoformat(value.replace("Z", "+00:00"))
            else:
                update_data[field] = value
    
    tournaments_col.update_one({"_id": ObjectId(tournament_id)}, {"$set": update_data})
    
    # Handle status changes
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    
    if data.status == "CANCELLED" and data.cancelReason:
        # Refund all registrations
        registrations = list(registrations_col.find({
            "tournamentId": tournament_id,
            "paymentStatus": "PAID"
        }))
        
        for reg in registrations:
            team = teams_col.find_one({"_id": ObjectId(reg["teamId"])})
            if team:
                for member_id in team.get("members", []):
                    # Credit wallet
                    user = users_col.find_one({"_id": ObjectId(member_id)})
                    if user:
                        share = reg["amountPaid"] / 4
                        new_balance = user.get("walletBalance", 0) + share
                        users_col.update_one(
                            {"_id": ObjectId(member_id)},
                            {"$set": {"walletBalance": new_balance}}
                        )
                        transactions_col.insert_one({
                            "userId": member_id,
                            "type": "REFUND",
                            "amount": share,
                            "description": f"Refund: {tournament['name']} cancelled",
                            "tournamentId": tournament_id,
                            "balanceBefore": user.get("walletBalance", 0),
                            "balanceAfter": new_balance,
                            "createdAt": datetime.now(timezone.utc)
                        })
            
            registrations_col.update_one(
                {"_id": reg["_id"]},
                {"$set": {"paymentStatus": "REFUNDED"}}
            )
        
        print(f"[EMAIL] Tournament cancelled notifications sent")
    
    return {"message": "Tournament updated"}

@app.post("/api/admin/tournaments/{tournament_id}/release-room")
async def release_room(tournament_id: str, admin: dict = Depends(get_admin_user)):
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    if not tournament.get("roomId") or not tournament.get("roomPassword"):
        raise HTTPException(status_code=400, detail="Room details not set")
    
    tournaments_col.update_one(
        {"_id": ObjectId(tournament_id)},
        {"$set": {"roomReleasedAt": datetime.now(timezone.utc)}}
    )
    
    # Notify all registered teams
    registrations = list(registrations_col.find({
        "tournamentId": tournament_id,
        "paymentStatus": "PAID"
    }))
    
    for reg in registrations:
        team = teams_col.find_one({"_id": ObjectId(reg["teamId"])})
        if team:
            for member_id in team.get("members", []):
                notifications_col.insert_one({
                    "userId": member_id,
                    "title": "🚨 ROOM OPEN!",
                    "message": f"Room details released for {tournament['name']}. Check now!",
                    "type": "WARNING",
                    "isRead": False,
                    "link": f"/tournaments/{tournament_id}",
                    "createdAt": datetime.now(timezone.utc)
                })
    
    # Broadcast SSE
    await broadcast_sse(f"tournament:{tournament_id}", {
        "type": "ROOM_RELEASED",
        "tournamentId": tournament_id,
        "roomId": tournament["roomId"],
        "roomPassword": tournament["roomPassword"]
    })
    
    print(f"[EMAIL] Room ID released notifications sent")
    
    return {"message": "Room details released"}

@app.get("/api/admin/tournaments/{tournament_id}/matches")
async def get_tournament_matches(tournament_id: str, admin: dict = Depends(get_admin_user)):
    """Get all matches with their current results for a tournament"""
    matches = list(matches_col.find(
        {"tournamentId": tournament_id}
    ).sort("matchNumber", ASCENDING))
    
    result = []
    for match in matches:
        match_id = str(match["_id"])
        results = list(match_results_col.find({"matchId": match_id}))
        
        # Get registered teams for this tournament
        registrations = list(registrations_col.find({
            "tournamentId": tournament_id,
            "paymentStatus": "PAID"
        }))
        
        teams_data = []
        for reg in registrations:
            team = teams_col.find_one({"_id": ObjectId(reg["teamId"])})
            if team:
                # Find existing result for this team
                existing = next((r for r in results if r["teamId"] == str(team["_id"])), None)
                teams_data.append({
                    "teamId": str(team["_id"]),
                    "teamName": team["name"],
                    "slotNumber": reg["slotNumber"],
                    "kills": existing["kills"] if existing else 0,
                    "placement": existing["placement"] if existing else 0,
                    "booyah": existing.get("booyah", False) if existing else False,
                    "placementPoints": existing["placementPoints"] if existing else 0,
                    "killPoints": existing["killPoints"] if existing else 0,
                    "totalPoints": existing["totalPoints"] if existing else 0,
                })
        
        # Sort by slot number
        teams_data.sort(key=lambda x: x["slotNumber"])
        
        result.append({
            "id": match_id,
            "matchNumber": match["matchNumber"],
            "mapName": match.get("mapName", ""),
            "status": match["status"],
            "teams": teams_data
        })
    
    return result

@app.post("/api/admin/matches/{match_id}/results")
async def save_match_results(match_id: str, results: List[MatchResultEntry], admin: dict = Depends(get_admin_user)):
    match = matches_col.find_one({"_id": ObjectId(match_id)})
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    
    for entry in results:
        points = calculate_match_points(entry.kills, entry.placement)
        
        match_results_col.update_one(
            {"matchId": match_id, "teamId": entry.teamId},
            {
                "$set": {
                    "matchId": match_id,
                    "teamId": entry.teamId,
                    "kills": entry.kills,
                    "placement": entry.placement,
                    "booyah": entry.placement == 1,
                    "placementPoints": points["placementPoints"],
                    "killPoints": points["killPoints"],
                    "totalPoints": points["totalPoints"]
                }
            },
            upsert=True
        )
    
    matches_col.update_one(
        {"_id": ObjectId(match_id)},
        {"$set": {"status": "COMPLETED", "playedAt": datetime.now(timezone.utc)}}
    )
    
    # Broadcast standings update
    standings = await get_tournament_standings(match["tournamentId"])
    await broadcast_sse(f"tournament:{match['tournamentId']}", {
        "type": "STANDINGS_UPDATE",
        "standings": standings
    })
    
    return {"message": "Results saved"}

@app.post("/api/admin/tournaments/{tournament_id}/distribute-prizes")
async def distribute_prizes(tournament_id: str, admin: dict = Depends(get_admin_user)):
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    standings = await get_tournament_standings(tournament_id)
    prize_pool = tournament.get("prizePool", {})
    per_kill = tournament.get("perKillPrize", 0)
    
    distributions = []
    
    for team in standings:
        rank = team["rank"]
        prize = prize_pool.get(str(rank), 0)
        kill_prize = team["totalKills"] * per_kill
        total_prize = prize + kill_prize
        
        if total_prize > 0:
            team_doc = teams_col.find_one({"_id": ObjectId(team["teamId"])})
            if team_doc:
                share = total_prize / len(team_doc.get("members", []))
                
                for member_id in team_doc.get("members", []):
                    try:
                        user = users_col.find_one({"_id": ObjectId(member_id)})
                    except Exception:
                        user = None
                    if user:
                        new_balance = user.get("walletBalance", 0) + share
                        users_col.update_one(
                            {"_id": ObjectId(member_id)},
                            {"$set": {"walletBalance": new_balance}}
                        )
                        transactions_col.insert_one({
                            "userId": member_id,
                            "type": "PRIZE",
                            "amount": share,
                            "description": f"Prize: {tournament['name']} - Rank #{rank}",
                            "tournamentId": tournament_id,
                            "balanceBefore": user.get("walletBalance", 0),
                            "balanceAfter": new_balance,
                            "createdAt": datetime.now(timezone.utc)
                        })
                        
                        notifications_col.insert_one({
                            "userId": member_id,
                            "title": "🏆 Prize Credited!",
                            "message": f"₹{share:.2f} credited for {tournament['name']} - Rank #{rank}",
                            "type": "SUCCESS",
                            "isRead": False,
                            "createdAt": datetime.now(timezone.utc)
                        })
                
                distributions.append({
                    "rank": rank,
                    "teamName": team["teamName"],
                    "totalPrize": total_prize,
                    "perPlayer": share
                })
    
    tournaments_col.update_one(
        {"_id": ObjectId(tournament_id)},
        {"$set": {"status": "COMPLETED"}}
    )
    
    print(f"[EMAIL] Prize distribution notifications sent")
    
    return {"message": "Prizes distributed", "distributions": distributions}

# ============== ADMIN PLAYER MANAGEMENT ==============
@app.get("/api/admin/players")
async def list_players(search: str = None, admin: dict = Depends(get_admin_user)):
    query = {}
    if search:
        query["$or"] = [
            {"ign": {"$regex": search, "$options": "i"}},
            {"ffUid": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"mobile": {"$regex": search, "$options": "i"}}
        ]
    
    players = list(users_col.find(query, {"passwordHash": 0}).limit(50))
    return [serialize_doc(p) for p in players]

@app.get("/api/admin/players/{player_id}")
async def get_player_detail(player_id: str, admin: dict = Depends(get_admin_user)):
    player = users_col.find_one({"_id": ObjectId(player_id)}, {"passwordHash": 0})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    # Get team
    team = teams_col.find_one({"members": player_id})
    
    # Get bans
    bans = list(bans_col.find({"userId": player_id}))
    
    # Get transactions
    transactions = list(transactions_col.find({"userId": player_id}).sort("createdAt", DESCENDING).limit(20))
    
    return {
        "player": serialize_doc(player),
        "team": serialize_doc(team) if team else None,
        "bans": [serialize_doc(b) for b in bans],
        "transactions": [serialize_doc(t) for t in transactions]
    }

@app.post("/api/admin/players/{player_id}/ban")
async def ban_player(player_id: str, data: BanCreate, admin: dict = Depends(get_admin_user)):
    player = users_col.find_one({"_id": ObjectId(player_id)})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    # Calculate expiry
    ban_durations = {
        "MATCH_TERMINATION": None,
        "THREE_DAYS": timedelta(days=3),
        "SEVEN_DAYS": timedelta(days=7),
        "THIRTY_DAYS": timedelta(days=30),
        "PERMANENT": None
    }
    
    duration = ban_durations.get(data.banType)
    expires_at = datetime.now(timezone.utc) + duration if duration else None
    
    ban_doc = {
        "userId": player_id,
        "banType": data.banType,
        "reason": data.reason,
        "evidence": data.evidence,
        "matchId": data.matchId,
        "tournamentId": data.tournamentId,
        "ipBanned": data.ipBanned,
        "isActive": True,
        "startsAt": datetime.now(timezone.utc),
        "expiresAt": expires_at,
        "createdBy": admin["id"],
        "createdAt": datetime.now(timezone.utc)
    }
    
    # Handle IP ban for permanent
    if data.banType == "PERMANENT" or data.ipBanned:
        ip = player.get("registeredIp") or player.get("lastLoginIp")
        if ip:
            ban_doc["bannedIp"] = ip
            banned_ips_col.update_one(
                {"ip": ip},
                {"$set": {"ip": ip, "reason": data.reason, "userId": player_id, "createdAt": datetime.now(timezone.utc)}},
                upsert=True
            )
    
    bans_col.insert_one(ban_doc)
    
    # Set user isActive=False for all bans except MATCH_TERMINATION
    if data.banType != "MATCH_TERMINATION":
        users_col.update_one(
            {"_id": ObjectId(player_id)},
            {"$set": {"isActive": False}}
        )
    
    # Create notification
    notifications_col.insert_one({
        "userId": player_id,
        "title": "⛔ Account Restricted",
        "message": f"Your account has been banned: {data.reason}",
        "type": "DANGER",
        "isRead": False,
        "link": "/dashboard/bans",
        "createdAt": datetime.now(timezone.utc)
    })
    
    print(f"[EMAIL] Ban notice sent to {player['email']}")
    
    return {"message": "Player banned"}

@app.post("/api/admin/players/{player_id}/unban")
async def unban_player(player_id: str, admin: dict = Depends(get_admin_user)):
    bans_col.update_many(
        {"userId": player_id, "isActive": True},
        {"$set": {"isActive": False, "resolvedAt": datetime.now(timezone.utc), "resolvedBy": admin["id"]}}
    )
    
    # Restore user account
    users_col.update_one(
        {"_id": ObjectId(player_id)},
        {"$set": {"isActive": True}}
    )
    
    # Remove IP bans
    player = users_col.find_one({"_id": ObjectId(player_id)})
    if player:
        banned_ips_col.delete_many({"userId": player_id})
    
    notifications_col.insert_one({
        "userId": player_id,
        "title": "Account Restored",
        "message": "Your ban has been lifted. Welcome back!",
        "type": "SUCCESS",
        "isRead": False,
        "createdAt": datetime.now(timezone.utc)
    })
    
    return {"message": "Player unbanned"}

# ============== ADMIN WITHDRAWALS ==============
@app.get("/api/admin/withdrawals")
async def list_withdrawals(status: str = None, admin: dict = Depends(get_admin_user)):
    query = {}
    if status:
        query["status"] = status
    
    withdrawals = list(withdrawals_col.find(query).sort("requestedAt", DESCENDING))
    
    result = []
    for w in withdrawals:
        user = users_col.find_one({"_id": ObjectId(w["userId"])}, {"passwordHash": 0})
        result.append({
            **serialize_doc(w),
            "playerName": user["fullName"] if user else "Unknown",
            "playerIgn": user["ign"] if user else "Unknown"
        })
    
    return result

@app.put("/api/admin/withdrawals/{withdrawal_id}")
async def process_withdrawal(withdrawal_id: str, data: WithdrawalProcess, admin: dict = Depends(get_admin_user)):
    withdrawal = withdrawals_col.find_one({"_id": ObjectId(withdrawal_id)})
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    update_data = {
        "status": data.status,
        "processedBy": admin["id"],
        "processedAt": datetime.now(timezone.utc)
    }
    
    if data.utrNumber:
        update_data["utrNumber"] = data.utrNumber
    
    if data.rejectionReason:
        update_data["rejectionReason"] = data.rejectionReason
        
        # Refund to wallet
        user = users_col.find_one({"_id": ObjectId(withdrawal["userId"])})
        new_balance = user.get("walletBalance", 0) + withdrawal["amount"]
        users_col.update_one(
            {"_id": ObjectId(withdrawal["userId"])},
            {"$set": {"walletBalance": new_balance}}
        )
        transactions_col.insert_one({
            "userId": withdrawal["userId"],
            "type": "CREDIT",
            "amount": withdrawal["amount"],
            "description": f"Withdrawal rejected: {data.rejectionReason}",
            "referenceId": withdrawal_id,
            "balanceBefore": user.get("walletBalance", 0),
            "balanceAfter": new_balance,
            "createdAt": datetime.now(timezone.utc)
        })
    
    withdrawals_col.update_one({"_id": ObjectId(withdrawal_id)}, {"$set": update_data})
    
    # Notify user
    status_msg = "approved" if data.status == "COMPLETED" else "rejected"
    notifications_col.insert_one({
        "userId": withdrawal["userId"],
        "title": f"Withdrawal {status_msg.title()}",
        "message": f"Your withdrawal of ₹{withdrawal['amount']} has been {status_msg}",
        "type": "SUCCESS" if data.status == "COMPLETED" else "DANGER",
        "isRead": False,
        "createdAt": datetime.now(timezone.utc)
    })
    
    print(f"[EMAIL] Withdrawal status update sent")
    
    return {"message": f"Withdrawal {status_msg}"}

# ============== SSE (Server-Sent Events) ==============
async def broadcast_sse(channel: str, data: dict):
    if channel in sse_clients:
        for queue in sse_clients[channel]:
            await queue.put(data)

@app.get("/api/sse/tournament/{tournament_id}")
async def tournament_sse(tournament_id: str):
    queue = asyncio.Queue()
    channel = f"tournament:{tournament_id}"
    
    if channel not in sse_clients:
        sse_clients[channel] = []
    sse_clients[channel].append(queue)
    
    async def event_generator():
        try:
            while True:
                data = await asyncio.wait_for(queue.get(), timeout=30)
                yield f"data: {json.dumps(data)}\n\n"
        except asyncio.TimeoutError:
            yield f"data: {json.dumps({'type': 'PING'})}\n\n"
        except Exception:
            pass
        finally:
            if channel in sse_clients:
                sse_clients[channel].remove(queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ============== STATS ==============
@app.get("/api/stats/global")
async def get_global_stats():
    total_tournaments = tournaments_col.count_documents({"status": "COMPLETED"})
    total_players = users_col.count_documents({"role": "PLAYER"})
    
    # Total prize distributed
    prize_txns = transactions_col.aggregate([
        {"$match": {"type": "PRIZE"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ])
    prize_list = list(prize_txns)
    total_prize = prize_list[0]["total"] if prize_list else 0
    
    return {
        "totalTournaments": total_tournaments,
        "totalPlayers": total_players,
        "totalPrizeDistributed": total_prize
    }

# ============== SEED DATA ==============
@app.post("/api/seed")
async def seed_database():
    # Create admin if not exists
    if not users_col.find_one({"email": "admin@osglive.in"}):
        users_col.insert_one({
            "fullName": "OSG Admin",
            "email": "admin@osglive.in",
            "emailVerified": True,
            "mobile": "9999999999",
            "mobileVerified": True,
            "passwordHash": get_password_hash("Admin@1234"),
            "ffUid": "999999999",
            "ign": "OSG_ADMIN",
            "state": "Maharashtra",
            "dob": datetime(1990, 1, 1, tzinfo=timezone.utc),
            "role": "ADMIN",
            "walletBalance": 0,
            "isActive": True,
            "registeredIp": "127.0.0.1",
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        })
    
    # NOTE: No auto-tournament creation — admin creates manually
    # Sample tournament removed to prevent duplicates on redeploy
    
    return {"message": "Seed complete. Admin: admin@osglive.in / Admin@1234"}

@app.post("/api/seed/test-tournament")
async def seed_test_tournament():
    """Create a complete test tournament with 12 fake teams and match results for testing"""
    import random

    team_names = [
        "TAMIZH TITANS", "X PR!ME", "SUNNY PLAYZE", "REX E ESPORTS",
        "TEAM MONSTERS", "TEAM KYT", "TPL ESPORTS", "TEAM HARRY",
        "BORN2BOTZ", "HIGHSPEED FF", "WARRIORS", "OSG ESPORTS"
    ]

    # Create or get test tournament
    existing = tournaments_col.find_one({"name": "OSG Test Tournament"})
    if existing:
        # Clean up old test data
        old_id = str(existing["_id"])
        old_matches = list(matches_col.find({"tournamentId": old_id}))
        for m in old_matches:
            match_results_col.delete_many({"matchId": str(m["_id"])})
        matches_col.delete_many({"tournamentId": old_id})
        old_regs = list(registrations_col.find({"tournamentId": old_id}))
        for r in old_regs:
            teams_col.delete_one({"_id": ObjectId(r["teamId"])})
        registrations_col.delete_many({"tournamentId": old_id})
        tournaments_col.delete_one({"_id": existing["_id"]})

    # Create tournament
    tournament = tournaments_col.insert_one({
        "name": "OSG Test Tournament",
        "map": "BERMUDA",
        "scheduledAt": datetime.now(timezone.utc),
        "entryFee": 50,
        "maxTeams": 12,
        "playersPerTeam": 4,
        "prizePool": {"1": 500, "2": 300, "3": 200},
        "perKillPrize": 5,
        "rules": "Test tournament - ignore",
        "status": "LIVE",
        "roomId": "TEST123",
        "roomPassword": "osg123",
        "roomReleasedAt": datetime.now(timezone.utc),
        "totalSlots": 12,
        "filledSlots": 12,
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc)
    })
    t_id = str(tournament.inserted_id)

    # Create 6 matches
    match_ids = []
    maps = ["BERMUDA", "PURGATORY", "KALAHARI", "ALPHINE", "NEXTERRA", "SOLARA"]
    for i in range(1, 7):
        m = matches_col.insert_one({
            "tournamentId": t_id,
            "matchNumber": i,
            "mapName": maps[i-1],
            "status": "COMPLETED",
            "playedAt": datetime.now(timezone.utc),
            "createdAt": datetime.now(timezone.utc)
        })
        match_ids.append(str(m.inserted_id))

    # Create 12 fake teams and register them
    team_ids = []
    for idx, name in enumerate(team_names):
        # Check if team exists
        existing_team = teams_col.find_one({"name": name})
        if existing_team:
            team_ids.append(str(existing_team["_id"]))
        else:
            admin_user = users_col.find_one({"role": "ADMIN"})
            admin_id = str(admin_user["_id"]) if admin_user else None
            if not admin_id:
                continue
            t_doc = teams_col.insert_one({
                "name": name,
                "captainId": admin_id,
                "members": [admin_id],
                "isActive": True,
                "isTestTeam": True,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc)
            })
            team_ids.append(str(t_doc.inserted_id))

        # Register team
        registrations_col.update_one(
            {"tournamentId": t_id, "teamId": team_ids[-1]},
            {"$set": {
                "tournamentId": t_id,
                "teamId": team_ids[-1],
                "slotNumber": idx + 1,
                "paymentStatus": "PAID",
                "amountPaid": 50,
                "registeredAt": datetime.now(timezone.utc),
                "confirmedAt": datetime.now(timezone.utc)
            }},
            upsert=True
        )

    # Generate realistic random match results for all 6 matches
    results_summary = []
    for match_num, match_id in enumerate(match_ids):
        # Random placement for each team (shuffle 1-12)
        placements = list(range(1, 13))
        random.shuffle(placements)

        for team_idx, team_id in enumerate(team_ids):
            placement = placements[team_idx]
            kills = random.randint(0, 8) if placement <= 6 else random.randint(0, 4)
            if placement == 1:
                kills = random.randint(3, 12)  # Winner usually has more kills

            pts = calculate_match_points(kills, placement)

            match_results_col.update_one(
                {"matchId": match_id, "teamId": team_id},
                {"$set": {
                    "matchId": match_id,
                    "teamId": team_id,
                    "kills": kills,
                    "placement": placement,
                    "booyah": placement == 1,
                    "placementPoints": pts["placementPoints"],
                    "killPoints": pts["killPoints"],
                    "totalPoints": pts["totalPoints"]
                }},
                upsert=True
            )

        results_summary.append(f"Match {match_num+1}: results saved")

    return {
        "message": "Test tournament created with 12 teams and 6 matches of results!",
        "tournamentId": t_id,
        "teamsCreated": len(team_ids),
        "matchesCreated": len(match_ids),
        "note": "Go to Admin → Tournaments → OSG Test Tournament → Standings to see results",
        "adminUrl": "/admin/tournaments/" + t_id
    }

@app.delete("/api/seed/test-tournament")
async def delete_test_tournament():
    """Delete all test tournament data"""
    deleted = {"tournaments": 0, "teams": 0, "registrations": 0, "matches": 0, "results": 0}
    
    # Find and delete test tournament
    test_t = tournaments_col.find_one({"name": "OSG Test Tournament"})
    if test_t:
        t_id = str(test_t["_id"])
        # Delete match results
        test_matches = list(matches_col.find({"tournamentId": t_id}))
        for m in test_matches:
            r = match_results_col.delete_many({"matchId": str(m["_id"])})
            deleted["results"] += r.deleted_count
        # Delete matches
        m = matches_col.delete_many({"tournamentId": t_id})
        deleted["matches"] = m.deleted_count
        # Delete registrations and test teams
        regs = list(registrations_col.find({"tournamentId": t_id}))
        for reg in regs:
            team = teams_col.find_one({"_id": ObjectId(reg["teamId"]), "isTestTeam": True})
            if team:
                teams_col.delete_one({"_id": team["_id"]})
                deleted["teams"] += 1
        r = registrations_col.delete_many({"tournamentId": t_id})
        deleted["registrations"] = r.deleted_count
        # Delete tournament
        tournaments_col.delete_one({"_id": test_t["_id"]})
        deleted["tournaments"] = 1
    
    return {"message": "Test tournament deleted", "deleted": deleted}

@app.post("/api/seed/test-users")
async def seed_test_users():
    """Create 12 test player accounts with teams"""
    from bson import ObjectId as BsonObjectId
    
    test_players = [
        {"ign": "TestPlayer1", "ffUid": "100000001", "mobile": "8000000001", "email": "test1@osglive.in"},
        {"ign": "TestPlayer2", "ffUid": "100000002", "mobile": "8000000002", "email": "test2@osglive.in"},
        {"ign": "TestPlayer3", "ffUid": "100000003", "mobile": "8000000003", "email": "test3@osglive.in"},
        {"ign": "TestPlayer4", "ffUid": "100000004", "mobile": "8000000004", "email": "test4@osglive.in"},
        {"ign": "TestPlayer5", "ffUid": "100000005", "mobile": "8000000005", "email": "test5@osglive.in"},
        {"ign": "TestPlayer6", "ffUid": "100000006", "mobile": "8000000006", "email": "test6@osglive.in"},
        {"ign": "TestPlayer7", "ffUid": "100000007", "mobile": "8000000007", "email": "test7@osglive.in"},
        {"ign": "TestPlayer8", "ffUid": "100000008", "mobile": "8000000008", "email": "test8@osglive.in"},
        {"ign": "TestPlayer9", "ffUid": "100000009", "mobile": "8000000009", "email": "test9@osglive.in"},
        {"ign": "TestPlayer10", "ffUid": "100000010", "mobile": "8000000010", "email": "test10@osglive.in"},
        {"ign": "TestPlayer11", "ffUid": "100000011", "mobile": "8000000011", "email": "test11@osglive.in"},
        {"ign": "TestPlayer12", "ffUid": "100000012", "mobile": "8000000012", "email": "test12@osglive.in"},
    ]
    
    created_users = []
    for p in test_players:
        existing = users_col.find_one({"ffUid": p["ffUid"]})
        if existing:
            created_users.append(str(existing["_id"]))
            continue
        result = users_col.insert_one({
            "fullName": p["ign"],
            "email": p["email"],
            "emailVerified": True,
            "mobile": p["mobile"],
            "mobileVerified": True,
            "passwordHash": get_password_hash("Test@1234"),
            "ffUid": p["ffUid"],
            "ign": p["ign"],
            "state": "Telangana",
            "dob": datetime(2000, 1, 1, tzinfo=timezone.utc),
            "role": "PLAYER",
            "walletBalance": 0,
            "isActive": True,
            "isTestUser": True,
            "registeredIp": "127.0.0.1",
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        })
        created_users.append(str(result.inserted_id))
    
    # Create 3 test teams of 4 players each
    team_data = [
        {"name": "Test Team Alpha", "members": created_users[0:4]},
        {"name": "Test Team Beta", "members": created_users[4:8]},
        {"name": "Test Team Gamma", "members": created_users[8:12]},
    ]
    
    created_teams = []
    for td in team_data:
        existing = teams_col.find_one({"name": td["name"]})
        if not existing:
            result = teams_col.insert_one({
                "name": td["name"],
                "captainId": td["members"][0],
                "members": td["members"],
                "isActive": True,
                "isTestTeam": True,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc)
            })
            created_teams.append(str(result.inserted_id))
        else:
            created_teams.append(str(existing["_id"]))
    
    return {
        "message": "12 test players + 3 teams created",
        "users": len(created_users),
        "teams": len(created_teams),
        "credentials": "All test players: password = Test@1234",
        "testLogins": [{"email": p["email"], "password": "Test@1234"} for p in test_players[:3]]
    }

@app.delete("/api/seed/test-users")
async def delete_test_users():
    """Delete all test users and their teams"""
    users_result = users_col.delete_many({"isTestUser": True})
    teams_result = teams_col.delete_many({"isTestTeam": True})
    return {
        "message": "Test users and teams deleted",
        "usersDeleted": users_result.deleted_count,
        "teamsDeleted": teams_result.deleted_count
    }

@app.delete("/api/admin/tournaments/{tournament_id}")
async def delete_tournament(tournament_id: str, admin: dict = Depends(get_admin_user)):
    """Delete a tournament and all its data"""
    tournament = tournaments_col.find_one({"_id": ObjectId(tournament_id)})
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    # Delete all related data
    matches = list(matches_col.find({"tournamentId": tournament_id}))
    for m in matches:
        match_results_col.delete_many({"matchId": str(m["_id"])})
    matches_col.delete_many({"tournamentId": tournament_id})
    registrations_col.delete_many({"tournamentId": tournament_id})
    tournaments_col.delete_one({"_id": ObjectId(tournament_id)})
    
    return {"message": f"Tournament '{tournament['name']}' deleted successfully"}

# ============== ADMIN APPEAL MANAGEMENT ==============
@app.get("/api/admin/appeals")
async def get_all_appeals(admin: dict = Depends(get_admin_user)):
    """Get all bans that have pending appeals"""
    bans_with_appeals = list(bans_col.find(
        {"appealStatus": {"$in": ["PENDING", "APPROVED", "REJECTED"]}}
    ).sort("appealedAt", DESCENDING))
    
    result = []
    for ban in bans_with_appeals:
        player = users_col.find_one(
            {"_id": ObjectId(ban["userId"])},
            {"passwordHash": 0}
        )
        result.append({
            **serialize_doc(ban),
            "player": {
                "id": str(player["_id"]) if player else None,
                "ign": player.get("ign", "Unknown") if player else "Unknown",
                "ffUid": player.get("ffUid", "") if player else "",
                "email": player.get("email", "") if player else "",
                "fullName": player.get("fullName", "") if player else ""
            }
        })
    
    return result

@app.put("/api/admin/appeals/{ban_id}")
async def process_appeal(
    ban_id: str,
    action: str,  # "APPROVED" or "REJECTED"
    note: Optional[str] = None,
    admin: dict = Depends(get_admin_user)
):
    """Approve or reject a ban appeal"""
    ban = bans_col.find_one({"_id": ObjectId(ban_id)})
    if not ban:
        raise HTTPException(status_code=404, detail="Ban not found")
    
    if ban.get("appealStatus") != "PENDING":
        raise HTTPException(status_code=400, detail="No pending appeal for this ban")
    
    if action not in ["APPROVED", "REJECTED"]:
        raise HTTPException(status_code=400, detail="Action must be APPROVED or REJECTED")
    
    update_data = {
        "appealStatus": action,
        "resolvedAt": datetime.now(timezone.utc),
        "resolvedBy": admin["id"]
    }
    
    if action == "APPROVED":
        # Lift the ban
        update_data["isActive"] = False
        message = "Your ban appeal has been approved. Your account is now active."
        notif_type = "SUCCESS"
    else:
        message = f"Your ban appeal has been rejected. {note or ''}"
        notif_type = "DANGER"
    
    bans_col.update_one({"_id": ObjectId(ban_id)}, {"$set": update_data})
    
    # Notify player
    notifications_col.insert_one({
        "userId": ban["userId"],
        "title": f"Appeal {action.title()}",
        "message": message,
        "type": notif_type,
        "isRead": False,
        "link": "/dashboard/bans",
        "createdAt": datetime.now(timezone.utc)
    })
    
    return {"message": f"Appeal {action.lower()} successfully"}

@app.get("/api/admin/appeals/count")
async def get_appeal_count(admin: dict = Depends(get_admin_user)):
    count = bans_col.count_documents({"appealStatus": "PENDING"})
    return {"pendingAppeals": count}

# Admin: clear rate limits (useful when testing)
@app.delete("/api/admin/rate-limits")
async def clear_all_rate_limits(admin: dict = Depends(get_admin_user)):
    rate_limits_col.delete_many({})
    return {"message": "All rate limits cleared"}

# ============== SPECIAL TOURNAMENT SYSTEM (48 teams) ==============

@app.post("/api/admin/special-tournaments")
async def create_special_tournament(data: dict, admin: dict = Depends(get_admin_user)):
    """
    Create a Special Tournament for 13-48 teams.
    This creates a parent 'special tournament' record only — 
    actual qualifier tournaments are created separately and linked.
    """
    name = data.get("name", "OSG Special Tournament")
    total_teams = int(data.get("totalTeams", 24))
    teams_per_qualifier = int(data.get("teamsPerQualifier", 12))
    advance_per_qualifier = int(data.get("advancePerQualifier", 6))
    entry_fee = float(data.get("entryFee", 50))
    prize_pool = data.get("prizePool", {"1": 5000, "2": 2500, "3": 1000})
    per_kill = float(data.get("perKillPrize", 5))
    rules = data.get("rules", "Special Tournament rules apply.")
    scheduled_at_str = data.get("scheduledAt")
    
    scheduled_at = datetime.now(timezone.utc) + timedelta(days=7)
    if scheduled_at_str:
        try:
            scheduled_at = datetime.fromisoformat(scheduled_at_str.replace("Z","")).replace(tzinfo=timezone.utc)
        except:
            pass
    
    # Calculate structure
    num_qualifiers = max(1, -(-total_teams // teams_per_qualifier))  # ceiling division
    total_in_semis = num_qualifiers * advance_per_qualifier
    
    # Determine if semis needed
    needs_semis = total_in_semis > 12
    
    structure = {
        "totalTeams": total_teams,
        "numQualifiers": num_qualifiers,
        "teamsPerQualifier": teams_per_qualifier,
        "advancePerQualifier": advance_per_qualifier,
        "totalInSemis": total_in_semis,
        "needsSemis": needs_semis,
        "finalsTeams": 12 if needs_semis else total_in_semis,
        "stage": "QUALIFIERS"
    }
    
    special = db["special_tournaments"].insert_one({
        "name": name,
        "totalTeams": total_teams,
        "entryFee": entry_fee,
        "prizePool": prize_pool,
        "perKillPrize": per_kill,
        "rules": rules,
        "scheduledAt": scheduled_at,
        "structure": structure,
        "status": "UPCOMING",
        "qualifierTournaments": [],
        "semiTournaments": [],
        "finalsTournamentId": None,
        "createdBy": admin["id"],
        "createdAt": datetime.now(timezone.utc),
    })
    
    special_id = str(special.inserted_id)
    
    return {
        "message": f"Special tournament '{name}' created",
        "specialId": special_id,
        "structure": structure,
        "instructions": f"Create {num_qualifiers} qualifier tournaments manually and link them using /api/admin/special-tournaments/{special_id}/link-qualifier"
    }

@app.post("/api/admin/special-tournaments/{special_id}/create-qualifier")
async def create_qualifier_for_special(special_id: str, data: dict, admin: dict = Depends(get_admin_user)):
    """Create a qualifier tournament and auto-link to special tournament"""
    special = db["special_tournaments"].find_one({"_id": ObjectId(special_id)})
    if not special:
        raise HTTPException(status_code=404, detail="Special tournament not found")
    
    qualifier_num = len(special.get("qualifierTournaments", [])) + 1
    max_qualifiers = special["structure"]["numQualifiers"]
    
    if qualifier_num > max_qualifiers:
        raise HTTPException(status_code=400, detail=f"All {max_qualifiers} qualifiers already created")
    
    name = data.get("name", f"{special['name']} — Qualifier {qualifier_num}")
    scheduled_at_str = data.get("scheduledAt")
    scheduled_at = datetime.now(timezone.utc) + timedelta(days=qualifier_num)
    if scheduled_at_str:
        try:
            scheduled_at = datetime.fromisoformat(scheduled_at_str.replace("Z","")).replace(tzinfo=timezone.utc)
        except:
            pass
    
    maps_list = data.get("maps", ["BERMUDA","PURGATORY","KALAHARI","ALPHINE","NEXTERRA","SOLARA"])
    
    t = tournaments_col.insert_one({
        "name": name,
        "specialTournamentId": special_id,
        "specialTournamentName": special["name"],
        "tournamentType": "SPECIAL_QUALIFIER",
        "qualifierNumber": qualifier_num,
        "map": maps_list[0],
        "maps": maps_list,
        "matchCount": len(maps_list),
        "scheduledAt": scheduled_at,
        "entryFee": special["entryFee"],
        "maxTeams": special["structure"]["teamsPerQualifier"],
        "playersPerTeam": 4,
        "mode": "BR",
        "prizePool": {},
        "perKillPrize": special["perKillPrize"],
        "rules": special["rules"],
        "status": "UPCOMING",
        "totalSlots": special["structure"]["teamsPerQualifier"],
        "filledSlots": 0,
        "advanceCount": special["structure"]["advancePerQualifier"],
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc)
    })
    t_id = str(t.inserted_id)
    
    # Create 6 matches with maps
    for mi, map_name in enumerate(maps_list):
        matches_col.insert_one({
            "tournamentId": t_id,
            "matchNumber": mi + 1,
            "mapName": map_name,
            "status": "PENDING",
            "createdAt": datetime.now(timezone.utc)
        })
    
    # Link to special tournament
    db["special_tournaments"].update_one(
        {"_id": ObjectId(special_id)},
        {"$push": {"qualifierTournaments": {"id": t_id, "name": name, "qualifierNumber": qualifier_num}}}
    )
    
    return {"message": f"Qualifier {qualifier_num} created", "tournamentId": t_id}

@app.get("/api/admin/special-tournaments/{special_id}/standings")
async def get_special_standings(special_id: str, stage: str = "qualifiers", admin: dict = Depends(get_admin_user)):
    """Get standings from all qualifier/semi tournaments for team selection"""
    special = db["special_tournaments"].find_one({"_id": ObjectId(special_id)})
    if not special:
        raise HTTPException(status_code=404, detail="Not found")
    
    if stage == "qualifiers":
        tourney_list = special.get("qualifierTournaments", [])
    else:
        tourney_list = special.get("semiTournaments", [])
    
    all_standings = []
    for t_info in tourney_list:
        standings = await get_tournament_standings(t_info["id"])
        for team in standings:
            all_standings.append({
                **team,
                "fromTournament": t_info["name"],
                "tournamentId": t_info["id"],
                "qualifierNumber": t_info.get("qualifierNumber", 0)
            })
    
    return all_standings

@app.post("/api/admin/special-tournaments/{special_id}/create-next-stage")
async def create_next_stage(special_id: str, data: dict, admin: dict = Depends(get_admin_user)):
    """
    Create Semi-Final or Final tournament with selected teams imported.
    data: {
        stage: "SEMI" | "FINALS",
        selectedTeams: [{ teamId, teamName, fromTournamentId }],
        name: str,
        scheduledAt: str,
        maps: [...],
        prizePool: {...} (only for finals)
    }
    """
    special = db["special_tournaments"].find_one({"_id": ObjectId(special_id)})
    if not special:
        raise HTTPException(status_code=404, detail="Not found")
    
    stage = data.get("stage", "SEMI")
    selected_teams = data.get("selectedTeams", [])
    name = data.get("name", f"{special['name']} — {stage}")
    maps_list = data.get("maps", ["BERMUDA","PURGATORY","KALAHARI","ALPHINE","NEXTERRA","SOLARA"])
    prize_pool = data.get("prizePool", special.get("prizePool", {}))
    
    scheduled_at = datetime.now(timezone.utc) + timedelta(days=14)
    if data.get("scheduledAt"):
        try:
            scheduled_at = datetime.fromisoformat(data["scheduledAt"].replace("Z","")).replace(tzinfo=timezone.utc)
        except:
            pass
    
    t_type = "SPECIAL_SEMI" if stage == "SEMI" else "SPECIAL_FINALS"
    
    t = tournaments_col.insert_one({
        "name": name,
        "specialTournamentId": special_id,
        "specialTournamentName": special["name"],
        "tournamentType": t_type,
        "map": maps_list[0],
        "maps": maps_list,
        "matchCount": len(maps_list),
        "scheduledAt": scheduled_at,
        "entryFee": 0,
        "maxTeams": len(selected_teams),
        "playersPerTeam": 4,
        "mode": "BR",
        "prizePool": prize_pool if stage == "FINALS" else {},
        "perKillPrize": special["perKillPrize"],
        "rules": special["rules"],
        "status": "UPCOMING",
        "totalSlots": len(selected_teams),
        "filledSlots": len(selected_teams),
        "advancingTeams": selected_teams,
        "isFinals": stage == "FINALS",
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc)
    })
    t_id = str(t.inserted_id)
    
    # Create matches
    for mi, map_name in enumerate(maps_list):
        matches_col.insert_one({
            "tournamentId": t_id,
            "matchNumber": mi + 1,
            "mapName": map_name,
            "status": "PENDING",
            "createdAt": datetime.now(timezone.utc)
        })
    
    # Auto-register selected teams
    for slot_i, team_info in enumerate(selected_teams):
        registrations_col.update_one(
            {"tournamentId": t_id, "teamId": team_info["teamId"]},
            {"$set": {
                "tournamentId": t_id,
                "teamId": team_info["teamId"],
                "slotNumber": slot_i + 1,
                "paymentStatus": "PAID",
                "amountPaid": 0,
                "fromTournament": team_info.get("fromTournamentId", ""),
                "registeredAt": datetime.now(timezone.utc),
                "confirmedAt": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        
        # Notify each team member of qualification
        team_doc = teams_col.find_one({"_id": ObjectId(team_info["teamId"])})
        if team_doc:
            for member_id in team_doc.get("members", []):
                notifications_col.insert_one({
                    "userId": member_id,
                    "title": f"🎉 Qualified for {name}!",
                    "message": f"Congratulations! Your team '{team_info['teamName']}' has qualified for {name}. Get ready!",
                    "type": "SUCCESS",
                    "isRead": False,
                    "link": f"/tournaments/{t_id}",
                    "createdAt": datetime.now(timezone.utc)
                })
    
    # Notify NON-qualified teams from source tournaments (disqualified teams)
    if stage == "SEMI" or stage == "FINALS":
        source_tournament_ids = list(set([t["fromTournamentId"] for t in selected_teams if t.get("fromTournamentId")]))
        qualified_team_ids = set(t["teamId"] for t in selected_teams)
        
        for src_t_id in source_tournament_ids:
            all_regs = list(registrations_col.find({"tournamentId": src_t_id, "paymentStatus": "PAID"}))
            for reg in all_regs:
                if reg["teamId"] not in qualified_team_ids:
                    team_doc = teams_col.find_one({"_id": ObjectId(reg["teamId"])})
                    if team_doc:
                        for member_id in team_doc.get("members", []):
                            notifications_col.insert_one({
                                "userId": member_id,
                                "title": "Tournament Update",
                                "message": f"Unfortunately your team did not advance from the qualifier. Thank you for participating in {special['name']}!",
                                "type": "INFO",
                                "isRead": False,
                                "createdAt": datetime.now(timezone.utc)
                            })
    
    # Update special tournament record
    field = "semiTournaments" if stage == "SEMI" else "finalsTournamentId"
    if stage == "SEMI":
        db["special_tournaments"].update_one(
            {"_id": ObjectId(special_id)},
            {"$push": {"semiTournaments": {"id": t_id, "name": name}}}
        )
    else:
        db["special_tournaments"].update_one(
            {"_id": ObjectId(special_id)},
            {"$set": {"finalsTournamentId": t_id, "status": "FINALS_CREATED"}}
        )
    
    return {
        "message": f"{stage} tournament created with {len(selected_teams)} teams",
        "tournamentId": t_id,
        "teamsImported": len(selected_teams)
    }

@app.get("/api/admin/special-tournaments")
async def list_special_tournaments(admin: dict = Depends(get_admin_user)):
    specials = list(db["special_tournaments"].find().sort("createdAt", DESCENDING))
    return [serialize_doc(s) for s in specials]

@app.get("/api/admin/special-tournaments/{special_id}")
async def get_special_tournament(special_id: str, admin: dict = Depends(get_admin_user)):
    special = db["special_tournaments"].find_one({"_id": ObjectId(special_id)})
    if not special:
        raise HTTPException(status_code=404, detail="Not found")
    return serialize_doc(special)

@app.post("/api/seed/special-test")
async def seed_special_test():
    """Create a test special tournament with 24 teams for testing the flow"""
    import random
    
    # Clean up old test
    old = db["special_tournaments"].find_one({"name": "TEST Special Tournament"})
    if old:
        for t_info in old.get("qualifierTournaments", []):
            t_id = t_info["id"]
            for m in matches_col.find({"tournamentId": t_id}):
                match_results_col.delete_many({"matchId": str(m["_id"])})
            matches_col.delete_many({"tournamentId": t_id})
            registrations_col.delete_many({"tournamentId": t_id})
            tournaments_col.delete_one({"_id": ObjectId(t_id)})
        db["special_tournaments"].delete_one({"_id": old["_id"]})
    
    # Create special
    special = db["special_tournaments"].insert_one({
        "name": "TEST Special Tournament",
        "totalTeams": 24,
        "entryFee": 50,
        "prizePool": {"1": 5000, "2": 2500, "3": 1000},
        "perKillPrize": 5,
        "rules": "Test rules",
        "scheduledAt": datetime.now(timezone.utc),
        "structure": {
            "totalTeams": 24,
            "numQualifiers": 2,
            "teamsPerQualifier": 12,
            "advancePerQualifier": 6,
            "totalInSemis": 12,
            "needsSemis": False,
            "finalsTeams": 12,
            "stage": "QUALIFIERS"
        },
        "status": "ACTIVE",
        "qualifierTournaments": [],
        "semiTournaments": [],
        "finalsTournamentId": None,
        "createdBy": "test",
        "createdAt": datetime.now(timezone.utc),
    })
    special_id = str(special.inserted_id)
    
    team_names_q1 = ["ALPHA SQUAD","BETA FORCE","GAMMA STRIKE","DELTA TEAM","EPSILON","ZETA","ETA","THETA","IOTA","KAPPA","LAMBDA","MU"]
    team_names_q2 = ["SIGMA TEAM","TAU FORCE","UPSILON","PHI SQUAD","CHI","PSI","OMEGA","ALPHA 2","BETA 2","GAMMA 2","DELTA 2","EPSILON 2"]
    maps_list = ["BERMUDA","PURGATORY","KALAHARI","ALPHINE","NEXTERRA","SOLARA"]
    
    admin_user = users_col.find_one({"role": "ADMIN"})
    admin_id = str(admin_user["_id"]) if admin_user else "000000000000000000000001"
    
    qualifier_ids = []
    for q_num, team_names in enumerate([team_names_q1, team_names_q2], 1):
        t = tournaments_col.insert_one({
            "name": f"TEST Special Tournament — Qualifier {q_num}",
            "specialTournamentId": special_id,
            "specialTournamentName": "TEST Special Tournament",
            "tournamentType": "SPECIAL_QUALIFIER",
            "qualifierNumber": q_num,
            "map": "BERMUDA",
            "maps": maps_list,
            "matchCount": 6,
            "scheduledAt": datetime.now(timezone.utc),
            "entryFee": 50,
            "maxTeams": 12,
            "playersPerTeam": 4,
            "mode": "BR",
            "prizePool": {},
            "perKillPrize": 5,
            "rules": "Test",
            "status": "LIVE",
            "totalSlots": 12,
            "filledSlots": 12,
            "advanceCount": 6,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        })
        t_id = str(t.inserted_id)
        qualifier_ids.append(t_id)
        
        # Create matches
        match_ids = []
        for mi, map_name in enumerate(maps_list):
            m = matches_col.insert_one({
                "tournamentId": t_id, "matchNumber": mi+1,
                "mapName": map_name, "status": "COMPLETED",
                "playedAt": datetime.now(timezone.utc),
                "createdAt": datetime.now(timezone.utc)
            })
            match_ids.append(str(m.inserted_id))
        
        # Create fake teams and results
        team_ids = []
        for idx, name in enumerate(team_names):
            existing = teams_col.find_one({"name": name, "isTestTeam": True})
            if existing:
                team_ids.append(str(existing["_id"]))
            else:
                tt = teams_col.insert_one({"name": name, "captainId": admin_id, "members": [admin_id], "isActive": True, "isTestTeam": True, "createdAt": datetime.now(timezone.utc), "updatedAt": datetime.now(timezone.utc)})
                team_ids.append(str(tt.inserted_id))
            
            registrations_col.update_one(
                {"tournamentId": t_id, "teamId": team_ids[-1]},
                {"$set": {"tournamentId": t_id, "teamId": team_ids[-1], "slotNumber": idx+1, "paymentStatus": "PAID", "amountPaid": 50, "registeredAt": datetime.now(timezone.utc), "confirmedAt": datetime.now(timezone.utc)}},
                upsert=True
            )
        
        # Generate results for all 6 matches
        for match_id in match_ids:
            placements = list(range(1, 13))
            random.shuffle(placements)
            for ti, team_id in enumerate(team_ids):
                placement = placements[ti]
                kills = random.randint(3, 12) if placement == 1 else random.randint(0, 6)
                pts = calculate_match_points(kills, placement)
                match_results_col.update_one(
                    {"matchId": match_id, "teamId": team_id},
                    {"$set": {"matchId": match_id, "teamId": team_id, "kills": kills, "placement": placement, "booyah": placement==1, **pts}},
                    upsert=True
                )
        
        db["special_tournaments"].update_one(
            {"_id": special.inserted_id},
            {"$push": {"qualifierTournaments": {"id": t_id, "name": f"Qualifier {q_num}", "qualifierNumber": q_num}}}
        )
    
    return {
        "message": "Test special tournament created with 2 qualifiers (12 teams each) and random results!",
        "specialId": special_id,
        "qualifier1Id": qualifier_ids[0] if qualifier_ids else None,
        "qualifier2Id": qualifier_ids[1] if len(qualifier_ids) > 1 else None,
        "instructions": "Go to Admin → Special Tournaments → TEST Special Tournament → View standings → Select top 6 from each → Create Finals"
    }

# ============== PLATFORM SETTINGS ==============
@app.get("/api/admin/platform-settings")
async def get_platform_settings(admin: dict = Depends(get_admin_user)):
    settings = db["platform_settings"].find_one({"key": "main"})
    if not settings:
        return {"emailVerifyRequired": False, "mobileVerifyRequired": False, "maintenanceMode": False, "discordUrl": "https://discord.gg/bpXVqbBN"}
    s = serialize_doc(settings)
    return s

@app.put("/api/admin/platform-settings")
async def update_platform_settings(data: dict, admin: dict = Depends(get_admin_user)):
    data["key"] = "main"
    data["updatedAt"] = datetime.now(timezone.utc)
    db["platform_settings"].update_one({"key": "main"}, {"$set": data}, upsert=True)
    return {"message": "Settings saved"}

@app.get("/api/platform-settings")
async def get_public_platform_settings():
    """Public endpoint — no auth needed"""
    settings = db["platform_settings"].find_one({"key": "main"})
    if not settings:
        return {"emailVerifyRequired": False, "mobileVerifyRequired": False, "discordUrl": "https://discord.gg/bpXVqbBN"}
    return {
        "emailVerifyRequired": settings.get("emailVerifyRequired", False),
        "mobileVerifyRequired": settings.get("mobileVerifyRequired", False),
        "discordUrl": settings.get("discordUrl", "https://discord.gg/bpXVqbBN")
    }

# Health check
@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
