import os
import time
import hmac
import hashlib
import secrets
import logging
import json
import subprocess

logger = logging.getLogger(__name__)

SESSION_SECRET = os.environ.get("SESSION_SECRET", "")
if not SESSION_SECRET:
    SESSION_SECRET = secrets.token_hex(32)
    logger.warning("SESSION_SECRET not set, using random key (sessions won't persist across restarts)")

ADMIN_PASSWORD_HASH = os.environ.get("ADMIN_PASSWORD_HASH", "")
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "")
MASKED_EMAIL = ADMIN_EMAIL[:1] + "***@" + ADMIN_EMAIL.split("@")[-1] if "@" in ADMIN_EMAIL else "***"

_pending_2fa = {}
_sessions = {}
_login_attempts = {}

TWO_FA_EXPIRY = 300
SESSION_EXPIRY = 86400
LOGIN_RATE_LIMIT = 5
LOGIN_RATE_WINDOW = 300


def _sign(data: str) -> str:
    return hmac.new(SESSION_SECRET.encode(), data.encode(), hashlib.sha256).hexdigest()


def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    attempts = _login_attempts.get(ip, [])
    attempts = [t for t in attempts if now - t < LOGIN_RATE_WINDOW]
    _login_attempts[ip] = attempts
    return len(attempts) < LOGIN_RATE_LIMIT


def _record_attempt(ip: str):
    now = time.time()
    if ip not in _login_attempts:
        _login_attempts[ip] = []
    _login_attempts[ip].append(now)


def verify_password(password: str) -> bool:
    if not ADMIN_PASSWORD_HASH:
        logger.error("ADMIN_PASSWORD_HASH not configured")
        return False
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    return hmac.compare_digest(pw_hash, ADMIN_PASSWORD_HASH)


def generate_2fa_code() -> str:
    code = f"{secrets.randbelow(1000000):06d}"
    return code


def store_pending_2fa(session_id: str, code: str):
    _pending_2fa[session_id] = {
        "code": code,
        "created_at": time.time(),
        "attempts": 0,
    }


def verify_2fa_code(session_id: str, code: str) -> bool:
    pending = _pending_2fa.get(session_id)
    if not pending:
        return False

    if time.time() - pending["created_at"] > TWO_FA_EXPIRY:
        del _pending_2fa[session_id]
        return False

    pending["attempts"] += 1
    if pending["attempts"] > 5:
        del _pending_2fa[session_id]
        return False

    if hmac.compare_digest(pending["code"], code.strip()):
        del _pending_2fa[session_id]
        return True

    return False


def create_session() -> str:
    session_id = secrets.token_urlsafe(32)
    signature = _sign(session_id)
    _sessions[session_id] = {
        "created_at": time.time(),
        "last_active": time.time(),
    }
    return f"{session_id}.{signature}"


def validate_session(token: str) -> bool:
    if not token or "." not in token:
        return False

    parts = token.rsplit(".", 1)
    if len(parts) != 2:
        return False

    session_id, signature = parts
    expected_sig = _sign(session_id)
    if not hmac.compare_digest(signature, expected_sig):
        return False

    session = _sessions.get(session_id)
    if not session:
        return False

    if time.time() - session["created_at"] > SESSION_EXPIRY:
        del _sessions[session_id]
        return False

    session["last_active"] = time.time()
    return True


def invalidate_session(token: str):
    if not token or "." not in token:
        return
    session_id = token.rsplit(".", 1)[0]
    _sessions.pop(session_id, None)


def send_2fa_email(code: str) -> bool:
    try:
        return _send_via_replit_mail(code)
    except Exception as e:
        logger.warning(f"Replit mail failed: {e}")

    try:
        return _send_via_sendgrid_api(code)
    except Exception as e:
        logger.warning(f"SendGrid API failed: {e}")

    logger.error("All email delivery methods failed. 2FA code could not be sent.")
    return False


def _send_via_replit_mail(code: str) -> bool:
    hostname = os.environ.get("REPLIT_CONNECTORS_HOSTNAME")
    if not hostname:
        raise Exception("REPLIT_CONNECTORS_HOSTNAME not available")

    try:
        result = subprocess.run(
            ["replit", "identity", "create", "--audience", f"https://{hostname}"],
            capture_output=True, text=True, timeout=10
        )
        auth_token = result.stdout.strip()
        if not auth_token:
            raise Exception("Could not get Replit identity token")
    except FileNotFoundError:
        raise Exception("replit CLI not available")

    import urllib.request
    html_body = f"""
    <div style="font-family: -apple-system, sans-serif; max-width: 480px; margin: 0 auto; padding: 32px;">
        <div style="background: #1B2A4A; color: white; padding: 20px; border-radius: 8px 8px 0 0; text-align: center;">
            <h2 style="margin: 0; font-size: 18px;">NOC-IS Analytics Platform</h2>
        </div>
        <div style="background: white; border: 1px solid #E2E8F0; border-top: none; padding: 32px; border-radius: 0 0 8px 8px;">
            <p style="color: #475569; margin: 0 0 16px;">Kode verifikasi login Anda:</p>
            <div style="background: #F1F5F9; border: 2px solid #1E40AF; border-radius: 8px; padding: 16px; text-align: center; margin: 0 0 16px;">
                <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #0F172A;">{code}</span>
            </div>
            <p style="color: #475569; font-size: 14px; margin: 0 0 8px;">Kode berlaku selama 5 menit.</p>
            <p style="color: #94A3B8; font-size: 12px; margin: 0;">Jika Anda tidak meminta kode ini, abaikan email ini.</p>
        </div>
    </div>
    """

    payload = json.dumps({
        "subject": "NOC-IS Login: Kode Verifikasi",
        "html": html_body,
        "text": f"Kode verifikasi NOC-IS Analytics Platform: {code}\nBerlaku 5 menit.",
    }).encode()

    req = urllib.request.Request(
        f"https://{hostname}/api/v2/mailer/send",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Replit-Authentication": f"Bearer {auth_token}",
        },
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=15) as resp:
        resp_data = json.loads(resp.read())
        logger.info(f"2FA email sent to {MASKED_EMAIL}")
        return True


def _send_via_sendgrid_api(code: str) -> bool:
    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        raise Exception("SENDGRID_API_KEY not configured")

    import urllib.request

    payload = json.dumps({
        "personalizations": [{"to": [{"email": ADMIN_EMAIL}]}],
        "from": {"email": "noreply@nocis-analytics.com", "name": "NOC-IS Analytics"},
        "subject": "NOC-IS Login: Kode Verifikasi",
        "content": [
            {"type": "text/plain", "value": f"Kode verifikasi: {code}\nBerlaku 5 menit."},
            {"type": "text/html", "value": f"""
            <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
                <h2 style="color: #1B2A4A;">NOC-IS Analytics Platform</h2>
                <p>Kode verifikasi login:</p>
                <div style="background: #F1F5F9; border: 2px solid #1E40AF; border-radius: 8px; padding: 16px; text-align: center;">
                    <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px;">{code}</span>
                </div>
                <p style="color: #475569; font-size: 14px;">Berlaku 5 menit.</p>
            </div>"""}
        ]
    }).encode()

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=15) as resp:
        logger.info(f"SendGrid email sent to {MASKED_EMAIL}")
        return True
