import logging
from fastapi import APIRouter, Request, Response
from pydantic import BaseModel

from backend.services.auth_service import (
    verify_password, generate_2fa_code, store_pending_2fa,
    verify_2fa_code, create_session, validate_session,
    invalidate_session, send_2fa_email, MASKED_EMAIL,
    _check_rate_limit, _record_attempt,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

SESSION_COOKIE = "nocis_session"


class LoginRequest(BaseModel):
    password: str


class Verify2FARequest(BaseModel):
    code: str
    session_id: str


@router.post("/login")
async def login(req: LoginRequest, request: Request, response: Response):
    client_ip = request.client.host if request.client else "unknown"

    if not _check_rate_limit(client_ip):
        return {"success": False, "error": "Terlalu banyak percobaan. Coba lagi dalam 5 menit."}

    _record_attempt(client_ip)

    if not verify_password(req.password):
        return {"success": False, "error": "Password salah"}

    session_id = create_session()
    code = generate_2fa_code()
    store_pending_2fa(session_id.split(".")[0], code)

    email_sent = send_2fa_email(code)

    if not email_sent:
        invalidate_session(session_id)
        return {"success": False, "error": "Gagal mengirim kode verifikasi. Coba lagi nanti."}

    return {
        "success": True,
        "step": "2fa",
        "session_id": session_id,
        "email_sent": True,
        "masked_email": MASKED_EMAIL,
    }


@router.post("/verify-2fa")
async def verify_2fa(req: Verify2FARequest, response: Response):
    raw_session_id = req.session_id.split(".")[0] if "." in req.session_id else req.session_id

    if verify_2fa_code(raw_session_id, req.code):
        response.set_cookie(
            key=SESSION_COOKIE,
            value=req.session_id,
            httponly=True,
            samesite="lax",
            max_age=86400,
            path="/",
        )
        return {"success": True, "message": "Login berhasil"}

    return {"success": False, "error": "Kode verifikasi salah atau kedaluwarsa"}


@router.get("/me")
async def me(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token and validate_session(token):
        return {"authenticated": True, "user": "Dr. Adam M."}
    return {"authenticated": False}


@router.post("/logout")
async def logout(request: Request, response: Response):
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        invalidate_session(token)
    response.delete_cookie(key=SESSION_COOKIE, path="/")
    return {"success": True}
