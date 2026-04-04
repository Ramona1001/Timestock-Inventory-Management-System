# "List" was added here
from typing import Optional, List 
from fastapi import APIRouter, Form, HTTPException, Header, Request
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_302_FOUND
from jose import jwt, JWTError
from datetime import datetime, timedelta

from . import database
import os

# This is new
from .app_schemas import UserListItem

router = APIRouter()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TEMPLATES_DIR = os.path.abspath(
    os.path.join(BASE_DIR, "..", "templates", "html")
)

templates = Jinja2Templates(directory=TEMPLATES_DIR)
print("TEMPLATES DIR:", TEMPLATES_DIR)
print("FILES:", os.listdir(TEMPLATES_DIR))

@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        request,
        "Login.html",
        {"request": request, "error": None},
    )

@router.post("/login")
async def login_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    accept: str = Header(default="")
):
    user = database.authenticate_user(email, password)

    if not user:
        if "text/html" in accept:
            return templates.TemplateResponse(
                request,
                "Login.html",
                {"request": request, "error": "Invalid email or password"},
                status_code=401,
            )
        raise HTTPException(status_code=401, detail="Invalid email or password")

    role = user["role"]

    if "text/html" in accept:
        request.session["user"] = {**user, "role": role}
        return RedirectResponse(url="/", status_code=302)

    token = create_access_token({"id": user["id"], "role": role})
    return {"access_token": token, "token_type": "bearer"}

#MOBILE APP
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 1 day

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


@router.get("/logout")
def logout_user(request: Request):
    request.session.clear()
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session", path="/")  
    return response


def get_current_user(request: Request):
    user = request.session.get("user")
    if not user or "id" not in user:
        return None
    return user

# This one is new:
@router.get("/api/users/list", response_model=List[UserListItem])
async def api_users_list(role: Optional[str] = "employee", q: Optional[str] = None, limit: int = 50):
    conn = database.get_db_connection()
    try:
        rows = database.list_active_users_by_role(conn, role=role, q=q, limit=limit)
        return [{"id": r["id"], "display_name": r["display_name"]} for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass