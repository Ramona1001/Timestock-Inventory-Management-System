from typing import Optional
from fastapi import APIRouter, Form, HTTPException, Header, Request
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_302_FOUND
from jose import jwt, JWTError
from datetime import datetime, timedelta

from . import database
import os

router = APIRouter()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "../templates/html"))

@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("Login.html", {"request": request, "error": None})

@router.post("/login")
async def login_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    accept: Optional[str] = Header(default="application/json")
):
    user = database.authenticate_user(email, password)
    if not user:
        if "text/html" in accept:
            return templates.TemplateResponse("Login.html", {
                "request": request,
                "error": "Invalid email or password"
            })
        raise HTTPException(status_code=401, detail="Invalid email or password")

    role = user["role"]

    if "text/html" in accept:
        request.session["user"] = {**user, "role": role}
        return RedirectResponse(url="/", status_code=302)
    else:
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
    return RedirectResponse(url="/login", status_code=HTTP_302_FOUND)

def get_current_user(request: Request):
    return request.session.get("user")
