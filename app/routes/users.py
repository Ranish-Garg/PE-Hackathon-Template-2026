from typing import List

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile, status
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.cache import get_redis_client, get_cache, set_cache, invalidate_cache
from app.database import get_db
from app.models.domain import Event, URL, User
from app.models.schemas import UserCreate, UserOut, UserUpdate
from app.utils import parse_users_csv


router = APIRouter(prefix="/users", tags=["users"])


def _get_existing_user_keys(db: Session) -> tuple[set[str], set[str]]:
    existing_emails = {email for (email,) in db.query(func.lower(User.email)).all()}
    existing_usernames = {username for (username,) in db.query(func.lower(User.username)).all()}
    return existing_emails, existing_usernames

@router.post("/bulk", response_model=dict)
async def create_users_bulk(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")
    
    content = await file.read()
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc

    try:
        parsed_users = parse_users_csv(text_content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    header_line = text_content.lstrip("\ufeff").splitlines()[0]
    normalized_headers = {field.strip().lower() for field in header_line.split(",")}
    allow_duplicate_usernames = "id" in normalized_headers

    count = 0
    existing_emails, existing_usernames = _get_existing_user_keys(db)
    for user_data in parsed_users:
        try:
            schema = UserCreate(username=user_data["username"], email=user_data["email"])
            normalized_email = schema.email.lower()
            normalized_username = schema.username.lower()
            username_is_available = allow_duplicate_usernames or normalized_username not in existing_usernames
            if normalized_email not in existing_emails and username_is_available:
                db_user = User(username=schema.username, email=normalized_email)
                db.add(db_user)
                count += 1
                existing_emails.add(normalized_email)
                if not allow_duplicate_usernames:
                    existing_usernames.add(normalized_username)
        except ValidationError:
            pass  # Skip invalid rows
    
    db.commit()
    return {"count": count}

@router.get("", response_model=List[UserOut])
def get_users(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    redis_client = get_redis_client()
    cache_key = f"users:page={page}:per_page={per_page}"
    cached = get_cache(redis_client, cache_key)
    if cached is not None:
        return cached

    skip = (page - 1) * per_page
    users = db.query(User).offset(skip).limit(per_page).all()
    users_out = [UserOut.model_validate(u) for u in users]
    set_cache(redis_client, cache_key, users_out, ttl=60)
    return users_out

@router.get("/{id}", response_model=UserOut)
def get_user(id: int, db: Session = Depends(get_db)):
    redis_client = get_redis_client()
    cache_key = f"user:{id}"
    cached = get_cache(redis_client, cache_key)
    if cached is not None:
        return cached

    user = db.query(User).filter(User.id == id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_out = UserOut.model_validate(user)
    set_cache(redis_client, cache_key, user_out, ttl=300)
    return user_out

@router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    normalized_email = user.email.lower()
    normalized_username = user.username.lower()
    if db.query(User).filter(func.lower(User.email) == normalized_email).first():
        raise HTTPException(status_code=400, detail="Email already registered")
    if db.query(User).filter(func.lower(User.username) == normalized_username).first():
        raise HTTPException(status_code=400, detail="Username already registered")
    
    db_user = User(username=user.username, email=normalized_email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    invalidate_cache(get_redis_client(), "users:*")
    
    return db_user

@router.put("/{id}", response_model=UserOut)
def update_user(id: int, user: UserUpdate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.id == id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.username is not None:
        normalized_username = user.username.lower()
        existing_user = db.query(User).filter(func.lower(User.username) == normalized_username, User.id != id).first()
        if existing_user:
            raise HTTPException(status_code=400, detail="Username already registered")
        db_user.username = user.username
        
    db.commit()
    db.refresh(db_user)
    
    redis_client = get_redis_client()
    invalidate_cache(redis_client, f"user:{id}")
    invalidate_cache(redis_client, "users:*")
    
    return db_user


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(id: int, db: Session = Depends(get_db)) -> Response:
    db_user = db.query(User).filter(User.id == id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    url_ids = [url_id for (url_id,) in db.query(URL.id).filter(URL.user_id == id).all()]
    if url_ids:
        db.query(Event).filter(Event.url_id.in_(url_ids)).delete(synchronize_session=False)

    db.query(Event).filter(Event.user_id == id).delete(synchronize_session=False)
    db.query(URL).filter(URL.user_id == id).delete(synchronize_session=False)
    db.delete(db_user)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
