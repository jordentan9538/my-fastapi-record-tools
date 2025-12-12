from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from fastapi import HTTPException, status
from sqlmodel import Session, select

from .models import SessionToken, User, UserRole
from .permissions import ALL_PERMISSION_KEYS, PERMISSIONS, sanitize_permissions
from .security import (
    create_access_token,
    create_refresh_token,
    generate_session_token,
    hash_password,
    session_expiry_datetime,
    session_token_hash,
    verify_password,
)
from .timezone_utils import now_myt

DEFAULT_ADMIN_USERNAME = os.getenv("BACKEND_ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASSWORD = os.getenv("BACKEND_ADMIN_PASSWORD", "admin123")

ALLOWED_STAFF_ROLES: Sequence[UserRole] = (UserRole.ADMIN, UserRole.CS, UserRole.ACCOUNT)


def list_permission_definitions() -> List:
    return PERMISSIONS


def _serialize_permissions(payload: Optional[Dict[str, bool]], role: UserRole) -> Optional[str]:
    if role == UserRole.ADMIN:
        return None
    if payload is None:
        return None
    sanitized = sanitize_permissions(payload)
    return json.dumps(sanitized, ensure_ascii=False)


def get_effective_permissions(user: User) -> Dict[str, bool]:
    if user.role == UserRole.ADMIN:
        return {key: True for key in ALL_PERMISSION_KEYS}
    if not user.permissions_json:
        return {}
    try:
        payload = json.loads(user.permissions_json)
    except (TypeError, json.JSONDecodeError):
        return {}
    return sanitize_permissions(payload)


def _normalize_username(username: str) -> str:
    return (username or "").strip().lower()


def ensure_default_admin(session: Session) -> User:
    admin = session.exec(select(User).where(User.role == UserRole.ADMIN)).first()
    if admin:
        return admin
    username = _normalize_username(DEFAULT_ADMIN_USERNAME)
    if not username:
        raise RuntimeError("Default admin username is empty; set BACKEND_ADMIN_USERNAME")
    password = DEFAULT_ADMIN_PASSWORD.strip()
    if not password:
        raise RuntimeError("Default admin password is empty; set BACKEND_ADMIN_PASSWORD")
    hashed = hash_password(password)
    admin = User(username=username, password_hash=hashed, role=UserRole.ADMIN, is_active=True)
    session.add(admin)
    session.commit()
    session.refresh(admin)
    print(f"Created default admin '{username}'. Please change the password immediately.")
    return admin


def list_users(session: Session) -> List[User]:
    return session.exec(select(User).order_by(User.created_at.asc())).all()


def get_user(session: Session, user_id: int) -> User:
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


def get_user_by_username(session: Session, username: str) -> Optional[User]:
    normalized = _normalize_username(username)
    if not normalized:
        return None
    return session.exec(select(User).where(User.username == normalized)).first()


def create_user(
    session: Session,
    *,
    username: str,
    password: str,
    role: UserRole,
    customer_id: Optional[int] = None,
    is_active: bool = True,
    permissions: Optional[Dict[str, bool]] = None,
) -> User:
    normalized = _normalize_username(username)
    if not normalized:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is required")
    existing = get_user_by_username(session, normalized)
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already exists")
    hashed = hash_password(password)
    now = now_myt()
    user = User(
        username=normalized,
        password_hash=hashed,
        role=role,
        customer_id=customer_id,
        is_active=is_active,
        created_at=now,
        updated_at=now,
        permissions_json=_serialize_permissions(permissions, role),
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def set_user_active(session: Session, user_id: int, is_active: bool) -> User:
    user = get_user(session, user_id)
    user.is_active = is_active
    user.updated_at = now_myt()
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def reset_user_password(session: Session, user_id: int, new_password: str) -> User:
    if not new_password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Password is required")
    user = get_user(session, user_id)
    user.password_hash = hash_password(new_password)
    user.updated_at = now_myt()
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def update_user_permissions(session: Session, user_id: int, permissions: Dict[str, bool]) -> User:
    user = get_user(session, user_id)
    user.permissions_json = _serialize_permissions(permissions, user.role)
    user.updated_at = now_myt()
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def update_user_profile(
    session: Session,
    user_id: int,
    *,
    username: Optional[str] = None,
    role: Optional[UserRole] = None,
    permissions: Optional[Dict[str, bool]] = None,
    is_active: Optional[bool] = None,
) -> User:
    user = get_user(session, user_id)
    current_permissions = get_effective_permissions(user)
    if username is not None:
        normalized = _normalize_username(username)
        if not normalized:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is required")
        existing = get_user_by_username(session, normalized)
        if existing and existing.id != user.id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already exists")
        user.username = normalized
    if role is not None and role != user.role:
        user.role = role
        if role in (UserRole.ADMIN, UserRole.CUSTOMER):
            user.permissions_json = None
        else:
            next_permissions = permissions if permissions is not None else current_permissions
            user.permissions_json = _serialize_permissions(next_permissions, role)
    elif permissions is not None:
        user.permissions_json = _serialize_permissions(permissions, user.role)
    if is_active is not None:
        user.is_active = is_active
    user.updated_at = now_myt()
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def authenticate_user(
    session: Session,
    *,
    username: str,
    password: str,
    allowed_roles: Optional[Iterable[UserRole]] = None,
) -> Optional[User]:
    user = get_user_by_username(session, username)
    if not user or not user.is_active:
        return None
    if allowed_roles and user.role not in allowed_roles:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def create_session_token(session: Session, user: User) -> str:
    raw_token = generate_session_token()
    hashed = session_token_hash(raw_token)
    expires_at = session_expiry_datetime()
    record = SessionToken(token_hash=hashed, user_id=user.id, expires_at=expires_at)
    session.add(record)
    session.commit()
    return raw_token


def revoke_session_token(session: Session, raw_token: str) -> None:
    hashed = session_token_hash(raw_token)
    record = session.exec(select(SessionToken).where(SessionToken.token_hash == hashed)).first()
    if not record:
        return
    record.revoked = True
    session.add(record)
    session.commit()


def get_user_by_session_token(session: Session, raw_token: str) -> Optional[User]:
    if not raw_token:
        return None
    hashed = session_token_hash(raw_token)
    record = session.exec(select(SessionToken).where(SessionToken.token_hash == hashed)).first()
    if not record or record.revoked:
        return None
    expires_at = record.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        record.revoked = True
        session.add(record)
        session.commit()
        return None
    user = session.get(User, record.user_id)
    if not user or not user.is_active:
        return None
    return user


def issue_customer_tokens(user: User) -> dict:
    if user.role != UserRole.CUSTOMER:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Not a customer account")
    access_token = create_access_token({"sub": str(user.id), "role": user.role.value})
    refresh_token = create_refresh_token(str(user.id))
    return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}
