from __future__ import annotations

import shutil
from contextlib import asynccontextmanager
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from fastapi import (
    Depends,
    FastAPI,
    File,
    HTTPException,
    Form,
    Request,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
    Cookie,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer
from sqlmodel import Session, select

from . import auth, crud
from .database import DATA_DIR, engine, init_db
from .models import Customer, Loan, Repayment, User, UserRole
from .schemas import (
    BankLedgerResponse,
    BankManualAdjustmentRequest,
    BankTransactionRead,
    AdminUserUpdate,
    CustomerBalanceTimelineResponse,
    CustomerBalanceUpdate,
    CustomerCreate,
    CustomerRead,
    CustomerUpdate,
    LoginRequest,
    LoanCreate,
    LoanRead,
    LoanUpdate,
    OverallReportSummary,
    OperationLogRead,
    PasswordResetRequest,
    PermissionCatalog,
    PermissionDefinitionSchema,
    RecordsResponse,
    RepaymentCreate,
    RepaymentRead,
    RepaymentUpdate,
    SummaryEntry,
    TokenResponse,
    UserCreate,
    UserRead,
    UserStatusUpdate,
)
from .timezone_utils import format_myt, now_myt, parse_myt_range_value
from .security import decode_token

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
UPLOAD_DIR = DATA_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def format_number(value: Any, decimals: int = 2) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        if value in (None, ""):
            number = 0.0
        else:
            return str(value)
    try:
        decimals_int = max(0, int(decimals))
    except (TypeError, ValueError):
        decimals_int = 2
    format_spec = f"{{:,.{decimals_int}f}}"
    return format_spec.format(number)

SESSION_COOKIE_NAME = os.getenv("BACKEND_SESSION_COOKIE", "session_token")
SESSION_COOKIE_MAX_AGE = int(os.getenv("BACKEND_SESSION_MAX_AGE", str(12 * 60 * 60)))
SESSION_COOKIE_SECURE = os.getenv("BACKEND_SESSION_COOKIE_SECURE", "false").lower() == "true"
SESSION_COOKIE_SAMESITE = os.getenv("BACKEND_SESSION_COOKIE_SAMESITE", "lax").lower()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/customer/auth/login")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    with Session(engine) as session:
        auth.ensure_default_admin(session)
    yield


app = FastAPI(title="Loan Record Backend", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["myt"] = format_myt
templates.env.filters["format_number"] = format_number
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.connections:
            self.connections.remove(websocket)

    async def send(self, websocket: WebSocket, message: Any) -> None:
        try:
            await websocket.send_json(message)
        except Exception:
            self.disconnect(websocket)

    async def broadcast(self, message: Any) -> None:
        for websocket in list(self.connections):
            try:
                await websocket.send_json(message)
            except Exception:
                self.disconnect(websocket)


manager = ConnectionManager()


def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


@dataclass
class StaffContext:
    session: Session
    user: User
    permissions: Dict[str, bool]


@dataclass
class CustomerContext:
    session: Session
    user: User
    customer: Customer


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=SESSION_COOKIE_MAX_AGE,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME)


def _user_to_schema(user: User) -> UserRead:
    payload = UserRead.model_validate(user, from_attributes=True)
    payload.permissions = auth.get_effective_permissions(user)
    return payload


def _get_user_from_cookie(session: Session, token: Optional[str]) -> Optional[User]:
    if not token:
        return None
    return auth.get_user_by_session_token(session, token)


def require_staff_context(
    session: Session = Depends(get_session),
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> StaffContext:
    user = _get_user_from_cookie(session, session_token)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    if user.role not in auth.ALLOWED_STAFF_ROLES:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
    permissions = auth.get_effective_permissions(user)
    return StaffContext(session=session, user=user, permissions=permissions)


def require_admin_context(
    session: Session = Depends(get_session),
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> StaffContext:
    ctx = require_staff_context(session=session, session_token=session_token)
    if ctx.user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin only")
    return ctx


def ensure_permission(ctx: StaffContext, key: str) -> None:
    if not ctx.permissions.get(key):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"缺少权限：{key}")


def require_customer_context(
    token: str = Depends(oauth2_scheme),
    session: Session = Depends(get_session),
) -> CustomerContext:
    try:
        payload = decode_token(token)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user_id = payload.get("sub")
    role = payload.get("role")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token subject")
    if not role:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token role")
    try:
        token_role = UserRole(role)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token role")
    if token_role != UserRole.CUSTOMER:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Customer token required")
    user = session.get(User, user_id_int)
    if not user or not user.is_active or user.role != token_role:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user")
    if user.customer_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User is not linked to a customer")
    customer = session.get(Customer, user.customer_id)
    if not customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found")
    return CustomerContext(session=session, user=user, customer=customer)


def build_summary_payload(summary: List[SummaryEntry]) -> dict[str, Any]:
    return {"type": "summary", "data": jsonable_encoder(summary)}


async def broadcast_summary(session: Session) -> None:
    summary = crud.get_summary(session)
    await manager.broadcast(build_summary_payload(summary))


@app.get("/auth/login", response_class=HTMLResponse)
def login_page(
    request: Request,
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    with Session(engine) as session:
        user = _get_user_from_cookie(session, session_token)
        if user and user.role in auth.ALLOWED_STAFF_ROLES:
            return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/auth/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    with Session(engine) as session:
        user = auth.authenticate_user(
            session,
            username=username,
            password=password,
            allowed_roles=auth.ALLOWED_STAFF_ROLES,
        )
        if not user:
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "error": "用户名或密码错误"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        token = auth.create_session_token(session, user)
    response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    set_session_cookie(response, token)
    return response


@app.post("/auth/logout")
def logout(
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    if session_token:
        with Session(engine) as session:
            auth.revoke_session_token(session, session_token)
    response = RedirectResponse(url="/auth/login", status_code=status.HTTP_302_FOUND)
    clear_session_cookie(response)
    return response



@app.get("/", response_class=HTMLResponse)
def read_root(request: Request, ctx: StaffContext = Depends(require_staff_context)):
    permissions = ctx.permissions
    customers = crud.list_customers(ctx.session) if permissions.get("customer.view") else []
    summary = crud.get_summary(ctx.session) if permissions.get("summary.view") else []
    loans = crud.list_loans(ctx.session) if permissions.get("loan.view") else []
    repayments = crud.list_repayments(ctx.session) if permissions.get("repayment.view") else []
    operation_logs: List[OperationLogRead] = []
    if permissions.get("operationlog.view"):
        operation_log_rows = crud.list_operation_logs(ctx.session)
        log_customer_codes = crud.map_operation_log_customer_codes(ctx.session, operation_log_rows)
        operation_logs = [
            crud.to_operation_log_read(log, customer_code=log_customer_codes.get(log.id)) for log in operation_log_rows
        ]
    admin_module_enabled = bool(permissions.get("admin.manage") or ctx.user.role == UserRole.ADMIN)
    admin_users: List[UserRead] = []
    admin_permission_defs: List[PermissionDefinitionSchema] = []
    staff_roles: List[UserRole] = []
    if admin_module_enabled:
        admin_users = [_user_to_schema(user) for user in auth.list_users(ctx.session)]
        permission_defs = auth.list_permission_definitions()
        admin_permission_defs = [
            PermissionDefinitionSchema(
                key=item.key,
                label=item.label,
                category=item.category,
                action=item.action,
                description=item.description,
            )
            for item in permission_defs
        ]
        staff_roles = [role for role in UserRole if role != UserRole.CUSTOMER]
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "current_user": ctx.user,
            "permissions": permissions,
            "customers": customers,
            "summary": summary,
            "loans": loans,
            "repayments": repayments,
            "operation_logs": operation_logs,
            "customers_payload": jsonable_encoder(customers),
            "summary_payload": jsonable_encoder(summary),
            "loans_payload": jsonable_encoder(loans),
            "repayments_payload": jsonable_encoder(repayments),
            "operation_logs_payload": jsonable_encoder(operation_logs),
            "admin_module_enabled": admin_module_enabled,
            "admin_users_payload": jsonable_encoder(admin_users),
            "admin_permissions_payload": jsonable_encoder(admin_permission_defs),
            "admin_staff_roles": staff_roles,
        },
    )


@app.get("/api/admin/users", response_model=List[UserRead])
def api_list_users(ctx: StaffContext = Depends(require_admin_context)):
    users = auth.list_users(ctx.session)
    return [_user_to_schema(user) for user in users]


@app.get("/api/admin/permissions", response_model=PermissionCatalog)
def api_list_permissions(ctx: StaffContext = Depends(require_admin_context)):
    permission_defs = auth.list_permission_definitions()
    schema = [
        PermissionDefinitionSchema(
            key=item.key,
            label=item.label,
            category=item.category,
            action=item.action,
            description=item.description,
        )
        for item in permission_defs
    ]
    return PermissionCatalog(permissions=schema)


@app.post("/api/admin/users", response_model=UserRead, status_code=201)
def api_create_user(payload: UserCreate, ctx: StaffContext = Depends(require_admin_context)):
    customer_id = payload.customer_id
    if payload.role == UserRole.CUSTOMER and not customer_id:
        raise HTTPException(status_code=400, detail="customer_id is required for customer role")
    if payload.role != UserRole.CUSTOMER:
        customer_id = None
    user = auth.create_user(
        ctx.session,
        username=payload.username,
        password=payload.password,
        role=payload.role,
        customer_id=customer_id,
        permissions=payload.permissions,
    )
    return _user_to_schema(user)


@app.put("/api/admin/users/{user_id}", response_model=UserRead)
def api_update_user(
    user_id: int,
    payload: AdminUserUpdate,
    ctx: StaffContext = Depends(require_admin_context),
):
    if user_id == ctx.user.id and payload.is_active is False:
        raise HTTPException(status_code=400, detail="Cannot disable yourself")
    user = auth.update_user_profile(
        ctx.session,
        user_id,
        username=payload.username,
        role=payload.role,
        permissions=payload.permissions,
        is_active=payload.is_active,
    )
    return _user_to_schema(user)


@app.post("/api/admin/users/{user_id}/status", response_model=UserRead)
def api_set_user_status(
    user_id: int,
    body: UserStatusUpdate,
    ctx: StaffContext = Depends(require_admin_context),
):
    if user_id == ctx.user.id and not body.is_active:
        raise HTTPException(status_code=400, detail="Cannot disable yourself")
    user = auth.set_user_active(ctx.session, user_id, body.is_active)
    return _user_to_schema(user)


@app.post("/api/admin/users/{user_id}/reset-password", response_model=UserRead)
def api_reset_password(
    user_id: int,
    body: PasswordResetRequest,
    ctx: StaffContext = Depends(require_admin_context),
):
    user = auth.reset_user_password(ctx.session, user_id, body.password)
    return _user_to_schema(user)


@app.get("/api/customers", response_model=List[CustomerRead])
def list_customers_api(ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "customer.view")
    return crud.list_customers(ctx.session)


@app.post("/api/customers", response_model=CustomerRead, status_code=201)
async def create_customer_api(payload: CustomerCreate, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "customer.manage")
    customer = Customer(**payload.model_dump())
    result = crud.create_customer(ctx.session, customer)
    await broadcast_summary(ctx.session)
    return result


@app.get("/api/customers/{customer_id}", response_model=CustomerRead)
def get_customer_api(customer_id: int, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "customer.view")
    return crud.get_customer(ctx.session, customer_id)


@app.put("/api/customers/{customer_id}", response_model=CustomerRead)
async def update_customer_api(
    customer_id: int,
    payload: CustomerUpdate,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "customer.manage")
    updates = payload.model_dump(exclude_unset=True, exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    customer = crud.update_customer(ctx.session, customer_id, updates)
    await broadcast_summary(ctx.session)
    return customer


@app.put("/api/customers/{customer_id}/balance", response_model=CustomerRead)
async def update_customer_balance_api(
    customer_id: int,
    payload: CustomerBalanceUpdate,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "customer.manage")
    customer = crud.update_customer_balance(ctx.session, customer_id, payload)
    await broadcast_summary(ctx.session)
    return customer


@app.post("/api/customers/{customer_id}/photo", response_model=CustomerRead)
async def upload_customer_photo(
    customer_id: int,
    file: UploadFile = File(...),
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "customer.manage")
    suffix = Path(file.filename or "").suffix.lower()
    allowed = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
    if suffix not in allowed:
        raise HTTPException(status_code=400, detail="Unsupported image format")

    filename = f"customer_{customer_id}_{int(now_myt().timestamp())}{suffix}"
    destination = UPLOAD_DIR / filename
    file.file.seek(0)
    with destination.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    photo_url = f"/uploads/{filename}"
    customer = crud.update_customer_photo(ctx.session, customer_id, photo_url)
    await broadcast_summary(ctx.session)
    return customer


@app.post("/api/loans", response_model=LoanRead, status_code=201)
async def create_loan_api(payload: LoanCreate, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "loan.manage")
    customer = crud.get_customer_by_code(ctx.session, payload.customer_code)
    loan_data = payload.model_dump(exclude={"customer_code"})
    loan = Loan(**loan_data, customer_id=customer.id)
    result = crud.create_loan(ctx.session, loan)
    await broadcast_summary(ctx.session)
    return crud.to_loan_read(ctx.session, result)
    
@app.delete("/api/loans/{loan_id}", response_model=LoanRead)
async def delete_loan_api(loan_id: int, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "loan.manage")
    deleted = crud.delete_loan(ctx.session, loan_id)
    await broadcast_summary(ctx.session)
    return deleted


@app.put("/api/loans/{loan_id}", response_model=LoanRead)
async def update_loan_api(loan_id: int, payload: LoanUpdate, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "loan.manage")
    loan = crud.update_loan(ctx.session, loan_id, payload)
    await broadcast_summary(ctx.session)
    return crud.to_loan_read(ctx.session, loan)


@app.get(
    "/api/customers/{customer_id}/balance-timeline",
    response_model=CustomerBalanceTimelineResponse,
)
def customer_balance_timeline_api(customer_id: int, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "customer.view")
    return crud.get_customer_balance_timeline(ctx.session, customer_id)


@app.post("/api/repayments", response_model=RepaymentRead, status_code=201)
async def create_repayment_api(
    payload: RepaymentCreate,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "repayment.manage")
    customer = crud.get_customer_by_code(ctx.session, payload.customer_code)
    repayment_data = payload.model_dump(exclude={"customer_code"})
    loan_code = repayment_data.pop("loan_code", None)
    loan_id = repayment_data.get("loan_id")
    if loan_code:
        loan = crud.get_loan_by_code(ctx.session, loan_code)
        if loan_id and loan_id != loan.id:
            raise HTTPException(status_code=400, detail="loan_id does not match loan_code")
        repayment_data["loan_id"] = loan.id
    repayment = Repayment(**repayment_data, customer_id=customer.id)
    result = crud.create_repayment(ctx.session, repayment)
    await broadcast_summary(ctx.session)
    return crud.to_repayment_read(ctx.session, result)
    
@app.delete("/api/repayments/{repayment_id}", response_model=RepaymentRead)
async def delete_repayment_api(repayment_id: int, ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "repayment.manage")
    deleted = crud.delete_repayment(ctx.session, repayment_id)
    await broadcast_summary(ctx.session)
    return deleted


@app.put("/api/repayments/{repayment_id}", response_model=RepaymentRead)
async def update_repayment_api(
    repayment_id: int,
    payload: RepaymentUpdate,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "repayment.manage")
    repayment = crud.update_repayment(ctx.session, repayment_id, payload)
    await broadcast_summary(ctx.session)
    return crud.to_repayment_read(ctx.session, repayment)


@app.websocket("/ws/summary")
async def summary_ws(websocket: WebSocket):
    session_token = websocket.cookies.get(SESSION_COOKIE_NAME)
    with Session(engine) as session:
        user = _get_user_from_cookie(session, session_token)
        if not user or user.role not in auth.ALLOWED_STAFF_ROLES:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        permissions = auth.get_effective_permissions(user)
        if not permissions.get("summary.view"):
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
    await manager.connect(websocket)
    try:
        with Session(engine) as session:
            summary = crud.get_summary(session)
        await manager.send(websocket, build_summary_payload(summary))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/api/summary", response_model=List[SummaryEntry])
def summary_api(ctx: StaffContext = Depends(require_staff_context)):
    ensure_permission(ctx, "summary.view")
    return crud.get_summary(ctx.session)


@app.get("/api/records", response_model=RecordsResponse)
def records_api(
    start_date: str,
    end_date: str,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "records.view")
    try:
        start = parse_myt_range_value(start_date, is_range_end=False)
        end = parse_myt_range_value(end_date, is_range_end=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format") from exc
    if end < start:
        raise HTTPException(status_code=400, detail="end_date must be after start_date")
    return crud.get_records_by_date(ctx.session, start, end)


@app.get("/api/bank/transactions", response_model=BankLedgerResponse)
def bank_transactions_api(
    limit: int = 20,
    page: int = 1,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search: Optional[str] = None,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "records.view")
    safe_limit = max(1, min(limit, 200))
    safe_page = max(1, page)
    offset = (safe_page - 1) * safe_limit
    start_at = None
    end_at = None
    if start_date:
        try:
            start_at = parse_myt_range_value(start_date, is_range_end=False)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid start_date") from exc
    if end_date:
        try:
            end_at = parse_myt_range_value(end_date, is_range_end=True)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid end_date") from exc
    if start_at and end_at and end_at < start_at:
        raise HTTPException(status_code=400, detail="end_date must be after start_date")
    return crud.get_bank_ledger(
        ctx.session,
        limit=safe_limit,
        offset=offset,
        start_at=start_at,
        end_at=end_at,
        search=search,
    )


@app.post("/api/bank/transactions/manual", response_model=BankTransactionRead, status_code=201)
def bank_manual_adjust_api(
    payload: BankManualAdjustmentRequest,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "loan.manage")
    entry = crud.create_manual_bank_adjustment(ctx.session, payload)
    return crud.to_bank_transaction_read(entry)


@app.get("/api/operation-logs", response_model=List[OperationLogRead])
def operation_logs_api(
    limit: int = 200,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "operationlog.view")
    start_at = None
    end_at = None
    if start_date:
        try:
            start_at = parse_myt_range_value(start_date, is_range_end=False)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid start_date") from exc
    if end_date:
        try:
            end_at = parse_myt_range_value(end_date, is_range_end=True)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid end_date") from exc
    if start_at and end_at and end_at < start_at:
        raise HTTPException(status_code=400, detail="end_date must be after start_date")
    logs = crud.list_operation_logs(
        ctx.session,
        limit=limit,
        entity_type=entity_type,
        entity_id=entity_id,
        start_at=start_at,
        end_at=end_at,
    )
    log_customer_codes = crud.map_operation_log_customer_codes(ctx.session, logs)
    return [crud.to_operation_log_read(log, customer_code=log_customer_codes.get(log.id)) for log in logs]


@app.get("/api/reports/overall", response_model=OverallReportSummary)
def overall_report_api(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ctx: StaffContext = Depends(require_staff_context),
):
    ensure_permission(ctx, "reports.view")
    start_at = None
    end_at = None
    if start_date:
        try:
            start_at = parse_myt_range_value(start_date, is_range_end=False)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid start_date") from exc
    if end_date:
        try:
            end_at = parse_myt_range_value(end_date, is_range_end=True)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid end_date") from exc
    if start_at and end_at and end_at < start_at:
        raise HTTPException(status_code=400, detail="end_date must be after start_date")
    return crud.get_overall_report(ctx.session, start_at=start_at, end_at=end_at)


@app.post("/customer/auth/login", response_model=TokenResponse)
def customer_login(payload: LoginRequest, session: Session = Depends(get_session)):
    user = auth.authenticate_user(
        session,
        username=payload.username,
        password=payload.password,
        allowed_roles=(UserRole.CUSTOMER,),
    )
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    tokens = auth.issue_customer_tokens(user)
    return TokenResponse(**tokens)


@app.get("/customer/api/profile", response_model=CustomerRead)
def customer_profile(ctx: CustomerContext = Depends(require_customer_context)):
    return ctx.customer


@app.get("/customer/api/loans", response_model=List[LoanRead])
def customer_loans(ctx: CustomerContext = Depends(require_customer_context)):
    loans = ctx.session.exec(select(Loan).where(Loan.customer_id == ctx.customer.id)).all()
    return [crud.to_loan_read(ctx.session, loan) for loan in loans]


@app.get("/customer/api/repayments", response_model=List[RepaymentRead])
def customer_repayments(ctx: CustomerContext = Depends(require_customer_context)):
    repayments = ctx.session.exec(select(Repayment).where(Repayment.customer_id == ctx.customer.id)).all()
    return [crud.to_repayment_read(ctx.session, repayment) for repayment in repayments]


@app.get("/customer/api/summary", response_model=SummaryEntry)
def customer_summary(ctx: CustomerContext = Depends(require_customer_context)):
    summary_entries = crud.get_summary(ctx.session)
    for entry in summary_entries:
        if entry.customer_id == ctx.customer.id:
            return entry
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Summary not found")


@app.get(
    "/customer/api/balance-timeline",
    response_model=CustomerBalanceTimelineResponse,
)
def customer_balance_timeline(ctx: CustomerContext = Depends(require_customer_context)):
    return crud.get_customer_balance_timeline(ctx.session, ctx.customer.id)
