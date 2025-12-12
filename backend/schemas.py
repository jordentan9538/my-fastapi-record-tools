from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, ValidationInfo, Field, field_validator, model_validator

from .models import UserRole


def _normalize_optional_customer_code(value: Optional[str]) -> Optional[str]:
    if value is None:
        return value
    trimmed = value.strip()
    if not trimmed:
        return None
    if not all(ch.isalnum() or ch == "-" for ch in trimmed):
        raise ValueError("customer_code must contain only letters, digits, or hyphen")
    return trimmed.upper()


def _normalize_required_customer_code(value: str) -> str:
    normalized = _normalize_optional_customer_code(value)
    if not normalized:
        raise ValueError("customer_code is required")
    return normalized


def _normalize_optional_loan_code(value: Optional[str]) -> Optional[str]:
    if value is None:
        return value
    trimmed = value.strip()
    if not trimmed:
        return None
    if not all(ch.isalnum() or ch == "-" for ch in trimmed):
        raise ValueError("loan_code must contain only letters, digits, or hyphen")
    return trimmed.upper()


def _normalize_required_loan_code(value: str) -> str:
    normalized = _normalize_optional_loan_code(value)
    if not normalized:
        raise ValueError("loan_code is required")
    return normalized


def _ensure_not_future(value: Optional[datetime], field_name: str) -> Optional[datetime]:
    if value is None:
        return value
    comparison_value = value
    if value.tzinfo is not None:
        comparison_value = value.astimezone(timezone.utc).replace(tzinfo=None)
    if comparison_value > datetime.utcnow():
        raise ValueError(f"{field_name} cannot be in the future")
    return value


class CustomerCreate(BaseModel):
    id: Optional[int] = None
    customer_code: Optional[str] = None
    name: str
    phone: str
    id_card: Optional[str] = None
    address: Optional[str] = None
    note: Optional[str] = None
    photo_url: Optional[str] = None

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value <= 0:
            raise ValueError("id must be a positive integer")
        return value

    @field_validator("customer_code")
    @classmethod
    def validate_customer_code(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_optional_customer_code(value)


class CustomerRead(CustomerCreate):
    id: int
    customer_code: str
    created_at: datetime
    projected_balance: float
    last_principal: float
    next_compound_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class CustomerUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    id_card: Optional[str] = None
    address: Optional[str] = None
    note: Optional[str] = None
    photo_url: Optional[str] = None
    customer_code: Optional[str] = None

    @field_validator("customer_code")
    @classmethod
    def validate_customer_code(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_optional_customer_code(value)


class CustomerBalanceUpdate(BaseModel):
    loan_code: Optional[str] = None
    loan_id: Optional[int] = None
    projected_balance: Optional[float] = None
    adjust_amount: float = 0.0
    next_compound_at: Optional[datetime] = None

    @field_validator("loan_code")
    @classmethod
    def validate_loan_code(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_optional_loan_code(value)

    @field_validator("loan_id")
    @classmethod
    def validate_loan_id(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value <= 0:
            raise ValueError("loan_id must be a positive integer")
        return value

    @field_validator("projected_balance")
    @classmethod
    def validate_projected_balance(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("projected_balance must be non-negative")
        return value

    @field_validator("adjust_amount")
    @classmethod
    def validate_adjust_amount(cls, value: float) -> float:
        if value != value:
            raise ValueError("adjust_amount must be a number")
        return value

    @model_validator(mode="after")
    def ensure_balance_reference(cls, values: "CustomerBalanceUpdate") -> "CustomerBalanceUpdate":
        projected = values.projected_balance
        adjust_amount = values.adjust_amount
        has_adjustment = projected is not None or abs(adjust_amount) > 1e-9
        if has_adjustment and not (values.loan_code or values.loan_id):
            raise ValueError("loan_code is required when adjusting balance")
        return values


class LoanCreate(BaseModel):
    customer_code: str
    loan_amount: float
    loan_date: datetime
    processing_fee: float = 0.0
    interest_rate: float = 0.0
    interest_type: str = "月息"
    note: Optional[str] = None

    @field_validator("loan_date", mode="before")
    @classmethod
    def normalize_loan_date(cls, value):
        if value is None:
            return value
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError("loan_date must be ISO 8601 date/datetime") from exc
        raise ValueError("Unsupported loan_date value")

    @field_validator("loan_date")
    @classmethod
    def validate_loan_date_not_future(cls, value: Optional[datetime]):
        return _ensure_not_future(value, "loan_date")

    @field_validator("interest_type")
    @classmethod
    def validate_interest_type(cls, value: str) -> str:
        allowed = {"月息", "日息"}
        if value not in allowed:
            raise ValueError("interest_type must be one of 月息 or 日息")
        return value

    @field_validator("processing_fee")
    @classmethod
    def validate_processing_fee(cls, value: float, info: ValidationInfo):
        if value < 0:
            raise ValueError("processing_fee must be non-negative")
        loan_amount = info.data.get("loan_amount") if info and info.data else None
        if loan_amount is not None and value > loan_amount:
            raise ValueError("processing_fee cannot exceed loan_amount")
        return value

    @field_validator("customer_code")
    @classmethod
    def validate_customer_code(cls, value: str) -> str:
        return _normalize_required_customer_code(value)


class LoanRead(BaseModel):
    id: int
    loan_code: str
    customer_code: str
    loan_amount: float
    processing_fee: float
    loan_date: datetime
    interest_rate: float
    interest_type: str
    note: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    last_operation_action: Optional[str] = None
    last_operation_description: Optional[str] = None
    last_operation_at: Optional[datetime] = None
    remaining_balance: Optional[float] = None
    model_config = ConfigDict(from_attributes=True)


class LoanUpdate(BaseModel):
    loan_amount: Optional[float] = None
    processing_fee: Optional[float] = None
    loan_date: Optional[datetime] = None
    interest_rate: Optional[float] = None
    interest_type: Optional[str] = None
    note: Optional[str] = None

    @field_validator("loan_date", mode="before")
    @classmethod
    def normalize_optional_loan_date(cls, value):
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError("loan_date must be ISO 8601 date/datetime") from exc
        raise ValueError("Unsupported loan_date value")

    @field_validator("loan_date")
    @classmethod
    def validate_optional_loan_date_not_future(cls, value: Optional[datetime]):
        return _ensure_not_future(value, "loan_date")

    @field_validator("processing_fee")
    @classmethod
    def validate_processing_fee(cls, value: Optional[float]) -> Optional[float]:
        if value is None:
            return value
        if value < 0:
            raise ValueError("processing_fee must be non-negative")
        return value

    @field_validator("loan_amount")
    @classmethod
    def validate_loan_amount(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("loan_amount must be non-negative")
        return value

    @field_validator("interest_type")
    @classmethod
    def validate_interest_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        allowed = {"月息", "日息"}
        if value not in allowed:
            raise ValueError("interest_type must be one of 月息 or 日息")
        return value


class RepaymentCreate(BaseModel):
    customer_code: str
    loan_code: str
    loan_id: Optional[int] = None
    repayment_amount: float
    repayment_date: datetime
    note: Optional[str] = None

    @field_validator("repayment_date", mode="before")
    @classmethod
    def normalize_repayment_date(cls, value):
        if value is None:
            return value
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError("repayment_date must be ISO 8601 date/datetime") from exc
        raise ValueError("Unsupported repayment_date value")

    @field_validator("repayment_date")
    @classmethod
    def validate_repayment_date_not_future(cls, value: Optional[datetime]):
        return _ensure_not_future(value, "repayment_date")

    @field_validator("customer_code")
    @classmethod
    def validate_customer_code(cls, value: str) -> str:
        return _normalize_required_customer_code(value)

    @field_validator("loan_code")
    @classmethod
    def validate_loan_code(cls, value: str) -> str:
        return _normalize_required_loan_code(value)

    @field_validator("loan_id")
    @classmethod
    def validate_loan_id(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value <= 0:
            raise ValueError("loan_id must be a positive integer")
        return value


class RepaymentRead(BaseModel):
    id: int
    customer_code: str
    loan_id: Optional[int] = None
    loan_code: Optional[str] = None
    repayment_amount: float
    repayment_date: datetime
    note: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    last_operation_action: Optional[str] = None
    last_operation_description: Optional[str] = None
    last_operation_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class RepaymentUpdate(BaseModel):
    repayment_amount: Optional[float] = None
    repayment_date: Optional[datetime] = None
    note: Optional[str] = None

    @field_validator("repayment_amount")
    @classmethod
    def validate_repayment_amount(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("repayment_amount must be non-negative")
        return value

    @field_validator("repayment_date", mode="before")
    @classmethod
    def normalize_optional_repayment_date(cls, value):
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError("repayment_date must be ISO 8601 date/datetime") from exc
        raise ValueError("Unsupported repayment_date value")

    @field_validator("repayment_date")
    @classmethod
    def validate_optional_repayment_date_not_future(cls, value: Optional[datetime]):
        return _ensure_not_future(value, "repayment_date")


class SummaryEntry(BaseModel):
    customer_id: int
    customer_code: str
    name: str
    phone: str
    id_card: Optional[str] = None
    address: Optional[str] = None
    note: Optional[str] = None
    total_loan: float
    total_repayment: float
    total_fee: float
    balance: float
    projected_balance: float
    next_compound_at: Optional[datetime]
    compound_rate: float
    last_update: Optional[datetime]
    photo_url: Optional[str] = None
    customer_created_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class RecordsResponse(BaseModel):
    loans: List[LoanRead]
    repayments: List[RepaymentRead]


class BankTransactionRead(BaseModel):
    id: int
    transaction_type: str
    amount: float
    balance_after: float
    reference_type: Optional[str] = None
    reference_id: Optional[int] = None
    customer_id: Optional[int] = None
    note: Optional[str] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class BankLedgerResponse(BaseModel):
    balance: float
    total: int = 0
    limit: int = 0
    offset: int = 0
    transactions: List[BankTransactionRead]


class BankManualAdjustmentRequest(BaseModel):
    amount: float = Field(gt=0)
    direction: Literal["deposit", "withdrawal"]
    note: Optional[str] = None


class OperationLogRead(BaseModel):
    id: int
    entity_type: str
    entity_id: Optional[int]
    action: str
    description: str
    metadata: Optional[Dict[str, Any]] = None
    customer_code: Optional[str] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class OverallReportSummary(BaseModel):
    total_loan_amount: float = 0.0
    total_repayment_amount: float = 0.0
    interest_profit: float = 0.0
    fee_income: float = 0.0
    net_profit: float = 0.0
    loan_count: int = 0
    repayment_count: int = 0
    total_customer_count: int = 0
    new_customer_count: int = 0


class BalanceTimelineEvent(BaseModel):
    event_type: str
    event_time: datetime
    change_amount: float
    balance_after: float
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class CustomerBalanceTimelineResponse(BaseModel):
    customer_id: int
    customer_code: str
    customer_name: Optional[str] = None
    projected_balance: float
    next_compound_at: Optional[datetime] = None
    events: List[BalanceTimelineEvent]


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    refresh_token: Optional[str] = None


class UserBase(BaseModel):
    username: str
    role: UserRole
    is_active: bool
    customer_id: Optional[int] = None
    permissions: Dict[str, bool] = Field(default_factory=dict)


class UserRead(UserBase):
    id: int
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


class UserCreate(BaseModel):
    username: str
    password: str
    role: UserRole
    customer_id: Optional[int] = None
    permissions: Optional[Dict[str, bool]] = None


class PasswordResetRequest(BaseModel):
    password: str


class UserStatusUpdate(BaseModel):
    is_active: bool


class UserPermissionUpdate(BaseModel):
    permissions: Dict[str, bool]


class PermissionDefinitionSchema(BaseModel):
    key: str
    label: str
    category: str
    action: str
    description: Optional[str] = None


class PermissionCatalog(BaseModel):
    permissions: List[PermissionDefinitionSchema]


class AdminUserUpdate(BaseModel):
    username: Optional[str] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None
    permissions: Optional[Dict[str, bool]] = None
