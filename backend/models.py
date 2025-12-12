from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, Relationship, SQLModel

from .timezone_utils import now_myt


class UserRole(str, Enum):
    ADMIN = "admin"
    CS = "cs"
    ACCOUNT = "account"
    CUSTOMER = "customer"


class Customer(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    customer_code: str = Field(default="", index=True, unique=True)
    name: str
    phone: str
    id_card: Optional[str] = None
    address: Optional[str] = None
    note: Optional[str] = None
    photo_url: Optional[str] = None
    created_at: datetime = Field(default_factory=now_myt)
    projected_balance: float = Field(default=0.0)
    last_principal: float = Field(default=0.0)
    next_compound_at: Optional[datetime] = Field(default=None)

    loans: list["Loan"] = Relationship(back_populates="customer")
    repayments: list["Repayment"] = Relationship(back_populates="customer")
    balance_events: list["CompoundBalanceEvent"] = Relationship(back_populates="customer")


class Loan(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    customer_id: int = Field(foreign_key="customer.id")
    loan_code: str = Field(default="", index=True, unique=True)
    loan_date: datetime = Field(default_factory=now_myt)
    loan_amount: float
    processing_fee: float = 0.0
    interest_rate: float = 0.0
    interest_type: str = Field(default="月息")
    note: Optional[str] = None
    created_at: datetime = Field(default_factory=now_myt)
    updated_at: datetime = Field(default_factory=now_myt)

    customer: Optional[Customer] = Relationship(back_populates="loans")


class Repayment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    customer_id: int = Field(foreign_key="customer.id")
    loan_id: Optional[int] = Field(default=None)
    repayment_date: datetime = Field(default_factory=now_myt)
    repayment_amount: float
    fee: float = 0.0
    note: Optional[str] = None
    created_at: datetime = Field(default_factory=now_myt)
    updated_at: datetime = Field(default_factory=now_myt)

    customer: Optional[Customer] = Relationship(back_populates="repayments")


class OperationLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    entity_type: str = Field(index=True)
    entity_id: Optional[int] = Field(default=None, index=True)
    action: str
    description: str = Field(default="")
    metadata_json: Optional[str] = None
    created_at: datetime = Field(default_factory=now_myt)


class CompoundBalanceEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    customer_id: int = Field(foreign_key="customer.id")
    event_type: str = Field(index=True)
    event_time: datetime = Field(default_factory=now_myt, index=True)
    change_amount: float
    balance_after: float
    description: Optional[str] = None
    metadata_json: Optional[str] = None

    customer: Optional[Customer] = Relationship(back_populates="balance_events")


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str
    role: UserRole = Field(default=UserRole.CS)
    is_active: bool = Field(default=True)
    customer_id: Optional[int] = Field(default=None, foreign_key="customer.id")
    created_at: datetime = Field(default_factory=now_myt)
    updated_at: datetime = Field(default_factory=now_myt)
    permissions_json: Optional[str] = Field(
        default=None,
        sa_column_kwargs={"server_default": "'{}'"},
    )


class SessionToken(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    token_hash: str = Field(index=True, unique=True)
    user_id: int = Field(foreign_key="user.id")
    expires_at: datetime = Field(index=True)
    created_at: datetime = Field(default_factory=now_myt)
    revoked: bool = Field(default=False)


class BankTransaction(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    transaction_type: str = Field(index=True)
    amount: float
    balance_after: float
    reference_type: Optional[str] = Field(default=None, index=True)
    reference_id: Optional[int] = Field(default=None, index=True)
    customer_id: Optional[int] = Field(default=None, foreign_key="customer.id")
    note: Optional[str] = None
    created_at: datetime = Field(default_factory=now_myt, index=True)
