from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set

import secrets
import string
import json
import re
from fastapi import HTTPException
from sqlalchemy import func, or_
from sqlmodel import Session, select

from .models import Customer, Loan, Repayment, OperationLog, CompoundBalanceEvent, BankTransaction
from .schemas import (
    BalanceTimelineEvent,
    BankLedgerResponse,
    BankManualAdjustmentRequest,
    BankTransactionRead,
    CustomerBalanceTimelineResponse,
    CustomerBalanceUpdate,
    LoanRead,
    LoanUpdate,
    OverallReportSummary,
    OperationLogRead,
    RecordsResponse,
    RepaymentRead,
    RepaymentUpdate,
    SummaryEntry,
)
from .timezone_utils import ensure_myt_datetime, now_myt, to_myt_datetime

COMPOUND_INTEREST_RATE = 0.20
COMPOUND_INTERVAL = timedelta(days=30)

LOAN_CREATE_RE = re.compile(r"创建借贷，金额\s*([\-\d.]+)，手续费\s*([\-\d.]+)")
LOAN_DELETE_RE = re.compile(r"删除借贷，冲销金额\s*([\-\d.]+)")

REPAYMENT_CREATE_RE = re.compile(r"新增还款，金额\s*([\-\d.]+)")
REPAYMENT_DELETE_RE = re.compile(r"删除还款，回退金额\s*([\-\d.]+)")

AMOUNT_ARROW_RE = re.compile(r"金额\s*([\-\d.]+)\s*→\s*([\-\d.]+)")
LOAN_UPDATE_PHRASES = {
    "更新借出日期": "log.loan.update.loan_date",
    "更新手续费": "log.loan.update.processing_fee",
    "更新利率": "log.loan.update.interest_rate",
    "更新计息方式": "log.loan.update.interest_type",
    "更新备注": "log.loan.update.note",
}
REPAYMENT_UPDATE_PHRASES = {
    "更新还款日期": "log.repayment.update.date",
    "更新备注": "log.repayment.update.note",
}

CUSTOMER_FIELD_CHANGE_MAP = {
    "customer_code": ("更新顾客编号", "log.customer.update.customer_code"),
    "name": ("更新姓名", "log.customer.update.name"),
    "phone": ("更新电话", "log.customer.update.phone"),
    "address": ("更新地址", "log.customer.update.address"),
    "id_card": ("更新身份证", "log.customer.update.id_card"),
    "note": ("更新备注", "log.customer.update.note"),
}


def _scalar_from_result(result):
    try:
        return result[0]
    except (TypeError, KeyError, IndexError):
        return result


def _stringify_log_value(value):
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, datetime):
        normalized = ensure_myt_datetime(value) or value
        return normalized.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    return text or "-"


def _append_change_entry(
    change_notes: list[str],
    change_i18n: list[dict],
    label: str,
    i18n_key: Optional[str],
    old_value,
    new_value,
):
    if old_value == new_value:
        return
    old_display = _stringify_log_value(old_value)
    new_display = _stringify_log_value(new_value)
    change_notes.append(f"{label} {old_display}→{new_display}")
    if i18n_key:
        change_i18n.append({"key": i18n_key, "params": {"old": old_display, "new": new_display}})


def _safe_myt(value, fallback):
    converted = to_myt_datetime(value)
    if converted is not None:
        return converted
    if fallback is None:
        return value
    converted_fallback = to_myt_datetime(fallback)
    return converted_fallback or fallback


def _generate_customer_code(session: Session) -> str:
    charset = string.ascii_uppercase + string.digits
    while True:
        code = "CUST-" + "".join(secrets.choice(charset) for _ in range(6))
        existing = session.exec(select(Customer).where(Customer.customer_code == code)).first()
        if not existing:
            return code


def _id_suffix(id_card: Optional[str]) -> str:
    digits = "".join(ch for ch in (id_card or "") if ch.isdigit())
    if not digits:
        return "".join(secrets.choice(string.digits) for _ in range(4))
    if len(digits) >= 4:
        return digits[-4:]
    return digits.zfill(4)


def _generate_loan_code(session: Session, customer: Optional[Customer]) -> str:
    suffix = _id_suffix(getattr(customer, "id_card", None))
    timestamp = now_myt().strftime("%Y%m%d%H%M%S")
    while True:
        random_part = "".join(secrets.choice(string.digits) for _ in range(2))
        code = f"LN-{suffix}-{timestamp}{random_part}"
        existing = session.exec(select(Loan).where(Loan.loan_code == code)).first()
        if not existing:
            return code


def _round_amount(value: Optional[float]) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        number = 0.0
    return round(number, 2)


def _latest_bank_transaction(session: Session) -> Optional[BankTransaction]:
    return session.exec(
        select(BankTransaction).order_by(BankTransaction.id.desc()).limit(1)
    ).first()


def get_bank_balance(session: Session) -> float:
    latest = _latest_bank_transaction(session)
    if not latest:
        return 0.0
    return _round_amount(latest.balance_after)


def record_bank_transaction(
    session: Session,
    *,
    transaction_type: str,
    amount: float,
    note: Optional[str] = None,
    reference_type: Optional[str] = None,
    reference_id: Optional[int] = None,
    customer_id: Optional[int] = None,
) -> Optional[BankTransaction]:
    normalized_amount = _round_amount(amount)
    if abs(normalized_amount) <= 1e-9:
        return None
    current_balance = get_bank_balance(session)
    new_balance = _round_amount(current_balance + normalized_amount)
    entry = BankTransaction(
        transaction_type=transaction_type,
        amount=normalized_amount,
        balance_after=new_balance,
        reference_type=reference_type,
        reference_id=reference_id,
        customer_id=customer_id,
        note=note,
    )
    session.add(entry)
    session.flush([entry])
    return entry


def _record_balance_event(
    session: Session,
    customer: Customer,
    *,
    event_type: str,
    change_amount: float,
    description: str,
    metadata: Optional[dict] = None,
    event_time: Optional[datetime] = None,
) -> Optional[CompoundBalanceEvent]:
    if not customer.id or abs(change_amount) <= 1e-9:
        return None
    payload = dict(metadata) if metadata else None
    event = CompoundBalanceEvent(
        customer_id=customer.id,
        event_type=event_type,
        event_time=_safe_myt(event_time, event_time) or now_myt(),
        change_amount=_round_amount(change_amount),
        balance_after=_round_amount(max(customer.projected_balance or 0.0, 0.0)),
        description=description,
        metadata_json=json.dumps(payload, ensure_ascii=False) if payload else None,
    )
    session.add(event)
    session.flush([event])
    return event


def _ensure_balance_event_baseline(session: Session, customer: Customer) -> None:
    if not customer.id:
        return
    existing = session.exec(
        select(CompoundBalanceEvent.id)
        .where(CompoundBalanceEvent.customer_id == customer.id)
        .limit(1)
    ).first()
    if existing:
        return
    balance = _round_amount(max(customer.projected_balance or 0.0, 0.0))
    if balance <= 0:
        return
    baseline = CompoundBalanceEvent(
        customer_id=customer.id,
        event_type="baseline",
        event_time=now_myt(),
        change_amount=balance,
        balance_after=balance,
        description="初始化复利余额",
        metadata_json=json.dumps({"source": "baseline"}, ensure_ascii=False),
    )
    session.add(baseline)
    session.commit()
def _interest_interval_days(interest_type: Optional[str]) -> int:
    if not interest_type:
        return 30
    normalized = interest_type.strip().lower()
    mapping = {
        "月息": 30,
        "monthly": 30,
        "month": 30,
        "日息": 1,
        "daily": 1,
        "day": 1,
    }
    return mapping.get(normalized, 30)


def _initial_compounded_amount(loan_amount: float) -> float:
    base = max(loan_amount, 0.0)
    compounded = base * (1 + COMPOUND_INTEREST_RATE)
    return round(compounded, 2)


def _log_operation(
    session: Session,
    *,
    entity_type: str,
    entity_id: Optional[int],
    action: str,
    description: str,
    metadata: Optional[dict] = None,
    i18n_key: Optional[str] = None,
    i18n_params: Optional[dict] = None,
    i18n_list: Optional[List[dict]] = None,
) -> OperationLog:
    metadata_payload: dict = dict(metadata) if metadata else {}
    if i18n_key:
        metadata_payload["_i18n_key"] = i18n_key
        if i18n_params:
            metadata_payload["_i18n_params"] = i18n_params
        else:
            metadata_payload["_i18n_params"] = {}
    if i18n_list:
        metadata_payload["_i18n_list"] = i18n_list

    log = OperationLog(
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        description=description,
        metadata_json=json.dumps(metadata_payload, ensure_ascii=False) if metadata_payload else None,
    )
    session.add(log)
    session.flush([log])
    return log


def _decode_metadata(metadata_json: Optional[str]) -> Optional[dict]:
    if not metadata_json:
        return None
    try:
        value = json.loads(metadata_json)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        return None


def _infer_log_i18n_metadata(log: OperationLog, metadata: Optional[dict]) -> Optional[dict]:
    metadata_copy = dict(metadata) if metadata else {}
    if metadata_copy.get("_i18n_key") or metadata_copy.get("_i18n_list"):
        return metadata_copy or None
    description = (log.description or "").strip()
    if not description:
        return metadata_copy or None

    entity_type = (log.entity_type or "").lower()
    action = (log.action or "").lower()

    def ensure_params(old: str, new: str) -> dict:
        return {"old": old.strip(), "new": new.strip()}

    if action == "create":
        if entity_type == "loan":
            match = LOAN_CREATE_RE.match(description)
            if match:
                metadata_copy["_i18n_key"] = "log.loan.create"
                metadata_copy["_i18n_params"] = {"amount": match.group(1), "fee": match.group(2)}
        elif entity_type == "repayment":
            match = REPAYMENT_CREATE_RE.match(description)
            if match:
                metadata_copy["_i18n_key"] = "log.repayment.create"
                metadata_copy["_i18n_params"] = {"amount": match.group(1)}
    elif action == "delete":
        if entity_type == "loan":
            match = LOAN_DELETE_RE.match(description)
            if match:
                metadata_copy["_i18n_key"] = "log.loan.delete"
                metadata_copy["_i18n_params"] = {"amount": match.group(1)}
        elif entity_type == "repayment":
            match = REPAYMENT_DELETE_RE.match(description)
            if match:
                metadata_copy["_i18n_key"] = "log.repayment.delete"
                metadata_copy["_i18n_params"] = {"amount": match.group(1)}
    elif action == "update":
        phrase_map = None
        amount_key = None
        if entity_type == "loan":
            phrase_map = LOAN_UPDATE_PHRASES
            amount_key = "log.loan.update.amount"
        elif entity_type == "repayment":
            phrase_map = REPAYMENT_UPDATE_PHRASES
            amount_key = "log.repayment.update.amount"
        if phrase_map and amount_key:
            inferred_list: List[dict] = []
            for piece in (segment.strip() for segment in description.split(";") if segment.strip()):
                arrow_match = AMOUNT_ARROW_RE.match(piece)
                if arrow_match:
                    inferred_list.append({"key": amount_key, "params": ensure_params(arrow_match.group(1), arrow_match.group(2))})
                    continue
                mapped_key = phrase_map.get(piece)
                if mapped_key:
                    inferred_list.append({"key": mapped_key})
            if inferred_list:
                metadata_copy["_i18n_list"] = inferred_list

    return metadata_copy or None


def _latest_operation_map(session: Session, entity_type: str, entity_ids: List[Optional[int]]) -> Dict[int, OperationLog]:
    ids = [item for item in entity_ids if item]
    if not ids:
        return {}
    stmt = (
        select(OperationLog)
        .where(OperationLog.entity_type == entity_type, OperationLog.entity_id.in_(ids))
        .order_by(OperationLog.created_at.desc())
    )
    rows = session.exec(stmt).all()
    latest: Dict[int, OperationLog] = {}
    for row in rows:
        if row.entity_id and row.entity_id not in latest:
            latest[row.entity_id] = row
    return latest


def _apply_compound_growth(customer: Customer, now: datetime, *, session: Optional[Session] = None) -> bool:
    """Bring the customer's projected balance current before further adjustments."""

    updated = False
    projected = max(customer.projected_balance or 0.0, 0.0)
    next_compound_at = ensure_myt_datetime(customer.next_compound_at)
    if next_compound_at != customer.next_compound_at:
        customer.next_compound_at = next_compound_at
        updated = True

    if projected <= 0:
        if customer.projected_balance != 0:
            customer.projected_balance = 0.0
            updated = True
        if customer.next_compound_at is not None:
            customer.next_compound_at = None
            updated = True
        return updated

    if next_compound_at is None:
        next_compound_at = now + COMPOUND_INTERVAL
        customer.next_compound_at = next_compound_at
        updated = True

    while next_compound_at and next_compound_at <= now:
        previous_balance = projected
        projected = round(projected * (1 + COMPOUND_INTEREST_RATE), 2)
        increment = projected - previous_balance
        customer.projected_balance = projected
        if session and increment > 1e-9:
            _record_balance_event(
                session,
                customer,
                event_type="compound_growth",
                change_amount=increment,
                description="复利增长",
                metadata={
                    "base_amount": previous_balance,
                    "result_amount": projected,
                    "interest_rate": round(COMPOUND_INTEREST_RATE * 100, 4),
                },
                event_time=next_compound_at,
            )
        next_compound_at = next_compound_at + COMPOUND_INTERVAL
        customer.next_compound_at = next_compound_at
        updated = True

    if abs(projected - (customer.projected_balance or 0.0)) > 1e-9:
        customer.projected_balance = projected
        updated = True

    return updated


def _adjust_projected_balance(
    customer: Customer,
    delta: float,
    now: datetime,
    *,
    session: Optional[Session] = None,
) -> bool:
    """Apply a delta directly inside the compounding balance after bringing it current."""

    if abs(delta) <= 1e-9:
        return False

    updated = _apply_compound_growth(customer, now, session=session)
    projected = max(customer.projected_balance or 0.0, 0.0)
    projected = max(projected + delta, 0.0)
    projected = round(projected, 2)

    if abs(projected - (customer.projected_balance or 0.0)) > 1e-9:
        customer.projected_balance = projected
        updated = True

    if projected <= 0:
        if customer.next_compound_at is not None:
            customer.next_compound_at = None
            updated = True
        return updated

    next_compound_at = ensure_myt_datetime(customer.next_compound_at)
    if next_compound_at != customer.next_compound_at:
        customer.next_compound_at = next_compound_at
        updated = True
    if customer.next_compound_at is None:
        customer.next_compound_at = now + COMPOUND_INTERVAL
        updated = True
    return updated


def _refresh_compounded_balance(
    customer: Customer,
    actual_balance: float,
    now: datetime,
    *,
    session: Optional[Session] = None,
) -> tuple[float, Optional[datetime], bool]:
    recorded = customer.last_principal or 0.0
    delta = actual_balance - recorded
    updated = False

    if abs(delta) > 1e-9:
        updated = _adjust_projected_balance(customer, delta, now, session=session) or updated
        customer.last_principal = actual_balance
        updated = True
    else:
        updated = _apply_compound_growth(customer, now, session=session)

    return customer.projected_balance or 0.0, customer.next_compound_at, updated


def _customer_code_for(session: Session, customer_id: Optional[int]) -> str:
    if not customer_id:
        return ""
    customer = session.get(Customer, customer_id)
    if not customer:
        return ""
    return (customer.customer_code or "").strip()


def _loan_code_for(session: Session, loan_id: Optional[int]) -> str:
    if not loan_id:
        return ""
    loan = session.get(Loan, loan_id)
    if not loan:
        return ""
    return _ensure_loan_code(session, loan)


def _ensure_loan_code(session: Session, loan: Loan, customer: Optional[Customer] = None) -> str:
    if loan.loan_code:
        return loan.loan_code
    customer = customer or (session.get(Customer, loan.customer_id) if loan.customer_id else None)
    loan.loan_code = _generate_loan_code(session, customer)
    session.add(loan)
    session.commit()
    session.refresh(loan)
    return loan.loan_code


def _loan_remaining_compounded_balance(
    session: Session,
    loan: Loan,
    *,
    exclude_repayment_id: Optional[int] = None,
) -> float:
    base_amount = _initial_compounded_amount(loan.loan_amount)
    stmt = select(func.coalesce(func.sum(func.abs(Repayment.repayment_amount)), 0)).where(Repayment.loan_id == loan.id)
    if exclude_repayment_id is not None:
        stmt = stmt.where(Repayment.id != exclude_repayment_id)
    paid_result = session.exec(stmt).one()
    paid_total = float(_scalar_from_result(paid_result) or 0.0)
    remaining = base_amount - paid_total
    return max(_round_amount(remaining), 0.0)


def _ensure_repayment_within_loan_balance(
    session: Session,
    loan: Loan,
    amount: float,
    *,
    exclude_repayment_id: Optional[int] = None,
) -> None:
    normalized_amount = max(_round_amount(abs(amount)), 0.0)
    allowed = _loan_remaining_compounded_balance(session, loan, exclude_repayment_id=exclude_repayment_id)
    loan_label = loan.loan_code or f"ID {loan.id}"
    if allowed <= 0 and normalized_amount > 0:
        raise HTTPException(status_code=400, detail=f"借贷 {loan_label} 的复利余额已为 0，无法继续还款")
    if normalized_amount - allowed > 1e-9:
        raise HTTPException(
            status_code=400,
            detail=f"借贷 {loan_label} 的复利余额仅剩 {allowed:.2f}，请输入不超过该数额的还款",
        )


def create_customer(session: Session, data: Customer) -> Customer:
    desired_code = (data.customer_code or "").strip().upper()
    if desired_code:
        existing = session.exec(select(Customer).where(Customer.customer_code == desired_code)).first()
        if existing:
            raise HTTPException(status_code=400, detail="customer_code already exists")
        data.customer_code = desired_code
    else:
        data.customer_code = _generate_customer_code(session)
    session.add(data)
    session.flush()
    _log_operation(
        session,
        entity_type="customer",
        entity_id=data.id,
        action="create",
        description=f"创建顾客 {data.customer_code}",
        metadata={"customer_code": data.customer_code},
        i18n_key="log.customer.create",
        i18n_params={"code": data.customer_code or ""},
    )
    session.commit()
    session.refresh(data)
    return data


def list_customers(session: Session) -> List[Customer]:
    return session.exec(select(Customer).order_by(Customer.created_at.desc())).all()


def list_loans(session: Session) -> List[LoanRead]:
    loan_rows = session.exec(select(Loan).order_by(Loan.loan_date.desc())).all()
    latest = _latest_operation_map(session, "loan", [loan.id for loan in loan_rows])
    return [to_loan_read(session, loan, latest.get(loan.id)) for loan in loan_rows]


def list_repayments(session: Session) -> List[RepaymentRead]:
    repayment_rows = session.exec(select(Repayment).order_by(Repayment.repayment_date.desc())).all()
    latest = _latest_operation_map(session, "repayment", [row.id for row in repayment_rows])
    return [to_repayment_read(session, repayment, latest.get(repayment.id)) for repayment in repayment_rows]


def list_operation_logs(
    session: Session,
    limit: int = 200,
    *,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    start_at: Optional[datetime] = None,
    end_at: Optional[datetime] = None,
) -> List[OperationLog]:
    safe_limit = max(1, min(limit, 500))
    stmt = select(OperationLog)
    if entity_type:
        stmt = stmt.where(OperationLog.entity_type == entity_type)
    if entity_id is not None:
        stmt = stmt.where(OperationLog.entity_id == entity_id)
    if start_at is not None:
        stmt = stmt.where(OperationLog.created_at >= start_at)
    if end_at is not None:
        stmt = stmt.where(OperationLog.created_at <= end_at)
    stmt = stmt.order_by(OperationLog.created_at.desc()).limit(safe_limit)
    return session.exec(stmt).all()


def get_overall_report(
    session: Session,
    *,
    start_at: Optional[datetime] = None,
    end_at: Optional[datetime] = None,
) -> OverallReportSummary:
    loan_stmt = select(
        func.count(Loan.id),
        func.coalesce(func.sum(Loan.loan_amount), 0),
        func.coalesce(func.sum(Loan.processing_fee), 0),
    )
    if start_at is not None:
        loan_stmt = loan_stmt.where(Loan.loan_date >= start_at)
    if end_at is not None:
        loan_stmt = loan_stmt.where(Loan.loan_date <= end_at)
    loan_result = session.exec(loan_stmt).one()
    loan_count = int(loan_result[0] or 0)
    total_loan_amount = float(loan_result[1] or 0.0)
    total_fee_income = float(loan_result[2] or 0.0)

    repayment_stmt = select(
        func.count(Repayment.id),
        func.coalesce(func.sum(Repayment.repayment_amount), 0),
    )
    if start_at is not None:
        repayment_stmt = repayment_stmt.where(Repayment.repayment_date >= start_at)
    if end_at is not None:
        repayment_stmt = repayment_stmt.where(Repayment.repayment_date <= end_at)
    repayment_result = session.exec(repayment_stmt).one()
    repayment_count = int(repayment_result[0] or 0)
    total_repayment_amount = float(repayment_result[1] or 0.0)

    net_profit = total_repayment_amount - total_loan_amount
    interest_profit = net_profit if net_profit > 0 else 0.0

    total_customer_result = session.exec(select(func.count(Customer.id))).one()
    total_customer_count = int(_scalar_from_result(total_customer_result) or 0)
    new_customer_stmt = select(func.count(Customer.id))
    if start_at is not None:
        new_customer_stmt = new_customer_stmt.where(Customer.created_at >= start_at)
    if end_at is not None:
        new_customer_stmt = new_customer_stmt.where(Customer.created_at <= end_at)
    if start_at is None and end_at is None:
        new_customer_count = total_customer_count
    else:
        new_customer_result = session.exec(new_customer_stmt).one()
        new_customer_count = int(_scalar_from_result(new_customer_result) or 0)

    return OverallReportSummary(
        total_loan_amount=round(total_loan_amount, 2),
        total_repayment_amount=round(total_repayment_amount, 2),
        interest_profit=round(interest_profit, 2),
        fee_income=round(total_fee_income, 2),
        net_profit=round(net_profit, 2),
        loan_count=loan_count,
        repayment_count=repayment_count,
        total_customer_count=int(total_customer_count or 0),
        new_customer_count=int(new_customer_count or 0),
    )


def map_operation_log_customer_codes(session: Session, logs: Sequence[OperationLog]) -> Dict[int, str]:
    if not logs:
        return {}
    customer_ids: Set[int] = set()
    loan_ids: Set[int] = set()
    repayment_ids: Set[int] = set()
    for log in logs:
        entity_id = log.entity_id
        if not entity_id:
            continue
        entity_type = (log.entity_type or "").lower()
        if entity_type == "customer":
            customer_ids.add(entity_id)
        elif entity_type == "loan":
            loan_ids.add(entity_id)
        elif entity_type == "repayment":
            repayment_ids.add(entity_id)

    loan_customer_map: Dict[int, Optional[int]] = {}
    if loan_ids:
        loan_rows = session.exec(select(Loan).where(Loan.id.in_(tuple(loan_ids)))).all()
        for loan in loan_rows:
            loan_customer_map[loan.id] = loan.customer_id
            if loan.customer_id:
                customer_ids.add(loan.customer_id)

    repayment_customer_map: Dict[int, Optional[int]] = {}
    if repayment_ids:
        repayment_rows = session.exec(select(Repayment).where(Repayment.id.in_(tuple(repayment_ids)))).all()
        for repayment in repayment_rows:
            repayment_customer_map[repayment.id] = repayment.customer_id
            if repayment.customer_id:
                customer_ids.add(repayment.customer_id)

    customer_code_map: Dict[int, str] = {}
    if customer_ids:
        customer_rows = session.exec(select(Customer).where(Customer.id.in_(tuple(customer_ids)))).all()
        for customer in customer_rows:
            code = (customer.customer_code or "").strip()
            if code:
                customer_code_map[customer.id] = code

    result: Dict[int, str] = {}
    for log in logs:
        entity_type = (log.entity_type or "").lower()
        entity_id = log.entity_id
        customer_id = None
        if entity_type == "customer" and entity_id:
            customer_id = entity_id
        elif entity_type == "loan" and entity_id:
            customer_id = loan_customer_map.get(entity_id)
        elif entity_type == "repayment" and entity_id:
            customer_id = repayment_customer_map.get(entity_id)
        if customer_id:
            code = customer_code_map.get(customer_id)
            if code:
                result[log.id] = code
    return result


def get_customer(session: Session, customer_id: int) -> Customer:
    customer = session.get(Customer, customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


def get_customer_by_code(session: Session, customer_code: str) -> Customer:
    normalized = (customer_code or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="customer_code is required")
    customer = session.exec(select(Customer).where(Customer.customer_code == normalized)).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


def get_loan_by_code(session: Session, loan_code: str) -> Loan:
    normalized = (loan_code or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="loan_code is required")
    loan = session.exec(select(Loan).where(Loan.loan_code == normalized)).first()
    if not loan:
        raise HTTPException(status_code=404, detail="Loan not found")
    return loan


def update_customer(session: Session, customer_id: int, payload: dict) -> Customer:
    customer = get_customer(session, customer_id)
    if "customer_code" in payload:
        code = (payload["customer_code"] or "").strip().upper()
        if code:
            existing = session.exec(select(Customer).where(Customer.customer_code == code, Customer.id != customer_id)).first()
            if existing:
                raise HTTPException(status_code=400, detail="customer_code already exists")
            payload["customer_code"] = code
        else:
            payload.pop("customer_code")
    change_notes: list[str] = []
    change_i18n: list[dict] = []
    for key, value in payload.items():
        old_value = getattr(customer, key)
        if old_value == value:
            continue
        setattr(customer, key, value)
        label, i18n_key = CUSTOMER_FIELD_CHANGE_MAP.get(key, (f"更新{key}", None))
        _append_change_entry(change_notes, change_i18n, label, i18n_key, old_value, value)
    session.add(customer)
    session.flush()
    if change_notes:
        _log_operation(
            session,
            entity_type="customer",
            entity_id=customer.id,
            action="update",
            description="; ".join(change_notes),
            i18n_list=change_i18n,
        )
    session.commit()
    session.refresh(customer)
    return customer


def update_customer_balance(
    session: Session, customer_id: int, payload: CustomerBalanceUpdate
) -> Customer:
    customer = get_customer(session, customer_id)
    now = now_myt()
    previous_balance = customer.projected_balance or 0.0
    change_notes: list[str] = []
    change_i18n: list[dict] = []
    metadata: dict[str, Any] = {}
    adjust_value = payload.adjust_amount or 0.0
    requires_loan = payload.projected_balance is not None or abs(adjust_value) > 1e-9
    loan_ref: Optional[Loan] = None
    loan_remaining: Optional[float] = None
    if payload.loan_id is not None:
        loan_ref = session.get(Loan, payload.loan_id)
        if loan_ref is None:
            raise HTTPException(status_code=404, detail="Loan not found")
    elif payload.loan_code:
        loan_ref = get_loan_by_code(session, payload.loan_code)
    if loan_ref and loan_ref.customer_id != customer.id:
        raise HTTPException(status_code=400, detail="Loan does not belong to the customer")
    if requires_loan:
        if loan_ref is None:
            raise HTTPException(status_code=400, detail="loan_code is required for balance adjustments")
        loan_remaining = _loan_remaining_compounded_balance(session, loan_ref)
        if loan_remaining <= 0:
            loan_label = _ensure_loan_code(session, loan_ref)
            raise HTTPException(status_code=400, detail=f"借贷 {loan_label} 的复利余额已为 0，无法调整")
    if payload.projected_balance is not None:
        target = max(payload.projected_balance, 0.0)
        delta = target - (customer.projected_balance or 0.0)
        if abs(delta) > 1e-9:
            _adjust_projected_balance(customer, delta, now, session=session)
            metadata["projected_balance"] = target
            change_notes.append(f"设置新的复利余额 {target:.2f}")
            change_i18n.append({
                "key": "log.customer.balance.set",
                "params": {"amount": f"{target:.2f}"},
            })
            event_metadata = {
                "target_balance": target,
                "previous_balance": previous_balance,
            }
            if loan_ref:
                event_metadata.update({
                    "loan_id": loan_ref.id,
                    "loan_code": loan_ref.loan_code,
                    "loan_remaining": loan_remaining,
                })
            _record_balance_event(
                session,
                customer,
                event_type="manual_override",
                change_amount=delta,
                description="手动设置复利余额",
                metadata=event_metadata,
                event_time=now,
            )
    else:
        if abs(adjust_value) > 1e-9:
            _adjust_projected_balance(customer, adjust_value, now, session=session)
            metadata["adjust_amount"] = adjust_value
            change_notes.append(f"调整复利余额 {adjust_value:+.2f}")
            change_i18n.append({
                "key": "log.customer.balance.adjust",
                "params": {"amount": f"{adjust_value:+.2f}"},
            })
            event_metadata = {
                "previous_balance": previous_balance,
            }
            if loan_ref:
                event_metadata.update({
                    "loan_id": loan_ref.id,
                    "loan_code": loan_ref.loan_code,
                    "loan_remaining": loan_remaining,
                })
            _record_balance_event(
                session,
                customer,
                event_type="manual_adjust",
                change_amount=adjust_value,
                description="手动调整复利余额",
                metadata=event_metadata,
                event_time=now,
            )

    if payload.next_compound_at is not None:
        new_next = ensure_myt_datetime(payload.next_compound_at)
        changed = new_next != customer.next_compound_at
        customer.next_compound_at = new_next
        if changed:
            change_notes.append("更新下次结息时间")
            change_i18n.append({"key": "log.customer.balance.next_compound"})

    session.add(customer)
    session.flush()
    if change_notes:
        metadata.setdefault("new_projected_balance", round(customer.projected_balance or 0.0, 2))
        if loan_ref:
            metadata.setdefault("loan_id", loan_ref.id)
            metadata.setdefault("loan_code", loan_ref.loan_code)
            if loan_remaining is not None:
                metadata.setdefault("loan_remaining", loan_remaining)
        _log_operation(
            session,
            entity_type="customer",
            entity_id=customer.id,
            action="update",
            description="; ".join(change_notes),
            metadata=metadata or None,
            i18n_list=change_i18n,
        )
    session.commit()
    session.refresh(customer)
    return customer


def update_customer_photo(session: Session, customer_id: int, photo_url: str) -> Customer:
    customer = get_customer(session, customer_id)
    customer.photo_url = photo_url
    session.add(customer)
    session.flush()
    _log_operation(
        session,
        entity_type="customer",
        entity_id=customer.id,
        action="update",
        description="更新顾客照片",
        metadata={"photo_url": photo_url},
        i18n_key="log.customer.photo.update",
    )
    session.commit()
    session.refresh(customer)
    return customer

def create_loan(session: Session, loan: Loan) -> Loan:
    customer = get_customer(session, loan.customer_id)
    loan.loan_date = ensure_myt_datetime(loan.loan_date) or loan.loan_date
    loan.loan_code = loan.loan_code or _generate_loan_code(session, customer)
    addition = _initial_compounded_amount(loan.loan_amount)
    now = now_myt()
    _adjust_projected_balance(customer, addition, now, session=session)
    customer.last_principal = (customer.last_principal or 0.0) + addition
    session.add(customer)
    session.add(loan)
    session.flush()
    _record_balance_event(
        session,
        customer,
        event_type="loan_disbursement",
        change_amount=addition,
        description="借贷入账",
        metadata={
            "loan_id": loan.id,
            "loan_code": loan.loan_code,
            "loan_amount": loan.loan_amount,
            "compounded_amount": addition,
        },
        event_time=loan.loan_date or now,
    )
    record_bank_transaction(
        session,
        transaction_type="loan_disbursement",
        amount=-_round_amount(loan.loan_amount),
        note=f"借贷 {loan.loan_code} 借出 {loan.loan_amount:.2f}",
        reference_type="loan",
        reference_id=loan.id,
        customer_id=loan.customer_id,
    )
    _log_operation(
        session,
        entity_type="loan",
        entity_id=loan.id,
        action="create",
        description=f"创建借贷，金额 {loan.loan_amount:.2f}，手续费 {loan.processing_fee:.2f}",
        metadata={"loan_amount": loan.loan_amount, "processing_fee": loan.processing_fee},
        i18n_key="log.loan.create",
        i18n_params={
            "amount": f"{loan.loan_amount:.2f}",
            "fee": f"{loan.processing_fee:.2f}",
        },
    )
    session.commit()
    session.refresh(loan)
    return loan


def create_repayment(session: Session, repayment: Repayment) -> Repayment:
    customer = get_customer(session, repayment.customer_id)
    loan_ref = session.get(Loan, repayment.loan_id) if repayment.loan_id else None
    if repayment.loan_id and loan_ref is None:
        raise HTTPException(status_code=404, detail="Loan not found")
    if loan_ref:
        _ensure_repayment_within_loan_balance(session, loan_ref, repayment.repayment_amount)
    repayment.repayment_date = ensure_myt_datetime(repayment.repayment_date) or repayment.repayment_date
    deduction = -abs(repayment.repayment_amount)
    now = now_myt()
    _apply_compound_growth(customer, now, session=session)
    previous_balance = _round_amount(max(customer.projected_balance or 0.0, 0.0))
    _adjust_projected_balance(customer, deduction, now, session=session)
    customer.last_principal = (customer.last_principal or 0.0) + deduction
    loan_code = _loan_code_for(session, repayment.loan_id)
    session.add(customer)
    session.add(repayment)
    session.flush()
    _record_balance_event(
        session,
        customer,
        event_type="repayment",
        change_amount=deduction,
        description="录入还款",
        metadata={
            "repayment_id": repayment.id,
            "repayment_amount": abs(repayment.repayment_amount),
            "loan_id": repayment.loan_id,
            "loan_code": loan_code,
            "previous_balance": previous_balance,
        },
        event_time=repayment.repayment_date or now,
    )
    record_bank_transaction(
        session,
        transaction_type="repayment_receipt",
        amount=_round_amount(abs(repayment.repayment_amount)),
        note=f"还款 {loan_code or repayment.id} 入账 {abs(repayment.repayment_amount):.2f}",
        reference_type="repayment",
        reference_id=repayment.id,
        customer_id=repayment.customer_id,
    )
    _log_operation(
        session,
        entity_type="repayment",
        entity_id=repayment.id,
        action="create",
        description=f"新增还款，金额 {repayment.repayment_amount:.2f}",
        metadata={"repayment_amount": repayment.repayment_amount},
        i18n_key="log.repayment.create",
        i18n_params={"amount": f"{repayment.repayment_amount:.2f}"},
    )
    session.commit()
    session.refresh(repayment)
    return repayment


def update_loan(session: Session, loan_id: int, payload: LoanUpdate) -> Loan:
    loan = session.get(Loan, loan_id)
    if not loan:
        raise HTTPException(status_code=404, detail="Loan not found")
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    customer = get_customer(session, loan.customer_id)
    now = now_myt()
    change_notes: list[str] = []
    change_i18n: list[dict] = []
    previous_cash_amount = loan.loan_amount
    if "loan_amount" in updates and updates["loan_amount"] is not None:
        new_amount = updates.pop("loan_amount")
        old_effect = _initial_compounded_amount(loan.loan_amount)
        new_effect = _initial_compounded_amount(new_amount)
        delta = new_effect - old_effect
        if abs(delta) > 1e-9:
            _adjust_projected_balance(customer, delta, now, session=session)
            customer.last_principal = (customer.last_principal or 0.0) + delta
            _record_balance_event(
                session,
                customer,
                event_type="loan_adjustment",
                change_amount=delta,
                description="借贷金额调整",
                metadata={
                    "loan_id": loan.id,
                    "loan_code": loan.loan_code,
                    "previous_effective_amount": old_effect,
                    "new_effective_amount": new_effect,
                },
                event_time=now,
            )
        loan.loan_amount = new_amount
        change_notes.append(f"金额 {old_effect:.2f}→{new_effect:.2f}")
        change_i18n.append({
            "key": "log.loan.update.amount",
            "params": {"old": f"{old_effect:.2f}", "new": f"{new_effect:.2f}"},
        })
        cash_delta = new_amount - previous_cash_amount
        if abs(cash_delta) > 1e-9:
            code = _ensure_loan_code(session, loan)
            record_bank_transaction(
                session,
                transaction_type="loan_adjustment",
                amount=-_round_amount(cash_delta),
                note=f"调整借贷 {code} 金额 {cash_delta:+.2f}",
                reference_type="loan",
                reference_id=loan.id,
                customer_id=loan.customer_id,
            )
        previous_cash_amount = loan.loan_amount
    if "loan_date" in updates and updates["loan_date"] is not None:
        old_value = loan.loan_date
        new_date = ensure_myt_datetime(updates.pop("loan_date")) or loan.loan_date
        loan.loan_date = new_date
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新借出日期",
            "log.loan.update.loan_date",
            old_value,
            new_date,
        )
    if "processing_fee" in updates and updates["processing_fee"] is not None:
        new_fee = updates.pop("processing_fee")
        old_fee = loan.processing_fee
        loan.processing_fee = new_fee
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新手续费",
            "log.loan.update.processing_fee",
            old_fee,
            new_fee,
        )
    if "interest_rate" in updates and updates["interest_rate"] is not None:
        new_rate = updates.pop("interest_rate")
        old_rate = loan.interest_rate
        loan.interest_rate = new_rate
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新利率",
            "log.loan.update.interest_rate",
            old_rate,
            new_rate,
        )
    if "interest_type" in updates and updates["interest_type"] is not None:
        new_type = updates.pop("interest_type")
        old_type = loan.interest_type
        loan.interest_type = new_type
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新计息方式",
            "log.loan.update.interest_type",
            old_type,
            new_type,
        )
    if "note" in updates:
        new_note = updates.pop("note")
        old_note = loan.note
        loan.note = new_note
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新备注",
            "log.loan.update.note",
            old_note,
            new_note,
        )

    session.add(customer)
    session.add(loan)
    session.flush()
    if change_notes:
        _log_operation(
            session,
            entity_type="loan",
            entity_id=loan.id,
            action="update",
            description="; ".join(change_notes),
            i18n_list=change_i18n,
        )
    session.commit()
    session.refresh(loan)
    return loan


def update_repayment(session: Session, repayment_id: int, payload: RepaymentUpdate) -> Repayment:
    repayment = session.get(Repayment, repayment_id)
    if not repayment:
        raise HTTPException(status_code=404, detail="Repayment not found")
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    customer = get_customer(session, repayment.customer_id)
    now = now_myt()
    loan_code = _loan_code_for(session, repayment.loan_id)
    loan_ref = None
    if repayment.loan_id:
        loan_ref = session.get(Loan, repayment.loan_id)
        if loan_ref is None:
            raise HTTPException(status_code=404, detail="Loan not found")
    change_notes: list[str] = []
    change_i18n: list[dict] = []
    previous_cash_amount = repayment.repayment_amount
    if "repayment_amount" in updates and updates["repayment_amount"] is not None:
        new_amount = abs(updates.pop("repayment_amount"))
        old_amount = abs(repayment.repayment_amount)
        if loan_ref and not math.isclose(new_amount, old_amount, abs_tol=1e-9):
            _ensure_repayment_within_loan_balance(
                session,
                loan_ref,
                new_amount,
                exclude_repayment_id=repayment.id,
            )
        delta = -new_amount - (-old_amount)
        if abs(delta) > 1e-9:
            _adjust_projected_balance(customer, delta, now, session=session)
            customer.last_principal = (customer.last_principal or 0.0) + delta
            _record_balance_event(
                session,
                customer,
                event_type="repayment_adjustment",
                change_amount=delta,
                description="还款金额调整",
                metadata={
                    "repayment_id": repayment.id,
                    "previous_amount": old_amount,
                    "new_amount": new_amount,
                    "loan_code": loan_code,
                },
                event_time=now,
            )
        repayment.repayment_amount = new_amount
        change_notes.append(f"金额 {old_amount:.2f}→{new_amount:.2f}")
        change_i18n.append({
            "key": "log.repayment.update.amount",
            "params": {"old": f"{old_amount:.2f}", "new": f"{new_amount:.2f}"},
        })
        cash_delta = new_amount - previous_cash_amount
        if abs(cash_delta) > 1e-9:
            record_bank_transaction(
                session,
                transaction_type="repayment_adjustment",
                amount=_round_amount(cash_delta),
                note=f"调整还款 {repayment.id} 金额 {cash_delta:+.2f}",
                reference_type="repayment",
                reference_id=repayment.id,
                customer_id=repayment.customer_id,
            )
        previous_cash_amount = repayment.repayment_amount
    if "repayment_date" in updates and updates["repayment_date"] is not None:
        old_value = repayment.repayment_date
        new_date = ensure_myt_datetime(updates.pop("repayment_date")) or repayment.repayment_date
        repayment.repayment_date = new_date
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新还款日期",
            "log.repayment.update.date",
            old_value,
            new_date,
        )
    if "note" in updates:
        new_note = updates.pop("note")
        old_note = repayment.note
        repayment.note = new_note
        _append_change_entry(
            change_notes,
            change_i18n,
            "更新备注",
            "log.repayment.update.note",
            old_note,
            new_note,
        )

    session.add(customer)
    session.add(repayment)
    session.flush()
    if change_notes:
        _log_operation(
            session,
            entity_type="repayment",
            entity_id=repayment.id,
            action="update",
            description="; ".join(change_notes),
            i18n_list=change_i18n,
        )
    session.commit()
    session.refresh(repayment)
    return repayment


def delete_loan(session: Session, loan_id: int) -> LoanRead:
    loan = session.get(Loan, loan_id)
    if not loan:
        raise HTTPException(status_code=404, detail="Loan not found")
    customer = get_customer(session, loan.customer_id)
    now = now_myt()
    deduction = -_initial_compounded_amount(loan.loan_amount)
    _adjust_projected_balance(customer, deduction, now, session=session)
    customer.last_principal = (customer.last_principal or 0.0) + deduction
    session.add(customer)
    _record_balance_event(
        session,
        customer,
        event_type="loan_writeoff",
        change_amount=deduction,
        description="借贷删除",
        metadata={
            "loan_id": loan.id,
            "loan_code": loan.loan_code,
            "loan_amount": loan.loan_amount,
        },
        event_time=now,
    )
    record_bank_transaction(
        session,
        transaction_type="loan_reversal",
        amount=_round_amount(abs(loan.loan_amount)),
        note=f"删除借贷 {loan.loan_code or loan.id}",
        reference_type="loan",
        reference_id=loan.id,
        customer_id=loan.customer_id,
    )
    log_entry = _log_operation(
        session,
        entity_type="loan",
        entity_id=loan.id,
        action="delete",
        description=f"删除借贷，冲销金额 {abs(deduction):.2f}",
        metadata={"loan_amount": loan.loan_amount},
        i18n_key="log.loan.delete",
        i18n_params={"amount": f"{abs(deduction):.2f}"},
    )
    response = to_loan_read(session, loan, log_entry)
    session.delete(loan)
    session.commit()
    session.refresh(customer)
    return response


def delete_repayment(session: Session, repayment_id: int) -> RepaymentRead:
    repayment = session.get(Repayment, repayment_id)
    if not repayment:
        raise HTTPException(status_code=404, detail="Repayment not found")
    customer = get_customer(session, repayment.customer_id)
    now = now_myt()
    addition = abs(repayment.repayment_amount)
    _adjust_projected_balance(customer, addition, now, session=session)
    customer.last_principal = (customer.last_principal or 0.0) + addition
    loan_code = _loan_code_for(session, repayment.loan_id)
    session.add(customer)
    _record_balance_event(
        session,
        customer,
        event_type="repayment_reversal",
        change_amount=addition,
        description="还款删除",
        metadata={
            "repayment_id": repayment.id,
            "repayment_amount": repayment.repayment_amount,
            "loan_id": repayment.loan_id,
            "loan_code": loan_code,
        },
        event_time=now,
    )
    record_bank_transaction(
        session,
        transaction_type="repayment_reversal",
        amount=-_round_amount(abs(repayment.repayment_amount)),
        note=f"删除还款 {repayment.id}",
        reference_type="repayment",
        reference_id=repayment.id,
        customer_id=repayment.customer_id,
    )
    log_entry = _log_operation(
        session,
        entity_type="repayment",
        entity_id=repayment.id,
        action="delete",
        description=f"删除还款，回退金额 {addition:.2f}",
        metadata={"repayment_amount": repayment.repayment_amount},
        i18n_key="log.repayment.delete",
        i18n_params={"amount": f"{addition:.2f}"},
    )
    response = to_repayment_read(session, repayment, log_entry)
    session.delete(repayment)
    session.commit()
    session.refresh(customer)
    return response


def list_bank_transactions(
    session: Session,
    *,
    limit: int = 20,
    offset: int = 0,
    start_at: Optional[datetime] = None,
    end_at: Optional[datetime] = None,
    search: Optional[str] = None,
) -> tuple[List[BankTransaction], int]:
    safe_limit = max(1, min(limit, 200))
    safe_offset = max(0, offset)
    stmt = select(BankTransaction)
    count_stmt = select(func.count(BankTransaction.id))

    def apply_common_filters(query):
        if start_at is not None:
            query = query.where(BankTransaction.created_at >= start_at)
        if end_at is not None:
            query = query.where(BankTransaction.created_at <= end_at)
        if search:
            trimmed = search.strip()
            if trimmed:
                pattern = f"%{trimmed}%"
                conditions = [
                    BankTransaction.transaction_type.ilike(pattern),
                    BankTransaction.note.ilike(pattern),
                    BankTransaction.reference_type.ilike(pattern),
                ]
                try:
                    search_id = int(trimmed)
                except ValueError:
                    search_id = None
                if search_id is not None:
                    conditions.append(BankTransaction.reference_id == search_id)
                    conditions.append(BankTransaction.customer_id == search_id)
                query = query.where(or_(*conditions))
        return query

    stmt = apply_common_filters(stmt)
    count_stmt = apply_common_filters(count_stmt)

    stmt = (
        stmt.order_by(BankTransaction.created_at.asc(), BankTransaction.id.asc())
        .offset(safe_offset)
        .limit(safe_limit)
    )
    entries = session.exec(stmt).all()
    total_result = session.exec(count_stmt).one()
    total = int(_scalar_from_result(total_result) or 0)
    return entries, total


def get_bank_ledger(
    session: Session,
    *,
    limit: int = 20,
    offset: int = 0,
    start_at: Optional[datetime] = None,
    end_at: Optional[datetime] = None,
    search: Optional[str] = None,
) -> BankLedgerResponse:
    entries, total = list_bank_transactions(
        session,
        limit=limit,
        offset=offset,
        start_at=start_at,
        end_at=end_at,
        search=search,
    )
    balance = get_bank_balance(session)
    return BankLedgerResponse(
        balance=_round_amount(balance),
        total=total,
        limit=max(1, min(limit, 200)),
        offset=max(0, offset),
        transactions=[to_bank_transaction_read(entry) for entry in entries],
    )


def create_manual_bank_adjustment(session: Session, payload: BankManualAdjustmentRequest) -> BankTransaction:
    direction = payload.direction
    signed_amount = _round_amount(payload.amount if direction == "deposit" else -payload.amount)
    entry = record_bank_transaction(
        session,
        transaction_type=f"manual_{direction}",
        amount=signed_amount,
        note=payload.note or ("手动增加余额" if signed_amount > 0 else "手动扣除余额"),
    )
    if entry is None:
        raise HTTPException(status_code=400, detail="amount must not be zero")
    session.commit()
    session.refresh(entry)
    return entry


def get_summary(session: Session) -> List[SummaryEntry]:
    customers = list_customers(session)
    summary: List[SummaryEntry] = []
    needs_commit = False
    now = now_myt()
    for customer in customers:
        code = customer.customer_code
        if not code:
            code = _generate_customer_code(session)
            customer.customer_code = code
            session.add(customer)
            needs_commit = True
        total_loan = session.exec(
            select(Loan).where(Loan.customer_id == customer.id)
        ).all()
        total_repayment = session.exec(
            select(Repayment).where(Repayment.customer_id == customer.id)
        ).all()

        loan_amount = sum(item.loan_amount for item in total_loan)
        repayment_amount = sum(item.repayment_amount for item in total_repayment)
        fee_amount = sum(item.processing_fee for item in total_loan)
        effective_loan_total = sum(_initial_compounded_amount(item.loan_amount) for item in total_loan)
        balance_raw = effective_loan_total - repayment_amount
        last_update = None
        if total_repayment:
            last_update = max(item.repayment_date for item in total_repayment)
        elif total_loan:
            last_update = max(item.loan_date for item in total_loan)

        projected_balance, next_compound_at, updated = _refresh_compounded_balance(customer, balance_raw, now, session=session)
        if updated:
            session.add(customer)
            needs_commit = True

        display_balance = max(balance_raw, 0.0)

        summary.append(
            SummaryEntry(
                customer_id=customer.id,
                customer_code=code,
                name=customer.name,
                phone=customer.phone,
                id_card=customer.id_card,
                address=customer.address,
                note=customer.note,
                total_loan=loan_amount,
                total_repayment=repayment_amount,
                total_fee=fee_amount,
                balance=display_balance,
                projected_balance=projected_balance,
                next_compound_at=_safe_myt(next_compound_at, None),
                compound_rate=COMPOUND_INTEREST_RATE,
                last_update=_safe_myt(last_update, None),
                photo_url=customer.photo_url,
                customer_created_at=_safe_myt(customer.created_at, customer.created_at),
            )
        )
    if needs_commit:
        session.commit()
    return summary


def get_records_by_date(session: Session, start_date: datetime, end_date: datetime) -> RecordsResponse:
    loan_rows = session.exec(
        select(Loan).where(Loan.loan_date.between(start_date, end_date))
    ).all()
    repayment_rows = session.exec(
        select(Repayment).where(Repayment.repayment_date.between(start_date, end_date))
    ).all()
    loan_logs = _latest_operation_map(session, "loan", [loan.id for loan in loan_rows])
    repayment_logs = _latest_operation_map(session, "repayment", [row.id for row in repayment_rows])
    loans = [to_loan_read(session, loan, loan_logs.get(loan.id)) for loan in loan_rows]
    repayments = [
        to_repayment_read(session, repayment, repayment_logs.get(repayment.id))
        for repayment in repayment_rows
    ]
    return RecordsResponse(loans=loans, repayments=repayments)


def get_customer_balance_timeline(session: Session, customer_id: int) -> CustomerBalanceTimelineResponse:
    customer = get_customer(session, customer_id)
    _ensure_balance_event_baseline(session, customer)
    events = session.exec(
        select(CompoundBalanceEvent)
        .where(CompoundBalanceEvent.customer_id == customer.id)
        .order_by(CompoundBalanceEvent.event_time.asc(), CompoundBalanceEvent.id.asc())
    ).all()
    serialized: list[BalanceTimelineEvent] = []
    for event in events:
        metadata = _decode_metadata(event.metadata_json) or {}
        serialized.append(
            BalanceTimelineEvent(
                event_type=event.event_type,
                event_time=_safe_myt(event.event_time, event.event_time),
                change_amount=event.change_amount,
                balance_after=event.balance_after,
                description=event.description or "",
                metadata=metadata,
            )
        )

    return CustomerBalanceTimelineResponse(
        customer_id=customer.id,
        customer_code=customer.customer_code,
        customer_name=customer.name,
        projected_balance=_round_amount(max(customer.projected_balance or 0.0, 0.0)),
        next_compound_at=_safe_myt(customer.next_compound_at, None),
        events=serialized,
    )

def to_loan_read(session: Session, loan: Loan, latest_log: Optional[OperationLog] = None) -> LoanRead:
    log = latest_log
    if log is None and loan.id:
        log = (
            session.exec(
                select(OperationLog)
                .where(OperationLog.entity_type == "loan", OperationLog.entity_id == loan.id)
                .order_by(OperationLog.created_at.desc())
            ).first()
        )
    remaining_balance = None
    if loan.id:
        remaining_balance = _loan_remaining_compounded_balance(session, loan)
    return LoanRead(
        id=loan.id,
        loan_code=_ensure_loan_code(session, loan),
        customer_code=_customer_code_for(session, loan.customer_id),
        loan_amount=loan.loan_amount,
        processing_fee=loan.processing_fee,
        loan_date=_safe_myt(loan.loan_date, None),
        interest_rate=loan.interest_rate,
        interest_type=loan.interest_type,
        note=loan.note,
        created_at=_safe_myt(loan.created_at, loan.loan_date),
        updated_at=_safe_myt(getattr(loan, "updated_at", None), loan.loan_date),
        last_operation_action=getattr(log, "action", None),
        last_operation_description=getattr(log, "description", None),
        last_operation_at=_safe_myt(getattr(log, "created_at", None), None),
        remaining_balance=remaining_balance,
    )


def to_repayment_read(
    session: Session,
    repayment: Repayment,
    latest_log: Optional[OperationLog] = None,
) -> RepaymentRead:
    loan_code = None
    if repayment.loan_id:
        loan_obj = session.get(Loan, repayment.loan_id)
        if loan_obj:
            loan_code = _ensure_loan_code(session, loan_obj)
    log = latest_log
    if log is None and repayment.id:
        log = (
            session.exec(
                select(OperationLog)
                .where(OperationLog.entity_type == "repayment", OperationLog.entity_id == repayment.id)
                .order_by(OperationLog.created_at.desc())
            ).first()
        )
    return RepaymentRead(
        id=repayment.id,
        customer_code=_customer_code_for(session, repayment.customer_id),
        loan_id=repayment.loan_id,
        loan_code=loan_code,
        repayment_amount=repayment.repayment_amount,
        repayment_date=_safe_myt(repayment.repayment_date, None),
        note=repayment.note,
        created_at=_safe_myt(repayment.created_at, repayment.repayment_date),
        updated_at=_safe_myt(getattr(repayment, "updated_at", None), repayment.repayment_date),
        last_operation_action=getattr(log, "action", None),
        last_operation_description=getattr(log, "description", None),
        last_operation_at=_safe_myt(getattr(log, "created_at", None), None),
    )


def to_bank_transaction_read(entry: BankTransaction) -> BankTransactionRead:
    return BankTransactionRead(
        id=entry.id,
        transaction_type=entry.transaction_type,
        amount=entry.amount,
        balance_after=entry.balance_after,
        reference_type=entry.reference_type,
        reference_id=entry.reference_id,
        customer_id=entry.customer_id,
        note=entry.note,
        created_at=_safe_myt(entry.created_at, entry.created_at),
    )


def to_operation_log_read(log: OperationLog, *, customer_code: Optional[str] = None) -> OperationLogRead:
    metadata = _decode_metadata(log.metadata_json)
    metadata = _infer_log_i18n_metadata(log, metadata)
    return OperationLogRead(
        id=log.id,
        entity_type=log.entity_type,
        entity_id=log.entity_id,
        action=log.action,
        description=log.description,
        metadata=metadata,
        customer_code=customer_code,
        created_at=_safe_myt(log.created_at, log.created_at),
    )
