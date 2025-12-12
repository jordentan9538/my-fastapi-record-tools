from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from sqlmodel import Session, select

from .. import crud
from ..database import engine
from ..models import CompoundBalanceEvent, Customer, Loan, Repayment


@dataclass
class AuditIssue:
    severity: str
    category: str
    entity: str
    entity_id: Optional[int]
    message: str
    details: Optional[Dict[str, Any]] = None

    def as_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "severity": self.severity,
            "category": self.category,
            "entity": self.entity,
            "entity_id": self.entity_id,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


@dataclass
class AuditReport:
    stats: Dict[str, int]
    issues: List[AuditIssue]

    @property
    def issue_count(self) -> int:
        return len(self.issues)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "stats": self.stats,
            "issue_count": self.issue_count,
            "issues": [issue.as_dict() for issue in self.issues],
        }


def run_audit(session: Session, *, tolerance: float = 0.01) -> AuditReport:
    tolerance = max(tolerance, 0.0)
    customers = session.exec(select(Customer)).all()
    loans = session.exec(select(Loan)).all()
    repayments = session.exec(select(Repayment)).all()
    events = session.exec(select(CompoundBalanceEvent)).all()

    stats = {
        "customers": len(customers),
        "loans": len(loans),
        "repayments": len(repayments),
        "balance_events": len(events),
    }

    issues: List[AuditIssue] = []
    customer_codes = Counter()
    for customer in customers:
        normalized = (customer.customer_code or "").strip().upper()
        if normalized:
            customer_codes[normalized] += 1

    loan_codes = Counter()
    for loan in loans:
        normalized = (loan.loan_code or "").strip().upper()
        if normalized:
            loan_codes[normalized] += 1

    loans_by_customer: Dict[int, List[Loan]] = defaultdict(list)
    for loan in loans:
        loans_by_customer[loan.customer_id].append(loan)

    repayments_by_customer: Dict[int, List[Repayment]] = defaultdict(list)
    for repayment in repayments:
        repayments_by_customer[repayment.customer_id].append(repayment)

    events_by_customer: Dict[int, List[CompoundBalanceEvent]] = defaultdict(list)
    for event in events:
        events_by_customer[event.customer_id].append(event)

    loan_lookup = {loan.id: loan for loan in loans if loan.id is not None}

    for customer in customers:
        normalized_code = (customer.customer_code or "").strip().upper()
        if not normalized_code:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="customer_code",
                    entity="customer",
                    entity_id=customer.id,
                    message="customer_code is missing",
                )
            )
        elif customer_codes[normalized_code] > 1:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="customer_code",
                    entity="customer",
                    entity_id=customer.id,
                    message="customer_code is duplicated",
                    details={"customer_code": normalized_code},
                )
            )

        loan_rows = loans_by_customer.get(customer.id, [])
        repayment_rows = repayments_by_customer.get(customer.id, [])
        effective_loan_total = sum(crud._initial_compounded_amount(max(loan.loan_amount, 0.0)) for loan in loan_rows)
        repayment_total = sum(max(repayment.repayment_amount, 0.0) for repayment in repayment_rows)
        raw_balance = round(max(effective_loan_total - repayment_total, 0.0), 2)
        last_principal = round(float(customer.last_principal or 0.0), 2)
        if abs(last_principal - raw_balance) > tolerance:
            issues.append(
                AuditIssue(
                    severity="warning",
                    category="customer_balance",
                    entity="customer",
                    entity_id=customer.id,
                    message="last_principal deviates from recomputed balance",
                    details={
                        "expected": raw_balance,
                        "actual": last_principal,
                    },
                )
            )

        projected = round(max(customer.projected_balance or 0.0, 0.0), 2)
        if projected < -tolerance:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="customer_balance",
                    entity="customer",
                    entity_id=customer.id,
                    message="projected_balance is negative",
                    details={"value": projected},
                )
            )

        event_rows = events_by_customer.get(customer.id, [])
        if event_rows:
            event_rows.sort(key=lambda event: (event.event_time, event.id or 0))
            final_balance = round(float(event_rows[-1].balance_after), 2)
            if abs(final_balance - projected) > tolerance:
                issues.append(
                    AuditIssue(
                        severity="warning",
                        category="balance_events",
                        entity="customer",
                        entity_id=customer.id,
                        message="latest balance event drift from projected_balance",
                        details={
                            "event_balance": final_balance,
                            "projected_balance": projected,
                        },
                    )
                )
            for entry in event_rows:
                if entry.balance_after < -tolerance:
                    issues.append(
                        AuditIssue(
                            severity="error",
                            category="balance_events",
                            entity="customer",
                            entity_id=customer.id,
                            message="balance event recorded negative balance",
                            details={
                                "event_id": entry.id,
                                "balance_after": entry.balance_after,
                            },
                        )
                    )

    customer_ids = {customer.id for customer in customers}

    for loan in loans:
        normalized_code = (loan.loan_code or "").strip().upper()
        if not normalized_code:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="loan_code",
                    entity="loan",
                    entity_id=loan.id,
                    message="loan_code is missing",
                )
            )
        elif loan_codes[normalized_code] > 1:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="loan_code",
                    entity="loan",
                    entity_id=loan.id,
                    message="loan_code is duplicated",
                    details={"loan_code": normalized_code},
                )
            )
        if loan.customer_id not in customer_ids:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="loan_reference",
                    entity="loan",
                    entity_id=loan.id,
                    message="customer_id does not point to an existing customer",
                    details={"customer_id": loan.customer_id},
                )
            )
        if loan.loan_amount < 0:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="loan_amount",
                    entity="loan",
                    entity_id=loan.id,
                    message="loan_amount cannot be negative",
                    details={"loan_amount": loan.loan_amount},
                )
            )

    for repayment in repayments:
        if repayment.repayment_amount <= 0:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="repayment_amount",
                    entity="repayment",
                    entity_id=repayment.id,
                    message="repayment_amount must be positive",
                    details={"repayment_amount": repayment.repayment_amount},
                )
            )
        if repayment.customer_id not in customer_ids:
            issues.append(
                AuditIssue(
                    severity="error",
                    category="repayment_reference",
                    entity="repayment",
                    entity_id=repayment.id,
                    message="customer_id does not point to an existing customer",
                    details={"customer_id": repayment.customer_id},
                )
            )
        if repayment.loan_id is not None:
            loan = loan_lookup.get(repayment.loan_id)
            if loan is None:
                issues.append(
                    AuditIssue(
                        severity="error",
                        category="repayment_reference",
                        entity="repayment",
                        entity_id=repayment.id,
                        message="loan_id does not point to an existing loan",
                        details={"loan_id": repayment.loan_id},
                    )
                )
            elif loan.customer_id != repayment.customer_id:
                issues.append(
                    AuditIssue(
                        severity="error",
                        category="repayment_reference",
                        entity="repayment",
                        entity_id=repayment.id,
                        message="repayment loan_id/customer_id mismatch",
                        details={
                            "repayment_customer_id": repayment.customer_id,
                            "loan_customer_id": loan.customer_id,
                            "loan_id": loan.id,
                        },
                    )
                )

    return AuditReport(stats=stats, issues=issues)


def format_issue(issue: AuditIssue) -> str:
    prefix = f"[{issue.severity.upper()}] {issue.entity}#{issue.entity_id or '-'} {issue.category}"
    if issue.details:
        return f"{prefix}: {issue.message} | {json.dumps(issue.details, ensure_ascii=False)}"
    return f"{prefix}: {issue.message}"


def print_report(report: AuditReport) -> None:
    stats = report.stats
    print(
        "Audited customers={customers}, loans={loans}, repayments={repayments}, balance_events={balance_events}".format(
            **stats
        )
    )
    if not report.issues:
        print("No consistency issues detected.")
        return
    print(f"Found {report.issue_count} issues:")
    for idx, issue in enumerate(report.issues, start=1):
        print(f"{idx:02d}. {format_issue(issue)}")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit data consistency for the loan record backend")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.01,
        help="Allowed rounding difference when comparing balances (default: 0.01)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the audit report as JSON",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    with Session(engine) as session:
        report = run_audit(session, tolerance=args.tolerance)
    if args.json:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    else:
        print_report(report)
    return 1 if report.issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
