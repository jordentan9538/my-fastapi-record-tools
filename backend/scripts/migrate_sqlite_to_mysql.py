from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Sequence, Type

from sqlmodel import SQLModel, Session, create_engine, delete, select
from ..models import (
    BankTransaction,
    CompoundBalanceEvent,
    Customer,
    Loan,
    OperationLog,
    Repayment,
    SessionToken,
    User,
)

DEFAULT_SQLITE_PATH = (Path(__file__).resolve().parents[1] / "data" / "loan_records.db").resolve()

MIGRATION_ORDER: Sequence[Type[SQLModel]] = (
    Customer,
    Loan,
    Repayment,
    BankTransaction,
    CompoundBalanceEvent,
    OperationLog,
    User,
    SessionToken,
)


def _iter_batches(session: Session, model: Type[SQLModel], batch_size: int) -> Iterable[List[SQLModel]]:
    offset = 0
    while True:
        results = session.exec(select(model).offset(offset).limit(batch_size)).all()
        if not results:
            break
        yield results
        offset += len(results)


def _copy_rows(dest_session: Session, model: Type[SQLModel], rows: Sequence[SQLModel]) -> int:
    for row in rows:
        payload = row.model_dump()
        dest_session.add(model(**payload))
    dest_session.commit()
    return len(rows)


def migrate(sqlite_url: str, mysql_url: str, batch_size: int, reset_destination: bool) -> None:
    sqlite_engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})
    mysql_engine = create_engine(mysql_url)

    SQLModel.metadata.create_all(mysql_engine)

    if reset_destination:
        with Session(mysql_engine) as dest:
            for model in reversed(MIGRATION_ORDER):
                dest.exec(delete(model))
            dest.commit()

    with Session(sqlite_engine) as source, Session(mysql_engine) as dest:
        for model in MIGRATION_ORDER:
            total_inserted = 0
            for batch in _iter_batches(source, model, batch_size):
                total_inserted += _copy_rows(dest, model, batch)
            print(f"Migrated {total_inserted} rows for {model.__name__}")

    print("Migration completed successfully.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy all SQLite data into a MySQL database.")
    parser.add_argument(
        "--sqlite-path",
        type=Path,
        default=DEFAULT_SQLITE_PATH,
        help=f"Path to the source SQLite file (default: {DEFAULT_SQLITE_PATH})",
    )
    parser.add_argument(
        "--sqlite-url",
        default=None,
        help="Optional explicit SQLite SQLAlchemy URL (overrides --sqlite-path)",
    )
    parser.add_argument(
        "--mysql-url",
        required=True,
        help="Destination MySQL SQLAlchemy URL, e.g. mysql+pymysql://user:pass@host/db?charset=utf8mb4",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows to copy per batch (default: 500)",
    )
    parser.add_argument(
        "--reset-destination",
        action="store_true",
        help="Delete existing rows in the destination database before migrating",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.sqlite_url:
        sqlite_url = args.sqlite_url
    else:
        sqlite_path = Path(args.sqlite_path).resolve() if args.sqlite_path else DEFAULT_SQLITE_PATH
        sqlite_url = f"sqlite:///{sqlite_path}"

    try:
        migrate(
            sqlite_url=sqlite_url,
            mysql_url=args.mysql_url,
            batch_size=max(1, args.batch_size),
            reset_destination=args.reset_destination,
        )
    except Exception as exc:  # pragma: no cover - utility script
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
