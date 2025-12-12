from datetime import date, timedelta
import sys
from pathlib import Path

# Ensure project root is on sys.path so `import backend` works when running the test directly
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from backend.app import app, get_session, require_admin_context, require_staff_context, StaffContext
from backend import crud
from backend.models import Customer, User, UserRole
from backend.timezone_utils import now_myt


def get_test_client():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)

    def override_session():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = override_session

    def override_staff():
        with Session(engine) as session:
            user = session.exec(select(User).where(User.username == "tester")).first()
            if not user:
                user = User(username="tester", password_hash="test", role=UserRole.ADMIN, is_active=True)
                session.add(user)
                session.commit()
                session.refresh(user)
            yield StaffContext(session=session, user=user)

    app.dependency_overrides[require_staff_context] = override_staff
    app.dependency_overrides[require_admin_context] = override_staff
    client = TestClient(app)
    client._engine = engine  # type: ignore[attr-defined]
    return client


def test_create_customer_and_summary():
    client = get_test_client()
    customer_payload = {
        "name": "测试顾客",
        "phone": "0123456789",
        "address": "吉隆坡",
    }
    response = client.post("/api/customers", json=customer_payload)
    assert response.status_code == 201
    customer = response.json()
    assert customer["name"] == "测试顾客"
    assert customer["customer_code"].startswith("CUST-")

    loan_payload = {
        "customer_code": customer["customer_code"],
        "loan_amount": 1000,
        "processing_fee": 50,
        "loan_date": date.today().isoformat(),
        "interest_rate": 1.5,
        "interest_type": "月息",
    }
    loan_resp = client.post("/api/loans", json=loan_payload)
    assert loan_resp.status_code == 201
    loan_data = loan_resp.json()
    assert loan_data["loan_code"].startswith("LN-")
    assert loan_data["processing_fee"] == 50

    repayment_payload = {
        "customer_code": customer["customer_code"],
        "loan_code": loan_data["loan_code"],
        "repayment_amount": 200,
        "repayment_date": date.today().isoformat(),
    }
    repayment_resp = client.post("/api/repayments", json=repayment_payload)
    assert repayment_resp.status_code == 201
    repayment_data = repayment_resp.json()
    assert repayment_data["loan_code"].startswith("LN-")

    summary_resp = client.get("/api/summary")
    assert summary_resp.status_code == 200
    summary = summary_resp.json()
    assert len(summary) == 1
    assert summary[0]["total_loan"] == 1000
    assert summary[0]["total_repayment"] == 200
    assert summary[0]["total_fee"] == 50
    assert summary[0]["balance"] == 1000
    assert summary[0]["projected_balance"] == 1000
    assert summary[0]["next_compound_at"] is not None


def test_compounded_balance_rolls_forward():
    client = get_test_client()
    customer_payload = {
        "name": "利息测试",
        "phone": "0190000000",
    }
    response = client.post("/api/customers", json=customer_payload)
    assert response.status_code == 201
    customer = response.json()

    loan_payload = {
        "customer_code": customer["customer_code"],
        "loan_amount": 500,
        "loan_date": date.today().isoformat(),
        "interest_rate": 0,
        "interest_type": "月息",
    }
    loan_resp = client.post("/api/loans", json=loan_payload)
    assert loan_resp.status_code == 201

    initial_summary = client.get("/api/summary").json()
    assert initial_summary[0]["projected_balance"] == 600

    with Session(client._engine) as session:  # type: ignore[attr-defined]
        db_customer = session.exec(
            select(Customer).where(Customer.customer_code == customer["customer_code"])
        ).first()
        assert db_customer is not None
        db_customer.next_compound_at = now_myt() - timedelta(days=1)
        session.add(db_customer)
        session.commit()

    summary_after = client.get("/api/summary").json()
    base_with_initial = round(500 * (1 + crud.COMPOUND_INTEREST_RATE), 2)
    expected = round(base_with_initial * (1 + crud.COMPOUND_INTEREST_RATE), 2)
    assert summary_after[0]["projected_balance"] == pytest.approx(expected)
    assert summary_after[0]["next_compound_at"] is not None


def test_negative_balance_still_adjusts_projected_balance():
    now = now_myt()
    customer = Customer(
        customer_code="TEST-CUST",
        name="Negative",
        phone="0100000000",
        projected_balance=200.0,
        last_principal=0.0,
        next_compound_at=now + timedelta(days=30),
    )

    projected, next_compound_at, updated = crud._refresh_compounded_balance(
        customer,
        actual_balance=-50.0,
        now=now,
    )

    assert updated is True
    assert projected == pytest.approx(150.0)
    assert customer.projected_balance == pytest.approx(150.0)
    assert customer.last_principal == pytest.approx(-50.0)
    assert next_compound_at is not None


def test_manual_balance_adjustment_endpoint():
    client = get_test_client()
    response = client.post(
        "/api/customers",
        json={"name": "余额调整", "phone": "01122223333"},
    )
    customer = response.json()
    loan_resp = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 400,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    )
    assert loan_resp.status_code == 201
    loan = loan_resp.json()

    adjust_resp = client.put(
        f"/api/customers/{customer['id']}/balance",
        json={"adjust_amount": 100, "loan_code": loan["loan_code"]},
    )
    assert adjust_resp.status_code == 200
    adjusted_customer = adjust_resp.json()
    assert adjusted_customer["projected_balance"] == pytest.approx(580)

    override_resp = client.put(
        f"/api/customers/{customer['id']}/balance",
        json={"projected_balance": 250, "loan_code": loan["loan_code"]},
    )
    assert override_resp.status_code == 200
    override_data = override_resp.json()
    assert override_data["projected_balance"] == pytest.approx(250)

    summary_resp = client.get("/api/summary")
    assert summary_resp.status_code == 200
    summary = summary_resp.json()
    assert summary[0]["projected_balance"] == pytest.approx(250)


def test_balance_adjustment_requires_loan_code():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "余额校验", "phone": "0171111222"},
    ).json()
    client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 300,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    )
    resp = client.put(
        f"/api/customers/{customer['id']}/balance",
        json={"adjust_amount": 50},
    )
    assert resp.status_code == 422
    detail = resp.json().get("detail")
    assert isinstance(detail, list)
    assert any(item.get("loc", [])[-1] == "loan_code" for item in detail)


def test_balance_adjustment_rejects_foreign_loan():
    client = get_test_client()
    cust_one = client.post(
        "/api/customers",
        json={"name": "顾客甲", "phone": "0101000000"},
    ).json()
    loan_one = client.post(
        "/api/loans",
        json={
            "customer_code": cust_one["customer_code"],
            "loan_amount": 500,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()
    cust_two = client.post(
        "/api/customers",
        json={"name": "顾客乙", "phone": "0102000000"},
    ).json()
    resp = client.put(
        f"/api/customers/{cust_two['id']}/balance",
        json={"adjust_amount": 25, "loan_code": loan_one["loan_code"]},
    )
    assert resp.status_code == 400
    assert "Loan does not belong" in resp.json()["detail"]


def test_delete_loan_rolls_back_balance():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "删除借贷", "phone": "012345678"},
    ).json()
    loan_resp = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 500,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    )
    assert loan_resp.status_code == 201
    loan = loan_resp.json()

    delete_resp = client.delete(f"/api/loans/{loan['id']}")
    assert delete_resp.status_code == 200

    summary = client.get("/api/summary").json()
    assert summary[0]["total_loan"] == pytest.approx(0)
    assert summary[0]["projected_balance"] == pytest.approx(0)


def test_delete_repayment_restores_balance():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "删除还款", "phone": "019999999"},
    ).json()
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 500,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()
    repayment_resp = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": 200,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert repayment_resp.status_code == 201
    repayment = repayment_resp.json()

    delete_resp = client.delete(f"/api/repayments/{repayment['id']}")
    assert delete_resp.status_code == 200

    summary = client.get("/api/summary").json()
    assert summary[0]["total_repayment"] == pytest.approx(0)
    expected_balance = round(500 * (1 + crud.COMPOUND_INTEREST_RATE), 2)
    assert summary[0]["projected_balance"] == pytest.approx(expected_balance)


def test_update_loan_amount_adjusts_projection():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "编辑借贷", "phone": "0128888888"},
    ).json()
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 600,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()

    update_resp = client.put(
        f"/api/loans/{loan['id']}",
        json={"loan_amount": 400},
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()
    assert updated["loan_amount"] == pytest.approx(400)

    summary = client.get("/api/summary").json()
    assert summary[0]["total_loan"] == pytest.approx(400)
    expected_projection = round(400 * (1 + crud.COMPOUND_INTEREST_RATE), 2)
    assert summary[0]["projected_balance"] == pytest.approx(expected_projection)


def test_update_repayment_amount_adjusts_projection():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "编辑还款", "phone": "0107777777"},
    ).json()
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 500,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()
    repayment = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": 200,
            "repayment_date": date.today().isoformat(),
        },
    ).json()

    update_resp = client.put(
        f"/api/repayments/{repayment['id']}",
        json={"repayment_amount": 350},
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()
    assert updated["repayment_amount"] == pytest.approx(350)

    summary = client.get("/api/summary").json()
    assert summary[0]["total_repayment"] == pytest.approx(350)
    expected_projection = round(500 * (1 + crud.COMPOUND_INTEREST_RATE) - 350, 2)
    assert summary[0]["projected_balance"] == pytest.approx(expected_projection)


def test_repayment_rejected_when_exceeding_loan_balance():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "复利校验", "phone": "0105555555"},
    ).json()
    loan_amount = 500
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": loan_amount,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()
    max_repay = crud._initial_compounded_amount(loan_amount)

    overshoot_resp = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": max_repay + 10,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert overshoot_resp.status_code == 400
    assert "复利余额" in overshoot_resp.json()["detail"]

    full_resp = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": max_repay,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert full_resp.status_code == 201

    redundant_resp = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": 1,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert redundant_resp.status_code == 400
    assert "复利余额" in redundant_resp.json()["detail"]


def test_repayment_update_cannot_exceed_remaining_balance():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "还款调整校验", "phone": "0104444444"},
    ).json()
    loan_amount = 400
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": loan_amount,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()
    repay_one = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": 200,
            "repayment_date": date.today().isoformat(),
        },
    ).json()
    repay_two = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": crud._initial_compounded_amount(loan_amount) - 200,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert repay_two.status_code == 201

    increase_resp = client.put(
        f"/api/repayments/{repay_one['id']}",
        json={"repayment_amount": 210},
    )
    assert increase_resp.status_code == 400
    assert "复利余额" in increase_resp.json()["detail"]

    reduce_resp = client.put(
        f"/api/repayments/{repay_one['id']}",
        json={"repayment_amount": 150},
    )
    assert reduce_resp.status_code == 200
    assert reduce_resp.json()["repayment_amount"] == pytest.approx(150)


def test_customer_balance_timeline_includes_disbursement_event():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "复利流水", "phone": "0191234567"},
    ).json()
    past_date = (date.today() - timedelta(days=90)).isoformat()
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 500,
            "loan_date": past_date,
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()

    timeline_resp = client.get(f"/api/customers/{customer['id']}/balance-timeline")
    assert timeline_resp.status_code == 200
    timeline = timeline_resp.json()
    event_types = [event["event_type"] for event in timeline["events"]]
    assert "loan_disbursement" in event_types
    disbursement = next(event for event in timeline["events"] if event["event_type"] == "loan_disbursement")
    expected_change = round(loan["loan_amount"] * (1 + crud.COMPOUND_INTEREST_RATE), 2)
    assert disbursement["change_amount"] == pytest.approx(expected_change)
    assert timeline["projected_balance"] == pytest.approx(expected_change)


def test_repayment_timeline_contains_loan_code_and_previous_balance():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "还款流水", "phone": "0192222222"},
    ).json()
    loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 300,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    ).json()
    repayment = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": loan["loan_code"],
            "repayment_amount": 120,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert repayment.status_code == 201

    timeline = client.get(f"/api/customers/{customer['id']}/balance-timeline").json()
    repayment_events = [event for event in timeline["events"] if event["event_type"] == "repayment"]
    assert repayment_events, "Expected at least one repayment event"
    latest = repayment_events[-1]
    metadata = latest.get("metadata") or {}
    assert metadata.get("loan_code") == loan["loan_code"]
    assert "previous_balance" in metadata
    assert metadata["previous_balance"] >= metadata.get("repayment_amount", 0)


def test_overall_report_respects_date_filters():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "总报表", "phone": "0188888888"},
    ).json()
    today = date.today()
    recent_date = today.isoformat()
    old_date = (today - timedelta(days=60)).isoformat()

    recent_loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 1000,
            "processing_fee": 50,
            "loan_date": recent_date,
            "interest_rate": 1.0,
            "interest_type": "月息",
        },
    ).json()
    old_loan = client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 400,
            "processing_fee": 10,
            "loan_date": old_date,
            "interest_rate": 1.0,
            "interest_type": "月息",
        },
    ).json()

    client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": recent_loan["loan_code"],
            "repayment_amount": 1200,
            "repayment_date": recent_date,
        },
    )
    client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "loan_code": old_loan["loan_code"],
            "repayment_amount": 300,
            "repayment_date": old_date,
        },
    )

    range_resp = client.get(
        "/api/reports/overall",
        params={
            "start_date": (today - timedelta(days=1)).isoformat(),
            "end_date": today.isoformat(),
        },
    )
    assert range_resp.status_code == 200
    data = range_resp.json()
    assert data["total_loan_amount"] == pytest.approx(1000)
    assert data["loan_count"] == 1
    assert data["total_repayment_amount"] == pytest.approx(1200)
    assert data["repayment_count"] == 1
    assert data["fee_income"] == pytest.approx(50)
    assert data["interest_profit"] == pytest.approx(200)
    assert data["net_profit"] == pytest.approx(200)

    all_resp = client.get("/api/reports/overall")
    assert all_resp.status_code == 200
    all_data = all_resp.json()
    assert all_data["total_loan_amount"] == pytest.approx(1400)
    assert all_data["loan_count"] == 2
    assert all_data["total_repayment_amount"] == pytest.approx(1500)
    assert all_data["repayment_count"] == 2
    assert all_data["fee_income"] == pytest.approx(60)
    assert all_data["interest_profit"] == pytest.approx(100)
    assert all_data["net_profit"] == pytest.approx(100)


def test_repayment_creation_requires_loan_code():
    client = get_test_client()
    customer = client.post(
        "/api/customers",
        json={"name": "必需贷款编号", "phone": "0173333444"},
    ).json()
    client.post(
        "/api/loans",
        json={
            "customer_code": customer["customer_code"],
            "loan_amount": 200,
            "loan_date": date.today().isoformat(),
            "interest_rate": 0,
            "interest_type": "月息",
        },
    )
    resp = client.post(
        "/api/repayments",
        json={
            "customer_code": customer["customer_code"],
            "repayment_amount": 50,
            "repayment_date": date.today().isoformat(),
        },
    )
    assert resp.status_code == 422
    detail = resp.json().get("detail")
    assert isinstance(detail, list)
    assert any(item.get("loc", [])[-1] == "loan_code" for item in detail)


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__]))
