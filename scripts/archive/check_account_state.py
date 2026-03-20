
from src.storage.db import get_db
from src.storage.repository import AccountStateModel
from sqlalchemy import desc

def check_state():
    db = get_db()
    with db.get_session() as session:
        state = session.query(AccountStateModel).order_by(desc(AccountStateModel.timestamp)).first()
        if state:
            print(f"Latest State: Equity=${state.equity}, Balance=${state.balance}, Margin=${state.margin_used}")
            print(f"Timestamp: {state.timestamp}")
        else:
            print("No account state found in DB.")

if __name__ == "__main__":
    check_state()
