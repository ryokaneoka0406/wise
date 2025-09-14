from __future__ import annotations

from wise.db import models


def main() -> None:
    models.init_db()
    print("db path:", models.get_db_path())

    acc_id = models.create_account("user@example.com", "tok123")
    print("account id:", acc_id)
    print("get by email:", dict(models.get_account_by_email("user@example.com")))

    sid = models.create_session(acc_id)
    print("session id:", sid)
    models.add_message(sid, "user", "Hello")
    models.add_message(sid, "assistant", "Hi there")
    print("messages:", [dict(r) for r in models.list_messages(sid)])

    # Design doc compliant: artifacts like datasets/queries/analysis are stored
    # on filesystem. No DB-side verification for those here.


if __name__ == "__main__":
    main()
