# Milestone 5 — Transactions and two-phase commit

The collector endpoint POST /transactions/two-phase coordinates live agent /prepare, /commit, and /abort calls. The coordinator writes its decision to SQLite before phase two; each agent persists participant state in its own SQLite volume. POST /transactions/recover retries incomplete decisions after a coordinator restart. This is a classroom 2PC implementation: a partition can block progress, and it is not a general consensus protocol.
