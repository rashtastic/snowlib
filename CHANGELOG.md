# Changelog

## 0.5.0

### Changed

- **Timestamp columns come back as `datetime64[us]`, not `datetime64[ns]`.** `QueryResult.to_df()` and `fetch_batches()` now ask the connector for microsecond precision (connector 4.3 or later). This is what pandas 3 does by default. Code that checks a column's exact dtype, or relies on nanosecond values, will see the difference.
- The `snowpark` extra allows `snowflake-connector-python` 4.x (below 5.0.0, as Snowpark itself requires), and installs Snowpark on Python 3.14.
- The `sqlalchemy` extra caps SQLAlchemy below 2.1.

### Added

- `Session.show(kind, in_=..., like=...)` runs SHOW for any listed object kind, including ones snowlib does not model, such as tasks, user functions, procedures, streams and pipes, and returns a DataFrame.
- `ddl()` on databases, schemas, tables, views, materialized views and dynamic tables returns `GET_DDL` output. `Session.get_ddl([DdlTarget(...), ...])` fetches DDL for many objects in one query, including functions and procedures with their argument types.
- `Schema.columns()` returns every table and view column in a schema from `information_schema.columns`, in one query.
- `Schema.dynamic_tables` lists dynamic tables. `Schema.tables` still includes them, since SHOW TABLES does.
- SHOW listings page past Snowflake's 10,000-row limit where SHOW supports it (databases, schemas, tables, views and dynamic tables, each in its direct parent). Kinds that cannot page, such as columns and stages, warn when they reach the limit.

### Fixed

- **A result containing a date outside pandas' nanosecond range (such as `9999-12-31`) no longer falls back to a slow fetch.** In a result large enough to arrive in several batches, such a date made the batches disagree on timestamp unit, and every column came back as Python objects with a "Falling back to native fetch" warning.
- **`pip install snowlib[sqlalchemy]` works again on Python 3.10 and later.** It resolved SQLAlchemy 2.1, which `snowflake-sqlalchemy` 1.x cannot import under.
- `fetch_batches()` no longer re-reads a partly consumed cursor when a batch fails after others were returned, which could repeat or skip rows. It raises instead.
- `describe()`, and listing the columns of a dynamic table or materialized view, produced invalid SQL (`DESCRIBE DYNAMICTABLE`, `IN MATERIALIZEDVIEW`).
- With connector 5, every connection warned about `use_keyring`, `keyring_service` and `keyring_username`, which snowlib passed through, and about `enable_server_session_keep_alive_auto_detection` being unset. snowlib's own profile keys no longer reach the connector, and on connector 5 snowlib sets the keep-alive detection to `True`, the current behavior, unless the profile sets it.
