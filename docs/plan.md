# Metadata plan

Changes to how snowlib connects and reads object metadata, most of them taken from what snowman (`D:\pyrash\snowman`) builds around snowlib today. Each item is a separate commit with tests, and the whole plan lands before the next version bump. The two fixes come first.

**Status, 2026-10-22: all seven items implemented**, one commit each (`9dccf5d`, `3f7aa54`, `e00f3f2`, `0d53bd7`, `e080259`, `e60327c`, `d28cc0f`), plus `fb0822d`, which fixes the same class-name bug as item 1 in `Show`'s `IN` clause. Unit tests pass in every tox environment. Nothing has been run against a live account: SHOW paging, GET_DDL types and `information_schema.columns` follow Snowflake's documentation, and the connection fix was checked against real connectors 4.8 and 5.1 by connecting to a refusing local port.

## 1. Fix `describe()` for multi-word object types

`SnowflakeObject.describe()` (`models/base/core.py:81`) builds `DESCRIBE <class name upper>`, which is invalid for classes whose names are two words: `DynamicTable` produces `DESCRIBE DYNAMICTABLE` and `MaterializedView` produces `DESCRIBE MATERIALIZEDVIEW`. `TableLike.drop()` already derives the keyword from `SHOW_PLURAL` (`SHOW_PLURAL.rstrip("S")`), so `describe()` should do the same.

Done when: unit tests assert the SQL for `Table`, `View`, `DynamicTable`, `MaterializedView`, `Stage`, `Schema` and `Database`, and one shared helper produces the singular keyword for both `describe()` and `drop()`.

## 2. Stop warning on every connection under connector 5

Seen in pkm-notes (snowlib 0.4.8, connector 5.0.0, no extras): every connection prints `Unknown parameter` for `use_keyring`, `keyring_service` and `keyring_username`, and a `FutureWarning` that `enable_server_session_keep_alive_auto_detection` was not set.

- **snowlib's own profile keys reach the connector.** `BaseConnector` reads `use_keyring`, `keyring_service`, `keyring_username` and `private_key_passphrase_env`, then `SnowflakeConnector.connect()` and `SnowparkConnector.session()` pass the whole profile on. Remove those keys from what is passed to the connector and to Snowpark; keep them readable for snowlib's own use.
- **The keep-alive default is about to change.** Connector 5.x warns when `enable_server_session_keep_alive_auto_detection` is unset, because its default will change from checking for running async queries before logout to always logging out on close. snowlib offers `execute_sql_async`, so it sets `True` explicitly, which is today's behavior. The parameter exists only in connector 5.x (checked in 3.17, 4.5, 4.8 and 5.1), and older connectors reject unknown parameters with a warning when `validate_default_parameters` is on, so it is set only when the connector's major version is 5 or later and the profile does not already set it.

The larger move to the connector's own profile loading stays in `local/backlog.md`.

Done when: unit tests assert the arguments passed to `snowflake.connector.connect` and to the Snowpark builder contain none of snowlib's keys, include the keep-alive setting on connector 5 and omit it on 4, and keep a value the profile sets.

## 3. Page through SHOW past 10,000 rows

SHOW returns at most 10,000 rows. `Show.execute` (`models/base/show.py:37`) returns whatever comes back, so `Schema.tables`, `Schema.views`, `Database.schemas` and the rest silently miss everything past the cut on a large container. snowman detects this (`databases.py:57`) and raises.

`Show.execute` pages instead: when a call returns exactly 10,000 rows, issue the same SHOW with `LIMIT 10000 FROM '<last name>'` and append, until a page is short. An explicit `limit` from the caller is still honored and is not paged past.

Done when: unit tests cover a single short page, two pages, a page of exactly 10,000 followed by an empty page, and a caller `limit`.

## 4. A public SHOW for any object kind

snowlib can only SHOW the kinds it models. snowman runs `execute_sql` and then its own `RESULT_SCAN` query with the query id interpolated into the SQL, for tasks, user functions, user procedures and dynamic tables (`capture.py:44`, `databases.py:93`).

Add `Show.rows(kind, container=None, like=None)` returning a DataFrame, built on the existing `SafeQuery` and `run_with_result_scan`, and expose it as `Session.show(kind, in_=...)`. `kind` is validated against an allowlist of SHOW keywords rather than interpolated freely. Item 3's paging applies.

Done when: snowman's `show_in_schema` and `read_show` could be replaced by it with no change in output, and unit tests cover container types and the allowlist rejection.

## 5. `ddl()` and batch GET_DDL

snowlib has no access to `GET_DDL`. Add `SnowflakeObject.ddl()` and a batch function that returns one row of DDL per object in a single `UNION ALL` query, taking from snowman:

- exact-case quoting of every name part, since SHOW returns exact case (`capture.py:68`)
- argument types for functions and procedures, parsed from SHOW's `arguments` column (`capture.py:77`)
- the batch query shape (`capture.py:109`)

Done when: unit tests cover the generated SQL for a table, a view, a dynamic table, a function with arguments, a procedure with none, and a name needing quotes.

## 6. Dynamic tables in `Schema`

SHOW TABLES lists dynamic tables as ordinary tables with `is_dynamic = 'Y'`; their target lag, refresh mode and definition are only in SHOW DYNAMIC TABLES. `Schema.tables` wraps every row as a plain `Table`, and there is no `Schema.dynamic_tables` although `DynamicTable` exists.

Add `Schema.dynamic_tables`, returning `DynamicTable` objects from SHOW DYNAMIC TABLES. `Schema.tables` is unchanged: `DynamicTable` subclasses `TableLike`, not `Table`, so returning it from `tables` would break `isinstance(x, Table)` checks and remove `write()`. Its docstring says that dynamic tables are included.

Done when: unit tests cover a schema with plain and dynamic tables through both properties.

## 7. All columns in a schema in one query

snowlib describes columns one object at a time. snowman reads `information_schema.columns` once per schema (`capture.py:50`). Add `Schema.columns()` returning that view filtered to the schema, ordered by table and ordinal position, with an `object_fqn` column first.

Done when: a unit test covers the generated query and the `object_fqn` column.

## Out of scope

Capture directories, manifests, dependency snapshots and whole-database census files stay in snowman: they decide what to capture and how to store it, not how to run the SQL.
