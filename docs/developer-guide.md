# snowlib Developer Guide

**Last updated: January 2026**

This document orients developers to the snowlib codebase. After reading this, you should know where to work on any snowlib-related task.

## Quick Reference

| Task | Location |
|------|----------|
| Add new SQL execution feature | `src/snowlib/primitives/execute.py` |
| Add new Snowflake object model | `src/snowlib/models/` |
| Modify connection handling | `src/snowlib/connection/` |
| Add configuration options | `src/snowlib/connection/paths.py`, `profiles.py` |
| Modify context-bound session | `src/snowlib/session.py` |
| JSON column handling | `src/snowlib/utils/json_columns.py` |
| Schema inference from DataFrames | `src/snowlib/utils/schema.py` |
| Stage file operations | `src/snowlib/models/stage.py` |
| Write unit tests | `tests/unit/` |
| Write integration tests | `tests/integration/` |
| Update public API | `src/snowlib/__init__.py` |
| Build/release workflows | `build/*.ipynb` |

**Key exports from `snowlib`**:
- Primitives: `execute_sql`, `execute_sql_async`, `execute_block`, `query`
- Results: `QueryResult`, `AsyncQuery`
- Context: `SnowflakeContext`
- Session: `Session`, `create_session`
- Models: `Database`, `Schema`, `Table`, `View`, `MaterializedView`, `DynamicTable`, `Column`, `Stage`, `StageObject`, `Show`
- Types: `TableLike`, `WriteMethod`
- Connection: `load_profile`, `list_profiles`, `SnowflakeConnector`, `SnowparkConnector`, `get_config_directory`

## Architecture: Onion Layers

Dependencies flow **inward only**. Inner layers are stable and explicit; outer layers are convenient and evolving.

```
+----------------------------------------------------------+
|                     SESSION (Optional)                   |
|  Context-bound convenience: Session, create_session      |
|  Location: src/snowlib/session.py                        |
+----------------------------------------------------------+
|                    Layer 3: MODELS                       |
|  Object-oriented: Database, Schema, Table, View, etc.    |
|  Location: src/snowlib/models/                           |
+----------------------------------------------------------+
|                   Layer 2: PRIMITIVES                    |
|  Stateless functions: execute_sql, fetch_df, query       |
|  Location: src/snowlib/primitives/                       |
+----------------------------------------------------------+
|                Layer 1: CONNECTION (Core)                |
|  Profiles, connectors: SnowflakeConnector, SnowparkConn  |
|  Location: src/snowlib/connection/                       |
+----------------------------------------------------------+
```

### Layer 1: Connection (Core)

**Location**: `src/snowlib/connection/`

Manages connection profiles and connection objects. This is the foundation that all other layers depend on.

| File | Purpose |
|------|---------|
| `paths.py` | Configuration directory resolution (`get_config_directory`, `CONF_DIR`), file path utilities |
| `profiles.py` | Load/list TOML connection profiles |
| `connection.py` | `SnowflakeConnector` and `SnowparkConnector` classes |
| `base.py` | `BaseConnector` with shared authentication logic |

**Key exports**: `load_profile`, `list_profiles`, `SnowflakeConnector`, `SnowparkConnector`

### Layer 2: Primitives

**Location**: `src/snowlib/primitives/`

Plain, stateless functions for executing SQL. Always require explicit context.

| File | Purpose |
|------|---------|
| `execute.py` | `execute_sql`, `execute_sql_async`, `execute_block`, `query` functions |
| `result.py` | `QueryResult` - unified interface for query results with `.to_df()`, `.fetch_all()`, etc. |
| `async_query.py` | `AsyncQuery` - handle asynchronous query execution |

**Note**: The `Executor` class exists but is not part of the public API. Use the module-level functions instead.

**Usage pattern**:
```python
from snowlib import execute_sql, query, SnowflakeContext

ctx = SnowflakeContext(profile="dev")

# Simple query
result = execute_sql("SELECT * FROM my_table", context=ctx)
df = result.to_df()

# With parameter bindings (safe from SQL injection)
result = execute_sql("SELECT * FROM t WHERE id = %s", context=ctx, bindings=[123])

# Direct to DataFrame
df = query("SELECT * FROM t WHERE status = %s", context=ctx, bindings=["active"])

# Disable Arrow fetch for problematic data (e.g., extreme dates like 9999-01-01)
df = query("SELECT * FROM t", context=ctx, arrow=False)
```

### Layer 3: Models

**Location**: `src/snowlib/models/`

Object-oriented abstractions for Snowflake objects. Objects are lightweight proxies instantiated with identity and a frozen context.

| File/Dir | Purpose |
|----------|---------|
| `database.py` | `Database` class |
| `schema.py` | `Schema` class |
| `table/` | `Table`, `View`, `MaterializedView`, `DynamicTable`, `TableLike`, `WriteMethod` |
| `column.py` | `Column` class |
| `stage.py` | `Stage` and `StageObject` classes for internal named stages |
| `base/` | Core abstractions: `SnowflakeObject`, `Container`, `FQN`, `Show` |

**Usage pattern**:
```python
from snowlib import Database, SnowflakeContext

ctx = SnowflakeContext(profile="dev")
db = Database("MY_DB", ctx)
schema = db.schema("PUBLIC")
table = schema.table("SALES")

df = table.read()  # Read table contents
table.write(new_df, if_exists="append")  # Write data

# Columns containing dicts/lists are automatically detected and stored as VARIANT
df_with_json = pd.DataFrame({
    "id": [1, 2, 3],
    "payload": [{"x": 1}, None, {"y": [1, 2, 3]}]
})
table.write(df_with_json, if_exists="replace")  # payload column becomes VARIANT

# Append to existing VARIANT column (automatically handled via temp table)
more_data = pd.DataFrame({
    "id": [4, 5],
    "payload": [{"z": 10}, {"nested": {"key": "value"}}]
})
table.write(more_data, if_exists="append")  # Works seamlessly with VARIANT
```

**Important**: The models layer only supports **unquoted identifiers**. For quoted identifiers (e.g., `"My Table"`), use the primitives layer directly.

**JSON Column Handling**: `Table.write()` automatically detects columns containing dicts or lists, serializes them to JSON strings, loads them as STRING, then converts them to VARIANT. For append operations to existing tables with VARIANT columns, data is routed through a temporary table to ensure proper type conversion. Top-level nulls (None, np.nan, pd.NA) become SQL NULL, while None inside structures becomes JSON null. Columns with np.nan or np.inf inside structures are rejected as non-JSON-eligible.

**Stage Usage**:
```python
from snowlib import Stage, SnowflakeContext
from pathlib import Path

ctx = SnowflakeContext(profile="dev")
stage = Stage("MY_DB", "MY_SCHEMA", "MY_STAGE", ctx)

# Create the stage
stage.create()

# Upload files (with optional progress bar if tqdm is installed)
stage.load([Path("data.csv"), Path("data2.csv")], auto_compress=True, overwrite=False)

# List objects in stage
for obj in stage.objects:
    print(f"{obj.name}: {obj.size} bytes")

# Individual file operations
obj = stage.list()[0]
print(obj.path)     # @DB.SCHEMA.STAGE/filename
obj.delete()        # Remove single file

# Clear all files
stage.clear()

# Drop the stage
stage.drop(if_exists=True)
```

## SnowflakeContext

**Location**: `src/snowlib/context.py`

The central object managing connection and cursor lifecycle. Prevents repeated authentication prompts (especially with SSO) by reusing connections.

```python
# Option 1: From profile
ctx = SnowflakeContext(profile="dev")

# Option 2: From existing connection
ctx = SnowflakeContext(connection=existing_conn, cursor=existing_cursor)

# Option 3: From profile with custom config file path
ctx = SnowflakeContext(profile="dev", config_path="/path/to/connections.toml")

# Context manager pattern
with SnowflakeContext(profile="dev") as ctx:
    result = execute_sql("SELECT 1", context=ctx)
# Connection closed automatically
```

**Properties**:
- `ctx.connection` - Snowflake connection (lazily created)
- `ctx.cursor` - Snowflake cursor (lazily created)
- `ctx.current_database` - Current database from session
- `ctx.current_schema` - Current schema from session

## Session

**Location**: `src/snowlib/session.py`

Context-bound wrapper that eliminates the need to pass context to every function call. Particularly useful for interactive work and scripts.

```python
# Create session from profile
from snowlib import create_session

with create_session(profile="dev") as session:
    # Primitives - no context needed
    session.query("SELECT 1")
    session.execute_sql("DELETE FROM temp", bindings=[...])
    
    # Models - factory methods bound to session (two patterns)
    # Pattern 1: Direct instantiation with positional args
    table = session.table("MY_DB", "MY_SCHEMA", "SALES")
    
    # Pattern 2: from_name with FQN string
    table = session.table.from_name("MY_DB.MY_SCHEMA.SALES")
    
    df = table.read()

# Custom config file path (useful for downstream libraries)
with create_session(profile="myapp", config_path="/path/to/connections.toml") as session:
    session.query("SELECT 1")
```

**When to use**:
- Interactive notebooks and scripts
- When you don't want to pass `ctx` to every call
- Single-session workflows

**When to use explicit context instead**:
- Multiple concurrent connections
- When clarity of connection source is important
- Library code (prefer explicit dependencies)

## JSON Column Handling

**Location**: `src/snowlib/utils/json_columns.py`

Automatic detection and conversion of pandas columns containing JSON-serializable data (dicts, lists) to Snowflake VARIANT columns.

**Key functions**:
- `is_json_eligible(series)` - Check if a pandas Series contains JSON-serializable structures
- `serialize_json_column(series)` - Convert eligible column to JSON strings
- `prepare_json_columns(df)` - Orchestrate detection and serialization across DataFrame

**Eligibility rules**:
- Column must have object dtype with at least one dict or list
- All non-null values must be JSON-serializable
- `np.nan` and `np.inf` inside structures make column ineligible
- Top-level nulls (None, np.nan, pd.NA) are preserved as SQL NULL

**Conversion flow for new tables**:
1. `Table.write()` calls `prepare_json_columns()` before `write_pandas`
2. Eligible columns serialized to JSON strings with `json.dumps()`
3. Data loaded to Snowflake via `write_pandas` (columns created as STRING)
4. Post-processing step converts STRING columns to VARIANT via `PARSE_JSON()`

**Append mode handling**:

When appending to existing tables with `if_exists="append"`, `Table.write()` handles three cases:

1. **Table doesn't exist** → Normal write path, JSON columns become VARIANT
2. **Table exists + column is VARIANT** → Data written to temporary table, converted to VARIANT, then inserted into target
3. **Table exists + column is NOT VARIANT** → JSON conversion skipped for that column, `write_pandas` handles type coercion

This ensures you can append to tables with existing VARIANT columns without schema conflicts.

```python
import pandas as pd
from snowlib import Table, SnowflakeContext

ctx = SnowflakeContext(profile="dev")
table = Table("MY_DB", "MY_SCHEMA", "EVENTS", ctx)

# Initial write - data column becomes VARIANT
df1 = pd.DataFrame({
    "id": [1, 2],
    "data": [{"event": "click"}, {"event": "view"}]
})
table.write(df1, if_exists="replace")
# Output: Loaded 1 column(s) as VARIANT: data

# Append more data - automatically handled via temp table
df2 = pd.DataFrame({
    "id": [3, 4],
    "data": [{"event": "submit"}, None]
})
table.write(df2, if_exists="append")
# Output: Appended 2 row(s) with 1 VARIANT column(s): data

# Utility functions for manual control
from snowlib.utils.json_columns import is_json_eligible, prepare_json_columns

series = pd.Series([{"a": 1}, {"b": 2}])
if is_json_eligible(series):
    print("Column contains JSON-eligible data")

df = pd.DataFrame({
    "id": [1, 2],
    "data": [{"x": 1}, {"y": 2}]
})
modified_df, json_cols = prepare_json_columns(df)
print(f"JSON columns: {json_cols}")  # ['data']
```

## Configuration System

**Location**: `~/.snowlib/` (default)

| File | Purpose |
|------|---------||
| `connections.toml` | Connection profiles (`[default]`, `[dev]`, `[prod]`, etc.) |
| `test_config.toml` | Test-specific settings for pytest |

**Config file resolution order** (first match wins):
1. Explicit `config_path` argument passed to `Session`, `SnowflakeContext`, or `SnowflakeConnector`
2. `SNOWLIB_CONFIG_DIR` environment variable (directory containing `connections.toml`)
3. `~/.snowlib/connections.toml` (default)

Example `connections.toml`:
```toml
[default]
account = "your-account.region"
user = "your_username"
warehouse = "YOUR_WAREHOUSE"
database = "YOUR_DATABASE"
authenticator = "externalbrowser"

[dev]
account = "your-account.region"
user = "your_username"
warehouse = "DEV_WH"
database = "DEV_DATABASE"
authenticator = "externalbrowser"
```

**Config directory access**:
```python
# Preferred: evaluates at call time (picks up SNOWLIB_CONFIG_DIR changes)
from snowlib.connection import get_config_directory
config_file = get_config_directory() / "connections.toml"

# Legacy: evaluated once at import time
from snowlib.connection import CONF_DIR
config_file = CONF_DIR / "connections.toml"
```

**Custom config path** (for downstream libraries):
```python
from snowlib import Session

# Point to a specific connections.toml file
session = Session(profile="myapp", config_path="/opt/myapp/.snowlib/connections.toml")
```

## Directory Structure

```
snowlib/
├── src/snowlib/              # Main package source
│   ├── __init__.py           # Public API exports
│   ├── context.py            # SnowflakeContext
│   ├── sqlalchemy.py         # SQLAlchemy integration
│   ├── connection/           # Layer 1: Core connectivity
│   ├── primitives/           # Layer 2: SQL execution functions
│   ├── models/               # Layer 3: OOP interface
│   ├── utils/                # Shared utilities (identifiers, query helpers, JSON columns, schema)
│   │   ├── identifiers.py    # Identifier validation
│   │   ├── query.py          # Query building helpers
│   │   ├── json_columns.py   # JSON column detection and serialization
│   │   └── schema.py         # Schema inference from pandas DataFrames
│   └── _data/                # Example config files (bundled with package)
├── tests/
│   ├── conftest.py           # Shared fixtures
│   ├── unit/                 # Unit tests (no Snowflake connection)
│   └── integration/          # Integration tests (requires Snowflake)
├── build/                    # Build and release notebooks
│   ├── 01_setup.ipynb        # Initial setup
│   ├── 02_prerelease.ipynb   # Pre-release checks (quality, tox, security)
│   ├── 03_publish.ipynb      # Publish to PyPI
│   └── build_tools/          # Build utility modules
├── local/                    # Local development docs (gitignored content)
│   └── docs/                 # Design documents, architecture notes
├── temp/                     # Scratch space for experiments (gitignored)
└── pyproject.toml            # Package configuration
```

## Development Setup

```powershell
# Clone and setup
git clone https://github.com/rashtastic/snowlib.git
cd snowlib

# Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install in editable mode with dev dependencies
pip install -e .[dev]

# Create configuration files
# Copy example files from src/snowlib/_data/ to ~/.snowlib/
```

## Testing

### Test Configuration

Create `~/.snowlib/test_config.toml`:
```toml
[test]
profile = "test"              # Profile from connections.toml
database = "YOUR_TEST_DB"
schema = "YOUR_TEST_SCHEMA"
write_table = "TEST_WRITE"    # Tables will be created/dropped
temp_table = "TEST_TEMP"
read_table = "DATABASE.SCHEMA.EXISTING_TABLE"  # Required: existing table for read tests
```

### Running Tests

```powershell
# Run all tests
pytest

# Run with parallel execution
pytest -n 24

# Run unit tests only (no Snowflake connection needed)
pytest tests/unit/

# Run integration tests only
pytest tests/integration/

# Run specific test file
pytest tests/unit/test_context.py

# Run with coverage
pytest --cov=snowlib
```

### Tox (Multi-version Testing)

```powershell
# Run all Python versions
tox

# Run specific version
tox -e py311

# Recreate environments (after dependency changes)
tox -r
```

### Test Fixtures

Key fixtures from `tests/conftest.py`:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `test_profile` | session | Profile name for integration tests |
| `test_profile2` | session | Optional second profile for testing different auth methods |
| `test_database` | session | Target database |
| `test_schema` | session | Target schema |
| `test_write_table` | session | Table name for write tests (created/dropped) |
| `test_temp_table` | session | Table name for temporary test data |
| `test_read_table` | session | Existing table for read tests |
| `check_pandas_integration` | session | Skip if pyarrow not available |

**Note**: The `ctx` fixture (SnowflakeContext) is defined in individual test files at class scope to enable connection reuse within test classes.

## Adding New Features

### Adding a New Primitive Function

1. Add function to `src/snowlib/primitives/execute.py`
2. Export from `src/snowlib/primitives/__init__.py`
3. Add to public API in `src/snowlib/__init__.py`
4. Write unit tests in `tests/unit/test_primitives_execution.py`
5. Write integration tests in `tests/integration/test_primitives_integration.py`

### Adding a New Model

1. Create class in appropriate file under `src/snowlib/models/`
2. Inherit from appropriate base class (`SnowflakeObject`, `Container`, `SchemaChild`)
3. Define `SHOW_PLURAL` and `SHOW_NAME_COLUMN` class variables
4. Export from `src/snowlib/models/__init__.py`
5. Add to public API in `src/snowlib/__init__.py`
6. Write tests

### Adding Configuration Options

1. Update example files in `src/snowlib/_data/`
2. Update loading logic in `src/snowlib/connection/profiles.py`
3. Document in README.md

## Optional Dependencies

| Extra | Packages | Purpose |
|-------|----------|---------|
| `snowpark` | snowflake-snowpark-python, pyarrow | Snowpark DataFrame API |
| `sqlalchemy` | sqlalchemy, snowflake-sqlalchemy | SQLAlchemy ORM integration |
| `excel` | xlrd, xlsxwriter, openpyxl, html5lib | Excel file support |
| `dev` | pytest, mypy, tox, etc. | Development tools |

Install with: `pip install snowlib[snowpark,sqlalchemy]`

## Code Style

- **Docstrings**: Single sentence, one line, no trailing period
- **Type hints**: Required for all public functions
- **Identifiers**: ASCII only, no emojis
- **Primitives**: Always stateless, require explicit context
- **Models**: Carry frozen context from instantiation
- **Config paths**: Use `get_config_directory()` (preferred) or `CONF_DIR` for config file paths

## Build and Release

Build notebooks in `build/` directory:

1. `01_setup.ipynb` - Initial environment setup
2. `02_prerelease.ipynb` - Quality checks, tox testing, security scans
3. `03_publish.ipynb` - Build and publish to PyPI

Quality checks include:
- mypy (type checking)
- pyright (type checking)
- pytest (unit and integration tests)
- bandit (security scanning)
- detect-secrets (secret detection)

## Key Design Decisions

1. **Onion architecture**: Inner layers never depend on outer layers
2. **Lazy connections**: SnowflakeContext creates connections on first use
3. **Unquoted identifiers only in models**: Simplifies FQN handling; use primitives for quoted identifiers
4. **Profile-based configuration**: Mirrors snowsql conventions
5. **Context reuse**: Single connection per SnowflakeContext minimizes SSO prompts

## Troubleshooting

### SSO prompts repeatedly

Use class-scoped `SnowflakeContext` fixtures in tests:
```python
@pytest.fixture(scope="class")
def ctx(test_profile):
    context = SnowflakeContext(profile=test_profile)
    yield context
    context.close()
```


### Configuration not found

Ensure `~/.snowlib/connections.toml` exists, or use one of these alternatives:
- Pass `config_path` to `Session`, `SnowflakeContext`, or `SnowflakeConnector`
- Set `SNOWLIB_CONFIG_DIR` environment variable (must be set before importing snowlib, or use `get_config_directory()` instead of `CONF_DIR`)
