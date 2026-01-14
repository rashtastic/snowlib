"""Table class with write operations"""

from __future__ import annotations

import logging
import secrets
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, TYPE_CHECKING
import pandas as pd

from snowlib.models.table.base import TableLike

if TYPE_CHECKING:
    from snowlib.utils.schema import ColumnSchema

logger = logging.getLogger(__name__)


class WriteMethod(Enum):
    """Method for writing DataFrame to Snowflake table"""
    AUTO = "auto"          # Let snowlib decide based on data
    SIMPLE = "simple"      # Use write_pandas (current behavior)
    EXPLICIT = "explicit"  # Use stage + COPY INTO with column projection


class Table(TableLike):
    """Represents a Snowflake table with full read/write capabilities"""

    SHOW_PLURAL: ClassVar[str] = "TABLES"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"

    def _get_variant_columns(self) -> set[str]:
        """Get set of column names that are VARIANT type in this table"""
        if not self.exists():
            return set()
        
        desc_df = self.describe()
        variant_cols = set()
        for _, row in desc_df.iterrows():
            col_name = row.get("name", "")
            col_type = row.get("type", "")
            if col_type.upper() == "VARIANT":
                variant_cols.add(col_name.upper())
        return variant_cols

    def write(
        self,
        df: pd.DataFrame,
        if_exists: str = "fail",
        method: WriteMethod | str = WriteMethod.AUTO,
    ) -> None:
        """Write DataFrame to table with optional creation, replacement, or appending.

        Args:
            df: DataFrame to write
            if_exists: How to handle existing table:
                - 'fail': Raise error if table exists
                - 'replace': Drop and recreate table
                - 'append': Insert rows into existing table
            method: Write method to use:
                - WriteMethod.AUTO: Use EXPLICIT if JSON columns detected, else SIMPLE
                - WriteMethod.SIMPLE: Use write_pandas (fast, but VARIANT requires post-conversion)
                - WriteMethod.EXPLICIT: Use stage + COPY INTO (native VARIANT support)
        
        Columns containing dicts or lists are automatically detected as VARIANT.
        
        Note: This temporarily sets the session database and schema to match the table,
        then restores the original session state after writing.
        """
        from snowlib.primitives import execute_sql
        from snowlib.utils.schema import detect_json_columns, resolve_target_schema

        if if_exists not in ("fail", "replace", "append"):
            raise ValueError(
                f"if_exists must be 'fail', 'replace', or 'append', got: {if_exists}"
            )

        # Normalize method to enum
        if isinstance(method, str):
            method = WriteMethod(method.lower())

        # Prepare DataFrame
        df = df.copy()
        df.columns = [str(col).upper() for col in df.columns]

        # Detect JSON-eligible columns
        json_columns = detect_json_columns(df)
        
        # Resolve method if AUTO
        if method == WriteMethod.AUTO:
            if json_columns:
                method = WriteMethod.EXPLICIT
                logger.info(f"Auto-selected EXPLICIT method due to JSON columns: {json_columns}")
            else:
                method = WriteMethod.SIMPLE
                logger.debug("Auto-selected SIMPLE method (no JSON columns)")

        # Check if table exists and handle replace
        table_exists = self.exists()
        
        if if_exists == "fail" and table_exists:
            raise ValueError(f"Table {self.fqn} already exists and if_exists='fail'")
        
        if if_exists == "replace" and table_exists:
            self.drop()
            table_exists = False

        # Resolve target schema
        target_schema = resolve_target_schema(self, df)

        # Save and switch session context
        original_db = self._context.current_database
        original_schema = self._context.current_schema
        
        needs_restore = (
            original_db != self._database_name or 
            original_schema != self._schema_name
        )
        
        if needs_restore:
            execute_sql(
                f"USE SCHEMA {self._database_name}.{self._schema_name}",
                context=self._context
            )

        try:
            if method == WriteMethod.EXPLICIT:
                self._write_explicit(df, target_schema, table_exists, json_columns)
            else:
                self._write_simple(df, target_schema, table_exists, json_columns)
        finally:
            if needs_restore and original_db and original_schema:
                execute_sql(
                    f"USE SCHEMA {original_db}.{original_schema}",
                    context=self._context
                )
            elif needs_restore and original_db:
                execute_sql(f"USE DATABASE {original_db}", context=self._context)

    def _write_simple(
        self,
        df: pd.DataFrame,
        target_schema: list['ColumnSchema'],
        table_exists: bool,
        json_columns: list[str],
    ) -> None:
        """Write using write_pandas with optional VARIANT post-conversion.
        
        This is the legacy/simple path that uses snowflake-connector's write_pandas.
        For new tables with JSON columns, it loads as STRING then converts to VARIANT.
        For appending to existing VARIANT columns, it uses a temp table approach.
        """
        from snowflake.connector.pandas_tools import write_pandas
        from snowlib.utils.json_columns import prepare_json_columns

        # Serialize JSON columns to strings for write_pandas
        df, _ = prepare_json_columns(df)
        
        # Determine which columns need VARIANT conversion
        cols_to_convert: list[str] = []
        cols_via_temp: list[str] = []
        
        if json_columns:
            if not table_exists:
                cols_to_convert = json_columns
            else:
                existing_variant_cols = self._get_variant_columns()
                for col in json_columns:
                    if col in existing_variant_cols:
                        cols_via_temp.append(col)
        
        conn = self._context.connection

        if cols_via_temp:
            # Append with VARIANT columns - use temp table route
            self._write_simple_via_temp(df, cols_via_temp, conn)
        else:
            write_pandas(
                conn=conn,
                df=df,
                table_name=self._name,
                schema=self._schema_name,
                database=self._database_name,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False,
                use_logical_type=True,
            )

            if cols_to_convert:
                self._convert_columns_to_variant(cols_to_convert)
                logger.info(f"Converted {len(cols_to_convert)} column(s) to VARIANT: {cols_to_convert}")

    def _write_simple_via_temp(
        self,
        df: pd.DataFrame,
        variant_columns: list[str],
        conn: 'Any'
    ) -> None:
        """Write data via temp table for appending to tables with VARIANT columns (simple method)"""
        from snowflake.connector.pandas_tools import write_pandas
        from snowlib.primitives import execute_sql

        random_suffix = secrets.token_hex(4).upper()
        temp_table_name = f"SNOWLIB_TMP_{random_suffix}"
        
        temp_table = Table(
            self._database_name,
            self._schema_name,
            temp_table_name,
            self._context
        )
        
        try:
            write_pandas(
                conn=conn,
                df=df,
                table_name=temp_table_name,
                schema=self._schema_name,
                database=self._database_name,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False,
                use_logical_type=True,
            )
            
            temp_table._convert_columns_to_variant(variant_columns)
            
            execute_sql(
                f"INSERT INTO {self.fqn} SELECT * FROM {temp_table.fqn}",
                context=self._context
            )
            
            logger.info(f"Appended {len(df)} row(s) with VARIANT columns via temp table")
            
        finally:
            try:
                temp_table.drop(if_exists=True)
            except Exception:
                pass

    def _write_explicit(
        self,
        df: pd.DataFrame,
        target_schema: list['ColumnSchema'],
        table_exists: bool,
        json_columns: list[str],
    ) -> None:
        """Write using stage + COPY INTO with explicit column projection.
        
        This method:
        1. Creates the table if needed (with correct VARIANT types)
        2. Writes DataFrame to a temporary Parquet file
        3. Uploads Parquet to a temporary stage
        4. Uses COPY INTO with SELECT projection to cast columns to target types
        5. Cleans up temp stage
        
        Advantages:
        - Native VARIANT support (no post-conversion needed)
        - Precise type control via explicit projection
        """
        from snowlib.primitives import execute_sql
        from snowlib.utils.schema import schema_to_ddl
        from snowlib.models import Stage
        
        # Create table if it doesn't exist
        if not table_exists:
            ddl = schema_to_ddl(target_schema)
            execute_sql(f"CREATE TABLE {self.fqn} ({ddl})", context=self._context)
            logger.info(f"Created table {self.fqn}")
            if json_columns:
                logger.info(f"Created {len(json_columns)} VARIANT column(s): {json_columns}")

        # Create temp stage
        random_suffix = secrets.token_hex(4).upper()
        temp_stage_name = f"SNOWLIB_TMP_STAGE_{random_suffix}"
        temp_stage = Stage(
            self._database_name,
            self._schema_name,
            temp_stage_name,
            self._context
        )
        
        try:
            temp_stage.create(if_not_exists=True)
            
            # Write DataFrame to temp Parquet file
            with tempfile.TemporaryDirectory() as temp_dir:
                parquet_path = Path(temp_dir) / "data.parquet"
                
                df.to_parquet(
                    parquet_path,
                    index=False,
                    compression='snappy',
                    coerce_timestamps='us',  # Required for Snowflake
                )
                
                # Upload to stage
                temp_stage.load(
                    [parquet_path],
                    auto_compress=False,  # Parquet is already compressed
                    overwrite=True,
                    show_progress=False,
                )
            
            # Build column projection for COPY INTO
            # Use lowercase for Parquet column access, target type for casting
            select_parts = []
            for col_schema in target_schema:
                col_name = col_schema.name
                col_type = col_schema.snowflake_type
                # Parquet columns are accessed via $1:column_name
                # Need to match case - parquet has uppercase from our df.columns transform
                select_parts.append(f"$1:{col_name}::{col_type} AS {col_name}")
            
            select_clause = ", ".join(select_parts)
            
            copy_sql = f"""
            COPY INTO {self.fqn}
            FROM (
                SELECT {select_clause}
                FROM {temp_stage.stage_path}
            )
            FILE_FORMAT = (TYPE = 'PARQUET' USE_LOGICAL_TYPE = TRUE)
            """
            
            result = execute_sql(copy_sql, context=self._context)
            copy_df = result.to_df()
            
            if len(copy_df) > 0:
                rows_loaded = copy_df['rows_loaded'].sum() if 'rows_loaded' in copy_df.columns else len(df)
                if json_columns:
                    logger.info(f"Loaded {rows_loaded} row(s) via COPY INTO with {len(json_columns)} VARIANT column(s): {json_columns}")
                else:
                    logger.info(f"Loaded {rows_loaded} row(s) via COPY INTO")
            
        finally:
            # Clean up temp stage
            try:
                temp_stage.drop(if_exists=True)
            except Exception:
                pass

    def _convert_columns_to_variant(self, columns: list[str]) -> None:
        """Convert STRING columns to VARIANT using PARSE_JSON via CTAS"""
        import secrets
        from snowlib.primitives import execute_sql
        from snowlib.utils.identifiers import is_valid_identifier

        # Get all columns from the table to preserve order
        all_columns = [col.name for col in self.columns]
        
        # Validate all column names first (required for AS aliases)
        for col in all_columns:
            if not is_valid_identifier(col):
                raise ValueError(f"Invalid column identifier: {col}")
        
        # Build SELECT list with PARSE_JSON for JSON columns
        # Use IDENTIFIER(%s) for source, validated string interpolation for AS alias
        select_parts = []
        bindings = []
        
        for col in all_columns:
            if col in columns:
                # Convert this column to VARIANT
                select_parts.append(f"PARSE_JSON(IDENTIFIER(%s)) AS {col}")
                bindings.append(col)
            else:
                # Keep as-is
                select_parts.append(f"IDENTIFIER(%s) AS {col}")
                bindings.append(col)
        
        select_clause = ", ".join(select_parts)
        
        # Create random temp table name (temporary tables auto-cleanup on session end)
        random_suffix = secrets.token_hex(4).upper()
        temp_table = f"SNOWLIB_TMP_{random_suffix}_{self._name}"
        if not is_valid_identifier(temp_table):
            raise ValueError(f"Invalid temporary table identifier: {temp_table}")
        
        # Use TEMPORARY TABLE so it auto-cleans on session end
        # No need to fully qualify - temp tables are session-scoped
        try:
            execute_sql(
                f"CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(%s) AS SELECT {select_clause} FROM {self.fqn}",
                context=self._context,
                bindings=[temp_table] + bindings
            )
            execute_sql(f"DROP TABLE {self.fqn}", context=self._context)
            execute_sql(
                f"CREATE TABLE {self.fqn} AS SELECT * FROM IDENTIFIER(%s)",
                context=self._context,
                bindings=[temp_table]
            )
            execute_sql(
                "DROP TABLE IDENTIFIER(%s)",
                context=self._context,
                bindings=[temp_table]
            )
        except Exception:
            # Try to clean up temp table if something went wrong
            # (though it will auto-cleanup on session end anyway)
            try:
                execute_sql(
                    "DROP TABLE IF EXISTS IDENTIFIER(%s)",
                    context=self._context,
                    bindings=[temp_table]
                )
            except Exception:
                pass  # Cleanup failure is not critical
            raise
        
        logger.info(f"Converted {len(columns)} column(s) to VARIANT: {columns}")

    def truncate(self) -> None:
        """Truncate table by removing all rows while keeping structure"""
        from snowlib.primitives import execute_sql

        execute_sql(f"TRUNCATE TABLE {self.fqn}", context=self._context)

    def insert(self, df: pd.DataFrame) -> None:
        """Insert DataFrame rows into existing table"""
        self.write(df, if_exists="append")
