"""Table class with write operations"""

import logging
import secrets
from typing import Any, ClassVar
import pandas as pd

from snowlib.models.table.base import TableLike

logger = logging.getLogger(__name__)


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

    def write(self, df: pd.DataFrame, if_exists: str = "fail") -> None:
        """Write DataFrame to table with optional creation, replacement, or appending

        Columns containing dicts or lists are automatically detected, serialized
        to JSON strings, and converted to VARIANT after loading.
        
        For append operations:
        - If target column is VARIANT: data is written to temp table, converted, then inserted
        - If target column is NOT VARIANT: JSON conversion is skipped for that column
        
        Note: This temporarily sets the session database and schema to match the table,
        then restores the original session state after writing.
        """
        from snowflake.connector.pandas_tools import write_pandas
        from snowlib.utils.json_columns import prepare_json_columns
        from snowlib.primitives import execute_sql
        from snowlib.utils.identifiers import is_valid_identifier

        if if_exists not in ("fail", "replace", "append"):
            raise ValueError(
                f"if_exists must be 'fail', 'replace', or 'append', got: {if_exists}"
            )

        df = df.copy()
        df.columns = [col.upper() for col in df.columns]

        # Detect JSON-eligible columns in the DataFrame
        df, json_columns = prepare_json_columns(df)
        
        # Check if table exists and handle append logic
        table_exists = self.exists()
        
        if if_exists == "replace" and table_exists:
            self.drop()
            table_exists = False
        
        # For append: filter JSON columns based on existing VARIANT columns
        cols_to_convert: list[str] = []
        cols_via_temp: list[str] = []  # Existing VARIANT cols need temp table route
        
        if json_columns:
            if not table_exists:
                # Table will be created - all JSON cols become VARIANT
                cols_to_convert = json_columns
            else:
                # Table exists - check existing column types
                existing_variant_cols = self._get_variant_columns()
                
                for col in json_columns:
                    if col in existing_variant_cols:
                        # Target is VARIANT - need temp table route
                        cols_via_temp.append(col)
                    # else: target is not VARIANT, skip conversion (let write_pandas handle it)
            
            if cols_to_convert or cols_via_temp:
                logger.info(f"Detected JSON columns: {json_columns}")

        # Save current session context
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
            conn = self._context.connection

            if cols_via_temp:
                # Append to existing table with VARIANT columns - use temp table route
                self._write_via_temp_table(df, cols_via_temp, conn)
            else:
                # Normal write path
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

                # Convert JSON string columns to VARIANT (only for new tables)
                if cols_to_convert:
                    self._convert_columns_to_variant(cols_to_convert)
                    print(f"Loaded {len(cols_to_convert)} column(s) as VARIANT: {', '.join(cols_to_convert)}")
        
        finally:
            if needs_restore and original_db and original_schema:
                execute_sql(
                    f"USE SCHEMA {original_db}.{original_schema}",
                    context=self._context
                )
            elif needs_restore and original_db:
                execute_sql(f"USE DATABASE {original_db}", context=self._context)

    def _write_via_temp_table(
        self,
        df: pd.DataFrame,
        variant_columns: list[str],
        conn: 'Any'
    ) -> None:
        """Write data via temp table for appending to tables with VARIANT columns"""
        from snowflake.connector.pandas_tools import write_pandas
        from snowlib.primitives import execute_sql

        # Create a random temp table name
        random_suffix = secrets.token_hex(4).upper()
        temp_table_name = f"SNOWLIB_TMP_{random_suffix}"
        
        # Create temp Table object for the temp table (same schema as target)
        temp_table = Table(
            self._database_name,
            self._schema_name,
            temp_table_name,
            self._context
        )
        
        try:
            # Write to temp table (this will convert JSON cols to VARIANT)
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
            
            # Convert the temp table's JSON columns to VARIANT
            temp_table._convert_columns_to_variant(variant_columns)
            
            # Insert from temp table into target table
            execute_sql(
                f"INSERT INTO {self.fqn} SELECT * FROM {temp_table.fqn}",
                context=self._context
            )
            
            print(f"Appended {len(df)} row(s) with {len(variant_columns)} VARIANT column(s): {', '.join(variant_columns)}")
            
        finally:
            # Clean up temp table
            try:
                temp_table.drop(if_exists=True)
            except Exception:
                pass  # Cleanup failure is not critical

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
