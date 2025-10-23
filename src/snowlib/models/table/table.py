"""Table class with write operations"""

from typing import ClassVar
import pandas as pd

from snowlib.models.table.base import TableLike


class Table(TableLike):
    """Represents a Snowflake table with full read/write capabilities"""

    SHOW_PLURAL: ClassVar[str] = "TABLES"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"

    def write(self, df: pd.DataFrame, if_exists: str = "fail") -> None:
        """Write DataFrame to table with optional creation, replacement, or appending"""
        from snowflake.connector.pandas_tools import write_pandas

        if if_exists not in ("fail", "replace", "append"):
            raise ValueError(
                f"if_exists must be 'fail', 'replace', or 'append', got: {if_exists}"
            )

        df = df.copy()
        df.columns = [col.upper() for col in df.columns]

        if if_exists == "replace" and self.exists():
            self.drop()

        conn = self._context.connection

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

    def truncate(self) -> None:
        """Truncate table by removing all rows while keeping structure"""
        from snowlib.primitives import execute_sql

        execute_sql(f"TRUNCATE TABLE {self.fqn}", context=self._context)

    def insert(self, df: pd.DataFrame) -> None:
        """Insert DataFrame rows into existing table"""
        self.write(df, if_exists="append")
