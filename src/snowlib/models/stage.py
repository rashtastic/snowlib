"""Stage object class for managing Snowflake stages"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Optional, Any, cast

from snowlib.context import SnowflakeContext
from snowlib.models.base import SchemaChild
from snowlib.primitives import execute_sql, query

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    tqdm = None  # type: ignore
    HAS_TQDM = False


@dataclass
class StageObject:
    """Represents a file or directory in a Snowflake stage"""
    
    name: str
    size: int
    md5: Optional[str]
    last_modified: datetime
    _stage: Stage
    
    @property
    def path(self) -> str:
        """Full path within the stage (e.g., @DB.SCHEMA.STAGE/path/to/file.csv)"""
        return f"@{self._stage.fqn}/{self.name}"
    
    def exists(self) -> bool:
        """Check if this object still exists in the stage"""
        df = query(f"LIST {self.path}", self._stage._context)
        return len(df) > 0
    
    def delete(self) -> dict[str, Any]:
        """Delete this object from the stage, returns the result info"""
        result = execute_sql(f"REMOVE {self.path}", self._stage._context)
        df = result.to_df()
        if len(df) > 0:
            return cast(dict[str, Any], df.iloc[0].to_dict())
        return {"status": "no result"}
    
    def __repr__(self) -> str:
        return f"StageObject({self.name!r}, size={self.size})"


class Stage(SchemaChild):
    """Represents a Snowflake internal named stage"""
    
    SHOW_PLURAL: ClassVar[str] = "STAGES"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    def __init__(
        self,
        database: str,
        schema: str,
        name: str,
        context: SnowflakeContext
    ):
        """Initialize stage object"""
        super().__init__(database, schema, name, context)
    
    @property
    def stage_path(self) -> str:
        """Stage path with @ prefix for use in SQL"""
        return f"@{self.fqn}"
    
    def create(self, if_not_exists: bool = True) -> Stage:
        """Create the stage in Snowflake"""
        sql = "CREATE STAGE"
        if if_not_exists:
            sql += " IF NOT EXISTS"
        sql += f" {self.fqn}"
        execute_sql(sql, self._context)
        return self
    
    def drop(self, if_exists: bool = False) -> None:
        """Drop the stage"""
        sql = "DROP STAGE"
        if if_exists:
            sql += " IF EXISTS"
        sql += f" {self.fqn}"
        execute_sql(sql, self._context)
    
    def list(self, pattern: Optional[str] = None) -> list[StageObject]:
        """List all objects in the stage"""
        sql = f"LIST {self.stage_path}"
        if pattern:
            sql += f" PATTERN = '{pattern}'"
        
        df = query(sql, self._context)
        
        objects = []
        for _, row in df.iterrows():
            obj = StageObject(
                name=row['name'].replace(self.stage_path + '/', ''),
                size=row['size'],
                md5=row.get('md5'),
                last_modified=row['last_modified'],
                _stage=self,
            )
            objects.append(obj)
        
        return objects
    
    @property
    def objects(self) -> list[StageObject]:
        """Get all objects in the stage"""
        return self.list()
    
    def clear(self) -> list[dict[str, Any]]:
        """Remove all files from the stage"""
        result = execute_sql(f"REMOVE {self.stage_path}", self._context)
        df = result.to_df()
        return cast(list[dict[str, Any]], df.to_dict('records')) if len(df) > 0 else []
    
    def load(
        self,
        files: list[Path],
        auto_compress: bool = True,
        overwrite: bool = False,
        show_progress: bool = True,
    ) -> list[dict[str, Any]]:
        """Upload local files to the stage with progress bar"""
        results = []
        
        iterator = tqdm(files, desc="Uploading") if (show_progress and HAS_TQDM and tqdm is not None) else files
        
        for file_path in iterator:
            if not file_path.exists():
                results.append({
                    "source": str(file_path),
                    "status": "ERROR",
                    "message": "File not found",
                })
                continue
            
            sql = f"PUT 'file://{file_path.as_posix()}' {self.stage_path}"
            sql += f" AUTO_COMPRESS = {str(auto_compress).upper()}"
            sql += f" OVERWRITE = {str(overwrite).upper()}"
            
            try:
                result = execute_sql(sql, self._context)
                df = result.to_df()
                if len(df) > 0:
                    row_dict = cast(dict[str, Any], df.iloc[0].to_dict())
                    row_dict["query_id"] = result.query_id
                    results.append(row_dict)
                else:
                    results.append({
                        "source": str(file_path),
                        "status": "UNKNOWN",
                        "query_id": result.query_id,
                    })
            except Exception as e:
                results.append({
                    "source": str(file_path),
                    "status": "ERROR",
                    "message": str(e),
                })
        
        # Print summary
        uploaded = sum(1 for r in results if r.get("status") == "UPLOADED")
        skipped = sum(1 for r in results if r.get("status") == "SKIPPED")
        failed = sum(1 for r in results if r.get("status") in ("ERROR", "UNKNOWN"))
        print(f"Upload complete: {uploaded} uploaded, {skipped} skipped, {failed} failed")
        
        return results
    
    def __repr__(self) -> str:
        return f"Stage({self.fqn!r})"
