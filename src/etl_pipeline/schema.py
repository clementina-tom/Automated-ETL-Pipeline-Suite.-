from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import json

import pandas as pd

from .exceptions import SchemaEvolutionError


class EvolutionStrategy(str, Enum):
    STRICT = "strict"
    ADDITIVE = "additive"
    IGNORE = "ignore"


@dataclass
class SchemaDef:
    name: str
    columns: dict[str, str]
    nullable: dict[str, bool] = field(default_factory=dict)


class SchemaRegistry:
    def __init__(self) -> None:
        self.schemas: dict[str, SchemaDef] = {}

    def infer_schema(self, name: str, df: pd.DataFrame) -> SchemaDef:
        columns = {col: str(dtype) for col, dtype in df.dtypes.items()}
        nullable = {col: bool(df[col].isna().any()) for col in df.columns}
        schema = SchemaDef(name=name, columns=columns, nullable=nullable)
        self.schemas[name] = schema
        return schema

    def validate(
        self,
        name: str,
        df: pd.DataFrame,
        strategy: EvolutionStrategy = EvolutionStrategy.STRICT,
    ) -> None:
        current = self.infer_schema(name, df)
        baseline = self.schemas.get(name)
        if baseline is None:
            self.schemas[name] = current
            return
        added = set(current.columns) - set(baseline.columns)
        removed = set(baseline.columns) - set(current.columns)
        if strategy == EvolutionStrategy.STRICT and (added or removed):
            raise SchemaEvolutionError(f"Schema changed for {name}: added={added}, removed={removed}")
        if strategy == EvolutionStrategy.ADDITIVE and removed:
            raise SchemaEvolutionError(f"Schema removed columns for {name}: {removed}")

    def save_json(self, path: Path) -> None:
        payload = {
            name: {"columns": schema.columns, "nullable": schema.nullable}
            for name, schema in self.schemas.items()
        }
        path.write_text(json.dumps(payload, indent=2))
