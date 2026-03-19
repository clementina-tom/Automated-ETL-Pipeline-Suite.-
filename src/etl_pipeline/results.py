from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class ExtractionResult:
    data: pd.DataFrame
    success: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class StageResult:
    name: str
    success: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    success: bool
    data: pd.DataFrame | None
    stage_results: list[StageResult] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
