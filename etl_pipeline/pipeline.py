from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from .base import BaseExtractor, BaseLoader, BaseTransformer
from .checkpoint import Checkpoint
from .hooks import NoopHook, PipelineHook
from .lineage import LineageContext
from .middleware import Middleware, MiddlewareContext
from .plugins import PluginRegistry
from .results import PipelineResult, StageResult
from .schema import EvolutionStrategy, SchemaRegistry


@dataclass
class JoinSpec:
    left: str
    right: str
    left_on: str
    right_on: str
    how: str = "left"


@dataclass
class Pipeline:
    extractors: dict[str, BaseExtractor]
    transformers: list[BaseTransformer] = field(default_factory=list)
    loaders: list[BaseLoader] = field(default_factory=list)
    joins: list[JoinSpec] = field(default_factory=list)
    hooks: list[PipelineHook] = field(default_factory=list)
    middleware: list[Middleware] = field(default_factory=list)
    schema_registry: SchemaRegistry | None = None
    schema_strategy: EvolutionStrategy = EvolutionStrategy.IGNORE
    checkpoint: Checkpoint | None = None

    @classmethod
    def from_dict(cls, config: dict[str, Any], registry: PluginRegistry) -> "Pipeline":
        extractors = {
            src["name"]: registry.extractors[src["type"]](name=src["name"], **src.get("params", {}))
            for src in config.get("sources", [])
        }
        transforms = [
            registry.transformers[item["type"]](**item.get("params", {}))
            for item in config.get("transforms", [])
        ]
        loaders = [
            registry.loaders[item["type"]](**item.get("params", {}))
            for item in config.get("sinks", [])
        ]
        joins = [JoinSpec(**j) for j in config.get("joins", [])]
        return cls(extractors=extractors, transformers=transforms, loaders=loaders, joins=joins)

    @classmethod
    def from_yaml(cls, path: Path, registry: PluginRegistry) -> "Pipeline":
        text = path.read_text()
        if path.suffix.lower() == ".json":
            payload = json.loads(text)
        else:
            import yaml  # type: ignore

            payload = yaml.safe_load(text)
        return cls.from_dict(payload, registry)

    def with_middleware(self, middleware: Middleware) -> "Pipeline":
        self.middleware.append(middleware)
        return self

    def _hooks(self) -> list[PipelineHook]:
        return self.hooks or [NoopHook()]

    def _run_transformers(self, df: pd.DataFrame, lineage: LineageContext) -> pd.DataFrame:
        out = df
        for transformer in self.transformers:
            before = len(out)
            out = transformer.transform(out)
            after = len(out)
            lineage.add_step(f"transform:{transformer.__class__.__name__} rows {before}->{after}")
            for hook in self._hooks():
                hook.on_transform(transformer.__class__.__name__, before, after)
        return out

    def _execute_joins(self, entities: dict[str, pd.DataFrame], lineage: LineageContext) -> pd.DataFrame:
        if not self.joins:
            return next(iter(entities.values()))
        merged = entities[self.joins[0].left]
        for join in self.joins:
            merged = merged.merge(
                entities[join.right],
                left_on=join.left_on,
                right_on=join.right_on,
                how=join.how,
            )
            lineage.add_step(f"join:{join.left}->{join.right} ({join.how})")
        return merged

    def run(self) -> PipelineResult:
        run_id = str(uuid4())
        lineage = LineageContext(run_id=run_id)
        stage_results: list[StageResult] = []
        errors: list[str] = []

        entities: dict[str, pd.DataFrame] = {}
        for name, extractor in self.extractors.items():
            ctx = MiddlewareContext(stage="extract", payload={"source": name})
            for mw in self.middleware:
                mw.before_extract(ctx)
            for hook in self._hooks():
                hook.on_extract_start(name)

            result = extractor.run_result()
            if not result.success:
                errors.extend(result.errors)
                stage_results.append(StageResult(name=f"extract:{name}", success=False, errors=result.errors))
                for hook in self._hooks():
                    hook.on_validate_fail(result.errors)
                for mw in self.middleware:
                    mw.on_error(ctx, Exception("; ".join(result.errors)))
                return PipelineResult(success=False, data=None, stage_results=stage_results, errors=errors)

            df = result.data
            if self.schema_registry is not None:
                self.schema_registry.validate(name, df, self.schema_strategy)

            for hook in self._hooks():
                hook.on_extract_end(name, len(df))
            for mw in self.middleware:
                mw.after_extract(ctx, result)

            entities[name] = df
            stage_results.append(StageResult(name=f"extract:{name}", success=True, metadata={"rows": len(df)}))

        merged = self._execute_joins(entities, lineage)
        transformed = self._run_transformers(merged, lineage)

        if self.checkpoint is not None:
            self.checkpoint.save("post_transform", transformed)

        for loader in self.loaders:
            loader.load(transformed)
            stage_results.append(StageResult(name=f"load:{loader.__class__.__name__}", success=True))
            for hook in self._hooks():
                hook.on_load_end(loader.__class__.__name__, len(transformed))

        metrics = {"rows_output": len(transformed), "lineage_steps": len(lineage.steps)}
        return PipelineResult(success=True, data=transformed, stage_results=stage_results, metrics=metrics)

    async def arun(self) -> PipelineResult:
        run_id = str(uuid4())
        lineage = LineageContext(run_id=run_id)

        async def _extract(name: str, extractor: BaseExtractor) -> tuple[str, pd.DataFrame]:
            for hook in self._hooks():
                hook.on_extract_start(name)
            df = await extractor.aextract()
            for hook in self._hooks():
                hook.on_extract_end(name, len(df))
            return name, df

        pairs = await asyncio.gather(*[_extract(name, ex) for name, ex in self.extractors.items()])
        entities = {name: df for name, df in pairs}

        merged = self._execute_joins(entities, lineage)
        out = merged
        for t in self.transformers:
            out = await t.atransform(out)

        for l in self.loaders:
            await l.aload(out)

        return PipelineResult(success=True, data=out, metrics={"rows_output": len(out)})
