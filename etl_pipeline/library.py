from dataclasses import dataclass, field

from .base import BaseExtractor, BaseLoader, BaseTransformer
from .hooks import PipelineHook
from .pipeline import JoinSpec, Pipeline


@dataclass
class PipelineConfig:
    source_mode: str = "custom"


@dataclass
class ETLPipeline:
    extractors: dict[str, BaseExtractor]
    transformers: list[BaseTransformer] = field(default_factory=list)
    loaders: list[BaseLoader] = field(default_factory=list)
    joins: list[JoinSpec] = field(default_factory=list)
    hooks: list[PipelineHook] = field(default_factory=list)

    def run_with_result(self):
        return Pipeline(
            extractors=self.extractors,
            transformers=self.transformers,
            loaders=self.loaders,
            joins=self.joins,
            hooks=self.hooks,
        ).run()

    def run(self):
        return self.run_with_result().data


class ETLPipelineBuilder:
    def __init__(self) -> None:
        self._extractors: dict[str, BaseExtractor] = {}
        self._transformers: list[BaseTransformer] = []
        self._loaders: list[BaseLoader] = []
        self._joins: list[JoinSpec] = []
        self._hooks: list[PipelineHook] = []

    def add_extractor(self, name: str, extractor: BaseExtractor) -> "ETLPipelineBuilder":
        self._extractors[name] = extractor
        return self

    def add_transformer(self, transformer: BaseTransformer) -> "ETLPipelineBuilder":
        self._transformers.append(transformer)
        return self

    def add_loader(self, loader: BaseLoader) -> "ETLPipelineBuilder":
        self._loaders.append(loader)
        return self

    def add_join(self, join: JoinSpec) -> "ETLPipelineBuilder":
        self._joins.append(join)
        return self

    def add_hook(self, hook: PipelineHook) -> "ETLPipelineBuilder":
        self._hooks.append(hook)
        return self

    def with_beneficiaries_extractor(self, extractor: BaseExtractor) -> "ETLPipelineBuilder":
        return self.add_extractor("beneficiaries", extractor)

    def with_gifts_extractor(self, extractor: BaseExtractor) -> "ETLPipelineBuilder":
        return self.add_extractor("gifts", extractor)

    def with_source_mode(self, mode: str) -> "ETLPipelineBuilder":
        _ = mode
        return self

    def build(self) -> ETLPipeline:
        joins = self._joins
        if not joins and {"beneficiaries", "gifts"}.issubset(self._extractors.keys()):
            joins = [JoinSpec(left="beneficiaries", right="gifts", left_on="beneficiary_id", right_on="beneficiary_id")]
        return ETLPipeline(
            extractors=self._extractors,
            transformers=self._transformers,
            loaders=self._loaders,
            joins=joins,
            hooks=self._hooks,
        )


def run_etl(
    extractors: dict[str, BaseExtractor] | None = None,
    transformers: list[BaseTransformer] | None = None,
    loaders: list[BaseLoader] | None = None,
    joins: list[JoinSpec] | None = None,
    config: PipelineConfig | None = None,
    beneficiaries_extractor: BaseExtractor | None = None,
    gifts_extractor: BaseExtractor | None = None,
):
    _ = config
    active_extractors = extractors or {}
    if beneficiaries_extractor is not None:
        active_extractors["beneficiaries"] = beneficiaries_extractor
    if gifts_extractor is not None:
        active_extractors["gifts"] = gifts_extractor

    active_joins = joins
    if active_joins is None and {"beneficiaries", "gifts"}.issubset(active_extractors.keys()):
        active_joins = [
            JoinSpec(left="beneficiaries", right="gifts", left_on="beneficiary_id", right_on="beneficiary_id")
        ]

    return ETLPipeline(
        extractors=active_extractors,
        transformers=transformers or [],
        loaders=loaders or [],
        joins=active_joins or [],
    ).run()
