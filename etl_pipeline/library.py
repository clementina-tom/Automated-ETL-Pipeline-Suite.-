"""Library-first API for embedding the ETL pipeline into other applications."""

from dataclasses import dataclass, field

import pandas as pd

from extractor.base_extractor import BaseExtractor
from loader.base_loader import BaseLoader
from pipeline import PipelineConfig, run_pipeline_sync


@dataclass
class ETLPipeline:
    """A reusable pipeline object suitable for library consumers."""

    config: PipelineConfig = field(default_factory=PipelineConfig)
    beneficiaries_extractor: BaseExtractor | None = None
    gifts_extractor: BaseExtractor | None = None
    loaders: list[BaseLoader] | None = None

    def run(self) -> pd.DataFrame:
        """Execute the configured ETL pipeline and return the master DataFrame."""
        return run_pipeline_sync(
            config_obj=self.config,
            beneficiaries_extractor=self.beneficiaries_extractor,
            gifts_extractor=self.gifts_extractor,
            loaders=self.loaders,
        )


class ETLPipelineBuilder:
    """Fluent builder for assembling ETLPipeline instances."""

    def __init__(self) -> None:
        self._config = PipelineConfig()
        self._beneficiaries_extractor: BaseExtractor | None = None
        self._gifts_extractor: BaseExtractor | None = None
        self._loaders: list[BaseLoader] | None = None

    def with_source_mode(self, mode: str) -> "ETLPipelineBuilder":
        self._config.source_mode = mode
        return self

    def with_beneficiaries_extractor(self, extractor: BaseExtractor) -> "ETLPipelineBuilder":
        self._beneficiaries_extractor = extractor
        return self

    def with_gifts_extractor(self, extractor: BaseExtractor) -> "ETLPipelineBuilder":
        self._gifts_extractor = extractor
        return self

    def with_loaders(self, loaders: list[BaseLoader]) -> "ETLPipelineBuilder":
        self._loaders = loaders
        return self

    def build(self) -> ETLPipeline:
        return ETLPipeline(
            config=self._config,
            beneficiaries_extractor=self._beneficiaries_extractor,
            gifts_extractor=self._gifts_extractor,
            loaders=self._loaders,
        )


def run_etl(
    config: PipelineConfig | None = None,
    beneficiaries_extractor: BaseExtractor | None = None,
    gifts_extractor: BaseExtractor | None = None,
    loaders: list[BaseLoader] | None = None,
) -> pd.DataFrame:
    """Convenience function for one-shot execution in library consumers."""
    pipeline = ETLPipeline(
        config=config or PipelineConfig(),
        beneficiaries_extractor=beneficiaries_extractor,
        gifts_extractor=gifts_extractor,
        loaders=loaders,
    )
    return pipeline.run()
