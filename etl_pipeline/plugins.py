from typing import Any

from .base import BaseExtractor, BaseTransformer, BaseLoader


class PluginRegistry:
    def __init__(self) -> None:
        self.extractors: dict[str, type[BaseExtractor]] = {}
        self.transformers: dict[str, type[BaseTransformer]] = {}
        self.loaders: dict[str, type[BaseLoader]] = {}

    def register_extractor(self, name: str, cls: type[BaseExtractor]) -> None:
        self.extractors[name] = cls

    def register_transformer(self, name: str, cls: type[BaseTransformer]) -> None:
        self.transformers[name] = cls

    def register_loader(self, name: str, cls: type[BaseLoader]) -> None:
        self.loaders[name] = cls

    def build_extractor(self, cfg: dict[str, Any]) -> BaseExtractor:
        type_name = cfg["type"]
        kwargs = cfg.get("params", {})
        return self.extractors[type_name](**kwargs)
