from dataclasses import dataclass
from time import perf_counter
from typing import Any


@dataclass
class MiddlewareContext:
    stage: str
    payload: dict[str, Any]


class Middleware:
    def before_extract(self, context: MiddlewareContext) -> None:
        return

    def after_extract(self, context: MiddlewareContext, result: Any) -> None:
        return

    def on_error(self, context: MiddlewareContext, error: Exception) -> None:
        return


class LoggingMiddleware(Middleware):
    def before_extract(self, context: MiddlewareContext) -> None:
        context.payload["_started_at"] = perf_counter()

    def after_extract(self, context: MiddlewareContext, result: Any) -> None:
        start = context.payload.get("_started_at", perf_counter())
        context.payload["duration_ms"] = int((perf_counter() - start) * 1000)
