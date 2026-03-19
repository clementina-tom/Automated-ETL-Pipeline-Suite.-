from contextlib import contextmanager


@contextmanager
def span(name: str):
    """No-op span context (OpenTelemetry pluggable point)."""
    yield
