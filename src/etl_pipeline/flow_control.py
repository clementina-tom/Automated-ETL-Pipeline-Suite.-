from dataclasses import dataclass


@dataclass
class FlowControlConfig:
    max_memory_mb: int = 512
    max_concurrent_extractions: int = 4
    queue_size: int = 100
    circuit_breaker_failures: int = 3
