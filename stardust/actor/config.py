from dataclasses import dataclass


@dataclass(frozen=True)
class SystemConfig:
    num_processes: int = 2
    port: int = 8888

    # Good enough for MVP
