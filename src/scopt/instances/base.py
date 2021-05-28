from dataclasses import dataclass


@dataclass(frozen=True)
class Instance:
    num_cores: int = 1
    memory_size: float = 1.0

    def __post_init__(self) -> None:
        if self.num_cores < 1:
            raise ValueError(
                f'num_cores must be more than 1, but actually {self.num_cores}'
            )
        if not self.memory_size > 0.0:
            raise ValueError(
                'memory_size must be more than 0, '
                f'but actually {self.memory_size}'
            )
