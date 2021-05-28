from dataclasses import dataclass


@dataclass(frozen=True)
class Instance:
    """Instance infomation used for Spark cluster nodes

    Args:
        num_cores (int, optional): Number of CPU cores. Defaults to 5.
        memory_size (float, optional): Memory size GB. Defaults to 1.0.
    """

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
