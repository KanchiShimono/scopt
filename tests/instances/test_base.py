import pytest

from scopt.instances import Instance


class TestInstance:
    def test_instance(self) -> None:
        instance = Instance(4, 32)
        assert instance.num_cores == 4
        assert instance.memory_size == 32

    def test_default_instance(self) -> None:
        instance = Instance()
        assert instance.num_cores == 1
        assert instance.memory_size == 1.0

    def test_insufficient_cpu(self) -> None:
        with pytest.raises(ValueError):
            Instance(0, 32)

    def test_insufficient_memory(self) -> None:
        with pytest.raises(ValueError):
            Instance(4, 0)
