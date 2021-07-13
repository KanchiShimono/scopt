import pytest

from scopt.instances import Instance
from scopt.instances.aws import AwsInstanceMapping


class TestAwsInstanceMapping:
    def test_getitem(self) -> None:
        mapping = AwsInstanceMapping()
        key, value = tuple(mapping._instance_dict.items())[0]
        assert mapping[key] == value

    def test_invalid_item_key(self) -> None:
        mapping = AwsInstanceMapping()
        with pytest.raises(KeyError):
            mapping['not_exist']

    def test_immutability(self) -> None:
        mapping = AwsInstanceMapping()
        with pytest.raises(TypeError):
            mapping['dummy'] = Instance(1, 1)

    def test_num_support_instances(self) -> None:
        mapping = AwsInstanceMapping()
        assert len(mapping._instance_dict) == 196
