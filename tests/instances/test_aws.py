import pytest

from scopt.instances import Instance
from scopt.instances.aws import AwsInstanceMap


class TestAwsInstanceMap:
    def test_getitem(self) -> None:
        mapping = AwsInstanceMap()
        key, value = tuple(mapping._instance_dict.items())[0]
        assert mapping[key] == value

    def test_invalid_item_key(self) -> None:
        mapping = AwsInstanceMap()
        with pytest.raises(KeyError):
            mapping['not_exist']

    def test_immutability(self) -> None:
        mapping = AwsInstanceMap()
        with pytest.raises(TypeError):
            mapping['dummy'] = Instance(1, 1)

    def test_num_support_instances(self) -> None:
        mapping = AwsInstanceMap()
        assert len(mapping._instance_dict) == 196
