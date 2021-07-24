import pytest

from scopt.instances import Instance
from scopt.optimizer import (
    ClientModeOptimizer,
    ClusterModeOptimizer,
    SparkConfOptimizer,
)


class TestClusterModeOptimizer:
    def test_properties(self) -> None:
        optimizer = ClusterModeOptimizer(Instance(32, 248), 10)
        assert optimizer.executor_cores == 5
        assert optimizer.executor_per_node == 6
        assert optimizer.total_executor_memory == 41
        assert optimizer.executor_memory == 36
        assert optimizer.executor_memory_overhead == 5
        assert optimizer.driver_cores == 5
        assert optimizer.driver_memory == 36
        assert optimizer.driver_memory_overhead == 5
        assert optimizer.executor_instances == 59
        assert optimizer.default_parallelism == 590
        assert optimizer.sql_shuffle_partitions == 590

    def test_small_cpu_instance(self) -> None:
        optimizer = ClusterModeOptimizer(Instance(4, 16), 10)
        assert optimizer.executor_cores == 3
        assert optimizer.executor_per_node == 1
        assert optimizer.total_executor_memory == 15
        assert optimizer.executor_memory == 13
        assert optimizer.executor_memory_overhead == 2
        assert optimizer.driver_cores == 3
        assert optimizer.driver_memory == 13
        assert optimizer.driver_memory_overhead == 2
        assert optimizer.executor_instances == 9
        assert optimizer.default_parallelism == 54
        assert optimizer.sql_shuffle_partitions == 54

    def test_insufficient_resource(self) -> None:
        with pytest.raises(ValueError):
            ClusterModeOptimizer(Instance(4, 16), 1)


class TestClientModeOptimizer:
    def test_properties(self) -> None:
        optimizer = ClientModeOptimizer(Instance(32, 248), 10)
        assert optimizer.executor_cores == 5
        assert optimizer.executor_per_node == 6
        assert optimizer.total_executor_memory == 41
        assert optimizer.executor_memory == 36
        assert optimizer.executor_memory_overhead == 5
        assert optimizer.driver_cores == 5
        assert optimizer.driver_memory == 36
        assert optimizer.driver_memory_overhead == 5
        assert optimizer.executor_instances == 60
        assert optimizer.default_parallelism == 600
        assert optimizer.sql_shuffle_partitions == 600

    def test_properties_specify_driver_instance(self) -> None:
        optimizer = ClientModeOptimizer(Instance(32, 248), 10, Instance(4, 16))
        assert optimizer.executor_cores == 5
        assert optimizer.executor_per_node == 6
        assert optimizer.total_executor_memory == 41
        assert optimizer.executor_memory == 36
        assert optimizer.executor_memory_overhead == 5
        assert optimizer.driver_cores == 3
        assert optimizer.driver_memory == 13
        assert optimizer.driver_memory_overhead == 2
        assert optimizer.executor_instances == 60
        assert optimizer.default_parallelism == 600
        assert optimizer.sql_shuffle_partitions == 600

    def test_properties_specify_small_driver_instance(self) -> None:
        optimizer = ClientModeOptimizer(Instance(4, 16), 10, Instance(32, 248))
        assert optimizer.executor_cores == 3
        assert optimizer.executor_per_node == 1
        assert optimizer.total_executor_memory == 15
        assert optimizer.executor_memory == 13
        assert optimizer.executor_memory_overhead == 2
        assert optimizer.driver_cores == 3
        assert optimizer.driver_memory == 13
        assert optimizer.driver_memory_overhead == 2
        assert optimizer.executor_instances == 10
        assert optimizer.default_parallelism == 60
        assert optimizer.sql_shuffle_partitions == 60


class TestSparkConfOptimizer:
    def test_cluster_mode(self) -> None:
        optimizer = SparkConfOptimizer(Instance(32, 248), 10, 'cluster')
        assert isinstance(optimizer.optimizer, ClusterModeOptimizer)

    def test_client_mode(self) -> None:
        optimizer = SparkConfOptimizer(
            Instance(32, 248), 10, 'client', Instance(32, 248)
        )
        assert isinstance(optimizer.optimizer, ClientModeOptimizer)

    def test_dynamic_allocation(self) -> None:
        optimizer = SparkConfOptimizer(
            Instance(32, 248),
            num_nodes=10,
            deploy_mode='client',
            dynamic_allocation=True,
        )
        assert optimizer.specified_num_nodes

        optimizer = SparkConfOptimizer(
            Instance(32, 248), deploy_mode='client', dynamic_allocation=True
        )
        assert not optimizer.specified_num_nodes
        assert optimizer.num_nodes == 2

        optimizer = SparkConfOptimizer(
            Instance(32, 248),
            num_nodes=10,
            deploy_mode='cluster',
            dynamic_allocation=True,
        )
        assert optimizer.specified_num_nodes

        optimizer = SparkConfOptimizer(
            Instance(32, 248), deploy_mode='cluster', dynamic_allocation=True
        )
        assert not optimizer.specified_num_nodes
        assert optimizer.num_nodes == 2

        with pytest.raises(ValueError):
            SparkConfOptimizer(
                Instance(32, 248),
                deploy_mode='client',
                dynamic_allocation=False,
            )

        with pytest.raises(ValueError):
            SparkConfOptimizer(
                Instance(32, 248),
                deploy_mode='cluster',
                dynamic_allocation=False,
            )

    def test_as_dict(self) -> None:
        optimizer = SparkConfOptimizer(Instance(32, 248), 10, 'client')
        expected = {
            'spark.driver.cores': 5,
            'spark.driver.memory': '36g',
            'spark.driver.memoryOvearhead': '5g',
            'spark.executor.cores': 5,
            'spark.executor.memory': '36g',
            'spark.executor.memoryOvearhead': '5g',
            'spark.executor.instances': 60,
            'spark.default.parallelism': 600,
            'spark.sql.shuffle.partitions': 600,
        }
        assert optimizer.as_dict() == expected

    def test_as_dict_dynamic_allocation(self) -> None:
        optimizer = SparkConfOptimizer(
            Instance(32, 248),
            10,
            deploy_mode='client',
            dynamic_allocation=True,
        )
        expected = {
            'spark.driver.cores': 5,
            'spark.driver.memory': '36g',
            'spark.driver.memoryOvearhead': '5g',
            'spark.executor.cores': 5,
            'spark.executor.memory': '36g',
            'spark.executor.memoryOvearhead': '5g',
            'spark.default.parallelism': 600,
            'spark.sql.shuffle.partitions': 600,
        }
        assert optimizer.as_dict() == expected

    def test_as_dict_dynamic_allocation_not_specify_num_nodes(self) -> None:
        optimizer = SparkConfOptimizer(
            Instance(32, 248), deploy_mode='client', dynamic_allocation=True
        )
        expected = {
            'spark.driver.cores': 5,
            'spark.driver.memory': '36g',
            'spark.driver.memoryOvearhead': '5g',
            'spark.executor.cores': 5,
            'spark.executor.memory': '36g',
            'spark.executor.memoryOvearhead': '5g',
        }
        assert optimizer.as_dict() == expected

    def test_as_list(self) -> None:
        optimizer = SparkConfOptimizer(Instance(32, 248), 10, 'client')
        expected = [
            ('spark.driver.cores', 5),
            ('spark.driver.memory', '36g'),
            ('spark.driver.memoryOvearhead', '5g'),
            ('spark.executor.cores', 5),
            ('spark.executor.memory', '36g'),
            ('spark.executor.memoryOvearhead', '5g'),
            ('spark.executor.instances', 60),
            ('spark.default.parallelism', 600),
            ('spark.sql.shuffle.partitions', 600),
        ]
        assert optimizer.as_list() == expected
