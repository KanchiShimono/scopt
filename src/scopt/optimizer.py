import math
from enum import Enum, unique
from typing import Dict, List, Optional, Protocol, Tuple, Union

from scopt.instances import Instance


@unique
class DeployMode(Enum):
    CLIENT = 'client'
    CLUSTER = 'cluster'


class Optimizer(Protocol):
    @property
    def executor_cores(self) -> int:
        ...

    @property
    def executor_per_node(self) -> int:
        ...

    @property
    def total_executor_memory(self) -> int:
        ...

    @property
    def executor_memory(self) -> int:
        ...

    @property
    def executor_memory_overhead(self) -> int:
        ...

    @property
    def driver_cores(self) -> int:
        ...

    @property
    def driver_memory(self) -> int:
        ...

    @property
    def driver_memory_overhead(self) -> int:
        ...

    @property
    def executor_instances(self) -> int:
        ...

    @property
    def default_parallelism(self) -> int:
        ...

    @property
    def sql_shuffle_partitions(self) -> int:
        ...


class ClusterModeOptimizer:
    def __init__(
        self,
        executor_instance: Instance,
        num_nodes: int,
    ) -> None:
        self.core_per_node = executor_instance.num_cores
        self.memory_per_node = executor_instance.memory_size
        self.num_nodes = num_nodes
        self.valid()

    @property
    def executor_cores(self) -> int:
        # keep one core for hadoop daemon when core of instance less than 5
        return 5 if self.core_per_node > 5 else max(self.core_per_node - 1, 1)

    @property
    def executor_per_node(self) -> int:
        # one core for hadoop daemon
        cpe = math.floor((self.core_per_node - 1) / self.executor_cores)
        return cpe if cpe > 0 else 1

    @property
    def total_executor_memory(self) -> int:
        # 1GB for hadoop daemon
        return math.floor((self.memory_per_node - 1) / self.executor_per_node)

    @property
    def executor_memory(self) -> int:
        return math.floor(self.total_executor_memory * 0.9)

    @property
    def executor_memory_overhead(self) -> int:
        return math.ceil(self.total_executor_memory * 0.1)

    @property
    def driver_cores(self) -> int:
        return self.executor_cores

    @property
    def driver_memory(self) -> int:
        return self.executor_memory

    @property
    def driver_memory_overhead(self) -> int:
        return self.executor_memory_overhead

    @property
    def executor_instances(self) -> int:
        # one instance for driver
        executor_instances = self.executor_per_node * self.num_nodes - 1
        if executor_instances < 1:
            raise ValueError(
                'Can not reserve cpu cores for executor. '
                'You shuld scale up instance size or increase number of nodes.'
            )
        return executor_instances

    @property
    def default_parallelism(self) -> int:
        return self.executor_instances * self.executor_cores * 2

    @property
    def sql_shuffle_partitions(self) -> int:
        return self.default_parallelism

    def valid(self) -> None:
        self.executor_instances


class ClientModeOptimizer:
    def __init__(
        self,
        executor_instance: Instance,
        num_nodes: int,
        driver_instance: Optional[Instance] = None,
    ) -> None:
        self.core_per_node = executor_instance.num_cores
        self.memory_per_node = executor_instance.memory_size
        self.num_nodes = num_nodes
        self.driver_instance = (
            executor_instance if driver_instance is None else driver_instance
        )
        self.valid()

    @property
    def executor_cores(self) -> int:
        # keep one core for hadoop daemon when core of instance less than 5
        return 5 if self.core_per_node > 5 else max(self.core_per_node - 1, 1)

    @property
    def executor_per_node(self) -> int:
        # one core for hadoop daemon
        cpe = math.floor((self.core_per_node - 1) / self.executor_cores)
        return cpe if cpe > 0 else 1

    @property
    def total_executor_memory(self) -> int:
        # 1GB for hadoop daemon
        return math.floor((self.memory_per_node - 1) / self.executor_per_node)

    @property
    def executor_memory(self) -> int:
        return math.floor(self.total_executor_memory * 0.9)

    @property
    def executor_memory_overhead(self) -> int:
        return math.ceil(self.total_executor_memory * 0.1)

    @property
    def driver_cores(self) -> int:
        # one core for system resource
        driver_cores = max(self.driver_instance.num_cores - 1, 1)
        return min(driver_cores, self.executor_cores)

    @property
    def total_driver_memory(self) -> int:
        # 1GB for system resource
        return math.floor(self.driver_instance.memory_size - 1)

    @property
    def driver_memory(self) -> int:
        driver_memory = math.floor(self.total_driver_memory * 0.9)
        return min(driver_memory, self.executor_memory)

    @property
    def driver_memory_overhead(self) -> int:
        driver_memory_overhead = math.ceil(self.total_driver_memory * 0.1)
        return min(driver_memory_overhead, self.executor_memory_overhead)

    @property
    def executor_instances(self) -> int:
        return self.executor_per_node * self.num_nodes

    @property
    def default_parallelism(self) -> int:
        return self.executor_instances * self.executor_cores * 2

    @property
    def sql_shuffle_partitions(self) -> int:
        return self.default_parallelism

    def valid(self) -> None:
        pass


def get_optimizer(
    executor_instance: Instance,
    num_nodes: int,
    deploy_mode: DeployMode,
    driver_instance: Optional[Instance] = None,
) -> Optimizer:
    if deploy_mode == DeployMode.CLUSTER and driver_instance is not None:
        raise ValueError('driver_instance can be specified only client_mode')

    if deploy_mode == DeployMode.CLUSTER:
        return ClusterModeOptimizer(executor_instance, num_nodes)
    return ClientModeOptimizer(executor_instance, num_nodes, driver_instance)


class SparkConfOptimizer:
    """Caliculate class for optimized Spark properties

    Assumed instance type of driver and executor are same.

    Args:
        executor_instance (Instance): Instance for executor.
        num_nodes (int): Number of Spark cluster nodes.
        deploy_mode (str, optional): Spark deploy mode. 'client' or 'cluster'.
            Defaults to 'client'.
        driver_instance (Optional[Instance], optional): Instance for driver.
            This can be enabled only 'client' mode. If not be specified,
            executor_instance is used.
            Defaults to None.

    ```python
    from pyspark import SparkConf
    from scopt import SparkConfOptimizer
    from scopt.instances import Instance


    >>> sco = SparkConfOptimizer(Instance(32, 250), 10, 'client')
    >>> print(sco)

    spark.driver.cores: 5
    spark.driver.memory: 36g
    spark.driver.memoryOvearhead: 5g
    spark.executor.cores: 5
    spark.executor.memory: 36g
    spark.executor.memoryOvearhead: 5g
    spark.executor.instances: 60
    spark.default.parallelism: 600
    spark.sql.shuffle.partitions: 600

    >>> conf = SparkConf().setAll(sco.as_list())
    >>> print(conf.getAll())
    dict_items([
        ('spark.driver.cores', '5'),
        ('spark.driver.memory', '36g'),
        ('spark.driver.memoryOvearhead', '5g'),
        ('spark.executor.cores', '5'),
        ('spark.executor.memory', '36g'),
        ('spark.executor.memoryOvearhead', '5g'),
        ('spark.executor.instances', '60'),
        ('spark.default.parallelism', '600'),
        ('spark.sql.shuffle.partitions', '600')
    ])
    ```
    """

    def __init__(
        self,
        executor_instance: Instance,
        num_nodes: int,
        deploy_mode: str = 'client',
        driver_instance: Optional[Instance] = None,
    ) -> None:
        mode = DeployMode(deploy_mode.lower())
        self.optimizer = get_optimizer(
            executor_instance, num_nodes, mode, driver_instance
        )
        self.executor_instance = executor_instance
        self.num_nodes = num_nodes
        self.deploy_mode = mode
        self.driver_instance = driver_instance

    def __str__(self) -> str:
        return '\n'.join([f'{k}: {v}' for k, v in self.as_dict().items()])

    def __repr__(self) -> str:
        args = [
            self.executor_instance,
            self.num_nodes,
            f'\'{self.deploy_mode.value}\'',
        ]
        if self.driver_instance is not None:
            args.append(self.driver_instance)

        return (
            f'{self.__class__.__module__}.{self.__class__.__qualname__}'
            f'({", ".join(map(str, args))})'
        )

    def _repr_html_(self) -> str:
        header_style = 'style="text-align:center"'
        body_style = 'style="text-align:left"'
        data = '\n'.join(
            [
                f'<tr><td {body_style}>{k}</td><td {body_style}>{v}</td></tr>'
                for k, v in self.as_dict().items()
            ]
        )
        return f'''
            <table>
                <thead>
                    <tr>
                        <td {header_style}>Property</td>
                        <td {header_style}>Value</td>
                    </tr>
                </thead>
                <tbody>
                    {data}
                </tbody>
            </table>
        '''

    def as_dict(self) -> Dict[str, Union[int, str]]:
        return {
            'spark.driver.cores': self.optimizer.driver_cores,
            'spark.driver.memory': f'{self.optimizer.driver_memory}g',
            'spark.driver.memoryOvearhead': f'{self.optimizer.driver_memory_overhead}g',  # noqa: E501
            'spark.executor.cores': self.optimizer.executor_cores,
            'spark.executor.memory': f'{self.optimizer.executor_memory}g',
            'spark.executor.memoryOvearhead': f'{self.optimizer.executor_memory_overhead}g',  # noqa: E501
            'spark.executor.instances': self.optimizer.executor_instances,
            'spark.default.parallelism': self.optimizer.default_parallelism,
            'spark.sql.shuffle.partitions': self.optimizer.sql_shuffle_partitions,  # noqa: E501
        }

    def as_list(self) -> List[Tuple[str, Union[int, str]]]:
        """Return list of tuple of Spark property

        This function is useful for setting properties to SparkConf at once by
        setAll method.

        Returns:
            List[Tuple[str, Union[int, str]]]: List of tuple of Spark property
        """

        return [(k, v) for k, v in self.as_dict().items()]
