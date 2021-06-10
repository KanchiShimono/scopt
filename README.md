# scopt

[![Unit test](https://github.com/KanchiShimono/scopt/actions/workflows/test.yml/badge.svg)](https://github.com/KanchiShimono/scopt/actions/workflows/test.yml)
[![Build Release draft](https://github.com/KanchiShimono/scopt/actions/workflows/release-drafter.yaml/badge.svg)](https://github.com/KanchiShimono/scopt/actions/workflows/release-drafter.yaml)

Spark Config Optimizer calculate optimal cpu cores and memory values for Spark executor and driver.

## Installing

Install scopt from PyPI via pip.

```sh
pip install scopt
```

## Usage

### Basic

```python
from scopt import SparkConfOptimizer
from scopt.instances import Instance

executor_instance = Instance(32, 250)
num_nodes = 10
deploy_mode = 'client'

sco = SparkConfOptimizer(executor_instance, num_nodes, deploy_mode)
print(sco)

# spark.driver.cores: 5
# spark.driver.memory: 36
# spark.driver.memoryOvearhead: 5
# spark.executor.cores: 5
# spark.executor.memory: 36
# spark.executor.memoryOvearhead: 5
# spark.executor.instances: 60
# spark.default.parallelism: 600
# spark.sql.shuffle.partitions: 600
```

Cluster mode is also supported.

```python
deploy_mode = 'cluster'

sco = SparkConfOptimizer(executor_instance, num_nodes, deploy_mode)
print(sco)

# spark.driver.cores: 5
# spark.driver.memory: 36
# spark.driver.memoryOvearhead: 5
# spark.executor.cores: 5
# spark.executor.memory: 36
# spark.executor.memoryOvearhead: 5
# spark.executor.instances: 59
# spark.default.parallelism: 590
# spark.sql.shuffle.partitions: 590
```

Different instance type for driver node is also supported.
Specifying driver instance is enabled only `client mode`.

```python
executor_instance = Instance(32, 250)
driver_instance = Instance(4, 30)
deploy_mode = 'client'

sco = SparkConfOptimizer(
    executor_instance,
    num_nodes,
    deploy_mode,
    driver_instance,
)
print(sco)

# spark.driver.cores: 3
# spark.driver.memory: 26
# spark.driver.memoryOvearhead: 3
# spark.executor.cores: 5
# spark.executor.memory: 36
# spark.executor.memoryOvearhead: 5
# spark.executor.instances: 60
# spark.default.parallelism: 600
# spark.sql.shuffle.partitions: 600
```

### Set properties to SparkConf

You can set properties to SparkConf directory via `as_list` method.

```python
from pyspark import SparkConf
from scopt import SparkConfOptimizer
from scopt.instances import Instance

executor_instance = Instance(32, 250)
num_nodes = 10
deploy_mode = 'client'

sco = SparkConfOptimizer(executor_instance, num_nodes, deploy_mode)

conf = SparkConf()
print(conf.getAll())
# Property has not be set yet.
# dict_items([])

conf.setAll(sco.as_list())
# dict_items([
#     ('spark.driver.cores', '5'),
#     ('spark.driver.memory', '36'),
#     ('spark.driver.memoryOvearhead', '5'),
#     ('spark.executor.cores', '5'),
#     ('spark.executor.memory', '36'),
#     ('spark.executor.memoryOvearhead', '5'),
#     ('spark.executor.instances', '60'),
#     ('spark.default.parallelism', '600'),
#     ('spark.sql.shuffle.partitions', '600')
# ])
```

## Reference

- [Best practices for successfully managing memory for Apache Spark applications on Amazon EMR](https://aws.amazon.com/jp/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/)
