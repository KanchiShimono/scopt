import math
from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag


@dataclass
class InstanceMemoryInfo:
    name: str
    memory: int


@dataclass
class InstanceInfo:
    name: str
    core: int
    memory: int

    def as_dict_element(self) -> str:
        return f'\'{self.name}\': Instance({self.core}, {self.memory})'  # noqa: E501


def scrape(core: BeautifulSoup, memory: BeautifulSoup) -> List[InstanceInfo]:
    def parse_core(memory_info: InstanceMemoryInfo) -> Optional[InstanceInfo]:
        name = memory_info.name
        memory = memory_info.memory
        for row in core.find_all('tr'):
            if name in str(row):
                num_cores = int(row.find_all('td')[1].text)
                return InstanceInfo(name, num_cores, memory)
        return None

    memory_tables = memory.find_all('div', attrs={'class': 'table-container'})
    memory_info_list = [parse_memory_table(t) for t in memory_tables]
    instance_info_list = [parse_core(m) for m in memory_info_list]
    return [i for i in instance_info_list if i is not None]


def parse_memory_table(table: Tag) -> InstanceMemoryInfo:
    name = table.find('div', attrs={'class': 'title'}).text
    memory_row = extract_memory_row(table)
    if memory_row is None:
        raise AttributeError(
            'Can not find `yarn.nodemanager.resource.memory-mb`'
        )
    memory_mb = extract_memory_mb(memory_row)
    memory_gb = math.floor(memory_mb / 1024)
    return InstanceMemoryInfo(name, memory_gb)


def extract_memory_row(table: Tag) -> Optional[Tag]:
    for row in table.find_all('tr'):
        if 'yarn.nodemanager.resource.memory-mb' in str(row):
            return row
    return None


def extract_memory_mb(row: Tag) -> int:
    memory_mb = row.find_all('td')[1].text
    return int(memory_mb)


def main() -> None:
    r = requests.get(
        'https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html'  # noqa: E501
    )
    memory_soup = BeautifulSoup(r.text, 'html.parser')
    r = requests.get('https://aws.amazon.com/ec2/instance-types/')
    core_soup = BeautifulSoup(r.text, 'html.parser')
    instance_info_list = scrape(core_soup, memory_soup)

    print('{')
    for info in instance_info_list:
        if info:
            print(f'{info.as_dict_element()},')
    print('}')


if __name__ == '__main__':
    main()
