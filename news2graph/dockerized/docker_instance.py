import docker
from docker.models.containers import Container, ContainerError

from typing import List

# make type annotations prettier
ContainerList = List[Container]
ContainerStatus = List[dict]

class DockerInstance(object):
    """Abstract for creating dockerized graph databse instance. Also provides 
    more generic utility function for general docker housekeeping.
    """
    def __init__(self):
        self.docker_client = docker.from_env()
        #self.docker_container = self.client.cotainers.get('wherever')

    def list_containers(self, all: bool = False) -> ContainerList:
        return self.docker_client.containers.list(all = all)

    def get_container_overview(self, in_name:str = '') -> ContainerStatus:
        containers = self.list_containers(all = True)
        return [
            {
                'name': container.name,
                'memory_usage': dict(container.stats(stream=False)['memory_stats']),
                'image': str(container.image.tags[0]),
                'status': container.status,

            } for container in containers if in_name in str(container.image)
        ]


    def start_db(self, container_id: str) -> Container:
        raise NotImplementedError

    def stop_db(self, container_id: str) -> bool:
        raise NotImplementedError

    def run_db(self, **kwargs) -> Container:
        raise NotImplementedError

    