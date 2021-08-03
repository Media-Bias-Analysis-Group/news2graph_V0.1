import unittest
import docker

from news2graph.dockerized.docker_instance import DockerInstance

class TestDockerInstance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docker_client = docker.from_env()
        # Run docker in docker, to provide reproducible state for testing        
        #self.dind = self.docker_client.containers.run('docker:dind', detach = True, privileged = True)
    
    def test_list_containers(self):
        docker = DockerInstance()
        overview = docker.get_container_overview('neo4j')
        print(overview)