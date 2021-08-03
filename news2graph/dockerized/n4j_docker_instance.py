from news2graph.dockerized.docker_instance import DockerInstance

import docker
from docker.models.containers import Container, ContainerError

import os
from typing import Dict

NodeRel = Dict[(str, str)]


class Neo4jDockerInstance(DockerInstance):
    def __init__(self):
        super()

    @classmethod
    def from_graph_folder(cls, path: str):
        # TODO: is this necessary?
        raise NotImplementedError

    def init_db_from_graph(self, import_folder_path: str, output_folder_path: str, node_rel_object: NodeRel, log_folder_path: str) -> bool:
        """Initiates neo4j database using the neo4j-admin import tool. This is 
        currently the fastest method for importing very large graphs. The container
        is only used for the import and then removes itself. Output folder contains
        the binary neo4j db which can be served by any neo4j db.

        Args:
            import_folder_path (str): Folder where the output from a corpus2graph processor are stored. Will be mounted as /import
            output_folder_path (str): Folder where neo4j will store the database files
            node_rel_object (NodeRel): Dict-like, where keys are the label and values a 2-tuple denoting 1) the type either Node or Relationship and 2) the pattern or path for the files
            log_folder_path (str): Folder for logs. Can be ommited

        Returns:
            bool: Returns true if succeeded.
        """
        try:
            os.mkdir(output_folder_path)
            os.mkdir(os.path.join(output_folder_path, 'data'))
        except:
            pass
        try:
            os.mkdir(log_folder_path)
        except:
            pass

        client = docker.from_env()
        # TODO: pin version of imag

        nodes_rel_string = ''

        for key, value in node_rel_object.items():
            # TODO: validate inputs
            n_r = '--nodes=' if value[0][0].lower() == 'n' else '--relationships='
            label = f'{key}='

            #path = f'"/import/{value[1]}" '
            path = ','.join(['/import/' + a.strip()
                             for a in value[1].split(',')])
            nodes_rel_string += n_r + label + path + ' '

       
        command = 'neo4j-admin import --delimiter="\\t" --quote="☃" --database neo4j --skip-bad-relationships=true --id-type STRING ' + nodes_rel_string
        # --nodes=Token=/import/header_dict_merged.txt,/import/dicts_and_encoded_texts/dict_merged.txt  \
        # --nodes=Article="/import/header_articles.txt,/import/dicts_and_encoded_texts/nodes_article.*"  \
        # --nodes=Author="/import/header_author_nodes.txt,/import/nodes_author.txt"  \
        # --relationships=WROTE="/import/header_author_edges.txt,/import/dicts_and_encoded_texts/edges_author.*"  \
        # --relationships=CO_OCCURS="/import/header_cooc.txt,/import/edges/.*counted.txt"   \
        # --relationships=APPEARS_IN="/import/header_word_count.txt,/import/dicts_and_encoded_texts/edges_word_count.*"'

        volumes = {
            import_folder_path: {'bind': '/import', 'mode': 'ro'},
            output_folder_path: {'bind': '/data', 'mode': 'rw'},
            log_folder_path: {'bind': '/logs'}
            # TODO: conf volume with optimized neo4j config file
        }
        try:
            client.containers.run(image='neo4j', user='1000:1000',
                                  volumes=volumes,
                                  environment=['NEO4J_AUTH=neo4j/test'],
                                  command=command)
        except ContainerError as exc:
            print(exc)
            print('\n')
            return False
        return True

    def init_db_from_data(self, data_folder: str, ports: tuple) -> Container:

        client = docker.from_env()

        volumes = {
            data_folder: {'bind': '/data', 'mode': 'rw'},
            os.path.join(data_folder, 'logs'): {'bind': '/logs', 'mode': 'rw'}
        }

        ports = {
            '7474': ports[0],
            '7687': ports[1]
        }
        
        #TODO: getuid getguid
        container = client.containers.run(image='neo4j', user='1000:1000',
                                          volumes=volumes,
                                          ports=ports,
                                          environment=['NEO4J_AUTH=neo4j/test',
                                          #enable graph data science library
                                                       'NEO4JLABS_PLUGINS=["graph-data-science"]'],
                                          detach=True)

        return container

    @staticmethod
    def start_db(container_id: str) -> Container:
        client = docker.from_env()
        try:
            container = client.containers.get(container_id=container_id)
        except:
            print('Container not found or docker host down')
            return

        container.start()
        container.reload()

        return container

    def stop_db(self, container_id: str) -> None:
        client = docker.from_env()
        try:
            container = client.containers.get(container_id=container_id)
        except:
            print('Container not found or docker host down')
            return

        container.stop()
        container.reload()
