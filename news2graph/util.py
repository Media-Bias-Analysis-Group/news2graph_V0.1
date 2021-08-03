from itertools import islice
from multiprocessing import Pool
import networkx


def merge_weighted_edges(G: networkx.Graph, H: networkx.Graph):
    """Merges all edges shared by G and H and sums all attributes they have in common

    Args:
        G (networkx.Graph): A networkx graph
        H (networkx.Graph): A networkx Graph object

    Yields:
        [type]: A Graph containing all nodes and edges from G and H, with edges merged
    """
    for u, v, h_data in H.edges(data=True):

        attr = dict((key, value) for key, value in h_data.items())

        g_data = G.get_edge_data(u, v, {})

        # sum weight here
        shared = set(g_data) & set(h_data)

        attr.update(dict((key, attr[key] + g_data[key]) for key in shared))

        non_shared = set(g_data) - set(h_data)

        attr.update(dict((key, g_data[key]) for key in non_shared))

        yield u, v, attr


def compose_weighted_graphs(G: networkx.Graph, H: networkx.Graph) -> networkx.Graph:

    R = G.__class__()
    R.graph.update(G.graph)

    R.add_nodes_from(G.nodes(data=True))
    R.add_nodes_from(H.nodes(data=True))

    R.add_edges_from(G.edges(data=True))
    R.add_edges_from(H.edges(data=True))

    # Sum weight of all edges appearing in G and H
    shared_edges = list(merge_weighted_edges(G, H))
    R.add_edges_from(shared_edges)

    return R


def _compose_all_weighted_graphs(graphs: list) -> networkx.Graph:
    # chunk
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    
    # TODO: get from num_processes, check if list < num_processses
    num_processes = 16
    pool = Pool(num_processes)
    graphs = pool.imap(compose_all_weighted_graphs, list(
        chunks(graphs, num_processes)),  chunksize=num_processes,)
    pool.close()
    final_graph = compose_all_weighted_graphs(graphs)
    return final_graph


def compose_all_weighted_graphs(graphs: list) -> networkx.Graph:
    if not graphs:
        raise ValueError(
            'Cannot call compose_all_weighted_graphs on empty list')
    graphs = iter(graphs)
    C = next(graphs)
    if isinstance(C, str):
        C = networkx.read_gpickle(C)

    for H in graphs:
        if isinstance(H, str):
            H = networkx.read_gpickle(H)
        C = compose_weighted_graphs(C, H)
        del H
    return C


def take_from_iter(n: int, iterable: iter) -> list:
    "Return first n items of the iterable as list"
    return list(islice(iterable, n))


def get_type_string(inp) -> str:
    _type = str(type(inp))
    _type = _type[_type.find("'")+1:_type.rfind("'")].replace('str', 'string')
    if _type == 'list':
        return f'[{get_type_string(inp[0])}]'
    else:
        return _type



def merge_unique_to_single_file(path:str, endswith:str, output_file: str) -> None:
    """Generates a single file consisting of unique records from a number of files

    Args:
        path (str): Base path where files are stored
        file_pattern (str): Pattern to match files against
    """
    import os

    file_list = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(endswith)]
    lines = []
    for file in file_list:
        with open(file) as f:
            for line in f:
                lines.append(line)
    
    lines = set(lines)

    with open(output_file, 'w') as out:
        for line in lines:
            out.write(line)


def merge_edgelists(path: str, output: str) -> None:
    all_files = [file for file in os.listdir(path)]
    import pandas as pd
    df = pd.concat((pd.read_csv(f, sep = '\t', header = None, names = ['from', 'to', 'weight'], quoting = 3) for f in all_files))
    df2 = df.groupby(['from', 'to']).sum().reset_index()


def mkdir_if_not_exits(path: str) -> None:
    import os
    if not os.path.exists(path):
        os.mkdir(path)