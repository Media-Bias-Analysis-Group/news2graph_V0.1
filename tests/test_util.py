import unittest
from news2graph import util

import networkx


class TestUtil(unittest.TestCase):

    def test_compose_weighted_graphs(self):
        G = networkx.Graph(
            [('a', 'b', {'weight': 5}), ('b', 'c', {'weight': 3}), ('g', 'h', {'weight': 6})])
        H = networkx.Graph([('a', 'b', {'weight': 10}), ('b', 'c', {
                           'weight': 6}), ('e', 'f', {'weight': 7})])

        M = util.compose_weighted_graphs(G, H)

        nodes_expected = list(set(list(G.nodes()) + list(H.nodes())))
        nodes_expected.sort()

        nodes_actual = list(M.nodes())
        nodes_actual.sort()
        # assert all nodes are there
        self.assertListEqual(nodes_expected, nodes_actual)

        # check that edges are actually summed up
        print(f'\n{M.edges(data=True)}\n')
        self.assertEqual(
            G.edges['a', 'b']['weight'] + H.edges['a', 'b']['weight'],
            M.edges['a', 'b']['weight']
        )

    def test_compose_all_weighted_graphs(self):
        G = networkx.Graph(
            [('a', 'b', {'weight': 5}), ('b', 'c', {'weight': 3}), ('g', 'h', {'weight': 6})])
        H = networkx.Graph([('a', 'b', {'weight': 10}), ('b', 'c', {
                           'weight': 6}), ('e', 'f', {'weight': 7})])
        I = networkx.Graph(
            [('x', 'y', {'weight': 7}), ('b', 'c', {'weight': 9})])
        M = util.compose_all_weighted_graphs([G, H, I])

        nodes_expected = list(
            set(list(G.nodes()) + list(H.nodes()) + list(I.nodes())))
        nodes_expected.sort()
        nodes_actual = list(M.nodes())
        nodes_actual.sort()
        # assert all nodes are there
        self.assertListEqual(nodes_expected, nodes_actual)

        print(f'\n{M.edges(data=True)}\n')

        self.assertEqual(
            G.edges['b', 'c']['weight'] + H.edges['b',
                'c']['weight'] + I.edges['b', 'c']['weight'],
            M.edges['b', 'c']['weight']
        )

    def test_reverse(self):
        G = networkx.Graph(
            [('a', 'b', {'weight': 5})])
        H = networkx.Graph([('b', 'a', {'weight': 10}), ('b', 'c', {
                           'weight': 6})])
        M = util.compose_all_weighted_graphs([G, H])
        print(f'\n{M.edges(data=True)}\n')

    def test_merge_files(self):
        util.merge_unique_to_single_file('/home/jakob/dev/test4/ne/', '.txt', '/home/jakob/dev/test4/ne_merged.txt')
