# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/graphs/directed_graph.html
# Copyright (c) 2005-2025 SQLAlchemy authors
# Licensed under the MIT License.
#
# Modified for SQLAlchemy-Mimer:
#   - Converted to unittest-style test case
#   - Updated database URI to use "mimer://" dialect
#   - Adjusted for compatibility with Mimer SQL syntax and behavior
#
# Copyright (c) 2025 Mimer Information Technology
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# See LICENSE for more details.
"""a directed graph example."""

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Node(Base):
    __tablename__ = "node"

    node_id = Column(Integer, primary_key=True)

    def higher_neighbors(self):
        return [x.higher_node for x in self.lower_edges]

    def lower_neighbors(self):
        return [x.lower_node for x in self.higher_edges]


class Edge(Base):
    __tablename__ = "edge"

    lower_id = Column(Integer, ForeignKey("node.node_id"), primary_key=True)

    higher_id = Column(Integer, ForeignKey("node.node_id"), primary_key=True)

    lower_node = relationship(
        Node, primaryjoin=lower_id == Node.node_id, backref="lower_edges"
    )

    higher_node = relationship(
        Node, primaryjoin=higher_id == Node.node_id, backref="higher_edges"
    )

    def __init__(self, n1, n2):
        self.lower_node = n1
        self.higher_node = n2

import unittest
import db_config
from test_utils import normalize_sql


class TestDirectedGraph(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"
    eng = None

    @classmethod
    def setUpClass(self):
        db_config.setup()
        self.eng = create_engine(self.url, echo=self.verbose)

    @classmethod
    def tearDownClass(self):
        if self.eng is not None:
            self.eng.dispose()
            self.eng = None
        db_config.teardown()

    def tearDown(self):
        pass

    def test_directed_graph(self):
        Base.metadata.create_all(self.eng)

        session = sessionmaker(self.eng)()

        # create a directed graph like this:
        #       n1 -> n2 -> n1
        #                -> n5
        #                -> n7
        #          -> n3 -> n6

        n1 = Node()
        n2 = Node()
        n3 = Node()
        n4 = Node()
        n5 = Node()
        n6 = Node()
        n7 = Node()

        Edge(n1, n2)
        Edge(n1, n3)
        Edge(n2, n1)
        Edge(n2, n5)
        Edge(n2, n7)
        Edge(n3, n6)

        session.add_all([n1, n2, n3, n4, n5, n6, n7])
        session.commit()

        assert [x for x in n3.higher_neighbors()] == [n6]
        assert [x for x in n3.lower_neighbors()] == [n1]
        assert [x for x in n2.lower_neighbors()] == [n1]
        assert [x for x in n2.higher_neighbors()] == [n1, n5, n7]

        session.close()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
