# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/association/dict_of_sets_with_default.html
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
"""An advanced association proxy example which
illustrates nesting of association proxies to produce multi-level Python
collections, in this case a dictionary with string keys and sets of integers
as values, which conceal the underlying mapped classes.

This is a three table model which represents a parent table referencing a
dictionary of string keys and sets as values, where each set stores a
collection of integers. The association proxy extension is used to hide the
details of this persistence. The dictionary also generates new collections
upon access of a non-existent key, in the same manner as Python's
"collections.defaultdict" object.

"""

from __future__ import annotations

import operator
from typing import Mapping

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import KeyFuncDict


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)


class GenDefaultCollection(KeyFuncDict[str, "B"]):
    def __missing__(self, key: str) -> B:
        self[key] = b = B(key)
        return b


class A(Base):
    __tablename__ = "a"
    associations: Mapped[Mapping[str, B]] = relationship(
        "B",
        collection_class=lambda: GenDefaultCollection(
            operator.attrgetter("key")
        ),
    )

    collections: AssociationProxy[dict[str, set[int]]] = association_proxy(
        "associations", "values"
    )
    """Bridge the association from 'associations' over to the 'values'
    association proxy of B.
    """


class B(Base):
    __tablename__ = "b"
    a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
    elements: Mapped[set[C]] = relationship("C", collection_class=set)
    key: Mapped[str]

    values: AssociationProxy[set[int]] = association_proxy("elements", "value")
    """Bridge the association from 'elements' over to the
    'value' element of C."""

    def __init__(self, key: str, values: set[int] | None = None) -> None:
        self.key = key
        if values:
            self.values = values


class C(Base):
    __tablename__ = "c"
    b_id: Mapped[int] = mapped_column(ForeignKey("b.id"))
    value: Mapped[int]

    def __init__(self, value: int) -> None:
        self.value = value


import unittest
import db_config
from test_utils import normalize_sql


class TestDictOfSetsWithDefault(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"

    @classmethod
    def setUpClass(self):
        db_config.setup()

    @classmethod
    def tearDownClass(self):
        db_config.teardown()

    def tearDown(self):
        pass

    def test_dict_of_sets_with_default(self):
        engine = create_engine(self.url, echo=self.verbose)
        Base.metadata.create_all(engine)
        session = Session(engine)

        # only "A" is referenced explicitly.  Using "collections",
        # we deal with a dict of key/sets of integers directly.

        session.add_all([A(collections={"1": {1, 2, 3}})])
        session.commit()

        a1 = session.scalars(select(A)).one()
        if self.verbose:
            print(a1.collections["1"])
        a1.collections["1"].add(4)
        session.commit()

        a1.collections["2"].update([7, 8, 9])
        session.commit()

        if self.verbose:
            print(a1.collections["2"])

        session.close()
        engine.dispose()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()