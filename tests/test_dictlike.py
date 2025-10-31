# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/vertical/dictlike.html
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
"""Mapping a vertical table as a dictionary.

This example illustrates accessing and modifying a "vertical" (or
"properties", or pivoted) table via a dict-like interface.  These are tables
that store free-form object properties as rows instead of columns.  For
example, instead of::

  # A regular ("horizontal") table has columns for 'species' and 'size'
  Table(
      "animal",
      metadata,
      Column("id", Integer, primary_key=True),
      Column("species", Unicode),
      Column("size", Unicode),
  )

A vertical table models this as two tables: one table for the base or parent
entity, and another related table holding key/value pairs::

  Table("animal", metadata, Column("id", Integer, primary_key=True))

  # The properties table will have one row for a 'species' value, and
  # another row for the 'size' value.
  Table(
      "properties",
      metadata,
      Column(
          "animal_id", Integer, ForeignKey("animal.id"), primary_key=True
      ),
      Column("key", UnicodeText),
      Column("value", UnicodeText),
  )

Because the key/value pairs in a vertical scheme are not fixed in advance,
accessing them like a Python dict can be very convenient.  The example below
can be used with many common vertical schemas as-is or with minor adaptations.

"""

from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import attribute_keyed_dict


class ProxiedDictMixin:
    """Adds obj[key] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.

    """

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]


import unittest
import db_config
from test_utils import normalize_sql


class TestDictLike(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"

    @classmethod
    def setUpClass(self):
        import warnings
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        db_config.setup()

    @classmethod
    def tearDownClass(self):
        db_config.teardown()

    def tearDown(self):
        pass

    def test_dictlike(self):
        Base = declarative_base()

        class AnimalFact(Base):
            """A fact about an animal."""

            __tablename__ = "animal_fact"

            animal_id = Column(ForeignKey("animal.id"), primary_key=True)
            key = Column(Unicode(64), primary_key=True)
            value = Column(UnicodeText)

        class Animal(ProxiedDictMixin, Base):
            """an Animal"""

            __tablename__ = "animal"

            id = Column(Integer, primary_key=True)
            name = Column(Unicode(100))

            facts = relationship(
                "AnimalFact", collection_class=attribute_keyed_dict("key")
            )

            _proxied = association_proxy(
                "facts",
                "value",
                creator=lambda key, value: AnimalFact(key=key, value=value),
            )

            def __init__(self, name):
                self.name = name

            def __repr__(self):
                return "Animal(%r)" % self.name

            @classmethod
            def with_characteristic(self, key, value):
                return self.facts.any(key=key, value=value)

        engine = create_engine(self.url, echo=self.verbose)
        Base.metadata.create_all(engine)

        session = Session(bind=engine)

        stoat = Animal("stoat")
        stoat["color"] = "reddish"
        stoat["cuteness"] = "somewhat"

        # dict-like assignment transparently creates entries in the
        # stoat.facts collection:
        if self.verbose:
            print(stoat.facts["color"])

        session.add(stoat)
        session.commit()

        critter = session.query(Animal).filter(Animal.name == "stoat").one()
        if self.verbose:
            print(critter["color"])
            print(critter["cuteness"])

        critter["cuteness"] = "very"

        if self.verbose:
            print("changing cuteness:")

        marten = Animal("marten")
        marten["color"] = "brown"
        marten["cuteness"] = "somewhat"
        session.add(marten)

        shrew = Animal("shrew")
        shrew["cuteness"] = "somewhat"
        shrew["poisonous-part"] = "saliva"
        session.add(shrew)

        loris = Animal("slow loris")
        loris["cuteness"] = "fairly"
        loris["poisonous-part"] = "elbows"
        session.add(loris)

        q = session.query(Animal).filter(
            Animal.facts.any(
                and_(AnimalFact.key == "color", AnimalFact.value == "reddish")
            )
        )
        res = q.all()
        if self.verbose:
            print("reddish animals", res)

        q = session.query(Animal).filter(
            Animal.with_characteristic("color", "brown")
        )
        res = q.all()
        if self.verbose:
            print("brown animals", res)

        q = session.query(Animal).filter(
            ~Animal.with_characteristic("poisonous-part", "elbows")
        )
        res = q.all()
        if self.verbose:
            print("animals without poisonous-part == elbows", res)

        q = session.query(Animal).filter(Animal.facts.any(value="somewhat"))
        res = q.all()
        if self.verbose:
            print('any animal with any .value of "somewhat"', res)

        session.close()
        engine.dispose()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()