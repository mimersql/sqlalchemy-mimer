# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/vertical/dictlike-polymorphic.html
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
"""Mapping a polymorphic-valued vertical table as a dictionary.

Builds upon the dictlike.py example to also add differently typed
columns to the "fact" table, e.g.::

  Table(
      "properties",
      metadata,
      Column("owner_id", Integer, ForeignKey("owner.id"), primary_key=True),
      Column("key", UnicodeText),
      Column("type", Unicode(16)),
      Column("int_value", Integer),
      Column("char_value", UnicodeText),
      Column("bool_value", Boolean),
      Column("decimal_value", Numeric(10, 2)),
  )

For any given properties row, the value of the 'type' column will point to the
'_value' column active for that row.

This example approach uses exactly the same dict mapping approach as the
'dictlike' example.  It only differs in the mapping for vertical rows.  Here,
we'll use a @hybrid_property to build a smart '.value' attribute that wraps up
reading and writing those various '_value' columns and keeps the '.type' up to
date.

"""

from sqlalchemy import and_
from sqlalchemy import Boolean
from sqlalchemy import case
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import String
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.interfaces import PropComparator
from test_dictlike import ProxiedDictMixin


class PolymorphicVerticalProperty:
    """A key/value pair with polymorphic value storage.

    The class which is mapped should indicate typing information
    within the "info" dictionary of mapped Column objects; see
    the AnimalFact mapping below for an example.

    """

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    @hybrid_property
    def value(self):
        fieldname, discriminator = self.type_map[self.type]
        if fieldname is None:
            return None
        else:
            return getattr(self, fieldname)

    @value.setter
    def value(self, value):
        py_type = type(value)
        fieldname, discriminator = self.type_map[py_type]

        self.type = discriminator
        if fieldname is not None:
            setattr(self, fieldname, value)

    @value.deleter
    def value(self):
        self._set_value(None)

    @value.comparator
    class value(PropComparator):
        """A comparator for .value, builds a polymorphic comparison
        via CASE."""

        def __init__(self, cls):
            self.cls = cls

        def _case(self):
            pairs = set(self.cls.type_map.values())
            whens = [
                (
                    literal_column("'%s'" % discriminator),
                    cast(getattr(self.cls, attribute), String),
                )
                for attribute, discriminator in pairs
                if attribute is not None
            ]
            return case(*whens, value=self.cls.type, else_=null())

        def __eq__(self, other):
            return self._case() == cast(other, String)

        def __ne__(self, other):
            return self._case() != cast(other, String)

    def __repr__(self):
        return "<%s %r=%r>" % (self.__class__.__name__, self.key, self.value)


@event.listens_for(
    PolymorphicVerticalProperty, "mapper_configured", propagate=True
)
def on_new_class(mapper, cls_):
    """Look for Column objects with type info in them, and work up
    a lookup table."""

    info_dict = {}
    info_dict[type(None)] = (None, "none")
    info_dict["none"] = (None, "none")

    for k in mapper.c.keys():
        col = mapper.c[k]
        if "type" in col.info:
            python_type, discriminator = col.info["type"]
            info_dict[python_type] = (k, discriminator)
            info_dict[discriminator] = (k, discriminator)
    cls_.type_map = info_dict


import unittest
import db_config
from test_utils import normalize_sql


class TestDictLikePolymorpic(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"
    eng = None

    @classmethod
    def setUpClass(self):
        import warnings
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
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

    def test_dictlike_polymorpic(self):
        Base = declarative_base()

        class AnimalFact(PolymorphicVerticalProperty, Base):
            """A fact about an animal."""

            __tablename__ = "animal_fact"

            animal_id = Column(ForeignKey("animal.id"), primary_key=True)
            key = Column(Unicode(64), primary_key=True)
            type = Column(Unicode(16))

            # add information about storage for different types
            # in the info dictionary of Columns
            int_value = Column(Integer, info={"type": (int, "integer")})
            char_value = Column(Unicode(1024), info={"type": (str, "string")})
            boolean_value = Column(Boolean, info={"type": (bool, "boolean")})

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

        Base.metadata.create_all(self.eng)
        session = Session(self.eng)

        stoat = Animal("stoat")
        stoat["color"] = "red"
        stoat["cuteness"] = 7
        stoat["weasel-like"] = True

        session.add(stoat)
        session.commit()

        critter = session.query(Animal).filter(Animal.name == "stoat").one()
        if self.verbose:
            print(critter["color"])
            print(critter["cuteness"])

            print("changing cuteness value and type:")
        critter["cuteness"] = "very cute"

        session.commit()

        marten = Animal("marten")
        marten["cuteness"] = 5
        marten["weasel-like"] = True
        marten["poisonous"] = False
        session.add(marten)

        shrew = Animal("shrew")
        shrew["cuteness"] = 5
        shrew["weasel-like"] = False
        shrew["poisonous"] = True

        session.add(shrew)
        session.commit()

        q = session.query(Animal).filter(
            Animal.facts.any(
                and_(AnimalFact.key == "weasel-like", AnimalFact.value == True)
            )
        )
        res = q.all()
        if self.verbose:
            print("weasel-like animals", res)

        q = session.query(Animal).filter(
            Animal.with_characteristic("weasel-like", True)
        )
        res = q.all()
        if self.verbose:
            print("weasel-like animals again", res)

        q = session.query(Animal).filter(
            Animal.with_characteristic("poisonous", False)
        )
        res = q.all()
        if self.verbose:
            print("animals with poisonous=False", res)

        q = session.query(Animal).filter(
            or_(
                Animal.with_characteristic("poisonous", False),
                ~Animal.facts.any(AnimalFact.key == "poisonous"),
            )
        )
        res = q.all()
        if self.verbose:
            print("non-poisonous animals", res)

        q = session.query(Animal).filter(Animal.facts.any(AnimalFact.value == 5))
        res = q.all()
        if self.verbose:
            print("any animal with a .value of 5", res)

        session.close()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
