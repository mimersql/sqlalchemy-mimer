# Based on SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/generic_associations/table_per_related.html
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
"""Illustrates a generic association which persists association
objects within individual tables, each one generated to persist
those objects on behalf of a particular parent class.

This configuration has the advantage that each type of parent
maintains its "Address" rows separately, so that collection
size for one type of parent will have no impact on other types
of parent.   Navigation between parent and "Address" is simple,
direct, and bidirectional.

This recipe is the most efficient (speed wise and storage wise)
and simple of all of them.

The creation of many related tables may seem at first like an issue
but there really isn't any - the management and targeting of these tables
is completely automated.

"""

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    """Base class which provides automated table name
    and surrogate primary key column.

    """

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id: Mapped[int] = mapped_column(primary_key=True)


class Address:
    """Define columns that will be present in each
    'Address' table.

    This is a declarative mixin, so additional mapped
    attributes beyond simple columns specified here
    should be set up using @declared_attr.

    """

    street: Mapped[str]
    city: Mapped[str]
    zip: Mapped[str]

    def __repr__(self):
        return "%s(street=%r, city=%r, zip=%r)" % (
            self.__class__.__name__,
            self.street,
            self.city,
            self.zip,
        )


class HasAddresses:
    """HasAddresses mixin, creates a new Address class
    for each parent.

    """

    @declared_attr
    def addresses(cls):
        cls.Address = type(
            f"{cls.__name__}Address",
            (Address, Base),
            dict(
                __tablename__=f"{cls.__tablename__}_address",
                parent_id=mapped_column(
                    Integer, ForeignKey(f"{cls.__tablename__}.id")
                ),
                parent=relationship(cls, overlaps="addresses"),
            ),
        )
        return relationship(cls.Address)


class Customer(HasAddresses, Base):
    name: Mapped[str]


class Supplier(HasAddresses, Base):
    company_name: Mapped[str]


import unittest
import db_config
from test_utils import normalize_sql

class TestTablePerRelated(unittest.TestCase):
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

    def test_table_per_related(self):
        Base.metadata.create_all(self.eng)

        session = Session(self.eng)

        session.add_all(
            [
                Customer(
                    name="customer 1",
                    addresses=[
                        Customer.Address(
                            street="123 anywhere street", city="New York", zip="10110"
                        ),
                        Customer.Address(
                            street="40 main street", city="San Francisco", zip="95732"
                        ),
                    ],
                ),
                Supplier(
                    company_name="Ace Hammers",
                    addresses=[
                        Supplier.Address(
                            street="2569 west elm", city="Detroit", zip="56785"
                        )
                    ],
                ),
            ]
        )

        session.commit()

        for customer in session.query(Customer):
            for address in customer.addresses:
                if self.verbose:
                    print(address)
                    print(address.parent)

        session.close()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
