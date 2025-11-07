# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/generic_associations/generic_fk.html
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
"""Illustrates a so-called "generic foreign key", in a similar fashion
to that of popular frameworks such as Django, ROR, etc.  This
approach bypasses standard referential integrity
practices, in that the "foreign key" column is not actually
constrained to refer to any particular table; instead,
in-application logic is used to determine which table is referenced.

This approach is not in line with SQLAlchemy's usual style, as foregoing
foreign key integrity means that the tables can easily contain invalid
references and also have no ability to use in-database cascade functionality.

However, due to the popularity of these systems, as well as that it uses
the fewest number of tables (which doesn't really offer any "advantage",
though seems to be comforting to many) this recipe remains in
high demand, so in the interests of having an easy StackOverflow answer
queued up, here it is.   The author recommends "table_per_related"
or "table_per_association" instead of this approach.

"""

from sqlalchemy import and_
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import backref
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import foreign
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import remote
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    """Base class which provides automated table name
    and surrogate primary key column.
    """

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id: Mapped[int] = mapped_column(primary_key=True)


class Address(Base):
    """The Address class.

    This represents all address records in a
    single table.
    """

    street: Mapped[str]
    city: Mapped[str]
    zip: Mapped[str]

    discriminator: Mapped[str]
    """Refers to the type of parent."""

    parent_id: Mapped[int]
    """Refers to the primary key of the parent.

    This could refer to any table.
    """

    @property
    def parent(self):
        """Provides in-Python access to the "parent" by choosing
        the appropriate relationship.
        """
        return getattr(self, f"parent_{self.discriminator}")

    def __repr__(self):
        return "%s(street=%r, city=%r, zip=%r)" % (
            self.__class__.__name__,
            self.street,
            self.city,
            self.zip,
        )


class HasAddresses:
    """HasAddresses mixin, creates a relationship to
    the address_association table for each parent.

    """


@event.listens_for(HasAddresses, "mapper_configured", propagate=True)
def setup_listener(mapper, class_):
    name = class_.__name__
    discriminator = name.lower()
    class_.addresses = relationship(
        Address,
        primaryjoin=and_(
            class_.id == foreign(remote(Address.parent_id)),
            Address.discriminator == discriminator,
        ),
        backref=backref(
            "parent_%s" % discriminator,
            primaryjoin=remote(class_.id) == foreign(Address.parent_id),
            overlaps="addresses, parent_customer",
        ),
        overlaps="addresses",
    )

    @event.listens_for(class_.addresses, "append")
    def append_address(target, value, initiator):
        value.discriminator = discriminator


class Customer(HasAddresses, Base):
    name: Mapped[str]


class Supplier(HasAddresses, Base):
    company_name: Mapped[str]

import unittest
import db_config
from test_utils import normalize_sql


class TestGenericFk(unittest.TestCase):
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

    def test_generic_fk(self):
        engine = create_engine(self.url, echo=self.verbose)
        Base.metadata.create_all(engine)

        session = Session(engine)

        session.add_all(
            [
                Customer(
                    name="customer 1",
                    addresses=[
                        Address(
                            street="123 anywhere street", city="New York", zip="10110"
                        ),
                        Address(
                            street="40 main street", city="San Francisco", zip="95732"
                        ),
                    ],
                ),
                Supplier(
                    company_name="Ace Hammers",
                    addresses=[
                        Address(street="2569 west elm", city="Detroit", zip="56785")
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
        engine.dispose()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()