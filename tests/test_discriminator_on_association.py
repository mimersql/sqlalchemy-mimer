# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/generic_associations/discriminator_on_association.html
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
"""Illustrates a mixin which provides a generic association
using a single target table and a single association table,
referred to by all parent tables.  The association table
contains a "discriminator" column which determines what type of
parent object associates to each particular row in the association
table.

SQLAlchemy's single-table-inheritance feature is used
to target different association types.

This configuration attempts to simulate a so-called "generic foreign key"
as closely as possible without actually foregoing the use of real
foreign keys.   Unlike table-per-related and table-per-association,
it uses a fixed number of tables to serve any number of potential parent
objects, but is also slightly more complex.

"""

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import backref
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


class AddressAssociation(Base):
    """Associates a collection of Address objects
    with a particular parent.
    """

    __tablename__ = "address_association"

    discriminator: Mapped[str] = mapped_column()
    """Refers to the type of parent."""
    addresses: Mapped[list["Address"]] = relationship(
        back_populates="association"
    )

    __mapper_args__ = {"polymorphic_on": discriminator}


class Address(Base):
    """The Address class.

    This represents all address records in a
    single table.
    """

    association_id: Mapped[int] = mapped_column(
        ForeignKey("address_association.id")
    )
    street: Mapped[str]
    city: Mapped[str]
    zip: Mapped[str]
    association: Mapped["AddressAssociation"] = relationship(
        back_populates="addresses"
    )

    parent = association_proxy("association", "parent")

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

    @declared_attr
    def address_association_id(cls) -> Mapped[int]:
        return mapped_column(ForeignKey("address_association.id"))

    @declared_attr
    def address_association(cls):
        name = cls.__name__
        discriminator = name.lower()

        assoc_cls = type(
            f"{name}AddressAssociation",
            (AddressAssociation,),
            dict(
                __tablename__=None,
                __mapper_args__={"polymorphic_identity": discriminator},
            ),
        )

        cls.addresses = association_proxy(
            "address_association",
            "addresses",
            creator=lambda addresses: assoc_cls(addresses=addresses),
        )
        return relationship(
            assoc_cls, backref=backref("parent", uselist=False)
        )


class Customer(HasAddresses, Base):
    name: Mapped[str]


class Supplier(HasAddresses, Base):
    company_name: Mapped[str]


import unittest
import db_config
from test_utils import normalize_sql


class TestDiscriminatorOnAssociation(unittest.TestCase):
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

    def test_discrimator_on_association(self):
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