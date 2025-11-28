# Copyright (c) 2025 Mimer Information Technology

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
# See license for more details.
"""
Example SQLAlchemy ORM usage with Mimer SQL database.
This example is copied from the main SQLAlchemy documentation with minor adjustments.
See: https://docs.sqlalchemy.org/en/20/orm/quickstart.html

The User object has an autoincrementing primary key, which demonstrates
the sequence creation behavior of the Mimer dialect. It also has a one-to-many
relationship with the Address object.

The Address object also has an autoincrementing primary key and a foreign key
relationship to the User object.

To show logging output, run this script with the --verbose flag:
    python examples/demo_orm.py --verbose
"""
from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import inspect, text
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy import MetaData
import unittest
import db_config

class TestORM(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"
    eng = None

    @classmethod
    def setUpClass(self):
        db_config.setup()
        self.eng = create_engine(self.url, echo=self.verbose, future=True)

    @classmethod
    def tearDownClass(self):
        if self.eng is not None:
            self.eng.dispose()
            self.eng = None
        db_config.teardown()

    def setUp(self):
        pass

    def tearDown(self):
        Base.metadata.drop_all(self.eng, checkfirst=True)

    def test_demo_orm(self):
        Base.metadata.create_all(self.eng)

        with Session(self.eng) as session:
            monica = User(
                name="monica",
                fullname="Monica Andersson",
                addresses=[Address(email_address="monica@somecompany.com")],
            )
            alex = User(
                name="alex",
                fullname="Alex Smith",
                addresses=[
                    Address(email_address="alsmith@somewhere.com"),
                    Address(email_address="alex@squirrelpower.org"),
                ],
            )
            george = User(name="george", fullname="George Buff")
            session.add_all([monica, alex, george])
            session.commit()

        session = Session(self.eng)

        stmt = select(User).where(User.name.in_(["monica", "alex"]))

        userlist = session.scalars(stmt).all()
        result_list = [
                         {"id": u.id, "name": u.name, "fullname": u.fullname} for u in userlist
                    ]

        expected_list = [
            {"id": 1, "name": "monica", "fullname": "Monica Andersson"},
            {"id": 2, "name": "alex", "fullname": "Alex Smith"},
]
        self.assertEqual(result_list, expected_list)

        stmt = (
            select(Address)
            .join(Address.user)
            .where(User.name == "alex")
            .where(Address.email_address == "alsmith@somewhere.com")
        )
        alex_address = session.scalars(stmt).one()
        self.assertEqual(alex_address.id, 2)
        self.assertEqual(alex_address.email_address, "alsmith@somewhere.com")

        stmt = select(User).where(User.name == "george")
        george = session.scalars(stmt).one()
        george.addresses.append(Address(email_address="georgeb@georgsdomain.com"))
        session.flush()

        self.assertEqual(len(george.addresses), 1)
        self.assertEqual(george.addresses[0].email_address, "georgeb@georgsdomain.com")
        self.assertEqual(george.id, 3)
        alex_address.email_address = "alex_cheeks@somecompany.com"
        self.assertEqual(alex_address.email_address, 'alex_cheeks@somecompany.com')
        session.commit()

        alex = session.get(User, 2)
        stmt = select(Address).join(Address.user).where(User.name == "alex")

        alex_address_before_delete = session.scalars(stmt).all()
        alex_address_session_query = session.query(Address).filter_by(user_id=alex.id).all()

        alex_address_before_delete_list = [
            {"id": a.id, "email_address": a.email_address}
            for a in alex_address_before_delete
        ]
        alex_address_session_query_list = [
            {"id": a.id, "email_address": a.email_address}
            for a in alex_address_session_query
        ]
        self.assertEqual(alex_address_before_delete_list, alex_address_session_query_list)

        alex.addresses.remove(alex_address)
        session.flush()
        alex_address_after_delete = session.scalars(stmt).all()
        alex_address_after_delete_list = [
            {"id": a.id, "email_address": a.email_address}
            for a in alex_address_after_delete
        ]
        self.assertNotEqual(alex_address_before_delete_list, alex_address_after_delete_list)

        patrik_before_delete = session.query(Address).filter_by(user_id=george.id).all()
        self.assertEqual(len(patrik_before_delete), 1)
        session.delete(george)
        patrik_after_delete = session.query(Address).filter_by(user_id=george.id).all()
        self.assertEqual(len(patrik_after_delete), 0)

        session.flush()

        session.commit()

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "user_account"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]
    addresses: Mapped[List["Address"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

class Address(Base):
    __tablename__ = "address"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    user: Mapped["User"] = relationship(back_populates="addresses")
    def __repr__(self) -> str:
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"


if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
