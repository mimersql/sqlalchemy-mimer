# Based on SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/inheritance/single.html
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

"""Single-table (table-per-hierarchy) inheritance example."""

from __future__ import annotations

from typing import Annotated

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import FromClause
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic

intpk = Annotated[int, mapped_column(primary_key=True)]
str50 = Annotated[str, mapped_column(String(50))]

# columns that are local to subclasses must be nullable.
# we can still use a non-optional type, however
str50subclass = Annotated[str, mapped_column(String(50), nullable=True)]


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "company"
    id: Mapped[intpk]
    name: Mapped[str50]

    employees: Mapped[list[Person]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"Company {self.name}"


class Person(Base):
    __tablename__ = "person"
    __table__: FromClause

    id: Mapped[intpk]
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]
    type: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {
        "polymorphic_identity": "person",
        "polymorphic_on": "type",
    }

    def __repr__(self):
        return f"Ordinary person {self.name}"


class Engineer(Person):
    # illustrate a single-inh "conflicting" mapped_column declaration,
    # where both subclasses want to share the same column that is nonetheless
    # not "local" to the base class
    @declared_attr
    def status(cls) -> Mapped[str50]:
        return Person.__table__.c.get(
            "status", mapped_column(String(30))  # type: ignore
        )

    engineer_name: Mapped[str50subclass]
    primary_language: Mapped[str50subclass]

    __mapper_args__ = {"polymorphic_identity": "engineer"}

    def __repr__(self):
        return (
            f"Engineer {self.name}, status {self.status}, "
            f"engineer_name {self.engineer_name}, "
            f"primary_language {self.primary_language}"
        )


class Manager(Person):
    manager_name: Mapped[str50subclass]

    # illustrate a single-inh "conflicting" mapped_column declaration,
    # where both subclasses want to share the same column that is nonetheless
    # not "local" to the base class
    @declared_attr
    def status(cls) -> Mapped[str50]:
        return Person.__table__.c.get(
            "status", mapped_column(String(30))  # type: ignore
        )

    __mapper_args__ = {"polymorphic_identity": "manager"}

    def __repr__(self):
        return (
            f"Manager {self.name}, status {self.status}, "
            f"manager_name {self.manager_name}"
        )

import unittest
import db_config
from test_utils import normalize_sql


class TestSingle(unittest.TestCase):
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

    def test_single(self):
        Base.metadata.create_all(self.eng)

        with Session(self.eng) as session:
            c = Company(
                name="company1",
                employees=[
                    Manager(
                        name="mr krabs",
                        status="AAB",
                        manager_name="manager1",
                    ),
                    Engineer(
                        name="spongebob",
                        status="BBA",
                        engineer_name="engineer1",
                        primary_language="java",
                    ),
                    Person(name="joesmith"),
                    Engineer(
                        name="patrick",
                        status="CGG",
                        engineer_name="engineer2",
                        primary_language="python",
                    ),
                    Manager(name="jsmith", status="ABA", manager_name="manager2"),
                ],
            )
            session.add(c)

            session.commit()

            for e in c.employees:
                if self.verbose:
                    print(e)

            spongebob = session.scalars(
                select(Person).filter_by(name="spongebob")
            ).one()
            spongebob2 = session.scalars(
                select(Engineer).filter_by(name="spongebob")
            ).one()
            assert spongebob is spongebob2

            spongebob2.engineer_name = "hes spongebob!"

            session.commit()

            # query using with_polymorphic.
            eng_manager = with_polymorphic(Person, [Engineer, Manager])
            res = session.scalars(
                    select(eng_manager).filter(
                        or_(
                            eng_manager.Engineer.engineer_name == "engineer1",
                            eng_manager.Manager.manager_name == "manager2",
                        )
                    )
                ).all()
            if self.verbose:
                print(res)

            # illustrate join from Company.
            res = session.scalars(
                    select(Company)
                    .join(Company.employees.of_type(eng_manager))
                    .filter(
                        or_(
                            eng_manager.Engineer.engineer_name == "engineer1",
                            eng_manager.Manager.manager_name == "manager2",
                        )
                    )
                ).all()
            if self.verbose:
                print(res)

            session.close()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
