# Adapted from SQLAlchemy example:
# https://docs.sqlalchemy.org/en/20/_modules/examples/inheritance/concrete.html
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
"""Concrete-table (table-per-class) inheritance example."""

from __future__ import annotations

from typing import Annotated

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext.declarative import ConcreteBase
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic


intpk = Annotated[int, mapped_column(primary_key=True)]
str50 = Annotated[str, mapped_column(String(50))]


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


class Person(ConcreteBase, Base):
    __tablename__ = "person"
    id: Mapped[intpk]
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {
        "polymorphic_identity": "person",
    }

    def __repr__(self):
        return f"Ordinary person {self.name}"


class Engineer(Person):
    __tablename__ = "engineer"

    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]
    status: Mapped[str50]
    engineer_name: Mapped[str50]
    primary_language: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "engineer", "concrete": True}


class Manager(Person):
    __tablename__ = "manager"

    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]
    status: Mapped[str50]
    manager_name: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "manager", "concrete": True}

    def __repr__(self):
        return (
            f"Manager {self.name}, status {self.status}, "
            f"manager_name {self.manager_name}"
        )

import unittest
import db_config
from test_utils import normalize_sql


class TestConcrete(unittest.TestCase):
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

    def test_concrete(self):
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

            if self.verbose:
                for e in c.employees:
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
            # when using ConcreteBase, use "*" to use the default selectable
            # setting specific entities won't work right now.
            eng_manager = with_polymorphic(Person, "*")
            out = session.scalars(
                select(eng_manager).filter(
                    or_(
                        eng_manager.Engineer.engineer_name == "engineer1",
                        eng_manager.Manager.manager_name == "manager2",
                    )
                )
            ).all()
            if self.verbose:
                print(out)

            # illustrate join from Company.
            out = session.scalars(
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
                print(out)
            session.commit()
            session.close()

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
