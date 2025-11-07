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
import argparse, getpass, os

# -- Drop tables and sequences before re-creating them --
def reset_schema(engine):
    insp = inspect(engine)
    with engine.begin() as conn:
        # Drop tables first (to remove foreign keys)
        for table_name in insp.get_table_names():
            if table_name == 'address' or table_name == 'user_account':
                conn.execute(text(f'DROP TABLE {table_name} CASCADE'))
                if engine.dialect.has_sequence(conn, f'{table_name}_id_autoinc_seq'):
                    conn.execute(text(f'DROP SEQUENCE {table_name}_id_autoinc_seq CASCADE'))
    print("Schema reset complete.\n")

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
    parser = argparse.ArgumentParser(description="Demo of ORM capabilities")

    parser.add_argument(
        '-d', '--database',
        help='Database name',
    )

    parser.add_argument(
        '-u', '--user',
        help='Database username',
    )

    parser.add_argument(
        '-p', '--password',
        help='Database password',
    )

    parser.add_argument(
        '-v', '--verbose',
        help='Show verbose output including SQLAlchemy logs',
        action='store_true',
    )

    args = parser.parse_args()

    #If no database is give, try to use MIMER_DATABASE
    database = args.database or os.environ.get("MIMER_DATABASE")
    if not database:
        parser.error("No database specified. Use -d/--database or set MIMER_DATABASE environment variable.")

    # Get password if not specified
    if not args.user:
        args.user = input("Username: ").strip()

    # Get password if not specified
    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user or 'user'}: ")

    # Build database URL
    db_url = f"mimer://{args.user}:{args.password}@{database}"


    engine = create_engine(db_url, echo=args.verbose)
    #engine = create_engine("sqlite://", echo=True)
    reset_schema(engine)
    Base.metadata.create_all(engine)


    print("Add users Monica, Alex, and George\n")
    with Session(engine) as session:
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
                Address(email_address="alex@mydomain.com"),
            ],
        )
        george = User(name="george", fullname="George Buff")
        session.add_all([monica, alex, george])
        session.commit()



    session = Session(engine)

    print("Search for users with username 'monica' or 'alex'")
    stmt = select(User).where(User.name.in_(["monica", "alex"]))
    for user in session.scalars(stmt):
        print("Users: ", user)

    stmt = (
        select(Address)
        .join(Address.user)
        .where(User.name == "alex")
        .where(Address.email_address == "alsmith@somewhere.com")
    )
    alex_address = session.scalars(stmt).one()
    print("\nAlex's address: ", alex_address)

    stmt = select(User).where(User.name == "george")
    george = session.scalars(stmt).one()
    george.addresses.append(Address(email_address="georgeb@georgsdomain.com"))
    session.flush()
    print("\nGeorge's addresses: ", george.addresses)
    alex_address.email_address = "alex_cheeks@somecompany.com"

    session.commit()

    print("\nRemove Alex's address")
    alex = session.get(User, 2)
    alex.addresses.remove(alex_address)
    session.flush()

    print("\nDelete 'george'")
    session.delete(george)

    session.commit()