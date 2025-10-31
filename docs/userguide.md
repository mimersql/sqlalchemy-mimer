# User Guide

This guide introduces the most common usage patterns for SQLAlchemy-Mimer — both Core and ORM examples.

---

## Connecting to Mimer SQL

```python
from sqlalchemy import create_engine

engine = create_engine("mimer://SYSADM:SYSPASS@localhost:1360/demo")
```

You can then use SQLAlchemy’s ORM or Core APIs as usual.

---

## Core Example

```python
from sqlalchemy import MetaData, Table, Column, Integer, String, select

metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(50)),
)

metadata.create_all(engine)

# Insert data
with engine.begin() as conn:
    conn.execute(users.insert(), [{"name": "Alex"}, {"name": "Bob"}])

# Query
with engine.connect() as conn:
    for row in conn.execute(select(users)):
        print(row)
```

---

## ORM Example with Relationships

Below is a simple example using two ORM-mapped classes related by a foreign key —  
just like in `examples/demo_orm.py`.

```python
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "user_account"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    fullname: Mapped[str]

    # Define the one-to-many relationship
    addresses: Mapped[list["Address"]] = relationship(back_populates="user")

class Address(Base):
    __tablename__ = "address"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))

    # Define the inverse side of the relationship
    user: Mapped["User"] = relationship(back_populates="addresses")

# Create tables
Base.metadata.create_all(engine)
```

### Inserting and Querying

```python
with Session(engine) as session:
    # Add users with addresses
    u1 = User(name="alice", fullname="Alice Smith")
    u1.addresses = [
        Address(email_address="alic@work.com"),
        Address(email_address="alsmith@mydomain.com"),
    ]

    u2 = User(name="sandy", fullname="Sandy Boyle")
    u2.addresses = [Address(email_address="sandy@mymmail.net")]

    session.add_all([u1, u2])
    session.commit()

# Query the results
with Session(engine) as session:
    stmt = select(User).order_by(User.id)
    for user in session.scalars(stmt):
        print(f"{user.name} ({len(user.addresses)} addresses)")
        for addr in user.addresses:
            print("   -", addr.email_address)
```

When you run this code, you should see output like:

```
alice (2 addresses)
   - alic@work.com"
   - alsmith@mydomain.com
sandy (1 addresses)
   - sandy@mymmail.net
```

---

## Notes on ORM Behavior

- The `user_account.id` column is **autoincremented** using Mimer SQL sequences.
- The foreign key between `address.user_id` and `user_account.id` is fully enforced by Mimer SQL.
- The dialect automatically retrieves generated IDs using `MimerGetSequenceValue()`,  
  so no extra `SELECT` is needed after `INSERT`.
- All standard SQLAlchemy ORM features (relationships, eager/lazy loading, cascading) work as expected.

---

## Next Steps

See also:
- [Autoincrement and Sequence Handling](autoincrement.md)
- [SQLAlchemy ORM documentation](https://docs.sqlalchemy.org/en/20/orm/)
