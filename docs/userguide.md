# User Guide

This guide introduces the most common usage patterns for SQLAlchemy-Mimer — both Core and ORM examples.

---

## Connecting to Mimer SQL

```python
from sqlalchemy import create_engine

engine = create_engine("mimer://SYSADM:SYSPASS@localhost:1360/demo")
```

You can then use SQLAlchemy’s ORM or Core APIs as usual.

### Connection options

Options for `mimerpy.connect()` can be given in the URL query string or with
`connect_args`. In the URL, the boolean options `autocommit`, `readonly` and
`trace_unsafe` accept `true`/`false`, `yes`/`no`, `on`/`off` or `1`/`0`:

```python
engine = create_engine("mimer://SYSADM:SYSPASS@demo?readonly=true")
engine = create_engine("mimer://SYSADM:SYSPASS@demo", connect_args={"readonly": True})
```

To run in autocommit mode, prefer SQLAlchemy's own isolation level setting:

```python
engine = create_engine("mimer://SYSADM:SYSPASS@demo", isolation_level="AUTOCOMMIT")
```

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

## Logging and tracing

There are two levels of logging, and they can be used together.

**SQLAlchemy logging** shows the SQL that SQLAlchemy sends to the driver, with
parameters. This is usually what you need when developing:

```python
engine = create_engine("mimer://SYSADM:SYSPASS@demo", echo=True)
```

or, through the standard `logging` module:

```python
import logging
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
```

Use `hide_parameters=True` in `create_engine()` to keep parameter values out of
the log. Literals in the SQL text are still shown.

**MimerPy SQL trace** (MimerPy 1.3.10 or later) logs what the driver actually
sends to Mimer SQL, including commits and rollbacks. By default it replaces
literals with `#` and leaves out parameter values, so it is safe to use in
production. It can be enabled without changing the application:

```bash
export MIMERPY_TRACE=1                 # log to stderr
export MIMERPY_TRACE=/tmp/mimer-sql.log  # log to a file
```

or per engine:

```python
engine = create_engine("mimer://SYSADM:SYSPASS@demo?trace=/tmp/mimer-sql.log")
engine = create_engine("mimer://SYSADM:SYSPASS@demo", connect_args={"trace": True})
```

Passing `trace=false` (or `connect_args={"trace": False}`) disables tracing for
that engine even when `MIMERPY_TRACE` is set. To log full SQL and parameter
values, set `trace_unsafe=true` or `MIMERPY_TRACE_UNSAFE=1`. Only do this where
the log can't expose sensitive data.

See the MimerPy user guide for details on the trace format.

---

## Next Steps

See also:
- [Autoincrement and Sequence Handling](autoincrement.md)
- [SQLAlchemy ORM documentation](https://docs.sqlalchemy.org/en/20/orm/)
