# SQLAlchemy-Mimer Documentation

SQLAlchemy-Mimer provides a **fully featured SQLAlchemy dialect** for [Mimer SQL](https://mimer.com), enabling seamless integration between the Mimer database and Python applications using SQLAlchemy’s ORM or Core APIs.

It offers full support for:

- Schema reflection (tables, views, constraints, domains, indexes)
- ORM integration and identity management
- Sequence-based autoincrement (with zero roundtrips)
- Native Mimer SQL syntax and type names
- Safe, idempotent DDL generation

---

## 📚 Table of contents

- [Installation](installation.md)
- [User Guide](userguide.md)
- [Autoincrement & Sequence Handling](autoincrement.md)
- [Changelog](changelog.md)

---

## 🔍 Quick start

```bash
python -m pip install sqlalchemy-mimer
```

Then connect to Mimer SQL:

```python
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

engine = create_engine("mimer://user:password@hostname:port/dbname")
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(50))
)

metadata.create_all(engine)
```
