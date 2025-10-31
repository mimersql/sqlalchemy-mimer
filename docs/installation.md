# Installation

SQLAlchemy-Mimer supports **Python 3.8+** and **SQLAlchemy 2.0+**.

---

## 1. Requirements

Before installation, ensure that:
- Mimer SQL is installed and reachable on your system.  
  You can download Mimer SQL for various platforms from the official website:  
  [https://developer.mimer.com/downloads/](https://developer.mimer.com/downloads/)
- The MimerPy driver (Python interface for Mimer SQL) is installed.

You can install both SQLAlchemy-Mimer and its dependencies using `pip`:

```bash
python -m pip install sqlalchemy-mimer
```

This will automatically install:
- `sqlalchemy` (if not already present)
- `mimerpy` (for database connectivity)

---

## 2. Verifying the installation

You can confirm the dialect is registered correctly:

```python
from sqlalchemy.dialects import registry

print(registry.load("mimer"))
# <class 'sqlalchemy_mimer.dialect.MimerDialect'>
```

Or simply try connecting:

```python
from sqlalchemy import create_engine
engine = create_engine("mimer://SYSADM:SYSPASS@localhost:1360/demo")
with engine.connect() as conn:
    print(conn.exec_driver_sql("select current_user from system.onerow").fetchone())
```

---

## 3. Optional development installation

To work with the source tree directly:

```bash
git clone https://github.com/yourname/sqlalchemy-mimer.git
cd sqlalchemy-mimer
python -m pip install -e .
```
