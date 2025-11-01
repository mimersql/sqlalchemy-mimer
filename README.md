# sqlalchemy-mimer

A SQLAlchemy 2.x dialect for [**Mimer SQL**](https://developer.mimer.com), built on top of the PEP 249-compliant driver [**MimerPy**](https://pypi.org/project/mimerpy/).

> **Status:** Beta — functional but under active development.

## 🔧 Installation

### From source (editable mode)

```bash
python -m pip install -e .
```

## Install (PyPi)

```bash
python -m pip install sqlalchemy-mimer
```

## 🔗 Connection

### Supported URL forms

The dialect supports several URI variants:

- `mimer://user:password@database`
- `mimer://user:password@host:port/database`
- `mimer://user:password@host:port/?dsn=database`
- `mimer+mimerpy://user:password@database`
- `mimer+mimerpy://user:password@host:port/database`
- `mimer+mimerpy://user:password@host:port/?dsn=database`
  
Example:

```python
from sqlalchemy import create_engine

engine = create_engine("mimer://SYSADM:SYSPASS@mimerdb")
engine = create_engine("mimer://SYSADM:SYSPASS@localhost:1360/mimerdb")
engine = create_engine("mimer+mimerpy://SYSADM:SYSPASS@localhost:1360/?dsn=mimerdb")

```

> **Note:** Until the Mimer SQL C API and MimerPy support host and port parameters,
> those parts of the URI are parsed but ignored.

## ▶️ Examples

Run basic demo programs:

```bash
python examples/demo_test.py "mimer://SYSADM:SYSPASS/mimerdb"
```

```bash
python examples/demo_create_table.py
```

Run a bit more complete demo that show some of the ORM capabilities:

```bash
python examples/demo_orm.py
```

## 🧪 Running Tests

Before running the tests a Mimer SQL database ident with
databank and ident privileges is needed. If no suitable ident exists it can
be created with:

```sql
create ident TST_MASTER as user using 'TST_MASTER_PWD';
grant ident to TST_MASTER;
grant databank to TST_MASTER with grant option;
````

To tell the unittests what ident and password to use, set them as
environment variables. On Linux and macOS you can do:

```bash
export MIMER_TEST_USER=TST_MASTER
export MIMER_TEST_PASSWORD=TST_MASTER_PWD
````

To run all unittests:

```bash
python -m unittest discover tests
```

Run a specific test file for detailed output:

```bash
python tests/test_basic_dml.py
python tests/test_constraints.py
python tests/test_orm.py
.
.
.
```

## Documentation

To build the documentation you have to install `mkdocs`:

```bash
python -m pip install mkdocs mkdocs-material
```

Build documentation:

```bash
mkdocs build
```

View the generated documentation, run:

```bash
mkdocs serve
```

Alternatively build the docs without flat structure that can be viewed in the browser directly without relying on `mkdocs serve`:

```bash
mkdocs build -f mkdir-nodir.yml
```
