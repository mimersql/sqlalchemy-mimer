# Changelog

## Unreleased
- Tested with SQLAlchemy 2.0 and 2.1, and MimerPy 1.3.11
- Support SQLAlchemy 2.1: render the `Double` type (used for `Mapped[float]`) as `DOUBLE PRECISION`
- Require MimerPy 1.3.11 (faster `executemany()` and fetching, SQL trace logging)
- Boolean connection options in the URL (`autocommit`, `readonly`, `trace_unsafe`)
  are converted to booleans; previously `?autocommit=false` was treated as true.
  `?trace=` accepts a boolean or a file path.
- Autocommit mode is set through MimerPy's `autocommit` property, so DDL on a
  `readonly=true` connection is now rejected instead of being executed
- Documentation for connection options and logging/tracing
- Table create/drop event hooks no longer run for non-Mimer dialects (importing `sqlalchemy_mimer` broke `create_all()` on e.g. SQLite)
- Rename `test_dictlike-polymorphic.py` so `unittest discover` runs it
- Fix mkdocs config file name in README

## 1.0.4 — 2025-12-10
- Remove beta status from README

## 1.0.3 — 2025-11-28
- Fix lifetime management in tests

## 1.0.2 — 2025-11-07
- Fixed sequence handling
- Fixed schema handling
  
## 1.0.1 — 2025-11-04
- Use new syntax for `CREATE SEQUENCE`

## 1.0.0 — 2025-10-31
First public release on GitHub and PyPI.

### Added
- Complete SQLAlchemy dialect for Mimer SQL.
- ORM and Core support verified against SQLAlchemy’s official test suite.
- Documentation and user guide.
- Packaging for PyPI (`sqlalchemy-mimer`).

