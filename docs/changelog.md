# Changelog

## Unreleased
- Table create/drop event hooks no longer run for non-Mimer dialects (importing `sqlalchemy_mimer` broke `create_all()` on e.g. SQLite)
- Rename `test_dictlike-polymorphic.py` so `unittest discover` runs it
- Fix mkdocs config file name in README

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

