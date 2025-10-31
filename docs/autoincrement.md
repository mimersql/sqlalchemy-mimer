# Autoincrement and Sequence Handling in Mimer SQLAlchemy Dialect

Mimer SQL does not have “IDENTITY” columns — autoincrement is implemented through **sequences**.  
The Mimer SQLAlchemy dialect handles this transparently and efficiently.

## 1. Table creation

For every `INTEGER`/`BIGINT` primary key with `autoincrement=True`,  
the dialect automatically:

- creates a sequence named `<table>_<column>_autoinc_seq`, and  
- emits a column default:
  ```sql
  DEFAULT NEXT VALUE FOR "<table>_<column>_autoinc_seq"
  ```

This ensures the same behavior as native identity columns in other RDBMS.

## 2. Sequence management

- `before_create_table()` ensures the sequence exists before `CREATE TABLE`.
- `after_drop_table()` removes the sequence when the table is dropped.
- Both events are idempotent — running `create_all()` or `drop_all()` repeatedly is safe.

## 3. Retrieving generated IDs

The MimerPy driver handles retrieval of the most recently generated sequence value for the current statement.  
This avoids an extra round trip to the server.

During an `INSERT`, `cursor.lastrowid` is set automatically.  
SQLAlchemy then reads it via:

```python
context.get_lastrowid()
```

→ ORM and Core inserts receive the generated primary key immediately, without extra queries.

## 4. Manual sequences

Manual use of sequences also works:

```python
seq = Sequence("my_manual_seq")
val = connection.scalar(seq)
```

because `MimerExecutionContext.fire_sequence()` executes  
`SELECT NEXT VALUE FOR my_manual_seq FROM system.onerow`.
