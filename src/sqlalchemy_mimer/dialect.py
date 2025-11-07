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
from __future__ import annotations

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine import reflection
from sqlalchemy.sql.compiler import TypeCompiler
from sqlalchemy import types as sqltypes
from sqlalchemy import Sequence, event, Table
from sqlalchemy import Integer, BigInteger, SmallInteger
from sqlalchemy import text
from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.engine.default import DefaultExecutionContext
from .compiler import MimerSQLCompiler, MimerDDLCompiler
from .types import MimerInterval

from sqlalchemy.dialects import registry
registry.register("mimer", "sqlalchemy_mimer.dialect", "MimerDialect")
registry.register("mimer.mimerpy", "sqlalchemy_mimer.dialect", "MimerDialect")

MIMER_RESERVED_WORDS = {
    # Reserved words in Mimer SQL (see https://docs.mimer.com)
    "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "AS", "ASYMMETRIC", "AT",
    "ATOMIC", "AUTHORIZATION", "BEGIN", "BETWEEN", "BOTH", "BY", "CALL",
    "CALLED", "CASE", "CAST", "CHECK", "CLOSE", "COLLATE", "COLUMN",
    "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CORRESPONDING",
    "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_PATH",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
    "DAY", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DESCRIBE",
    "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DO", "DROP", "ELSE",
    "ELSEIF", "END", "ESCAPE", "EXCEPT", "EXECUTE", "EXISTS", "EXTERNAL",
    "FALSE", "FETCH", "FIRST", "FOR", "FOREIGN", "FROM", "FULL",
    "FUNCTION", "GET", "GLOBAL", "GRANT", "GROUP", "HANDLER", "HAVING",
    "HOLD", "HOUR", "IDENTITY", "IF", "IN", "INDICATOR", "INNER", "INOUT",
    "INSERT", "INTERSECT", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN",
    "LANGUAGE", "LARGE", "LEADING", "LEAVE", "LEFT", "LIKE", "LOCAL",
    "LOCALTIME", "LOCALTIMESTAMP", "LOOP", "MATCH", "MEMBER", "METHOD",
    "MINUTE", "MODIFIES", "MODULE", "MONTH", "NATIONAL", "NATURAL", "NEW",
    "NEXT", "NO", "NOT", "NULL", "OF", "OFFSET", "OLD", "ON", "OPEN", "OR",
    "ORDER", "OUT", "OVERLAPS", "PARAMETER", "PRECISION", "PREPARE",
    "PRIMARY", "PROCEDURE", "READS", "RECURSIVE", "REFERENCES",
    "REFERENCING", "RELEASE", "REPEAT", "RESIGNAL", "RESULT", "RETURN",
    "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SCROLL",
    "SECOND", "SELECT", "SESSION_USER", "SET", "SIGNAL", "SOME",
    "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
    "START", "STATIC", "SYMMETRIC", "SYSTEM_USER", "TABLE", "THEN",
    "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TREAT",
    "TRIGGER", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UNTIL", "UPDATE",
    "USER", "USING", "VALUE", "VALUES", "VARYING", "WHEN", "WHERE",
    "WHILE", "WITH", "WITHOUT", "YEAR"
}


class MimerExecutionContext(DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        """Return next value from a Mimer SQL sequence."""
        seq_name = self.dialect.identifier_preparer.format_sequence(seq)
        self.cursor.execute(f"SELECT NEXT VALUE FOR {seq_name} FROM system.onerow")
        row = self.cursor.fetchone()
        return row[0]

    def get_lastrowid(self):
        # Return value captured by MimerPy after INSERT
        return getattr(self.cursor, "lastrowid", None)


class MimerIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect):
        super().__init__(dialect)
        # Normalize reserved words to lowercase for case-insensitive lookup
        self.reserved_words = {w.lower() for w in MIMER_RESERVED_WORDS}

    def _requires_quotes(self, value):
        # Ensure reserved words are matched case-insensitively
        if value.lower() in self.reserved_words:
            return True
        return super()._requires_quotes(value)

class MimerTypeCompiler(TypeCompiler):
    def visit_boolean(self, type_, **kw):
        return "BOOLEAN"

    def visit_integer(self, type_, **kw):
        return "INTEGER"

    def visit_big_integer(self, type_, **kw):
        return "BIGINT"

    def visit_small_integer(self, type_, **kw):
        return "SMALLINT"

    def visit_numeric(self, type_, **kw):
        if type_.precision is not None:
            if type_.scale is not None:
                return f"DECIMAL({type_.precision},{type_.scale})"
            return f"DECIMAL({type_.precision})"
        return "DECIMAL"


    def visit_float(self, type_, **kw):
        if type_.precision and type_.precision <= 53:
            return f"FLOAT({type_.precision})"
        return "DOUBLE PRECISION"

    def visit_string(self, type_, **kw):
        length = getattr(type_, "length", None)
        if length:
            return f"VARCHAR({length})"
        return "VARCHAR(255)"

    def visit_char(self, type_, **kw):
        if type_.length:
            return f"CHAR({type_.length})"
        return "CHAR(1)"

    def visit_unicode(self, type_, **kw):
        length = getattr(type_, "length", None)
        if length:
            return f"NVARCHAR({length})"
        return "NVARCHAR(255)"

    def visit_unicode_text(self, type_, **kw):
        return "NCLOB"

    def visit_text(self, type_, **kw):
        return "CLOB"

    def visit_large_binary(self, type_, **kw):
        return "BLOB"

    def visit_binary(self, type_, **kw):
        length = getattr(type_, "length", None)
        if length:
            return f"BINARY({length})"
        return "BLOB"

    def visit_varbinary(self, type_, **kw):
        length = getattr(type_, "length", None)
        if length:
            return f"VARBINARY({length})"
        return "BLOB"

    def visit_date(self, type_, **kw):
        return "DATE"

    def visit_time(self, type_, **kw):
        return "TIME"

    def visit_datetime(self, type_, **kw):
        # Mimer supports TIMESTAMP
        return "TIMESTAMP"

    def visit_uuid(self, type_, **kw):
        # Mimer SQL built-in UUID type
        return "BUILTIN.UUID"

    def visit_interval(self, type_, **kw):
        if not getattr(type_, "native", True):
            # Defer to the underlying datetime implementation when SQLAlchemy
            # requests non-native interval handling.
            return self.visit_datetime(type_.impl, **kw)

        text = "INTERVAL"
        fields = getattr(type_, "fields", None)
        day_precision = getattr(type_, "day_precision", None)
        second_precision = getattr(type_, "second_precision", None)
        precision = getattr(type_, "precision", None)

        if fields:
            text += f" {fields}"
            if second_precision is not None and "SECOND" in fields.upper():
                text += f"({second_precision})"
        else:
            if day_precision is not None and second_precision is not None:
                text += f" DAY({day_precision}) TO SECOND({second_precision})"
            else:
                if day_precision is not None:
                    text += f" DAY({day_precision})"
                if second_precision is not None:
                    text += f" SECOND({second_precision})"

        if precision is not None and "SECOND" not in (fields or "").upper():
            text += f"({precision})"

        return text

    def visit_type_decorator(self, type_, **kw):
        if isinstance(type_, sqltypes.Interval):
            return self.visit_interval(type_, **kw)
        # Delegate to the wrapped implementation so SQLAlchemy's generic
        # TypeDecorator instances render correctly.
        return self.process(type_.impl, **kw)

    visit_decimal = visit_numeric
    visit_double_precision = visit_float
    visit_TIMESTAMP = visit_datetime
    visit_DOUBLE_PRECISION = visit_float
    visit_DECIMAL = visit_numeric
    visit_NUMERIC = visit_numeric
    visit_SMALLINT = visit_small_integer
    visit_BIGINT = visit_big_integer
    visit_BOOLEAN = visit_boolean
    visit_DATE = visit_date
    visit_TIME = visit_time
    visit_CLOB = visit_text
    visit_NCLOB = visit_unicode_text
    visit_BLOB = visit_large_binary
    visit_VARBINARY = visit_varbinary
    visit_BINARY = visit_binary
    visit_NVARCHAR = visit_unicode
    visit_VARCHAR = visit_string
    visit_CHAR = visit_char
    visit_INTERVAL = visit_interval
    visit_UUID = visit_uuid

class MimerDialect(DefaultDialect):
    name = "mimer"
    driver = "mimerpy"
    paramstyle = "qmark"
    statement_compiler = MimerSQLCompiler
    ddl_compiler = MimerDDLCompiler
    type_compiler = MimerTypeCompiler
    preparer = MimerIdentifierPreparer
    execution_ctx_cls = MimerExecutionContext

    supports_native_boolean = True
    supports_native_decimal = True
    supports_sane_rowcount = True
    supports_native_uuid = True
    supports_sequences = True
    supports_native_sequences = True
    supports_sequence_per_table = False
    supports_sequence_per_column = True
    supports_sequence_execution = True
    sequences_optional = False
    supports_statement_cache = True
    supports_native_autocommit = True
    supports_sane_multi_rowcount = True
    preexecute_autoincrement_sequences = False
    supports_default_values = True
    supports_empty_insert = False
    supports_identity_columns = False
    supports_comments = True
    supports_default_metavalue = True
    default_isolation_level = "READ COMMITTED"
    ddl_implicit_commit = True
    supports_autoincrement = True
    postfetch_lastrowid = True
    implicit_returning = False
    use_insertmanyvalues = False
    supports_native_autoincrement = True
    insert_returning = False
    colspecs = {
        sqltypes.Interval: MimerInterval,
    }



    def set_isolation_level(self, connection, level):
        if level == "AUTOCOMMIT":
            connection.autocommitmode = True
        else:
            connection.autocommitmode = False

    def create_connect_args(self, url):
        """Build arguments for mimerpy.connect().

        Supports both DSN-only URLs (mimer://user:pass@mimerdb)
        and full host/port/database forms (mimer://user:pass@host:port/dbname).
        Currently host/port are accepted but ignored.
        """
        opts = dict(url.query)
        user = url.username
        password = url.password
        host = url.host
        port = url.port
        database = url.database

        # Build keyword args for mimerpy.connect()
        connect_args = []
        connect_kwargs = {"user": user, "password": password}

        # Prefer explicit dsn in query string
        dsn = opts.pop("dsn", None)

        # Fall back to path-style database name
        if not dsn:
            # SQLAlchemy sets url.database from the path segment after '/'
            # e.g. "mimer://user:pass@localhost:1360/mimerdb" → url.database == "mimerdb"
            dsn = database or host or ""

        connect_kwargs["dsn"] = dsn

        # Optionally retain host/port for future use (ignored for now)
        # if host:
        #     connect_kwargs["host"] = host
        # if port:
        #     connect_kwargs["port"] = port

        # Include any extra query parameters (e.g. autocommit=true)
        connect_kwargs.update(opts)

        return connect_args, connect_kwargs


    def _sa_type_from_decl(self, decl: str, char_len, prec, scale):
        """Map Mimer SQL type declaration to SQLAlchemy type."""
        d = decl.upper()

        # --- Integer and numeric types ---
        if d == "INTEGER":
            return sqltypes.Integer()
        if d == "BIGINT":
            return sqltypes.BigInteger()
        if d == "SMALLINT":
            return sqltypes.SmallInteger()
        if d == "BOOLEAN":
            return sqltypes.Boolean()

        # --- Date/time types ---
        if d == "DATE":
            return sqltypes.Date()
        if d == "TIME":
            return sqltypes.Time()
        if d == "TIMESTAMP":
            return sqltypes.DateTime()
        if d == "INTERVAL":
            return sqltypes.Interval()

        # --- Decimal / exact numeric types ---
        if d == "DECIMAL":
            return sqltypes.DECIMAL(precision=prec or 18, scale=scale or 0)

        # --- Character types ---
        if d == "CHARACTER":
            return sqltypes.CHAR(length=char_len or 1)
        if d == "CHARACTER VARYING":
            return sqltypes.String(length=char_len or 255)
        if d == "CHARACTER LARGE OBJECT":
            return sqltypes.Text()
        if d == "NATIONAL CHARACTER":
            return sqltypes.Unicode(length=char_len or 1)
        if d == "NATIONAL CHARACTER VARYING":
            return sqltypes.Unicode(length=char_len or 255)
        if d == "NATIONAL CHARACTER LARGE OBJECT":
            return sqltypes.UnicodeText()

        # --- Binary types ---
        if d == "BINARY":
            return sqltypes.BINARY(length=char_len or 1)
        if d == "BINARY VARYING":
            return sqltypes.VARBINARY(length=char_len or 255)
        if d == "BINARY LARGE OBJECT":
            return sqltypes.LargeBinary()

        # --- Floating-point types ---
        if d in ("REAL", "DOUBLE PRECISION", "FLOAT"):
            return sqltypes.Float(precision=prec or None)

        # --- Fallback ---
        return sqltypes.NULLTYPE


    def _resolve_schema(self, connection, schema):
        """Determine the effective schema to use for reflection.
        If no schema is provided, query the database for the current schema.
        Cache the result in connection.info for efficiency."""
        if schema:
            return schema
        if "_mimer_default_schema" in connection.info:
            return connection.info["_mimer_default_schema"]
        try:
            resolved = connection.exec_driver_sql("select current_schema from system.onerow").scalar()
        except Exception:
            resolved = connection.exec_driver_sql("select current_user from system.onerow").scalar()
        connection.info["_mimer_default_schema"] = resolved
        return resolved


    @classmethod
    def import_dbapi(cls):
        import mimerpy
        return mimerpy

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        schema = self._resolve_schema(connection, schema)
        rows = connection.exec_driver_sql(
            """
            SELECT
                COLUMN_NAME,
                UPPER(DATA_TYPE) AS DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS NULLABLE,
                COLUMN_DEFAULT,
                USER_DEFINED_TYPE_SCHEMA,
                USER_DEFINED_TYPE_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
            """,
            {"schema": schema, "table": table_name},
        ).mappings().all()

        cols = []
        for r in rows:
            # --- detect BUILTIN.UUID or other UDTs ---
            if (
                r["DATA_TYPE"] == "USER-DEFINED"
                and r["USER_DEFINED_TYPE_SCHEMA"] == "BUILTIN"
                and r["USER_DEFINED_TYPE_NAME"] == "UUID"
            ):
                sa_type = sqltypes.Uuid()
            else:
                sa_type = self._sa_type_from_decl(
                    r["DATA_TYPE"],
                    r["CHARACTER_MAXIMUM_LENGTH"],
                    r["NUMERIC_PRECISION"],
                    r["NUMERIC_SCALE"],
                )

            cols.append(
                {
                    "name": r["COLUMN_NAME"],
                    "type": sa_type,
                    "nullable": bool(r["NULLABLE"]),
                    "default": r["COLUMN_DEFAULT"],
                }
            )
        return cols


    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        schema = self._resolve_schema(connection, schema)
        res = connection.exec_driver_sql(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            """,
            {"schema": schema, "table": table_name},
        ).scalar()
        return bool(res)

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        schema = self._resolve_schema(connection, schema)

        rows = connection.exec_driver_sql(
            """
            SELECT kcu.COLUMN_NAME, tc.CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
               AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = :schema
              AND tc.TABLE_NAME = :table
            ORDER BY kcu.ORDINAL_POSITION
            """,
            {"schema": schema, "table": table_name},
        ).mappings().all()
        cols = [r["COLUMN_NAME"] for r in rows]
        name = rows[0]["CONSTRAINT_NAME"] if rows else None
        return {"constrained_columns": cols, "name": name}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        schema = self._resolve_schema(connection, schema)

        rows = connection.exec_driver_sql(
            """
            SELECT
                rc.CONSTRAINT_NAME,
                rc.UNIQUE_CONSTRAINT_SCHEMA AS REFERENCED_SCHEMA,
                rc.UNIQUE_CONSTRAINT_NAME AS REFERENCED_CONSTRAINT_NAME,
                kcu.COLUMN_NAME AS LOCAL_COLUMN,
                kcu2.TABLE_NAME AS REFERENCED_TABLE,
                kcu2.COLUMN_NAME AS REFERENCED_COLUMN
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
            ON rc.UNIQUE_CONSTRAINT_SCHEMA = kcu2.CONSTRAINT_SCHEMA
            AND rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME
            AND kcu.ORDINAL_POSITION = kcu2.ORDINAL_POSITION
            WHERE kcu.TABLE_SCHEMA = :schema
            AND kcu.TABLE_NAME = :table
            ORDER BY rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """,
            {"schema": schema, "table": table_name},
        ).mappings().all()

        fks = {}
        for r in rows:
            cname = r["CONSTRAINT_NAME"]
            fk = fks.setdefault(
                cname,
                {
                    "name": cname,
                    "constrained_columns": [],
                    "referred_schema": r["REFERENCED_SCHEMA"],
                    "referred_table": r["REFERENCED_TABLE"],
                    "referred_columns": [],
                },
            )
            fk["constrained_columns"].append(r["LOCAL_COLUMN"])
            fk["referred_columns"].append(r["REFERENCED_COLUMN"])

        return list(fks.values())

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        schema = self._resolve_schema(connection, schema)

        rows = connection.exec_driver_sql(
            """
            SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = :schema
            AND tc.TABLE_NAME = :table
            AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """,
            {"schema": schema, "table": table_name},
        ).mappings().all()

        uniques = {}
        for r in rows:
            cname = r["CONSTRAINT_NAME"]
            uc = uniques.setdefault(
                cname,
                {"name": cname, "column_names": [], "duplicates_index": False},
            )
            uc["column_names"].append(r["COLUMN_NAME"])

        return list(uniques.values())

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        """Return CHECK constraints for a given table."""
        schema = self._resolve_schema(connection, schema)

        rows = connection.exec_driver_sql(
            """
            SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
            ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = :schema
            AND tc.TABLE_NAME = :table
            AND tc.CONSTRAINT_TYPE = 'CHECK'
            ORDER BY cc.CONSTRAINT_NAME
            """,
            {"schema": schema, "table": table_name},
        ).mappings().all()

        return [
            {
                "name": r["CONSTRAINT_NAME"],
                "sqltext": r["CHECK_CLAUSE"].strip() if r["CHECK_CLAUSE"] else None,
            }
            for r in rows
        ]


    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        schema = self._resolve_schema(connection, schema)

        rows = connection.exec_driver_sql(
            """
            SELECT
                i.INDEX_NAME,
                i.IS_UNIQUE,
                ic.COLUMN_NAME,
                ic.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.EXT_INDEXES i
            JOIN INFORMATION_SCHEMA.EXT_INDEX_COLUMN_USAGE ic
            ON i.INDEX_SCHEMA = ic.INDEX_SCHEMA
            AND i.TABLE_NAME = ic.TABLE_NAME
            AND i.INDEX_NAME = ic.INDEX_NAME
            WHERE i.INDEX_SCHEMA = :schema
            AND i.TABLE_NAME = :table
            ORDER BY i.INDEX_NAME, ic.ORDINAL_POSITION
            """,
            {"schema": schema, "table": table_name},
        ).mappings().all()

        indexes = {}
        for r in rows:
            iname = r["INDEX_NAME"]
            idx = indexes.setdefault(
                iname,
                {"name": iname, "column_names": [], "unique": r["IS_UNIQUE"] == "YES"},
            )
            idx["column_names"].append(r["COLUMN_NAME"])

        return list(indexes.values())

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of user table names for the given schema."""
        schema = self._resolve_schema(connection, schema)

        query = text("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema
            AND TABLE_TYPE = 'BASE TABLE'
        """)
        rows = connection.execute(query, {"schema": schema}).fetchall()
        return [r[0] for r in rows]

    @reflection.cache
    def get_domains(self, connection, schema=None, **kw):
        """Return a list of user-defined domains for the given schema."""
        schema = self._resolve_schema(connection, schema)

        rows = connection.exec_driver_sql(
            """
            SELECT
                DOMAIN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DOMAIN_DEFAULT
            FROM INFORMATION_SCHEMA.DOMAINS
            WHERE DOMAIN_SCHEMA = :schema
            ORDER BY DOMAIN_NAME
            """,
            {"schema": schema},
        ).mappings().all()

        domains = []
        for r in rows:
            domains.append(
                {
                    "name": r["DOMAIN_NAME"],
                    "type": self._sa_type_from_decl(
                        r["DATA_TYPE"],
                        r["CHARACTER_MAXIMUM_LENGTH"],
                        r["NUMERIC_PRECISION"],
                        r["NUMERIC_SCALE"],
                    ),
                    "nullable": r.get("IS_NULLABLE", "YES") == "YES",
                    "default": r["DOMAIN_DEFAULT"],
                }
            )

        return domains

    @reflection.cache
    def get_schema_names(self, connection, include_system: bool = True, **kw):
        """Return a list of available schema names in the current database.

        Args:
            connection: SQLAlchemy connection object.
            include_system (bool): If False, excludes system/internal schemas
                like INFORMATION_SCHEMA and SYSTEM. Defaults to True.
        """
        rows = connection.exec_driver_sql(
            """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            ORDER BY SCHEMA_NAME
            """
        ).fetchall()

        schemas = [r[0] for r in rows if r[0]]

        if not include_system:
            system_schemas = {"MIMER", "ODBC"}
            schemas = [s for s in schemas if s.upper() not in system_schemas]

        return schemas

    @reflection.cache
    def get_default_schema_name(self, connection):
        """Return the default schema name for the current connection."""
        return self._resolve_schema(connection, None)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of view names for the given schema."""
        schema = self._resolve_schema(connection, schema)

        query = text("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = :schema
        """)
        rows = connection.execute(query, {"schema": schema}).fetchall()
        return [r[0] for r in rows]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """Return the SQL definition text for a given view."""
        schema = self._resolve_schema(connection, schema)

        query = text("""
            SELECT VIEW_DEFINITION
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :view_name
        """)
        result = connection.execute(query, {"schema": schema, "view_name": view_name}).scalar()
        return result


    def _run_autocommit_ddl(self, cursor, statement, parameters):
        conn = cursor.connection
        if conn._transaction:
            conn.rollback()
        old_mode = conn.autocommitmode
        conn.autocommitmode = True
        try:
            cursor.execute(statement, parameters or ())
        finally:
            conn.autocommitmode = old_mode

    def do_execute(self, cursor, statement, parameters, context=None):
        if statement.lstrip().upper().startswith(("CREATE ", "DROP ", "ALTER ")):
            self._run_autocommit_ddl(cursor, statement, parameters)
        else:
            cursor.execute(statement, parameters or ())

    def do_execute_ddl(self, cursor, statement, parameters, context=None):
        self._run_autocommit_ddl(cursor, statement, parameters)


    def get_pk_sequence(self, column):
        """Return a SQLAlchemy Sequence for a PK integer column."""
        seq_name = f"{column.table.name}_{column.name}_autoinc_seq"
        seq_schema = column.table.schema
        return Sequence(seq_name, schema=seq_schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        """Return True if the named sequence exists in the given or current schema."""
        params = {"name": sequence_name.upper()}
        schema = self._resolve_schema(connection, schema)

        query = text("""
            SELECT SEQUENCE_NAME
            FROM INFORMATION_SCHEMA.SEQUENCES
            WHERE SEQUENCE_SCHEMA = :schema
            AND SEQUENCE_NAME = :name
        """)
        params["schema"] = schema.upper()

        result = connection.execute(query, params)
        return result.scalar() is not None

    def before_create_table(target, connection, **kw):
        # Resolve the correct schema (CURRENT_USER or explicit)
        schema = target.schema
        dialect = connection.dialect
        resolved_schema = dialect._resolve_schema(connection, schema)

        preparer = dialect.identifier_preparer

        for col in target.columns:
            if (
                col.primary_key
                and col.autoincrement
                and isinstance(col.type, (Integer, BigInteger, SmallInteger))
            ):
                seq_name = f"{col.table.name}_{col.name}_autoinc_seq"
                if not dialect.has_sequence(connection, seq_name, schema=resolved_schema):
                    seq = Sequence(seq_name, schema=resolved_schema)
                    qualified = preparer.format_sequence(seq, use_schema=True)
                    connection.execute(
                        text(f"CREATE SEQUENCE {qualified} AS BIGINT NO CYCLE")
                    )


    def after_drop_table(target, connection, **kw):
        """Drop sequences associated with autoincrement columns, if Mimer didn't."""
        dialect = connection.dialect
        schema = target.schema
        resolved_schema = dialect._resolve_schema(connection, schema)

        preparer = dialect.identifier_preparer

        for col in target.columns:
            if col.primary_key and col.autoincrement:
                seq_name = f"{col.table.name}_{col.name}_autoinc_seq"
                # only drop if it still exists
                if dialect.has_sequence(connection, seq_name, schema=resolved_schema):
                    seq = Sequence(seq_name, schema=resolved_schema)
                    qualified = preparer.format_sequence(seq, use_schema=True)
                    connection.execute(text(f"DROP SEQUENCE {qualified}"))

    # Attach to Table events
    event.listen(Table, "before_create", before_create_table)
    event.listen(Table, "after_drop", after_drop_table)
