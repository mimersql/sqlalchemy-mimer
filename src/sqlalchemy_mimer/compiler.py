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
from sqlalchemy.sql.compiler import SQLCompiler, DDLCompiler
from sqlalchemy import Sequence, Integer, SmallInteger, BigInteger

class MimerSQLCompiler(SQLCompiler):
    """Compiler for Mimer SQL dialect."""

    def visit_sequence(self, sequence, **kw):
        preparer = self.dialect.identifier_preparer
        seq_text = preparer.format_sequence(sequence, use_schema=True)
        return f"NEXT VALUE FOR {seq_text}"

    def visit_current_timestamp_func(self, fn, **kw):
        # Mimer SQL uses LOCALTIMESTAMP instead of CURRENT_TIMESTAMP
        return "LOCALTIMESTAMP"

    def visit_current_time_func(self, fn, **kw):
        return "LOCALTIME"

    def limit_clause(self, select, **kw):
        # SQL standard style OFFSET / FETCH (preferred form)
        text = ""
        if select._offset is not None:
            text += f" OFFSET {select._offset} ROWS"
        if select._limit is not None:
            text += f" FETCH FIRST {select._limit} ROWS ONLY"
        return text



class MimerDDLCompiler(DDLCompiler):
    """DDL Compiler for Mimer SQL dialect."""
    def get_column_default_string(self, column):
        default = column.default
        # Handle Sequence defaults for autoincrementing columns
        if isinstance(default, Sequence):
            seq_name = self.preparer.format_sequence(default, use_schema=True)
            return f"NEXT VALUE FOR {seq_name}"
        # Fall back to SQLAlchemy’s default handling
        return super().get_column_default_string(column)


    def get_column_specification(self, column, **kw):
        # Bas: kolumnnamn + typ
        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(column.type, **kw)

        # 1) Explicit Sequence på kolumnen → använd den
        default = column.default
        if isinstance(default, Sequence):
            seq_name = self.preparer.format_sequence(default, use_schema=True)
            colspec += f" DEFAULT NEXT VALUE FOR {seq_name}"
            return colspec

        # 2) Respektera server_default om satt (t.ex. text('NEXT VALUE FOR ...') eller func.current_timestamp())
        if column.server_default is not None:
            default_expr = self.get_column_default_string(column)
            if default_expr:
                colspec += f" DEFAULT {default_expr}"
            return colspec

        # 3) Implicit autoincrement för PK av heltalstyp utan explicit default
        if (
            column.primary_key
            and getattr(column, "autoincrement", True)  # default i SA är "auto"
            and column.default is None
            and isinstance(column.type, (Integer, BigInteger, SmallInteger))
        ):
            # matchar namnschemat du använder i before_create_table
            seq_name = f"{column.table.name}_{column.name}_autoinc_seq"
            seq_schema = column.table.schema
            seq = Sequence(seq_name, schema=seq_schema)
            qualified_name = self.preparer.format_sequence(seq, use_schema=True)
            # här *renderar* vi bara DEFAULT …; vi ändrar inte column.default
            # (så before_create_table kan skapa sekvensen utan sidoeffekter)
            colspec += f" DEFAULT NEXT VALUE FOR {qualified_name}"

        return colspec


    def visit_create_domain_type(self, create, **kw):
        """
        Generate a CREATE DOMAIN statement for Mimer SQL.
        SQLAlchemy does not yet expose CreateDomain for external dialects.
        """
        domain = create.element
        opts = []

        # COLLATE clause (Mimer SQL supports standard collations)
        if getattr(domain, "collation", None) is not None:
            opts.append(f"COLLATE {self.preparer.quote(domain.collation)}")

        # DEFAULT clause
        if getattr(domain, "default", None) is not None:
            # Render literal or SQL expression for the default
            default_val = self.sql_compiler.render_literal_value(domain.default.arg, domain.data_type)
            opts.append(f"DEFAULT {default_val}")

        # CHECK constraint (Mimer SQL supports standard CHECK)
        if getattr(domain, "check", None) is not None:
            check_sql = self.sql_compiler.process(domain.check, literal_binds=True)
            opts.append(f"CHECK ({check_sql})")

        # NOT NULL (Mimer SQL allows NOT NULL on domains)
        if getattr(domain, "not_null", False):
            opts.append("NOT NULL")

        # Compose final CREATE DOMAIN statement
        sql = (
            f"CREATE DOMAIN {self.preparer.format_type(domain)} AS "
            f"{self.type_compiler.process(domain.data_type)} "
        )
        if opts:
            sql += " ".join(opts)

        return sql.strip()

    def visit_drop_domain_type(self, drop, **kw):
        """Generate a DROP DOMAIN statement for Mimer SQL."""
        domain = drop.element
        return f"DROP DOMAIN {self.preparer.format_type(domain)} RESTRICT"
