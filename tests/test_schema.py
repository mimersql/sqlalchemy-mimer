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
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    inspect,
    select,
    Sequence,
)
import unittest
import db_config


class TestSchema(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"
    eng = None

    @classmethod
    def setUpClass(self):
        db_config.setup()
        self.eng = create_engine(self.url, echo=self.verbose, future=True)

    @classmethod
    def tearDownClass(self):
        if self.eng is not None:
            self.eng.dispose()
            self.eng = None
        db_config.teardown()

    def tearDown(self):
        pass

    def test_schema(self):
        meta = MetaData()

        t = Table("meta_demo", meta,
                Column("id", Integer, primary_key=True),
                Column("name", String(40)), schema="myschema")

        with self.eng.begin() as conn:
            meta.create_all(conn)
            insp = inspect(conn)
            tables = insp.get_table_names(schema="myschema")
            self.assertIn('meta_demo', tables)
            cols = insp.get_columns("meta_demo", schema="myschema")
            expected_cols_v110 = [{'name': 'id', 'type': Integer(), 'nullable': False, 'default': 'NEXT_VALUE OF "myschema"."meta_demo_id_autoinc_seq"'}, {'name': 'name', 'type': String(length=40), 'nullable': True, 'default': None}]
            expected_cols_v111 = [{'name': 'id', 'type': Integer(), 'nullable': False, 'default': 'NEXT VALUE FOR "myschema"."meta_demo_id_autoinc_seq"'}, {'name': 'name', 'type': String(length=40), 'nullable': True, 'default': None}]
            self.assertIn( str(cols), (str(expected_cols_v110), str(expected_cols_v111)))

            conn.execute(t.insert(), [{"name": "alpha"}, {"name": "beta"}])
            rows = conn.execute(select(t).order_by(t.c.id)).all()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]._mapping["name"], "alpha")
            self.assertEqual(rows[1]._mapping["name"], "beta")
            self.assertEqual(rows[0]._mapping["id"], 1)
            self.assertEqual(rows[1]._mapping["id"], 2)

            if self.verbose:
                print("Tables:", tables)
                print("Columns:", cols)
                print("Rows:", rows)
            meta.drop_all(conn)

    def test_schema_sequence_cleanup(self):
        meta = MetaData()

        t = Table(
            "meta_cleanup",
            meta,
            Column("id", Integer, primary_key=True),
            Column("name", String(40)),
            schema="myschema",
        )

        seq_name = f"{t.name}_id_autoinc_seq"

        with self.eng.begin() as conn:
            meta.create_all(conn)
            meta.drop_all(conn)
            seq_exists = self.eng.dialect.has_sequence(conn, seq_name, schema="myschema")
            self.assertFalse(seq_exists, f"Sequence {seq_name} should be dropped")

    def test_default_schema_roundtrip(self):
        meta = MetaData()

        t = Table(
            "default_demo",
            meta,
            Column("id", Integer, primary_key=True),
            Column("note", String(40)),
        )

        with self.eng.begin() as conn:
            default_schema = self.eng.dialect.get_default_schema_name(conn)
            meta.create_all(conn)
            conn.execute(t.insert(), [{"note": "plain"}])
            rows = conn.execute(select(t).order_by(t.c.id)).all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]._mapping["note"], "plain")
            self.assertEqual(rows[0]._mapping["id"], 1)

            tables = inspect(conn).get_table_names(schema=default_schema)
            self.assertIn(t.name, tables)
            meta.drop_all(conn)

    def test_metadata_schema_attribute_roundtrip(self):
        meta = MetaData(schema="myschema")

        t = Table(
            "meta_schema_demo",
            meta,
            Column("id", Integer, primary_key=True),
            Column("note", String(40)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            conn.execute(t.insert(), [{"note": "scoped"}])
            rows = conn.execute(select(t)).all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]._mapping["note"], "scoped")
            tables = inspect(conn).get_table_names(schema="myschema")
            self.assertIn("meta_schema_demo", tables)
            meta.drop_all(conn)

    def test_explicit_sequence_schema_usage(self):
        meta = MetaData()

        named_seq = Sequence("explicit_id_seq", schema="myschema")
        t = Table(
            "explicit_seq_demo",
            meta,
            Column("id", Integer, named_seq, primary_key=True),
            Column("label", String(40)),
            schema="myschema",
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            conn.execute(t.insert(), [{"label": "first"}, {"label": "second"}])
            rows = conn.execute(select(t).order_by(t.c.id)).all()
            self.assertEqual([row._mapping["label"] for row in rows], ["first", "second"])
            self.assertEqual([row._mapping["id"] for row in rows], [1, 2])

            if self.verbose:
                print("Explicit seq rows:", rows)

            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
