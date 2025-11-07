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
import unittest
from sqlalchemy import (
    create_engine,
    text,
    Sequence,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    inspect,
    select,
)
import db_config
from sqlalchemy_mimer.dialect import MimerDialect


class TestSequences(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"

    @classmethod
    def setUpClass(self):
        db_config.setup()

    @classmethod
    def tearDownClass(self):
        db_config.teardown()

    def setUp(self):
        self.eng = create_engine(self.url, echo=self.verbose, future=True)
        self.meta = MetaData()

    def tearDown(self):
        self.meta.drop_all(self.eng, checkfirst=True)
        

    def test_manual_sequence(self):
        seq = Sequence("seq_manual_test")
        with self.eng.begin() as conn:
            conn.execute(text("CREATE SEQUENCE seq_manual_test AS BIGINT"))
            val1 = conn.scalar(seq)
            val2 = conn.scalar(seq)
            self.assertEqual(val2, val1 + 1)
            conn.execute(text("DROP SEQUENCE seq_manual_test"))

    def test_manual_sequence_via_compiler(self):
        seq = Sequence("seq_manual_compiled")
        with self.eng.begin() as conn:
            seq.create(bind=conn)
            try:
                val1 = conn.scalar(seq)
                val2 = conn.scalar(seq)
                self.assertEqual(val2, val1 + 1)
            finally:
                seq.drop(bind=conn)

    def test_autoincrement_sequence_created(self):
        users = Table(
            "seq_users", self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(40))
        )
        self.meta.create_all(self.eng)
        with self.eng.connect() as conn:
            self.assertTrue(self.eng.dialect.has_sequence(conn, "SEQ_USERS_ID_AUTOINC_SEQ"))

    def test_explicit_sequence_default_schema(self):
        named_seq = Sequence("explicit_seq_default")
        t = Table(
            "explicit_seq_default", self.meta,
            Column("id", Integer, named_seq, primary_key=True),
            Column("label", String(40)),
        )

        with self.eng.begin() as conn:
            conn.execute(text("CREATE SEQUENCE explicit_seq_default AS BIGINT"))
            self.meta.create_all(conn)
            conn.execute(t.insert(), [{"label": "one"}, {"label": "two"}])
            rows = conn.execute(select(t).order_by(t.c.id)).all()
            self.assertEqual([row._mapping["id"] for row in rows], [1, 2])
            self.assertEqual([row._mapping["label"] for row in rows], ["one", "two"])
            self.meta.drop_all(conn)

    def test_explicit_sequence_custom_schema(self):
        named_seq = Sequence("explicit_seq_myschema", schema="myschema")
        t = Table(
            "explicit_seq_myschema", self.meta,
            Column("id", Integer, named_seq, primary_key=True),
            Column("label", String(40)),
            schema="myschema",
        )

        with self.eng.begin() as conn:
            conn.execute(text("CREATE SEQUENCE myschema.explicit_seq_myschema AS BIGINT"))
            self.meta.create_all(conn)
            conn.execute(t.insert(), [{"label": "alpha"}, {"label": "beta"}])
            rows = conn.execute(select(t).order_by(t.c.id)).all()
            self.assertEqual([row._mapping["id"] for row in rows], [1, 2])
            self.assertEqual([row._mapping["label"] for row in rows], ["alpha", "beta"])
            self.meta.drop_all(conn)


if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
