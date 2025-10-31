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
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy import ForeignKey, inspect
from sqlalchemy.schema import CreateTable
import unittest
import db_config
from test_utils import normalize_sql

class TestConstraint(unittest.TestCase):
    url = db_config.make_tst_uri()
    verbose = __name__ == "__main__"

    @classmethod
    def setUpClass(self):
        db_config.setup()

    @classmethod
    def tearDownClass(self):
        db_config.teardown()

    def tearDown(self):
        pass

    def test_basic_constraint(self):
        eng = create_engine(self.url, echo=self.verbose, future=True)
        meta = MetaData()

        parent = Table("parent", meta,
                    Column("id", Integer, primary_key=True),
                    Column("name", String(20)))

        child = Table("child", meta,
                    Column("id", Integer, primary_key=True),
                    Column("parent_id", Integer, ForeignKey("parent.id")),
                    Column("value", String(20)))

        parent_sql = str(CreateTable(parent).compile(dialect=eng.dialect))
        child_sql = str(CreateTable(child).compile(dialect=eng.dialect))
        p_normalized = normalize_sql(parent_sql)
        c_normalized = normalize_sql(child_sql)
        self.assertEqual(p_normalized,
                         'CREATE TABLE parent ( id INTEGER DEFAULT NEXT VALUE FOR parent_id_autoinc_seq, name VARCHAR(20), PRIMARY KEY (id) )')
        self.assertEqual(c_normalized, 
                         'CREATE TABLE child ( id INTEGER DEFAULT NEXT VALUE FOR child_id_autoinc_seq, parent_id INTEGER, "value" VARCHAR(20), PRIMARY KEY (id), FOREIGN KEY(parent_id) REFERENCES parent (id) )')

        with eng.begin() as conn:
            meta.create_all(conn)
            insp = inspect(conn)
            tables = insp.get_table_names()
            self.assertEqual(tables, ['child', 'parent'])
            parent_pk = insp.get_pk_constraint("parent")
            pk_col = parent_pk.get('constrained_columns')[0]
            self.assertEqual(pk_col, 'id')
            child_pk = insp.get_pk_constraint("child")
            pk_col = child_pk.get('constrained_columns')[0]
            self.assertEqual(pk_col, 'id')
            child_fk = insp.get_foreign_keys("child")
            fk_col = child_fk[0].get('constrained_columns')[0]
            self.assertEqual(fk_col, 'parent_id')
            if self.verbose:
                print("Tables:", tables)
                print("PKs on parent:", parent_pk)
                print("PKs on child:", child_pk)
                print("FKs on child:", child_fk)
            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
