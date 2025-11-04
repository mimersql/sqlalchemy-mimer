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
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, inspect
import unittest
import db_config


class TestSchema(unittest.TestCase):
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

    def test_schema(self):
        eng = create_engine(self.url, echo=self.verbose, future=True)
        meta = MetaData()

        t = Table("meta_demo", meta,
                Column("id", Integer, primary_key=True),
                Column("name", String(40)))

        with eng.begin() as conn:
            meta.create_all(conn)
            insp = inspect(conn)
            tables = insp.get_table_names()
            self.assertListEqual(tables, ['meta_demo'])
            cols = insp.get_columns("meta_demo")
            expected_cols_v110 = [{'name': 'id', 'type': Integer(), 'nullable': False, 'default': 'NEXT_VALUE OF "SQLALCHEMY"."meta_demo_id_autoinc_seq"'}, {'name': 'name', 'type': String(length=40), 'nullable': True, 'default': None}]
            expected_cols_v111 = [{'name': 'id', 'type': Integer(), 'nullable': False, 'default': 'NEXT VALUE FOR "SQLALCHEMY"."meta_demo_id_autoinc_seq"'}, {'name': 'name', 'type': String(length=40), 'nullable': True, 'default': None}]
            self.assertIn( str(cols), (str(expected_cols_v110), str(expected_cols_v111)))
            if self.verbose:
                print("Tables:", tables)
                print("Columns:", cols)
            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
