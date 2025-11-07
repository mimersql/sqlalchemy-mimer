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
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String
import db_config


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
        self.table = Table("vw_test_base", self.meta,
                           Column("id", Integer, primary_key=True),
                           Column("name", String(40)))
        self.meta.create_all(self.eng)

    def tearDown(self):
        with self.eng.begin() as conn:
            conn.execute(text("DROP VIEW vw_test_view")) if self.has_view(conn) else None
        self.meta.drop_all(self.eng, checkfirst=True)

    def has_view(self, conn):
        res = conn.execute(text("""
            SELECT 1 FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = CURRENT_USER AND TABLE_NAME = 'VW_TEST_VIEW'
        """)).scalar()
        return bool(res)

    def test_create_and_reflect_view(self):
        with self.eng.begin() as conn:
            conn.execute(text("CREATE VIEW vw_test_view AS SELECT id, name FROM vw_test_base"))
        with self.eng.connect() as conn:
            views = self.eng.dialect.get_view_names(conn)
            self.assertIn("VW_TEST_VIEW", [v.upper() for v in views])
            definition = self.eng.dialect.get_view_definition(conn, "VW_TEST_VIEW")
            self.assertIn("SELECT", definition.upper())

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()