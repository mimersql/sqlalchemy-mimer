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
from sqlalchemy import create_engine, text, Sequence, MetaData, Table, Column, Integer, String, inspect
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

    def test_autoincrement_sequence_created(self):
        users = Table(
            "seq_users", self.meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(40))
        )
        self.meta.create_all(self.eng)
        with self.eng.connect() as conn:
            self.assertTrue(self.eng.dialect.has_sequence(conn, "SEQ_USERS_ID_AUTOINC_SEQ"))


if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()