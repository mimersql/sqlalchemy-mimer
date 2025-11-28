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
from sqlalchemy import create_engine, text
import unittest
import db_config


class TestTransaction(unittest.TestCase):
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

    def test_transaction(self):
        with self.eng.begin() as conn:
            conn.execute(text("create table tx_test (id integer primary key, val varchar(20))"))

        with self.eng.connect() as conn:
            trans = conn.begin()
            res = conn.execute(text("insert into tx_test values (1, 'A')"))
            self.assertEqual(res.rowcount, 1)
            trans.rollback()

        with self.eng.connect() as conn:
            result = conn.execute(text("select count(*) from tx_test")).scalar()
            self.assertEqual(result, 0)
            if self.verbose:
                print("After rollback:", result)

            res = conn.execute(text("insert into tx_test values (2, 'B')"))
            self.assertEqual(res.rowcount, 1)
            conn.commit()
            result = conn.execute(text("select count(*) from tx_test")).scalar()
            self.assertEqual(result, 1)
            if self.verbose:
                print("After commit:", result)

        with self.eng.begin() as conn:
            conn.execute(text("drop table tx_test"))

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
