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
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, update, delete
import unittest
import db_config
from test_utils import normalize_sql


class TestDML(unittest.TestCase):
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

    def test_basic_dml(self):
        eng = create_engine(self.url, echo=self.verbose, future=True)
        meta = MetaData()

        users = Table(
            "users", meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(50)),
        )
        from sqlalchemy.schema import CreateTable
        from sqlalchemy_mimer.dialect import MimerDialect  # adjust import path if needed

        sql = str(CreateTable(users).compile(dialect=MimerDialect()))
        if self.verbose:
            print(sql)
        normalized = normalize_sql(sql)
        self.assertEqual(normalized,'CREATE TABLE users ( id INTEGER DEFAULT NEXT VALUE FOR users_id_autoinc_seq, name VARCHAR(50), PRIMARY KEY (id) )')
        with eng.begin() as conn:
            try:
                conn.execute(delete(users))
                conn.execute("drop table users")
            except Exception:
                pass
            meta.create_all(conn)
            conn.execute(users.insert(), [{"name": "Alice"}, {"name": "Bob"}])

            rows = conn.execute(select(users)).all()
            if self.verbose:
                print("Users:", rows)

            conn.execute(update(users).where(users.c.name == "Bob").values(name="Robert"))
            conn.execute(delete(users).where(users.c.name == "Alice"))
            rows = conn.execute(select(users)).all()
            if self.verbose:
                print("After changes:", rows)

            if self.verbose:
                print("Dropping table")
            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()