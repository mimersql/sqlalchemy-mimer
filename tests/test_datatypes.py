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
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, Time, Date, DateTime, String, Uuid, insert, select
from datetime import date, datetime, time
import uuid as UUID
from sqlalchemy.schema import CreateTable
import unittest
import db_config
from test_utils import normalize_sql

class TestDatatypes(unittest.TestCase):
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

    def test_basic_datatypes(self):
        eng = create_engine(self.url, echo=self.verbose, future=True)
        meta = MetaData()
        t = Table("types_test", meta,
                Column("id", Integer, primary_key=True),
                Column("val_int", Integer),
                Column("val_float", Float),
                Column("val_date", Date),
                Column("val_ts", DateTime),
                Column("val_time", Time),
                Column("val_str", String(40)),
                Column("val_uuid", Uuid))

        sql = str(CreateTable(t).compile(dialect=eng.dialect))
        nsql = normalize_sql(sql)
        self.assertEqual(nsql,
                         'CREATE TABLE types_test ( id INTEGER DEFAULT NEXT VALUE FOR types_test_id_autoinc_seq, val_int INTEGER, val_float DOUBLE PRECISION, val_date DATE, val_ts TIMESTAMP, val_time TIME, val_str VARCHAR(40), val_uuid BUILTIN.UUID, PRIMARY KEY (id) )')
  
        with eng.begin() as conn:
            meta.create_all(conn)
            conn.execute(insert(t), [{
                "val_int": 42,
                "val_float": 3.1415,
                "val_date": date.today(),
                "val_ts": datetime.now(),
                "val_time": time(14, 30, 0),
                "val_str": "Hello Mimer",
                "val_uuid": UUID.uuid4(),
            }])
            if self.verbose:
                print(conn.execute(select(t)).first())
            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
