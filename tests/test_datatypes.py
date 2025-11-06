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
from datetime import date, datetime, time
import uuid as UUID
from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    Unicode,
    UnicodeText,
    Uuid,
    create_engine,
    insert,
    select,
    types as sqltypes
)
from sqlalchemy.schema import CreateTable
from sqlalchemy_mimer.dialect import MimerDialect
from sqlalchemy_mimer.types import MimerInterval
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

    def test_all_supported_datatypes_compile(self):
        eng = create_engine(self.url, echo=self.verbose, future=True)
        meta = MetaData()
        data_types = Table(
            "datatype_table",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("big_val", BigInteger()),
            Column("small_val", SmallInteger()),
            Column("numeric_val", Numeric(10, 2)),
            Column("numeric_no_scale", Numeric(12)),
            Column("float_precise", Float(10)),
            Column("float_double", Float(54)),
            Column("string_val", String(120)),
            Column("string_default", String()),
            Column("char_val", CHAR(3)),
            Column("text_val", Text()),
            Column("unicode_val", Unicode(100)),
            Column("unicode_text_val", UnicodeText()),
            Column("binary_val", LargeBinary()),
            Column("fixed_binary_val", sqltypes.BINARY(16)),
            Column("varbinary_val", sqltypes.VARBINARY(32)),
            Column("boolean_val", Boolean()),
            Column("date_val", Date()),
            Column("time_val", Time()),
            Column("datetime_val", DateTime()),
            Column("interval_day_5", sqltypes.Interval(day_precision=5)),
            Column("interval_second_4", sqltypes.Interval(second_precision=4)),            
            Column("interval_day_5_to_second_2", sqltypes.Interval(day_precision=5, second_precision=2)),
            Column("interval_year", MimerInterval(fields="YEAR")),
            Column("interval_year_2", MimerInterval(fields="YEAR", precision=2)),
            Column("interval_year_to_month", MimerInterval(fields="YEAR TO MONTH")),
            Column("interval_day_to_second", MimerInterval(fields="DAY TO SECOND", second_precision=5)),     
            Column("uuid_val", sqltypes.Uuid()),
        )
        compiled_sql = str(CreateTable(data_types).compile(dialect=MimerDialect()))
        normalized = normalize_sql(compiled_sql)
        expected_sql = (
            "CREATE TABLE datatype_table ( "
            "id INTEGER DEFAULT NEXT VALUE FOR datatype_table_id_autoinc_seq, "
            "big_val BIGINT, "
            "small_val SMALLINT, "
            "numeric_val DECIMAL(10,2), "
            "numeric_no_scale DECIMAL(12), "
            "float_precise FLOAT(10), "
            "float_double DOUBLE PRECISION, "
            "string_val VARCHAR(120), "
            "string_default VARCHAR(255), "
            "char_val CHAR(3), "
            "text_val CLOB, "
            "unicode_val NVARCHAR(100), "
            "unicode_text_val NCLOB, "
            "binary_val BLOB, "
            "fixed_binary_val BINARY(16), "
            "varbinary_val VARBINARY(32), "
            "boolean_val BOOLEAN, "
            "date_val DATE, "
            "time_val TIME, "
            "datetime_val TIMESTAMP, "
            "interval_day_5 INTERVAL DAY(5), "
            "interval_second_4 INTERVAL SECOND(4), "
            "interval_day_5_to_second_2 INTERVAL DAY(5) TO SECOND(2), "
            "interval_year INTERVAL YEAR, "
            "interval_year_2 INTERVAL YEAR(2), "
            "interval_year_to_month INTERVAL YEAR TO MONTH, "
            "interval_day_to_second INTERVAL DAY TO SECOND(5), "
            "uuid_val BUILTIN.UUID, "
            "PRIMARY KEY (id) "
            ")"
        )
        self.assertEqual(normalized, expected_sql)
        with eng.begin() as conn:
            meta.create_all(conn)
            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
