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
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import math
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
    select,
    text,
    insert,
    types as sqltypes,
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

    def test_basic_datatypes(self):
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

        sql = str(CreateTable(t).compile(dialect=self.eng.dialect))
        nsql = normalize_sql(sql)
        self.assertEqual(nsql,
                         'CREATE TABLE types_test ( id INTEGER DEFAULT NEXT VALUE FOR types_test_id_autoinc_seq, val_int INTEGER, val_float DOUBLE PRECISION, val_date DATE, val_ts TIMESTAMP, val_time TIME, val_str VARCHAR(40), val_uuid BUILTIN.UUID, PRIMARY KEY (id) )')

        with self.eng.begin() as conn:
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
        with self.eng.begin() as conn:
            meta.create_all(conn)
            meta.drop_all(conn)

    def test_numeric(self):
        meta = MetaData()
        numeric_table = Table(
            "numeric_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("int_val", Integer),
            Column("big_val", BigInteger()),
            Column("small_val", SmallInteger()),
            Column("numeric_val", Numeric(10, 2)),
            Column("numeric_no_scale", Numeric(12)),
            Column("float_precise", Float(10)),
            Column("float_double", Float(54)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "int_val": 123,
                "big_val": 9876543210,
                "small_val": 321,
                "numeric_val": Decimal("12345.67"),
                "numeric_no_scale": Decimal("4321"),
                "float_precise": 1.2345,
                "float_double": 9.87654321,
            }
            conn.execute(numeric_table.insert().values(**values))
            row = conn.execute(select(numeric_table)).one()._mapping

            self.assertEqual(row["int_val"], values["int_val"])
            self.assertEqual(row["big_val"], values["big_val"])
            self.assertEqual(row["small_val"], values["small_val"])
            self.assertEqual(Decimal(row["numeric_val"]), values["numeric_val"])
            self.assertEqual(Decimal(row["numeric_no_scale"]), values["numeric_no_scale"])
            self.assertAlmostEqual(row["float_precise"], values["float_precise"], places=7)
            self.assertAlmostEqual(row["float_double"], values["float_double"], places=7)

            meta.drop_all(conn)

    def test_boolean_roundtrip(self):
        meta = MetaData()
        boolean_table = Table(
            "boolean_roundtrip",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("flag", Boolean),
            Column("label", String(20)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            conn.execute(
                boolean_table.insert(),
                [
                    {"flag": True, "label": "t"},
                    {"flag": False, "label": "f"},
                    {"flag": None, "label": "n"},
                ],
            )
            rows = conn.execute(select(boolean_table).order_by(boolean_table.c.id)).all()
            self.assertEqual([row._mapping["flag"] for row in rows], [True, False, None])
            self.assertEqual([row._mapping["label"] for row in rows], ["t", "f", "n"])
            meta.drop_all(conn)

    def test_numeric_zero_values(self):
        meta = MetaData()
        zero_table = Table(
            "numeric_zero_values",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("int_val", Integer),
            Column("big_val", BigInteger()),
            Column("small_val", SmallInteger()),
            Column("numeric_val", Numeric(10, 2)),
            Column("numeric_no_scale", Numeric(12)),
            Column("float_precise", Float(10)),
            Column("float_double", Float(54)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "int_val": 0,
                "big_val": 0,
                "small_val": 0,
                "numeric_val": Decimal("0.00"),
                "numeric_no_scale": Decimal("0"),
                "float_precise": 0.0,
                "float_double": 0.0,
            }
            conn.execute(zero_table.insert().values(**values))
            row = conn.execute(select(zero_table)).one()._mapping

            self.assertEqual(row["int_val"], 0)
            self.assertEqual(row["big_val"], 0)
            self.assertEqual(row["small_val"], 0)
            self.assertEqual(Decimal(row["numeric_val"]), values["numeric_val"])
            self.assertEqual(Decimal(row["numeric_no_scale"]), values["numeric_no_scale"])
            self.assertAlmostEqual(row["float_precise"], values["float_precise"], places=7)
            self.assertAlmostEqual(row["float_double"], values["float_double"], places=7)

            meta.drop_all(conn)

    def test_numeric_negative_values(self):
        meta = MetaData()
        negative_table = Table(
            "numeric_negative_values",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("int_val", Integer),
            Column("big_val", BigInteger()),
            Column("small_val", SmallInteger()),
            Column("numeric_val", Numeric(10, 2)),
            Column("numeric_no_scale", Numeric(12)),
            Column("float_precise", Float(10)),
            Column("float_double", Float(54)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "int_val": -1,
                "big_val": -9223372036854775808,
                "small_val": -32768,
                "numeric_val": Decimal("-99999999.99"),
                "numeric_no_scale": Decimal("-999999999999"),
                "float_precise": -12345.6789,
                "float_double": -9876543210.12345,
            }
            conn.execute(negative_table.insert().values(**values))
            row = conn.execute(select(negative_table)).one()._mapping

            self.assertEqual(row["int_val"], values["int_val"])
            self.assertEqual(row["big_val"], values["big_val"])
            self.assertEqual(row["small_val"], values["small_val"])
            self.assertEqual(Decimal(row["numeric_val"]), values["numeric_val"])
            self.assertEqual(Decimal(row["numeric_no_scale"]), values["numeric_no_scale"])
            self.assertTrue(
                math.isclose(row["float_precise"], values["float_precise"], rel_tol=1e-9)
            )
            self.assertTrue(
                math.isclose(row["float_double"], values["float_double"], rel_tol=1e-15)
            )

            meta.drop_all(conn)

    def test_numeric_precision_limits(self):
        meta = MetaData()
        precision_table = Table(
            "numeric_precision_limits",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("numeric_val", Numeric(10, 2)),
            Column("numeric_no_scale", Numeric(12)),
            Column("float_precise", Float(10)),
            Column("float_double", Float(54)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "numeric_val": Decimal("99999999.99"),
                "numeric_no_scale": Decimal("999999999999"),
                "float_precise": 98765.432109,
                "float_double": 1.23456789012345e100,
            }
            conn.execute(precision_table.insert().values(**values))
            row = conn.execute(select(precision_table)).one()._mapping

            self.assertEqual(Decimal(row["numeric_val"]), values["numeric_val"])
            self.assertEqual(Decimal(row["numeric_no_scale"]), values["numeric_no_scale"])
            self.assertTrue(
                math.isclose(row["float_precise"], values["float_precise"], rel_tol=1e-9)
            )
            self.assertTrue(
                math.isclose(row["float_double"], values["float_double"], rel_tol=1e-15)
            )

            meta.drop_all(conn)

    def test_numeric_float_extremes(self):
        meta = MetaData()
        float_table = Table(
            "numeric_float_extremes",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("float_small", Float(10)),
            Column("float_large", Float(54)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "float_small": 1.0e-30,
                "float_large": 9.223372036854776e18,
            }
            conn.execute(float_table.insert().values(**values))
            row = conn.execute(select(float_table)).one()._mapping

            self.assertTrue(
                math.isclose(row["float_small"], values["float_small"], rel_tol=1e-15)
            )
            self.assertTrue(
                math.isclose(row["float_large"], values["float_large"], rel_tol=1e-15)
            )

            meta.drop_all(conn)

    def test_character(self):
        meta = MetaData()
        char_table = Table(
            "character_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("string_val", String(120)),
            Column("string_default", String()),
            Column("char_val", CHAR(3)),
            Column("text_val", Text()),
            Column("unicode_val", Unicode(100)),
            Column("unicode_text_val", UnicodeText()),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "string_val": "Mimer SQL",
                "string_default": "Default length string ÅÄÖ",
                "char_val": "ÅÖÄ",
                "text_val": "Lorem ipsum dolor sit amet",
                "unicode_val": "Unicode Café 漢字 μερικά ελληνικά",
                "unicode_text_val": "Extended unicode value with 汉字 and ÅÄÖ",
            }
            conn.execute(char_table.insert().values(**values))
            row = conn.execute(select(char_table)).one()._mapping

            for key, expected in values.items():
                self.assertEqual(row[key], expected)

            meta.drop_all(conn)

    def test_binary(self):
        meta = MetaData()
        binary_table = Table(
            "binary_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("binary_val", LargeBinary()),
            Column("fixed_binary_val", sqltypes.BINARY(16)),
            Column("varbinary_val", sqltypes.VARBINARY(32)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "binary_val": b"\x00\x01\x02",
                "fixed_binary_val": b"\xAA" * 16,
                "varbinary_val": b"\x10\x20\x30\x40",
            }
            conn.execute(binary_table.insert().values(**values))
            row = conn.execute(select(binary_table)).one()._mapping

            for key, expected in values.items():
                actual = row[key]
                self.assertEqual(bytes(actual), expected)

            meta.drop_all(conn)

    def test_interval(self):
        meta = MetaData()
        interval_table = Table(
            "interval_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("interval_day_5", sqltypes.Interval(day_precision=5)),
            Column("interval_second_4", sqltypes.Interval(second_precision=4)),
            Column("interval_day_5_to_second_2", sqltypes.Interval(day_precision=5, second_precision=2)),
            Column("interval_year", MimerInterval(fields="YEAR")),
            Column("interval_year_2", MimerInterval(fields="YEAR", precision=2)),
            Column("interval_year_to_month", MimerInterval(fields="YEAR TO MONTH")),
            Column("interval_day_to_second", MimerInterval(fields="DAY TO SECOND", second_precision=5)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            conn.execute(
                text(
                    "INSERT INTO interval_types "
                    "(interval_day_5, interval_second_4, interval_day_5_to_second_2, "
                    "interval_year, interval_year_2, interval_year_to_month, interval_day_to_second) "
                    "VALUES (INTERVAL '12' DAY, INTERVAL '34' SECOND(4), INTERVAL '3 00:00:45' DAY TO SECOND(2), "
                    "INTERVAL '02' YEAR, INTERVAL '03' YEAR, INTERVAL '1-02' YEAR TO MONTH, "
                    "INTERVAL '4 05:06:07.12345' DAY TO SECOND(5))"
                )
            )

            cast_sql = text(
                "SELECT "
                "CAST(interval_day_5 AS VARCHAR(20)), "
                "CAST(interval_second_4 AS VARCHAR(20)), "
                "CAST(interval_day_5_to_second_2 AS VARCHAR(30)), "
                "CAST(interval_year AS VARCHAR(20)), "
                "CAST(interval_year_2 AS VARCHAR(20)), "
                "CAST(interval_year_to_month AS VARCHAR(20)), "
                "CAST(interval_day_to_second AS VARCHAR(30)) "
                "FROM interval_types"
            )
            row = conn.execute(cast_sql).one()

            expectations = [
                "12",
                "34",
                "3 00:00:45",
                "2",
                "3",
                "1-02",
                "4 05:06:07.12345",
            ]
            for actual, snippet in zip(row, expectations):
                self.assertIn(snippet, actual)

            meta.drop_all(conn)

    def test_interval_python_values(self):
        meta = MetaData()
        interval_table = Table(
            "interval_python_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("interval_day_5", sqltypes.Interval(day_precision=5)),
            Column("interval_second_4", sqltypes.Interval(second_precision=4)),
            Column("interval_day_5_to_second_2", sqltypes.Interval(day_precision=5, second_precision=2)),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            values = {
                "interval_day_5": timedelta(days=7),
                "interval_second_4": timedelta(seconds=42, microseconds=120000),
                "interval_day_5_to_second_2": timedelta(days=2, seconds=13),
            }
            conn.execute(interval_table.insert().values(**values))

            native_row = conn.execute(select(interval_table)).one()._mapping
            for key, expected in values.items():
                self.assertEqual(native_row[key], expected)

            cast_sql = text(
                "SELECT "
                "CAST(interval_day_5 AS VARCHAR(20)), "
                "CAST(interval_second_4 AS VARCHAR(20)), "
                "CAST(interval_day_5_to_second_2 AS VARCHAR(30)) "
                "FROM interval_python_types"
            )
            row = conn.execute(cast_sql).one()

            expectations = [
                "7",
                "42",
                "2 00:00:13",
            ]
            for actual, snippet in zip(row, expectations):
                self.assertIn(snippet, actual)

            meta.drop_all(conn)

    def test_uuid(self):
        meta = MetaData()
        uuid_table = Table(
            "uuid_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("uuid_val", Uuid),
            Column("label", String(40)),
            Column("is_active", Boolean()),
        )

        with self.eng.begin() as conn:
            meta.create_all(conn)
            value = {
                "uuid_val": UUID.uuid4(),
                "label": "UUID example",
                "is_active": True,
            }
            conn.execute(uuid_table.insert().values(**value))
            row = conn.execute(select(uuid_table)).one()._mapping

            self.assertEqual(str(row["uuid_val"]), str(value["uuid_val"]))
            self.assertEqual(row["label"], value["label"])
            self.assertTrue(row["is_active"])

            meta.drop_all(conn)

    def test_uuid_string_input(self):
        meta = MetaData()
        uuid_table = Table(
            "uuid_string_types",
            meta,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("uuid_val", Uuid),
            Column("label", String(40)),
        )

        string_uuid = str(UUID.uuid4())

        with self.eng.begin() as conn:
            meta.create_all(conn)
            conn.execute(uuid_table.insert(), {"uuid_val": string_uuid, "label": "string"})
            row = conn.execute(select(uuid_table)).one()._mapping

            self.assertEqual(str(row["uuid_val"]), string_uuid)
            self.assertEqual(row["label"], "string")

            meta.drop_all(conn)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
