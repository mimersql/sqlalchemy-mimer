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
"""
Mimer SQL-specific SQLAlchemy type definitions.
"""

from sqlalchemy import types as sqltypes


# --- Numeric types  ---

class MimerInteger(sqltypes.Integer):
    __visit_name__ = "integer"


class MimerBigInteger(sqltypes.BigInteger):
    __visit_name__ = "bigint"


class MimerSmallInteger(sqltypes.SmallInteger):
    __visit_name__ = "smallint"


class MimerNumeric(sqltypes.Numeric):
    __visit_name__ = "numeric"


class MimerFloat(sqltypes.Float):
    __visit_name__ = "float"


# --- Character types ---

class MimerString(sqltypes.String):
    """CHARACTER or CHARACTER VARYING"""
    __visit_name__ = "varchar"


class MimerText(sqltypes.Text):
    """CHARACTER LARGE OBJECT"""
    __visit_name__ = "clob"


class MimerUnicode(sqltypes.Unicode):
    """NATIONAL CHARACTER or NVARCHAR"""
    __visit_name__ = "nvarchar"


class MimerUnicodeText(sqltypes.UnicodeText):
    """NATIONAL CHARACTER LARGE OBJECT"""
    __visit_name__ = "nclob"


# --- Binary types  ----

class MimerBinary(sqltypes._Binary):
    """Fixed-length BINARY(n)"""
    __visit_name__ = "binary"


class MimerVarBinary(sqltypes._Binary):
    """Variable-length BINARY VARYING(n)"""
    __visit_name__ = "varbinary"


class MimerBinaryLargeObject(sqltypes.LargeBinary):
    """BINARY LARGE OBJECT"""
    __visit_name__ = "blob"


# --- Logical and date/time types ---

class MimerBoolean(sqltypes.Boolean):
    __visit_name__ = "boolean"


class MimerDate(sqltypes.Date):
    __visit_name__ = "date"


class MimerTime(sqltypes.Time):
    __visit_name__ = "time"


class MimerDateTime(sqltypes.DateTime):
    __visit_name__ = "timestamp"


class MimerInterval(sqltypes.Interval):
    __visit_name__ = "interval"

    def __init__(self, *args, fields=None, precision=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields = fields
        self.precision = precision

# --- Other types  -----
class MimerUUID(sqltypes.Uuid):
    __visit_name__ = "uuid"
