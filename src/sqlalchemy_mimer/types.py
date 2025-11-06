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

from datetime import timedelta
from decimal import Decimal, ROUND_HALF_EVEN

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
        if fields is not None:
            self.fields = fields
        else:
            self.fields = getattr(self, "fields", None)
        self.precision = precision

    def bind_processor(self, dialect):
        super_process = super().bind_processor(dialect)

        def process(value):
            if value is None:
                return None if super_process is None else super_process(value)
            if isinstance(value, timedelta):
                value = self._timedelta_to_string(value)
            if super_process and not isinstance(value, str):
                return super_process(value)
            return value

        return process

    def result_processor(self, dialect, coltype):
        super_process = super().result_processor(dialect, coltype)

        def process(value):
            if super_process and not isinstance(value, str):
                value = super_process(value)
            if value is None:
                return None
            if isinstance(value, str):
                converted = self._string_to_timedelta(value)
                if converted is not None:
                    return converted
            return value

        return process

    # Helpers -----------------------------------------------------------------

    def _resolve_fields(self):
        if self.fields:
            return self.fields.upper()
        day_precision = getattr(self, "day_precision", None)
        second_precision = getattr(self, "second_precision", None)
        if day_precision is not None and second_precision is None:
            return "DAY"
        if day_precision is None and second_precision is not None:
            return "SECOND"
        if day_precision is not None and second_precision is not None:
            return "DAY TO SECOND"
        return "DAY TO SECOND"

    def _timedelta_to_string(self, value: timedelta) -> str:
        fields = self._resolve_fields()
        total_micro = (
            value.days * 86400_000_000
            + value.seconds * 1_000_000
            + value.microseconds
        )
        negative = total_micro < 0
        total_micro = abs(total_micro)

        if fields == "DAY":
            days, remainder = divmod(total_micro, 86400_000_000)
            if remainder != 0:
                raise ValueError("timedelta has sub-day precision but column stores DAY interval")
            sign = "-" if negative else ""
            return f"{sign}{days}"

        if fields == "SECOND":
            seconds, micro = divmod(total_micro, 1_000_000)
            sign = "-" if negative else ""
            frac_digits = self._fractional_digits()
            if micro == 0 or frac_digits == 0:
                if micro != 0 and frac_digits == 0:
                    raise ValueError("timedelta has fractional seconds but column precision is 0")
                return f"{sign}{seconds}"
            seconds_decimal = (
                Decimal(seconds) + (Decimal(micro) / Decimal(1_000_000))
            )
            quant = Decimal(1) / (Decimal(10) ** frac_digits)
            seconds_decimal = seconds_decimal.quantize(quant, rounding=ROUND_HALF_EVEN)
            formatted = format(seconds_decimal, "f").rstrip("0").rstrip(".")
            return f"{sign}{formatted}"

        if fields == "DAY TO SECOND":
            days, remainder = divmod(total_micro, 86400_000_000)
            hours, remainder = divmod(remainder, 3600_000_000)
            minutes, remainder = divmod(remainder, 60_000_000)
            seconds, micro = divmod(remainder, 1_000_000)
            frac_digits = self._fractional_digits()
            if micro != 0 and frac_digits == 0:
                raise ValueError("timedelta has fractional seconds but column precision is 0")
            seconds_decimal = (
                Decimal(seconds) + (Decimal(micro) / Decimal(1_000_000))
            )
            if frac_digits > 0:
                quant = Decimal(1) / (Decimal(10) ** frac_digits)
                seconds_decimal = seconds_decimal.quantize(quant, rounding=ROUND_HALF_EVEN)
            elif micro != 0:
                seconds_decimal = Decimal(seconds_decimal.to_integral_value(rounding=ROUND_HALF_EVEN))
            formatted_seconds = format(seconds_decimal, "f").rstrip("0").rstrip(".")
            whole, _, frac = formatted_seconds.partition(".")
            whole = whole.zfill(2)
            seconds_str = whole + (f".{frac}" if frac else "")
            sign = "-" if negative else ""
            return f"{sign}{days} {hours:02d}:{minutes:02d}:{seconds_str}"

        # Fallback to default string conversion for intervals outside DAY/SECOND family.
        return str(value)

    def _string_to_timedelta(self, value: str):
        fields = self._resolve_fields()
        text_value = value.strip()
        if not text_value:
            return None

        if fields == "DAY":
            try:
                days = int(text_value)
            except ValueError:
                return None
            return timedelta(days=days)

        if fields == "SECOND":
            try:
                seconds = Decimal(text_value)
            except Exception:
                return None
            sign = -1 if seconds < 0 else 1
            seconds = abs(seconds)
            total_micro = int((seconds * Decimal(1_000_000)).to_integral_value(rounding=ROUND_HALF_EVEN))
            whole_seconds, micro = divmod(total_micro, 1_000_000)
            result = timedelta(seconds=whole_seconds, microseconds=micro)
            return result if sign > 0 else -result

        if fields == "DAY TO SECOND":
            sign = 1
            raw = text_value
            if raw[0] in "+-":
                if raw[0] == "-":
                    sign = -1
                raw = raw[1:].lstrip()
            if " " not in raw:
                return None
            day_part, time_part = raw.split(" ", 1)
            try:
                days = int(day_part)
            except ValueError:
                return None
            time_part = time_part.strip()
            components = time_part.split(":")
            if len(components) != 3:
                return None
            hours_part, minutes_part, seconds_part = components
            try:
                hours = int(hours_part)
                minutes = int(minutes_part)
            except ValueError:
                return None
            seconds_split = seconds_part.split(".")
            try:
                seconds = int(seconds_split[0])
            except ValueError:
                return None
            micro = 0
            if len(seconds_split) == 2:
                frac = seconds_split[1][:6].ljust(6, "0")
                if not frac.isdigit():
                    return None
                micro = int(frac)
            result = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds, microseconds=micro)
            return result if sign > 0 else -result

        return None

    def _fractional_digits(self) -> int:
        precision = getattr(self, "second_precision", None)
        if precision is None:
            return 6
        if precision < 0:
            return 0
        return min(precision, 6)

# --- Other types  -----
class MimerUUID(sqltypes.Uuid):
    __visit_name__ = "uuid"
