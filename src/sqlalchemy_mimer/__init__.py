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
SQLAlchemy dialect and type definitions for Mimer SQL.

This package provides:

- ``MimerDialect`` — a fully featured SQLAlchemy dialect for Mimer SQL,
  including sequence-based autoincrement, reflection, and ORM support.

- Mimer-specific SQL type classes such as ``MimerInteger``, ``MimerDateTime``,
  ``MimerUnicodeText``, etc., that can be used when you need explicit control
  over Mimer SQL type names in DDL generation.

Importing this package automatically registers the dialect with SQLAlchemy’s
dialect registry, so you can use it directly with:

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mimer://user:password@host:port/dbname")

The dialect will be selected automatically.
"""

from .dialect import MimerDialect
from .types import (
    MimerInteger,
    MimerBigInteger,
    MimerSmallInteger,
    MimerNumeric,
    MimerFloat,
    MimerString,
    MimerText,
    MimerUnicode,
    MimerUnicodeText,
    MimerBinary,
    MimerVarBinary,
    MimerBinaryLargeObject,
    MimerBoolean,
    MimerDate,
    MimerTime,
    MimerDateTime,
    MimerInterval,
    MimerUUID,
)

__all__ = [
    "MimerDialect",
    "MimerInteger",
    "MimerBigInteger",
    "MimerSmallInteger",
    "MimerNumeric",
    "MimerFloat",
    "MimerString",
    "MimerText",
    "MimerUnicode",
    "MimerUnicodeText",
    "MimerBinary",
    "MimerVarBinary",
    "MimerBinaryLargeObject",
    "MimerBoolean",
    "MimerDate",
    "MimerTime",
    "MimerDateTime",
    "MimerInterval",
    "MimerUUID",
]

# Automatically register the dialect with SQLAlchemy
from sqlalchemy.dialects import registry
registry.register("mimer", "sqlalchemy_mimer.dialect", "MimerDialect")
registry.register("mimer.mimerpy", "sqlalchemy_mimer.dialect", "MimerDialect")