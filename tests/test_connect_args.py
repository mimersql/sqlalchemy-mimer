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
from sqlalchemy.engine import make_url
from sqlalchemy_mimer.dialect import MimerDialect


def connect_kwargs(url):
    return MimerDialect().create_connect_args(make_url(url))[1]


class TestConnectArgs(unittest.TestCase):
    """URL to mimerpy.connect() argument mapping. Needs no database."""

    def test_dsn_forms(self):
        for url in ("mimer://u:p@mydb",
                    "mimer://u:p@localhost:1360/mydb",
                    "mimer+mimerpy://u:p@localhost:1360/?dsn=mydb"):
            kw = connect_kwargs(url)
            self.assertEqual(kw, {"user": "u", "password": "p", "dsn": "mydb"}, url)

    def test_boolean_options(self):
        for key in ("autocommit", "readonly", "trace_unsafe"):
            self.assertIs(connect_kwargs(f"mimer://u:p@db?{key}=true")[key], True)
            self.assertIs(connect_kwargs(f"mimer://u:p@db?{key}=1")[key], True)
            self.assertIs(connect_kwargs(f"mimer://u:p@db?{key}=false")[key], False)
            self.assertIs(connect_kwargs(f"mimer://u:p@db?{key}=0")[key], False)

    def test_invalid_boolean_option(self):
        with self.assertRaises(ValueError):
            connect_kwargs("mimer://u:p@db?trace_unsafe=maybe")

    def test_trace(self):
        self.assertIs(connect_kwargs("mimer://u:p@db?trace=yes")["trace"], True)
        self.assertIs(connect_kwargs("mimer://u:p@db?trace=False")["trace"], False)
        self.assertEqual(connect_kwargs("mimer://u:p@db?trace=/tmp/sql.log")["trace"],
                         "/tmp/sql.log")

    def test_other_options_pass_through(self):
        self.assertEqual(connect_kwargs("mimer://u:p@db?foo=bar")["foo"], "bar")


if __name__ == '__main__':
    unittest.main()
