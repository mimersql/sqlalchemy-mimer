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
from sqlalchemy import (
    create_engine,
    text,
    String,
    inspect,
)
import db_config


class TestDomains(unittest.TestCase):
    """Tests for Mimer SQL domain DDL, reflection, and domain usage in tables."""

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

    def tearDown(self):
        self.eng.dispose()

    def test_create_and_drop_domain(self):
        """Verify CREATE DOMAIN and DROP DOMAIN SQL syntax and execution."""

        create_sql = (
            "CREATE DOMAIN dom_text AS VARCHAR(30) "
            "COLLATE SWEDISH_1 DEFAULT 'anon' "
            "CHECK (char_length(VALUE) <= 30)"
        )
        drop_sql = "DROP DOMAIN dom_text RESTRICT"

        with self.eng.begin() as conn:
            conn.execute(text(create_sql))

            insp = inspect(self.eng)
            domains = insp.dialect.get_domains(conn)
            names = [d["name"].upper() for d in domains]
            self.assertIn("DOM_TEXT", names)

            conn.execute(text(drop_sql))
            conn.commit()

    def test_domain_reflection_with_check_and_default(self):
        """Verify reflection of domains with CHECK and DEFAULT clauses."""
        with self.eng.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE DOMAIN dom_checked AS CHARACTER VARYING(40)
                    DEFAULT 'guest'
                    CHECK (char_length(VALUE) <= 40)
                    """
                )
            )

            insp = inspect(self.eng)
            domains = insp.dialect.get_domains(conn)
            dom_names = [d["name"].upper() for d in domains]
            self.assertIn("DOM_CHECKED", dom_names)

            dom = next(d for d in domains if d["name"].upper() == "DOM_CHECKED")
            self.assertEqual(dom["default"], "'guest'")
            self.assertIn("VARCHAR", str(dom["type"]).upper())

    def test_domain_usage_in_table(self):
        """Verify a domain can be used as a column type in a table and reflected."""

        with self.eng.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE DOMAIN dom_text AS VARCHAR(20)
                    DEFAULT 'hello'
                    CHECK (char_length(VALUE) <= 20)
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE dom_table (
                        id INTEGER PRIMARY KEY,
                        comment dom_text
                    )
                    """
                )
            )

            # Verify table and column reflection
            insp = inspect(self.eng)
            cols = insp.get_columns("dom_table")
            col_names = [c["name"] for c in cols]
            self.assertIn("comment", col_names)

            comment_col = next(c for c in cols if c["name"] == "comment")
            self.assertIn("VARCHAR", str(comment_col["type"]).upper())

            # Clean up
            conn.execute(text("DROP TABLE dom_table RESTRICT"))
            conn.execute(text("DROP DOMAIN dom_text RESTRICT"))


if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()