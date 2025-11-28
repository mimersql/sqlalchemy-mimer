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

# The database to run the tests on. Having it empty means that the
# environment variable MIMER_DATABASE controls the database to connect to.
DBNAME = ''

# The test connects to SYSADM_USR and creates a subident that is used for all
# the tests. After testing, the subident is dropped and everything goes away.


#################################################################
## Stuff below defines routines for all tests.
## Do not change for configuration purposes
#################################################################

import mimerpy
import os
import getpass
from platform import system

# Get default database, or use 'mimerdb' if not set
DBNAME = os.environ.get('MIMER_DATABASE', 'mimerdb')

# Get user and password for user with enough privileges
# to create users and databanks
SYSTEM_USR = os.environ.get('MIMER_TEST_USER', 'SYSADM')
SYSTEM_PWD = os.environ.get('MIMER_TEST_PASSWORD', 'SYSADM')

# Connection arguments for SYSADM
SYSUSR = dict(dsn      = DBNAME,
              user     = SYSTEM_USR,
              password = SYSTEM_PWD)

# Connection arguments for test user
TSTUSR = dict(dsn      = DBNAME,
              user     = 'SQLALCHEMY',
              password = 'PySecret')

OSUSER = getpass.getuser()
QUALIFIED_OSUSER = getpass.getuser()
plat = system()
if plat == 'Windows':
    QUALIFIED_OSUSER = os.getenv("USERDOMAIN") + "\\" + OSUSER 

KEEP_SQLALCHEMY_IDENT = os.environ.get('MIMER_KEEP_SQLALCHEMY_IDENT', 'true') == 'true'
MIMERPY_STABLE = os.environ.get('MIMERPY_STABLE', 'True')
MIMERPY_TRACE = os.environ.get('MIMERPY_TRACE')

def setup():
    syscon = mimerpy.connect(**SYSUSR)
    with syscon.cursor() as c:
        try:
            c.execute("DROP IDENT SQLALCHEMY CASCADE")
        except mimerpy.DatabaseError as de:
            if de.errno == -12564 or de.errno == -12517: # ident does not exist
                pass
            else:
                raise

        c.execute("CREATE IDENT SQLALCHEMY AS USER USING 'PySecret'")
        c.execute("GRANT DATABANK,IDENT, SCHEMA TO SQLALCHEMY")
    syscon.commit()
    tstcon = mimerpy.connect(**TSTUSR)
    with tstcon.cursor() as c:
        c.execute("CREATE DATABANK SQLALCBANK")
        c.execute("CREATE SCHEMA MYSCHEMA")
        try:
            c.execute(F"CREATE IDENT \"{OSUSER}\" AS USER")
            c.execute(F"ALTER IDENT \"{OSUSER}\" ADD OS_USER '{QUALIFIED_OSUSER}' ")
        except Exception:
            pass
        
    tstcon.commit()
    tstcon.close()
    syscon.close()


def teardown():
        syscon = mimerpy.connect(**SYSUSR)
        if not KEEP_SQLALCHEMY_IDENT:
            with syscon.cursor() as c:
                c.execute("DROP IDENT SQLALCHEMY CASCADE")
            syscon.commit()
        syscon.close()

def make_tst_uri():
    user = TSTUSR.get("user")
    password = TSTUSR.get("password")
    dsn = TSTUSR.get("dsn")

    # Build the URI — host/port may be omitted if DSN is enough
    return f"mimer://{user}:{password}@{dsn}"
