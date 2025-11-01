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
import os, argparse, getpass
from sqlalchemy import create_engine, text

def core(url, verbose:bool):
    eng = create_engine(url, echo=verbose, future=True)
    with eng.begin() as conn:
        try:
            conn.exec_driver_sql("DROP TABLE sa_demo")
        except Exception:
            pass
        try:
            conn.exec_driver_sql("drop sequence sa_demo_id_seq")
        except Exception:
            pass
        conn.exec_driver_sql("create unique sequence sa_demo_id_seq as bigint")
        conn.exec_driver_sql("CREATE TABLE sa_demo (id bigint primary key default next value for sa_demo_id_seq, name VARCHAR(40) NOT NULL, created TIMESTAMP DEFAULT LOCALTIMESTAMP)")
        conn.execute(text("INSERT INTO sa_demo(name) VALUES (:n)"), [{"n": "Alice"}, {"n": "Bob"}])
        result = conn.execute(text("SELECT name, created FROM sa_demo ORDER BY created"))
        for r in result:
            print(dict(r._mapping))


        print("\nUsing Session")
        from sqlalchemy.orm import Session
        stmt = text("SELECT id, name FROM sa_demo WHERE id > :y ORDER BY name, id")
        with Session(eng) as session:
            result = session.execute(stmt, {"y": 1})
            for row in result:
                print(f"id: {row.id}  name: {row.name}")     
        
        conn.exec_driver_sql("drop table sa_demo")
        conn.exec_driver_sql("drop sequence sa_demo_id_seq")
   

def meta(url, verbose:bool):
    eng = create_engine(url, echo=verbose, future=True)
    with eng.connect() as conn: 
        print("\nUsing SQLAlchemy MetaData")
        from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
        metadata_obj = MetaData()
        user_table = Table(
            "demo_user_account",
            metadata_obj,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(30)),
            Column("fullname", String),
        )

        address_table = Table(
            "demo_address",
            metadata_obj,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("user_id", ForeignKey("demo_user_account.id"), nullable=False),
            Column("email_address", String, nullable=False),
        )
        metadata_obj.create_all(eng)
        metadata_obj.drop_all(eng)

        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Basic demo")
    
    parser.add_argument(
        '-d', '--database',
        help='Database name',
    )

    parser.add_argument(
        '-u', '--user',
        help='Database username',
    )

    parser.add_argument(
        '-p', '--password',
        help='Database password',
    )

    parser.add_argument(
        '-v', '--verbose',
        help='Show verbose output including SQLAlchemy logs',
        action='store_true',
    )

    args = parser.parse_args()

    #If no database is give, try to use MIMER_DATABASE
    database = args.database or os.environ.get("MIMER_DATABASE")
    if not database:
        parser.error("No database specified. Use -d/--database or set MIMER_DATABASE environment variable.")

    # Get password if not specified
    if not args.user:
        args.user = input("Username: ").strip()

    # Get password if not specified
    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user or 'user'}: ")

    # Build database URL
    db_url = f"mimer://{args.user}:{args.password}@{database}"
    core(db_url, args.verbose)
    meta(db_url, args.verbose)
    print("Done.")