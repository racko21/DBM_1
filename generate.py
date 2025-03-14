import sys
import argparse
import psycopg

parser = argparse.ArgumentParser("generate")
parser.add_argument("num_tuples", help = "spezifiziert die Anzahl der Tupel in H", type = int)
parser.add_argument("sparsity", help = "spezifiziert den (durchschnittlichen) Anteil der Tupel pro Attribut mit dem Attributwert null", type = float)
parser.add_argument("num_attributes", help = "spezifiziert die Anzahl an Attributen pro Tupel", type = float)
args = parser.parse_args()

with pscycopg.connect("dbname=racko21 user=racko21") as conn:
    with conn.cursor() as cur:
        cur.execute("select * from information_schema.tables with table_name = "H" ")
        if bool(cur.rowcount):
            cur.execute("drop table H")
        else:
            cur.execute("""
                create table H (
                    o1 serial primary key,
                    a1 text,
                    a2 text,
                    a3 int)
                """)

