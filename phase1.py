import psycopg
import config

data = [
    ("a", "b", None),
    (None, "c", "2"),
    (None, None, "3"),
    (None, None, None)
]
with psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}") as conn:
    with conn.cursor() as cur:
        cur.execute("drop table if exists h_toy cascade;")
        cur.execute("""
            CREATE TABLE H_toy (
                oid serial PRIMARY KEY,
                a1 TEXT,
                a2 TEXT,
                a3 TEXT)
            """)
        cur.executemany(
            "INSERT INTO H_Toy (a1, a2, a3) VALUES( %s, %s, %s )",
            data
        )
        conn.commit


# racko21=# CREATE VIEW h2v_toy AS
# SELECT
#     o1,
#     MAX(CASE WHEN key = 'a1' THEN value END) AS a1,
#     MAX(CASE WHEN key = 'a2' THEN value END) AS a2,
#     MAX(CASE WHEN key = 'a3' THEN value END) AS a3
# FROM V_toy
# GROUP BY o1 order by o1;
#
# racko21=# create view v_toy_string as
# select * from v_toy
# where key = 'a1' or key = 'a2';
#
# racko21=# create view v_toy_int as
# select * from v_toy
# where key = 'a3';
#
# racko21=# create view v_toy_all as select * from v_toy_int union all select * from v_toy_string order by key;
