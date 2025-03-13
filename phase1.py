import psycopg

data = [
    ("a", "b", None),
    (None, "c", "2"),
    (None, None, "3"),
    (None, None, None)
]
with psycopg.connect("dbname=racko21 user=racko21") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE H_toy (
                o1 serial PRIMARY KEY,
                a1 TEXT,
                a2 TEXT,
                a3 TEXT)
            """)
        cur.executemany(
            "INSERT INTO H_Toy (a1, a2, a3) VALUES( %s, %s, %s )",
            data
        )
        conn.commit
