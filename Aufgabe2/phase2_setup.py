import psycopg

def connect_db(dbname: str, user: str):
    """
    Verbindung zur PostgreSQL-Datenbank herstellen.
    """
    return psycopg.connect(dbname=dbname, user=user)

def create_tables_sparse(conn):
    """
    Erstellt (oder ersetzt) die Tabellen A_sparse und B_sparse
    für die sparse Darstellung (ein Tupel pro Nicht-Null-Eintrag).
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS A_sparse;")
        cur.execute("DROP TABLE IF EXISTS B_sparse;")
        cur.execute("""
            CREATE TABLE A_sparse (
                i   INT,
                j   INT,
                val DOUBLE PRECISION
            );
        """)
        cur.execute("""
            CREATE TABLE B_sparse (
                i   INT,
                j   INT,
                val DOUBLE PRECISION
            );
        """)
    conn.commit()

def create_tables_vector(conn, l: int):
    """
    Erstellt (oder ersetzt) die Tabellen A_vec und B_vec
    für die Vektor-Darstellung (je ein Array pro Zeile/Spalte).
    l ist die Länge der Arrays (gemeinsame Dimension).
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS A_vec;")
        cur.execute("DROP TABLE IF EXISTS B_vec;")
        cur.execute(f"""
            CREATE TABLE A_vec (
                i   INT PRIMARY KEY,
                row DOUBLE PRECISION[{l}]
            );
        """)
        cur.execute(f"""
            CREATE TABLE B_vec (
                j   INT PRIMARY KEY,
                col DOUBLE PRECISION[{l}]
            );
        """)
    conn.commit()

def create_dotproduct_function(conn):
    """
    Definiert die UDF dotproduct(array, array) in PL/pgSQL,
    die das Skalarprodukt zweier gleichlanger Arrays liefert.
    """
    with conn.cursor() as cur:
        cur.execute("""
        CREATE OR REPLACE FUNCTION dotproduct(vec1 DOUBLE PRECISION[], vec2 DOUBLE PRECISION[])
          RETURNS DOUBLE PRECISION AS $$
        BEGIN
          RETURN (
            SELECT SUM(x * y)
            FROM unnest(vec1, vec2) AS t(x, y)
          );
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
        """)
    conn.commit()

def insert_sparse(conn, A: list, B: list):
    """
    Fügt alle Nicht-Null-Werte aus den Matrizen A und B in A_sparse/B_sparse ein.
    A: m×l, B: l×n
    """
    with conn.cursor() as cur:
        # A_sparse
        rows = [
            (i, j, float(val))
            for i, row in enumerate(A)
            for j, val in enumerate(row)
            if val != 0
        ]
        cur.executemany(
            "INSERT INTO A_sparse (i, j, val) VALUES (%s, %s, %s);",
            rows
        )
        # B_sparse
        rows = [
            (i, j, float(val))
            for i, row in enumerate(B)
            for j, val in enumerate(row)
            if val != 0
        ]
        cur.executemany(
            "INSERT INTO B_sparse (i, j, val) VALUES (%s, %s, %s);",
            rows
        )
    conn.commit()

def insert_vector(conn, A: list, B: list):
    """
    Fügt die ganzen Zeilen von A und Spalten von B als Arrays ein.
    A: m×l  ⇒  A_vec(i, row=array der Länge l)
    B: l×n  ⇒  B_vec(j, col=array der Länge l)
    """
    with conn.cursor() as cur:
        # A_vec
        rows = [(i, row) for i, row in enumerate(A)]
        cur.executemany(
            "INSERT INTO A_vec (i, row) VALUES (%s, %s);",
            rows
        )
        # B_vec: für jede Spalte j in B (j < n) das Array [B[0][j], B[1][j], ..., B[l-1][j]]
        l = len(B)
        n = len(B[0]) if B else 0
        rows = []
        for j in range(n):
            col = [B[i][j] for i in range(l)]
            rows.append((j, col))
        cur.executemany(
            "INSERT INTO B_vec (j, col) VALUES (%s, %s);",
            rows
        )
    conn.commit()
