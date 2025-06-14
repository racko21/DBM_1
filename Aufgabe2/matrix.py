import psycopg


def connect_db(dbname: str, user: str):
    """
    Stellt eine Verbindung zur PostgreSQL-Datenbank her.
    Gibt das Connection-Objekt zurück.
    """
    conn = psycopg.connect(dbname=dbname, user=user)
    return conn

def create_tables(conn):
    """
    Erstellt die Tabellen A und B (Sparse Matrix Representation) in der Datenbank.
    Bestehende Tabellen gleichen Namens werden zuvor entfernt.
    """
    with conn.cursor() as cur:
        # Existierende Tabellen löschen, um Neuaufbau zu ermöglichen
        cur.execute("DROP TABLE IF EXISTS A;")
        cur.execute("DROP TABLE IF EXISTS B;")
        # Tabellen A und B erstellen mit Spalten: i (INT), j (INT), val (DOUBLE PRECISION)
        cur.execute("""
            CREATE TABLE A (
                i   INT,
                j   INT,
                val DOUBLE PRECISION
            );
        """)
        cur.execute("""
            CREATE TABLE B (
                i   INT,
                j   INT,
                val DOUBLE PRECISION
            );
        """)
    conn.commit()

def insert_matrix(conn, table_name: str, matrix: list):
    """
    Fügt alle Nicht-Null-Werte der gegebenen Matrix in die angegebene Tabelle ein.
    - table_name: Name der Zieltabelle ('A' oder 'B').
    - matrix: 2D-Liste mit Matrixwerten.
    """
    with conn.cursor() as cur:
        rows = []
        for i, row in enumerate(matrix):
            for j, val in enumerate(row):
                if val != 0 and val is not None:
                    # Nur Nicht-Null-Werte einfügen
                    rows.append((i, j, float(val)))
        cur.executemany(
            f"INSERT INTO {table_name} (i, j, val) VALUES (%s, %s, %s);",
            rows
        )
    conn.commit()
