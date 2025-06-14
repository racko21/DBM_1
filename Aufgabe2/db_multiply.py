import psycopg

def multiply_sql(conn):
    """
    Führt die Matrixmultiplikation in der PostgreSQL-Datenbank mittels SQL-Join aus (Ansatz 1).
    Vorausgesetzt: Tabellen A und B mit Spalten (i, j, val) existieren und sind mit den Matrixdaten gefüllt.
    Rückgabe: Liste von Tupeln (i, j, value) entsprechend der nicht-null Einträge der Ergebnismatrix C.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT A.i, B.j, SUM(A.val * B.val) AS value
            FROM A
            JOIN B ON A.j = B.i
            GROUP BY A.i, B.j;
        """)
        result = cur.fetchall()
    # Optional: sortieren nach i,j für konsistente Ausgabe (falls gewünscht)
    result.sort(key=lambda x: (x[0], x[1]))
    return result
