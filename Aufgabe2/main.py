from generate import generate
from matrix import connect_db, create_tables, insert_matrix
from multiply import multiply_naive
from db_multiply import multiply_sql
import config


def print_matrix(matrix: list):
    """Hilfsfunktion: Druckt eine 2D-Matrix zeilenweise (für kleine Matrizen)."""
    for row in matrix:
        print("  ", row)

if __name__ == "__main__":
    # --- 1. Verbindung zur PostgreSQL-Datenbank herstellen ---
    # Hinweis: Bitte die Verbindungsparameter anpassen:
    conn = connect_db(dbname=config.DB_NAME, user=config.DB_USER)

    # --- 2. Tabellen A und B in der Datenbank erstellen ---
    create_tables(conn)
    print("Tabellen A und B wurden in der Datenbank erstellt.")

    # --- 3. Toy-Beispiel: manuell nachvollziehbare kleine Matrizen mit Nullwerten ---
    toy_A = [
        [2, 0, 3],
        [0, 4, 0]
    ]
    toy_B = [
        [0, 5],
        [7, 0],
        [0, 1]
    ]
    print("\nToy-Beispiel Matrizen A und B:")
    print("Matrix A:")
    print_matrix(toy_A)
    print("Matrix B:")
    print_matrix(toy_B)
    # Erwartetes Ergebnis C (bereits manuell berechnet in Beschreibung):
    expected_C = [
        [0, 13],
        [28, 0]
    ]
    print("Erwartetes Ergebnis C = A * B:")
    print_matrix(expected_C)

    # Lade Toy-Beispiel Matrizen in die DB
    insert_matrix(conn, "A", toy_A)
    insert_matrix(conn, "B", toy_B)
    print("Toy-Beispiel Matrizen wurden in die DB eingefügt (Nur Nicht-Null-Werte).")

    # Ansatz 0: Berechnung in Python
    C_naive = multiply_naive(toy_A, toy_B)
    print("\nAnsatz 0 Ergebnis (berechnet in Python):")
    print_matrix(C_naive)

    # Ansatz 1: Berechnung in der DB via SQL
    result_sql = multiply_sql(conn)
    print("\nAnsatz 1 Ergebnis (aus DB-Query als Liste von Tupeln):")
    for (i, j, val) in result_sql:
        print(f"  (i={i}, j={j}) -> {val}")
    # Zur besseren Lesbarkeit konvertieren wir die Tupel in eine Matrixdarstellung
    # (Nur gültig für kleine Matrizen oder wenn vollständige Matrix gewünscht)
    m = len(toy_A)
    n = len(toy_B[0])
    C_sql_matrix = [[0.0 for _ in range(n)] for _ in range(m)]
    for (i, j, val) in result_sql:
        C_sql_matrix[i][j] = val
    print("Ansatz 1 Ergebnis in Matrixform:")
    print_matrix(C_sql_matrix)

    # Validierung: Vergleich der Ergebnisse von Ansatz 0 und Ansatz 1
    assert C_naive == C_sql_matrix, "Ergebnis von Ansatz 1 stimmt nicht mit Ansatz 0 überein!"
    print("\nValidation: Die Ergebnisse von Ansatz 0 und Ansatz 1 stimmen überein.")

    # --- 4. Zusätzlich: Größeres zufälliges Beispiel (optional für Test/Leistung) ---
    l = 10  # z.B. 10x9 * 9x10 Matrix ergibt 9x9 Ergebnis
    sparsity = 0.5  # 50% der Einträge sind Null
    A_rand, B_rand = generate(l, sparsity)
    # Tabellen zurücksetzen und zufällige Matrizen laden
    create_tables(conn)  # existierende Daten löschen
    insert_matrix(conn, "A", A_rand)
    insert_matrix(conn, "B", B_rand)
    # Berechnungen durchführen
    C_rand_naive = multiply_naive(A_rand, B_rand)
    result_rand_sql = multiply_sql(conn)
    # (Optional: könnten hier z.B. die Anzahl Nicht-Null-Werte oder Dauer messen)
    # Ergebnismatrizen vergleichen
    # Konvertiere SQL-Ergebnis in dichte Matrix für Vergleich
    m2 = len(A_rand)
    n2 = len(B_rand[0])
    C_rand_sql_matrix = [[0.0 for _ in range(n2)] for _ in range(m2)]
    for (i, j, val) in result_rand_sql:
        C_rand_sql_matrix[i][j] = val
    print(f"\nZufälliger Test mit l={l}, sparsity={sparsity}: Vergleich der Resultate...")
    if C_rand_naive == C_rand_sql_matrix:
        print("Erfolg: Beide Ansätze liefern identische Ergebnisse für das zufällige Beispiel.")
    else:
        print("Fehler: Die Ergebnisse unterscheiden sich!")
        # (Bei korrekter Implementierung sollte dieser Fall nicht eintreten.)
    # Verbindung schließen
    conn.close()
