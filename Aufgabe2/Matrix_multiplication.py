import numpy as np 
import psycopg2
import time

# Datenbankverbindung
try:
    conn = psycopg2.connect(
        dbname="matrix_multiplication",
        user="postgres",
        password="meinpasswort",  
        host="localhost",
        port="5432"
    )
    print("Datenbankverbindung erfolgreich!")
except Exception as e:
    print("Fehler bei der Datenbankverbindung:", e)
    exit()

# Datengenerator für zwei Matrizen A= (l-1) x 1 und B= l x (l-1) 
def generate(l, sparsity):
    m = l - 1
    n = l - 1
    A = np.random.rand(m, l) #fülle Matrizen mit Zufallswerten 
    B = np.random.rand(l, n)
    A[np.random.rand(m, l) < sparsity] = 0
    B[np.random.rand(l, n) < sparsity] = 0
    return A, B

# Import der Matrizen in die Datenbank, speichert nur Nicht-Null-Werte, um die Effizienz zu erhöhen
def import_matrices(A, B, conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS A; DROP TABLE IF EXISTS B;")
    cur.execute("CREATE TABLE A (i INT, j INT, val DOUBLE PRECISION, PRIMARY KEY (i, j))")
    cur.execute("CREATE TABLE B (i INT, j INT, val DOUBLE PRECISION, PRIMARY KEY (i, j))")
    
    a_values = [(i + 1, j + 1, float(A[i, j])) for i in range(A.shape[0]) for j in range(A.shape[1]) if A[i, j] != 0]
    b_values = [(i + 1, j + 1, float(B[i, j])) for i in range(B.shape[0]) for j in range(B.shape[1]) if B[i, j] != 0]
    
    cur.executemany("INSERT INTO A (i, j, val) VALUES (%s, %s, %s)", a_values)
    cur.executemany("INSERT INTO B (i, j, val) VALUES (%s, %s, %s)", b_values)
    
    conn.commit()
    cur.close()

# Ansatz 0 (Matrixmultiplikation auf Computer) Laufzeitmessung 
def approach_0(A, B):
    start = time.time()
    if A.shape[1] != B.shape[0]:
        raise ValueError("Anzahl der Spalten von A muss gleich der Anzahl der Zeilen von B sein.")
    m, n = A.shape[0], B.shape[1]
    C = np.zeros((m, n)) #Ergebnismatrix mit Größe m x n
    for i in range(m):
        for j in range(n):
            for k in range(A.shape[1]):
                C[i, j] += A[i, k] * B[k, j]
    end = time.time() #misst Laufzeit
    return C, end - start 

# Ansatz 1 (Matrixmultiplikation in Datenbank)
def approach_1(conn):
    cur = conn.cursor()
    start = time.time()
    cur.execute("""
        SELECT A.i, B.j, SUM(A.val * B.val) AS product
        FROM A, B
        WHERE A.j = B.i
        GROUP BY A.i, B.j;
    """)
    result = cur.fetchall()
    end = time.time()
    cur.close()
    return result, end - start

# Toy-Beispiel (Testfall von Korrektheit der Funktionen)
A_toy = np.array([[3, 2, 1], [1, 0, 2]])
B_toy = np.array([[1, 2], [0, 1], [4, 0]])

# Dokumentation Toy-Beispiels
"""
Toy-Beispiel für Korrektheitstests:
Matrix A = [[3, 2, 1], [1, 0, 2]]
Matrix B = [[1, 2], [0, 1], [4, 0]]
Tabelle A (i, j, val) für Nicht-Null-Werte:
(1, 1, 3), (1, 2, 2), (1, 3, 1), (2, 1, 1), (2, 3, 2)
Tabelle B (i, j, val) für Nicht-Null-Werte:
(1, 1, 1), (1, 2, 2), (2, 2, 1), (3, 1, 4)
Manuelle Berechnung von C = A × B:
C[1,1] = 3*1 + 2*0 + 1*4 = 7
C[1,2] = 3*2 + 2*1 + 1*0 = 8
C[2,1] = 1*1 + 0*0 + 2*4 = 9
C[2,2] = 1*2 + 0*1 + 2*0 = 2
Ergebnis C = [[7, 8], [9, 2]]
"""

# testlauf
print("Teste Toy-Beispiel...")
import_matrices(A_toy, B_toy, conn)
C_0, time_0 = approach_0(A_toy, B_toy)
result_1, time_1 = approach_1(conn)

print("Ansatz 0 Ergebnis:\n", C_0)
print("Zeit:", time_0, "Sekunden")
print("Ansatz 1 Ergebnis:", result_1)
print("Zeit:", time_1, "Sekunden")

conn.close()
