import random

def generate(l: int, sparsity: float):
    """
    Erzeugt zwei Matrizen A und B mit gegebenem sparsity-Anteil an Nullwerten.
    - Dimensionen: A ist (m x l) und B ist (l x n), wobei m = l-1 und n = l-1.
    - sparsity: Anteil der Einträge, die Null sein sollen (Wertebereich 0 bis 1).
    Rückgabe: Tupel (A, B) mit A und B als 2D-Listen (List of Lists).
    """
    # Berechne m und n aus l (m+1 = l = n+1)
    m = l - 1  # Zeilenanzahl von A
    n = l - 1  # Spaltenanzahl von B
    # Initialisiere A und B mit Nullen
    A = [[0 for _ in range(l)] for _ in range(m)]
    B = [[0 for _ in range(n)] for _ in range(l)]
    # Fülle Matrix A
    for i in range(m):
        for j in range(l):
            # Erzeuge einen Wert ungleich 0 mit Wahrscheinlichkeit (1 - sparsity)
            if random.random() >= sparsity:
                # Zufälligen Double-Wert erzeugen (hier z.B. zwischen 1.0 und 10.0)
                value = random.uniform(1.0, 10.0)
                # Optional: auf 2 Nachkommastellen runden für Übersichtlichkeit
                A[i][j] = round(value, 2)
            else:
                A[i][j] = 0.0  # mit gegebener Wahrscheinlichkeit sparsity -> Null
    # Fülle Matrix B
    for i in range(l):
        for j in range(n):
            if random.random() >= sparsity:
                value = random.uniform(1.0, 10.0)
                B[i][j] = round(value, 2)
            else:
                B[i][j] = 0.0
    return A, B
