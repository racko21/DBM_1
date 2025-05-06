def multiply_naive(A: list, B: list):
    """
    Führt eine naive Matrixmultiplikation von A und B durch (drei geschachtelte Schleifen).
    A: 2D-Liste der Dimension m x l
    B: 2D-Liste der Dimension l x n
    Rückgabe: 2D-Liste C der Dimension m x n, mit C = A * B
    """
    m = len(A)         # Zeilen von A
    l = len(A[0])      # Spalten von A (und Zeilen von B)
    n = len(B[0])      # Spalten von B
    # Dimensionen prüfen
    assert len(B) == l, "DimensionMismatch: Die Spaltenzahl von A entspricht nicht der Zeilenzahl von B."
    # Ergebnismatrix C initialisieren mit Nullen (m x n)
    C = [[0.0 for _ in range(n)] for _ in range(m)]
    # Alle Zeilen i von A
    for i in range(m):
        # Alle Spalten j von B
        for j in range(n):
            summation = 0.0
            # Über die gemeinsame Dimension k iterieren
            for k in range(l):
                # Beitrag zum Eintrag C[i][j] summieren
                summation += A[i][k] * B[k][j]
            C[i][j] = summation
    return C
