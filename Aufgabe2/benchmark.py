import time
import matplotlib.pyplot as plt
from generate import generate
from multiply import multiply_naive
from phase2_setup import (
    connect_db,
    create_tables_sparse,
    create_tables_vector,
    create_dotproduct_function,
    insert_sparse,
    insert_vector,
)
import psycopg
import config

def multiply_sql_sparse(conn):
    """
    Ansatz 1: SQL-Join auf sparse Darstellung.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT A_sparse.i, B_sparse.j, SUM(A_sparse.val * B_sparse.val)
            FROM A_sparse
            JOIN B_sparse ON A_sparse.j = B_sparse.i
            GROUP BY A_sparse.i, B_sparse.j;
        """)
        return cur.fetchall()

def multiply_sql_vector(conn):
    """
    Ansatz 2: CROSS JOIN auf Vektor-Tabellen mit UDF dotproduct.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT A_vec.i, B_vec.j, dotproduct(A_vec.row, B_vec.col)
            FROM A_vec
            CROSS JOIN B_vec;
        """)
        return cur.fetchall()

def run_benchmark(conn, sizes, sparsities, repeats=3):
    """
    Führt die drei Multiplikationsansätze für alle Kombinationen von
    Matrixgröße l und Sparsity s aus und sammelt die mittleren Laufzeiten.
    """
    results = {
        'python': {s: [] for s in sparsities},
        'sparse': {s: [] for s in sparsities},
        'vector': {s: [] for s in sparsities},
    }

    for l in sizes:
        print(f"\n---- Matrixgröße l = {l} ----")
        for s in sparsities:
            # Zufallsdaten generieren
            A, B = generate(l, s)

            # Tabelle sparse neu anlegen und Daten laden
            create_tables_sparse(conn)
            insert_sparse(conn, A, B)
            # Tabelle vector neu anlegen und Daten laden
            create_tables_vector(conn, l)
            insert_vector(conn, A, B)

            # 1) Python-Ansatz
            times = []
            for _ in range(repeats):
                start = time.perf_counter()
                multiply_naive(A, B)
                times.append(time.perf_counter() - start)
            t_py = sum(times) / repeats
            results['python'][s].append((l, t_py))

            # 2) SQL sparse
            times = []
            for _ in range(repeats):
                start = time.perf_counter()
                multiply_sql_sparse(conn)
                times.append(time.perf_counter() - start)
            t_sp = sum(times) / repeats
            results['sparse'][s].append((l, t_sp))

            # 3) SQL vector
            times = []
            for _ in range(repeats):
                start = time.perf_counter()
                multiply_sql_vector(conn)
                times.append(time.perf_counter() - start)
            t_vec = sum(times) / repeats
            results['vector'][s].append((l, t_vec))

            print(f"s={s:.1f} ➞ Python {t_py:.3f}s, sparse SQL {t_sp:.3f}s, vector SQL {t_vec:.3f}s")

    return results

def plot_results(results, sizes, sparsities):
    """
    Zeichnet mit Matplotlib:
     - Für jeden Ansatz eine Kurve pro Sparsity
    """
    plt.figure(figsize=(12, 8))
    for approach, style in [('python', '--'), ('sparse', '-'), ('vector', ':')]:
        for s in sparsities:
            xs = [l for (l, _) in results[approach][s]]
            ys = [t for (_, t) in results[approach][s]]
            label = f"{approach} s={s:.1f}"
            plt.plot(xs, ys, style, marker='o', label=label)
    plt.xlabel("Matrixgröße l")
    plt.ylabel("Durchschnittliche Laufzeit [s]")
    plt.title("Benchmark: Matrixmultiplikation (3 Ansätze)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_bar_results(results, sizes, sparsities):
    # Gruppiertes Balkendiagramm pro Sparsity
    import numpy as np

    approaches = ['python', 'sparse', 'vector']
    x = np.arange(len(sizes))  # Position der Gruppen
    total_width = 0.8
    single_width = total_width / len(approaches)

    plt.figure(figsize=(14, 8))
    for idx, approach in enumerate(approaches):
        for jdx, s in enumerate(sparsities):
            ys = [t for (_, t) in results[approach][s]]
            # Offset für jede Sparsity-Kurve
            offset = (idx - 1) * single_width + (jdx - len(sparsities)/2) * (single_width/len(sparsities))
            plt.bar(x + offset, ys, width=single_width/len(sparsities), align='center',
                    label=f"{approach} s={s:.1f}" if idx == 0 else None)
    plt.xticks(x, sizes)
    plt.xlabel("Matrixgröße l")
    plt.ylabel("Durchschnittliche Laufzeit [s]")
    plt.title("Benchmark: Matrixmultiplikation (Balkendiagramm)")
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # 1) DB-Verbindung und Setup
    conn = connect_db(dbname=config.DB_NAME, user=config.DB_USER)
    create_dotproduct_function(conn)

    # 2) Benchmark-Parameter
    sizes = [32, 64, 128, 256]
    sparsities = [0.1, 0.3, 0.5, 0.7, 0.9]
    repeats = 3

    # 3) Benchmark durchführen
    results = run_benchmark(conn, sizes, sparsities, repeats)

    # 4) Ergebnisse plotten
    plot_results(results, sizes, sparsities)
    plot_bar_results(results, sizes, sparsities)

    conn.close()
