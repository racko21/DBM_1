import subprocess
import time
import random
import psycopg
import config
import pandas as pd
import matplotlib.pyplot as plt

# Parameterbereiche
H_sizes = [4096, 16384, 65536]
A_counts = [5, 50, 100]
sparsities = [0.5, 0.75, 0.875]

# Ergebnisse werden hier gesammelt
results = []

def measure_conversion(command_args):
    """Misst die Dauer eines Umwandlungsaufrufs (z.B. h2v oder v2h) über subprocess."""
    start_time = time.perf_counter()
    subprocess.run(command_args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.perf_counter() - start_time

def measure_throughput(query, params_generator, duration):
    """
    Führt für die gegebene Dauer (in Sekunden) möglichst viele Abfragen aus und
    berechnet den Durchsatz (Queries pro Sekunde).
    """
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    with conn.cursor() as cur:
        end_time = time.perf_counter() + duration
        count = 0
        while time.perf_counter() < end_time:
            params = params_generator()
            cur.execute(query, params)
            _ = cur.fetchall()  # Ergebnisse holen, um die Query vollständig auszuführen
            count += 1
    conn.close()
    return count / duration

def get_random_oid(H):
    return random.randint(1, H)

def main():
    print(f"{'|H|':>5}  {'|A|':>4}  {'S':>6}  {'Layout':>8}  {'Type':>5}  {'Throughput(Q/s)':>16}  {'ConvTime(s)':>11}")
    for H in H_sizes:
        for A in A_counts:
            for S in sparsities:
                # 1. Tabelle H erzeugen
                subprocess.run(
                    ["python", "generate.py", str(H), str(S), str(A)],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )

                # 2. Umwandlung H -> V_all (h2v) ohne index
                conv_time_h2v = measure_conversion(["python", "phase2.py", "h2v", "H"])
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Index": "no",
                    "Type": "conv",
                    "Throughput": conv_time_h2v,
                })

                # 3. Query-Durchsatzmessung auf V_all, Query Typ i: SELECT * FROM V_all WHERE oid = ?
                def params_gen_v_i():
                    return (get_random_oid(H),)
                qps_v_i_no_idx = measure_throughput("SELECT * FROM V_all WHERE oid = %s", params_gen_v_i, 10.0)
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Index": "no",
                    "Type": "i",
                    "Throughput": qps_v_i_no_idx,
                })

                # 4. Query-Durchsatzmessung auf V_all, Query Typ ii: SELECT oid FROM V_all WHERE attribute = ? AND value = ?
                conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT attribute, value FROM V_all ORDER BY RANDOM() LIMIT 100")
                    samples = cur.fetchall()
                conn.close()
                if samples:
                    sample_pairs_v = samples
                else:
                    sample_pairs_v = [(f"A{random.randint(1, A)}", None) for _ in range(100)]
                def params_gen_v_ii():
                    return random.choice(sample_pairs_v)
                qps_v_i_no_idx = measure_throughput("SELECT oid FROM V_all WHERE attribute = %s AND value = %s", params_gen_v_ii, 10.0)
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Index": "no",
                    "Type": "ii",
                    "Throughput": qps_v_i_no_idx,
                })

                # 5. Umwandlung V_all -> H_view (v2h) und Messung der Dauer
                conv_time_v2h = measure_conversion(["python", "phase2.py", "v2h", "V_all"])
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Index": "no",
                    "Type": "conv",
                    "Throughput": conv_time_v2h,
                })

                # 6. Query-Durchsatzmessung auf H_view, Query Typ i: SELECT * FROM H_view WHERE oid = ?
                def params_gen_h_i():
                    return (get_random_oid(H),)
                qps_h_i_no_idx = measure_throughput("SELECT * FROM H_view WHERE oid = %s", params_gen_h_i, 10.0)
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Index": "no",
                    "Type": "i",
                    "Throughput": qps_h_i_no_idx,
                    "ConvTime": None
                })

                # 7. Query-Durchsatzmessung auf H_view, Query Typ ii: SELECT oid FROM H_view WHERE <attribute> = ?
                conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM H_view ORDER BY RANDOM() LIMIT 100")
                    rows = cur.fetchall()
                conn.close()
                sample_pairs_h = []
                if rows:
                    for row in rows:
                        if len(row) >= 2:
                            attr_index = random.randint(1, len(row) - 2)
                            attr_name = f"A{attr_index}"
                            sample_pairs_h.append((attr_name, row[attr_index]))
                if not sample_pairs_h:
                    sample_pairs_h = [(f"A{random.randint(1, A)}", None) for _ in range(100)]
                
                def measure_h_view_typeii():
                    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        end_time = time.perf_counter() + 10.0
                        count = 0
                        while time.perf_counter() < end_time:
                            attr, val = random.choice(sample_pairs_h)
                            query = f"SELECT oid FROM H_view WHERE {attr} = %s"
                            cur.execute(query, (val,))
                            _ = cur.fetchall()
                            count += 1
                    conn.close()
                    return count / 10.0
                qps_h_ii_no_idx = measure_h_view_typeii()
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Index": "no",
                    "Type": "ii",
                    "Throughput": qps_h_ii_no_idx,
                    "ConvTime": None
                })

                # 2. Umwandlung H -> V_all (h2v) ohne index
                conv_time_h2v = measure_conversion(["python", "phase3.py", "h2v", "H"])
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Index": "yes",
                    "Type": "conv",
                    "Throughput": conv_time_h2v,
                })

                # 3. Query-Durchsatzmessung auf V_all, Query Typ i: SELECT * FROM V_all WHERE oid = ?
                qps_v_i_idx = measure_throughput("SELECT * FROM V_all WHERE oid = %s", params_gen_v_i, 10.0)
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Index": "yes",
                    "Type": "i",
                    "Throughput": qps_v_i_idx,
                })

                # 4. Query-Durchsatzmessung auf V_all, Query Typ ii: SELECT oid FROM V_all WHERE attribute = ? AND value = ?
                conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT attribute, value FROM V_all ORDER BY RANDOM() LIMIT 100")
                    samples = cur.fetchall()
                conn.close()
                if samples:
                    sample_pairs_v = samples
                else:
                    sample_pairs_v = [(f"A{random.randint(1, A)}", None) for _ in range(100)]
                qps_v_ii_idx = measure_throughput("SELECT oid FROM V_all WHERE attribute = %s AND value = %s", params_gen_v_ii, 10.0)
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Index": "yes",
                    "Type": "ii",
                    "Throughput": qps_v_ii_idx,
                })

                # 5. Umwandlung V_all -> H_view (v2h) und Messung der Dauer
                conv_time_v2h = measure_conversion(["python", "phase3.py", "v2h", "V_all"])
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Index": "yes",
                    "Type": "conv",
                    "Throughput": conv_time_v2h,
                })

                # 6. Query-Durchsatzmessung auf H_view, Query Typ i: SELECT * FROM H_view WHERE oid = ?
                qps_h_i_idx = measure_throughput("SELECT * FROM H_view WHERE oid = %s", params_gen_h_i, 10.0)
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Index": "yes",
                    "Type": "i",
                    "Throughput": qps_h_i_idx,
                    "ConvTime": None
                })

                # 7. Query-Durchsatzmessung auf H_view, Query Typ ii: SELECT oid FROM H_view WHERE <attribute> = ?
                conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM H_view ORDER BY RANDOM() LIMIT 100")
                    rows = cur.fetchall()
                conn.close()
                sample_pairs_h = []
                if rows:
                    for row in rows:
                        if len(row) >= 2:
                            attr_index = random.randint(1, len(row) - 2)
                            attr_name = f"A{attr_index}"
                            sample_pairs_h.append((attr_name, row[attr_index]))
                if not sample_pairs_h:
                    sample_pairs_h = [(f"A{random.randint(1, A)}", None) for _ in range(100)]
                qps_h_ii_idx = measure_h_view_typeii()
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Index": "yes",
                    "Type": "ii",
                    "Throughput": qps_h_ii_idx,
                    "ConvTime": None
                })


    # Erstelle einen Pandas DataFrame aus den gesammelten Ergebnissen
    df = pd.DataFrame(results)
    print("\nZusammenfassung der Ergebnisse:")
    print(df)

    # --- Neue Graphenerstellung: Separate, facettierte Diagramme für Query Typ i und ii ---
    query_types = ['i', 'ii']
    A_unique = sorted(df['A'].unique())
    S_unique = sorted(df['S'].unique())

    for qtype in query_types:
        fig, axes = plt.subplots(nrows=len(A_unique), ncols=len(S_unique),
                                 figsize=(4*len(S_unique), 3*len(A_unique)), squeeze=False)
        for i, A_val in enumerate(A_unique):
            for j, S_val in enumerate(S_unique):
                ax = axes[i][j]
                # Filtere Daten für aktuellen Querytyp, Attributanzahl und Sparsity
                sub_df = df[(df['Type'] == qtype) & (df['A'] == A_val) & (df['S'] == S_val)]
                for index_variant in ['yes', 'no']:
                    variant_df = sub_df[sub_df['Index'] == index_variant]
                    if not variant_df.empty:
                        group = variant_df.groupby('H')['Throughput'].mean().reset_index()
                        label = f"Index: {index_variant}"
                        ax.plot(group['H'], group['Throughput'], marker='o', label=label)
                ax.set_xlabel("Anzahl Tupel |H|")
                ax.set_ylabel("Durchsatz (Q/s)")
                ax.set_title(f"Query {qtype} - A={A_val}, S={S_val}")
                ax.legend()
                ax.grid(True)
        fig.suptitle(f"Durchsatzvergleich für Query Typ {qtype}", fontsize=16)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.show()

    # --- Conversion-Zeiten: Vergleich der Umwandlungszeiten für V_ALL und H_VIEW ---
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    conv_df = df[df["Type"] == "conv"]
    for layout in conv_df["Layout"].unique():
        sub_df = conv_df[conv_df["Layout"] == layout]
        group = sub_df.groupby("H")["ConvTime"].mean().reset_index()
        ax2.plot(group["H"], group["ConvTime"], marker="s", label=f"{layout} Conversion")
    ax2.set_xlabel("Anzahl der Tupel |H|")
    ax2.set_ylabel("Umwandlungszeit (s)")
    ax2.set_title("Vergleich der Umwandlungszeiten")
    ax2.legend()
    ax2.grid(True)
    plt.show()

if __name__ == '__main__':
    main()
