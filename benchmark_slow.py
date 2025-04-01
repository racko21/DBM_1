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

results = []

def measure_conversion(command_args):
    start_time = time.time()
    return time.time() - start_time

def measure_throughput_v(query, params_generator, duration, H, S, A):
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    with conn.cursor() as cur:
        end_time = time.time() + duration
        subprocess.run(["python", "phase2.py", "h2v", "H"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        count = 0
        while time.time() < end_time:
            params = params_generator()
            cur.execute(query, params)
            _ = cur.fetchall()  # Ergebnisse holen, um die Query vollständig auszuführen
            count += 1
    conn.close()
    return count / duration

def measure_throughput_h(query, params_generator, duration, H, S, A):
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    with conn.cursor() as cur:
        end_time = time.time() + duration
        subprocess.run(["python", "phase2.py", "v2h", "v_all"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        count = 0
        while time.time() < end_time:
            params = params_generator()
            cur.execute(query, params)
            _ = cur.fetchall()  # Ergebnisse holen, um die Query vollständig auszuführen
            count += 1
    conn.close()
    return count / duration

def get_random_oid(H):
    return random.randint(1, H)

def main():
    # Kopfzeile der Ergebnistabelle
    print(f"{'|H|':>5}  {'|A|':>4}  {'S':>6}  {'Layout':>6}  {'Type':>5}  {'Throughput(Q/s)':>16}")

    for H in H_sizes:
        for A in A_counts:
            for S in sparsities:
                # 1. Tabelle H erzeugen
                subprocess.run(
                    ["python", "generate.py", str(H), str(S), str(A)],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )

                # 3. Query-Durchsatzmessung auf V_all, Query Typ i: SELECT * FROM V_all WHERE oid = ?
                def params_gen_v_i():
                    return (get_random_oid(H),)
                qps_v_i = measure_throughput_v("SELECT * FROM V_all WHERE oid = %s", params_gen_v_i, 10.0, H, S, A)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'V_ALL':>6}  {'i':>5}  {qps_v_i:16.1f}")
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Type": "i",
                    "Throughput": qps_v_i,
                    "ConvTime": None
                })

                # 4. Query-Durchsatzmessung auf V_all, Query Typ ii: SELECT oid FROM V_all WHERE attribute = ? AND value = ?
                # Hier holen wir zunächst 100 zufällige (attribute, value)-Paare aus V_all
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
                qps_v_ii = measure_throughput_v("SELECT oid FROM V_all WHERE attribute = %s AND value = %s", params_gen_v_ii, 10.0, H, S, A)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'V_ALL':>6}  {'ii':>5}  {qps_v_ii:16.1f}")
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "V_ALL",
                    "Type": "ii",
                    "Throughput": qps_v_ii,
                    "ConvTime": None
                })

                # 6. Query-Durchsatzmessung auf H, Query Typ i: SELECT * FROM H WHERE oid = ?
                def params_gen_h_i():
                    return (get_random_oid(H),)
                qps_h_i = measure_throughput_h("SELECT * FROM H_view WHERE oid = %s", params_gen_h_i, 10.0, H, S, A)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'H':>6}  {'i':>5}  {qps_h_i:16.1f}")
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Type": "i",
                    "Throughput": qps_h_i,
                })

                # 7. Query-Durchsatzmessung auf H, Query Typ ii: SELECT oid FROM H
                # Für H wird das Attribut direkt als Spaltenname verwendet.
                # Wir holen 100 zufällige Zeilen aus H, um vorhandene Attributwerte zu ermitteln.
                conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM H ORDER BY RANDOM() LIMIT 100")
                    rows = cur.fetchall()
                conn.close()
                sample_pairs_h = []
                if rows:
                    # Annahme: Erste Spalte ist oid, danach folgen A1, A2, ... 
                    for row in rows:
                        if len(row) >= 2:
                            # Wähle zufällig eine der Attribut-Spalten (Index 1 bis len(row)-1)
                            attr_index = random.randint(1, len(row) - 1)
                            attr_name = f"A{attr_index}"
                            sample_pairs_h.append((attr_name, str(row[attr_index])))
                if not sample_pairs_h:
                    sample_pairs_h = [(f"A{random.randint(1, A)}", None) for _ in range(100)]
                
                # Für H muss der Spaltenname dynamisch in den Query eingebaut werden.
                def measure_H():
                    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        end_time = time.time() + 10.0
                        subprocess.run(["python", "phase2.py", "v2h", "v_all"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        count = 0
                        while time.time() < end_time:
                            attr, val = random.choice(sample_pairs_h)
                            query = f"SELECT oid FROM H_view WHERE {attr} = %s"
                            cur.execute(query, (val,))
                            _ = cur.fetchall()
                            count += 1
                    conn.close()
                    return count / 10.0
                qps_h_ii = measure_H()
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'H':>6}  {'ii':>5}  {qps_h_ii:16.1f}")
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "Layout": "H_VIEW",
                    "Type": "ii",
                    "Throughput": qps_h_ii,
                })

    df = pd.DataFrame(results)
    fig, ax = plt.subplots(figsize=(8, 6))
    for layout in df[df["Type"].isin(["i", "ii"])]["Layout"].unique():
        for qtype in ["i", "ii"]:
            sub_df = df[(df["Layout"] == layout) & (df["Type"] == qtype)]
            group = sub_df.groupby("H")["Throughput"].mean().reset_index()
            label = f"{layout} Query {qtype}"
            marker = "o" if qtype == "i" else "s"
            ax.plot(group["H"], group["Throughput"], marker=marker, label=label)
    ax.set_xlabel("Anzahl der Tupel |H|")
    ax.set_ylabel("Durchsatz (Q/s)")
    ax.set_title("Durchsatzvergleich: Query Typ i und ii")
    ax.legend()
    ax.grid(True)
    plt.show()

if __name__ == '__main__':
    main()
