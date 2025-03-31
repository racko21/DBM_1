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

def measure_throughput(query, params_generator, duration):
    """Misst den Durchsatz (Queries pro Sekunde) für eine gegebene Query."""
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    with conn.cursor() as cur:
        end_time = time.perf_counter() + duration
        count = 0
        while time.perf_counter() < end_time:
            params = params_generator()
            cur.execute(query, params)
            _ = cur.fetchall()
            count += 1
    conn.close()
    return count / duration

def measure_throughput_qii(params_generator, duration):
    """
    Misst den Durchsatz für q_ii.
    Hier wird in jeder Iteration der Datentyp des zweiten Parameters geprüft und
    der Query-String entsprechend angepasst.
    """
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    with conn.cursor() as cur:
        end_time = time.perf_counter() + duration
        count = 0
        while time.perf_counter() < end_time:
            attr, val = params_generator()
            # Bestimme den passenden Type-Cast für p_value:
            if isinstance(val, int):
                query = "SELECT * FROM public.q_ii(%s, %s::integer)"
            elif isinstance(val, str):
                query = "SELECT * FROM public.q_ii(%s, %s::text)"
            else:
                # Falls kein passender Typ ermittelt werden kann, nutze TEXT
                query = "SELECT * FROM public.q_ii(%s, %s::text)"
            cur.execute(query, (attr, val))
            _ = cur.fetchall()
            count += 1
    conn.close()
    return count / duration

def get_random_oid(H):
    return random.randint(1, H)

def benchmark_api():
    print(f"{'|H|':>5}  {'|A|':>4}  {'S':>6}  {'API':>8}  {'Type':>6}  {'Throughput (Q/s)':>18}")
    for H in H_sizes:
        for A in A_counts:
            for S in sparsities:
                # 1. Erzeugen der Testdaten (Tabelle H)
                subprocess.run(
                    ["python", "generate.py", str(H), str(S), str(A)],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                # 2. Erzeugen der Views (H_VIEW wird via phase3.py erzeugt)
                subprocess.run(
                    ["python", "phase3.py", "v2h", "V_all"],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                # 3. Erstellen der API-Funktionen (q_i und q_ii) – falls nicht bereits vorhanden
                subprocess.run(
                    ["python", "create_api.py"],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                
                # 4. API Benchmark für Query Typ i: SELECT * FROM public.q_i(%s::integer)
                def params_gen_qi():
                    return (get_random_oid(H),)
                qps_qi = measure_throughput("SELECT * FROM public.q_i(%s::integer)", params_gen_qi, 10.0)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'API':>8}  {'q_i':>6}  {qps_qi:18.1f}")
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "API": "q_i",
                    "Throughput": qps_qi
                })
                
                # 5. API Benchmark für Query Typ ii: SELECT * FROM public.q_ii(%s, %s)
                # Hole 100 zufällige Zeilen aus H_VIEW, um (Attribut, Wert)-Paare zu sammeln.
                conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM H_VIEW ORDER BY RANDOM() LIMIT 100")
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]
                conn.close()
                # Wähle alle Attribute außer oid
                available_attrs = [col for col in colnames if col != "oid"]
                sample_pairs = []
                if rows and available_attrs:
                    for row in rows:
                        attr = random.choice(available_attrs)
                        idx = colnames.index(attr)
                        sample_pairs.append((attr, row[idx]))
                if not sample_pairs:
                    sample_pairs = [(available_attrs[0], None)] if available_attrs else [("dummy", None)]
                
                def params_gen_qii():
                    return random.choice(sample_pairs)
                
                qps_qii = measure_throughput_qii(params_gen_qii, 10.0)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'API':>8}  {'q_ii':>6}  {qps_qii:18.1f}")
                results.append({
                    "H": H,
                    "A": A,
                    "S": S,
                    "API": "q_ii",
                    "Throughput": qps_qii
                })
                
    # Ergebnisse in einem DataFrame zusammenfassen und ausgeben
    df = pd.DataFrame(results)
    print("\nZusammenfassung der API-Benchmark-Ergebnisse:")
    print(df)
    
    # Plot: Durchsatzvergleich für q_i und q_ii über verschiedene |H|
    fig, ax = plt.subplots(figsize=(8, 6))
    for api in df["API"].unique():
        sub_df = df[df["API"] == api]
        group = sub_df.groupby("H")["Throughput"].mean().reset_index()
        ax.plot(group["H"], group["Throughput"], marker="o", label=f"{api}")
    ax.set_xlabel("Anzahl der Tupel |H|")
    ax.set_ylabel("Durchsatz (Queries/s)")
    ax.set_title("API-Benchmark: Durchsatzvergleich (q_i und q_ii)")
    ax.legend()
    ax.grid(True)
    plt.show()

if __name__ == '__main__':
    benchmark_api()
