
import subprocess
import time
import random
import psycopg
import config

# Parameterbereiche
H_sizes = [4096, 16384, 65536]
A_counts = [5, 50, 100]
sparsities = [0.5, 0.75, 0.875]

def measure_conversion(command_args):
    """Misst die Dauer eines Umwandlungsaufrufs (z.B. h2v oder v2h) über subprocess."""
    start_time = time.time()
    subprocess.run(command_args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.time() - start_time

def measure_throughput(query, params_generator, duration):
    """
    Führt für die gegebene Dauer (in Sekunden) möglichst viele Abfragen aus und
    berechnet den Durchsatz (Queries pro Sekunde).
    
    :param query: SQL-Query (mit Platzhaltern)
    :param params_generator: Funktion, die bei jedem Aufruf ein Parameter-Tupel zurückgibt.
    :param duration: Messdauer in Sekunden.
    :param conn_params: Dictionary mit Verbindungsparametern für psycopg.connect.
    """
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    with conn.cursor() as cur:
        end_time = time.time() + duration
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
    print(f"{'|H|':>5}  {'|A|':>4}  {'S':>6}  {'Layout':>6}  {'Type':>5}  {'Throughput(Q/s)':>16}  {'ConvTime(s)':>11}")

    for H in H_sizes:
        for A in A_counts:
            for S in sparsities:
                # 1. Tabelle H erzeugen
                subprocess.run(
                    ["python", "generate.py", str(H), str(S), str(A)],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )

                # 2. Umwandlung H -> V_all (h2v) und Messung der Dauer
                conv_time_h2v = measure_conversion(["python", "phase2.py", "h2v", "H"])
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'V_ALL':>6}  {'conv':>5}  {'-':>16}  {conv_time_h2v:11.2f}")

                # 3. Query-Durchsatzmessung auf V_all, Query Typ i: SELECT * FROM V_all WHERE oid = ?
                def params_gen_v_i():
                    return (get_random_oid(H),)
                qps_v_i = measure_throughput("SELECT * FROM V_all WHERE oid = %s", params_gen_v_i, 1.0)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'V_ALL':>6}  {'i':>5}  {qps_v_i:16.1f}  {'-':>11}")

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
                qps_v_ii = measure_throughput("SELECT oid FROM V_all WHERE attribute = %s AND value = %s", params_gen_v_ii, 1.0)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'V_ALL':>6}  {'ii':>5}  {qps_v_ii:16.1f}  {'-':>11}")

                # 5. Umwandlung V_all -> H (v2h) und Messung der Dauer
                conv_time_v2h = measure_conversion(["python", "phase2.py", "v2h", "V_all"])
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'H':>6}  {'conv':>5}  {'-':>16}  {conv_time_v2h:11.2f}")

                # 6. Query-Durchsatzmessung auf H, Query Typ i: SELECT * FROM H WHERE oid = ?
                def params_gen_h_i():
                    return (get_random_oid(H),)
                qps_h_i = measure_throughput("SELECT * FROM H WHERE oid = %s", params_gen_h_i, 1.0)
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'H':>6}  {'i':>5}  {qps_h_i:16.1f}  {'-':>11}")

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
                            sample_pairs_h.append((attr_name, row[attr_index]))
                if not sample_pairs_h:
                    sample_pairs_h = [(f"A{random.randint(1, A)}", None) for _ in range(100)]
                
                # Für H muss der Spaltenname dynamisch in den Query eingebaut werden.
                def measure_H():
                    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        end_time = time.time() + 1.0
                        count = 0
                        while time.time() < end_time:
                            attr, val = random.choice(sample_pairs_h)
                            query = f"SELECT oid FROM H WHERE {attr} = %s"
                            cur.execute(query, (val,))
                            _ = cur.fetchall()
                            count += 1
                    conn.close()
                    return count / 1.0
                qps_h_ii = measure_H()
                print(f"{H:5d}  {A:4d}  {S:<6.3f}  {'H':>6}  {'ii':>5}  {qps_h_ii:16.1f}  {'-':>11}")

if __name__ == '__main__':
    main()
