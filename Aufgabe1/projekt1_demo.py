import psycopg
import random
import time
import math
import itertools
import string
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import config
import re

###########################
# Hilfsfunktionen zur Ausgabe von Tabellen
###########################
def print_table_contents(conn, table_name, title=None, limit=None):
    """Liest die Daten der angegebenen Tabelle oder Sicht und gibt sie als formatiertes DataFrame aus."""
    cur = conn.cursor()
    query = f"SELECT * FROM {table_name}"
    if limit:
        query += f" LIMIT {limit}"
    cur.execute(query)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=headers)
    if not title:
        title = f"Inhalt von {table_name}"
    print(f"\n{title}:")
    print(df.to_string(index=False))
    cur.close()

###########################
# Datenbankverbindung
###########################
def connect_db():
    try:
        conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
        conn.autocommit = True
        print("Verbindung zur Datenbank hergestellt.")
        return conn
    except Exception as e:
        print("Fehler beim Herstellen der Verbindung:", e)
        raise

###########################
# Daten generieren (analog zu generate.py)
###########################
def generate_letter_pool(n):
    pool = []
    length = 1
    while len(pool) < n:
        for p in itertools.product(string.ascii_lowercase, repeat=length):
            pool.append(''.join(p))
            if len(pool) == n:
                break
        length += 1
    return pool

def generate_data(conn, num_tuples, sparsity, num_attributes):
    cur = conn.cursor()
    # Alte Tabelle H löschen
    cur.execute("DROP TABLE IF EXISTS H CASCADE;")
    cur.execute("DROP INDEX IF EXISTS idx_h_oid;")
    
    attributes = []
    att_types  = []
    for i in range(1, num_attributes+1):
        # Zufällig INTEGER oder TEXT wählen
        att_type = "INTEGER" if random.choice([0, 1]) == 1 else "TEXT"
        att_types.append(att_type)
        attributes.append(f"A{i} {att_type}")
        
    create_table_sql = f"CREATE TABLE H (oid SERIAL PRIMARY KEY, {', '.join(attributes)});"
    cur.execute(create_table_sql)
    print(f"Tabelle H mit {num_tuples} Tupeln und {num_attributes} Attributen erstellt.")
    
    # Daten für jede Spalte generieren
    col_data = [None] * num_attributes
    for col in range(num_attributes):
        col_values = [None] * num_tuples
        non_null_idx = []
        for idx in range(num_tuples):
            if random.random() >= sparsity:
                non_null_idx.append(idx)
        distinct_count = math.ceil(len(non_null_idx) / random.choice([1, 2, 3, 4, 5]))
        if att_types[col] == "INTEGER":
            pool = list(range(1, distinct_count + 1))
        else:
            pool = generate_letter_pool(distinct_count)
        # Erzeuge einen Pool, in dem jeder Wert bis zu 5-mal vorkommt
        value_pool = []
        for val in pool:
            value_pool.extend([val] * 5)
        random.shuffle(value_pool)
        for j, pos in enumerate(non_null_idx):
            col_values[pos] = value_pool[j]
        col_data[col] = col_values
    
    # Tupel einfügen
    for i in range(num_tuples):
        row = [col_data[col][i] for col in range(num_attributes)]
        placeholders = ", ".join(["%s"] * num_attributes)
        col_names = ", ".join([f"A{j}" for j in range(1, num_attributes+1)])
        insert_sql = f"INSERT INTO H ({col_names}) VALUES ({placeholders});"
        cur.execute(insert_sql, row)
    
    # Index auf oid und weitere Indizes erstellen
    cur.execute("CREATE INDEX idx_h_oid ON H (oid);")
    cur.execute("""
        DO $$
        DECLARE
            col record;
        BEGIN
            FOR col IN
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'H' AND column_name <> 'oid'
            LOOP
                EXECUTE format('CREATE INDEX IF NOT EXISTS idx_hview_%I ON H (%I)', col.column_name, col.column_name);
            END LOOP;
        END$$;
    """)
    conn.commit()
    cur.close()
    print("Daten in Tabelle H wurden eingefügt.")

###########################
# H2V-Operator (Horizontal zu Vertikal)
###########################
def h2v(conn, table_name):
    cur = conn.cursor()
    try:
        # Alte vertikale Tabellen löschen
        cur.execute("DROP TABLE IF EXISTS V_string CASCADE;")
        cur.execute("DROP TABLE IF EXISTS V_integer CASCADE;")
        # Erstellen der Tabellen für String- und Integer-Werte
        cur.execute("CREATE TABLE V_string (oid INTEGER, attribute TEXT, value TEXT);")
        cur.execute("CREATE TABLE V_integer (oid INTEGER, attribute TEXT, value INTEGER);")
        
        # Spaltennamen und Datentypen der horizontalen Tabelle ermitteln
        cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name.lower()}';")
        string_columns = []
        integer_columns = []
        for row in cur.fetchall():
            column_name, data_type = row
            if column_name != "oid":
                if data_type in ["character varying", "text"]:
                    string_columns.append(column_name)
                elif data_type == "integer":
                    integer_columns.append(column_name)
        
        # Daten in V_string einfügen
        for column in string_columns:
            insert_sql = f"""
                INSERT INTO V_string (oid, attribute, value)
                SELECT oid, '{column}', {column}
                FROM {table_name}
                WHERE {column} IS NOT NULL
                ORDER BY oid;
            """
            cur.execute(insert_sql)
        
        # Daten in V_integer einfügen
        for column in integer_columns:
            insert_sql = f"""
                INSERT INTO V_integer (oid, attribute, value)
                SELECT oid, '{column}', {column}
                FROM {table_name}
                WHERE {column} IS NOT NULL
                ORDER BY oid;
            """
            cur.execute(insert_sql)
        
        # Für vollständig leere Zeilen einen Dummy-Eintrag einfügen
        if (string_columns or integer_columns):
            condition = " AND ".join([f"{col} IS NULL" for col in (string_columns + integer_columns)])
        else:
            condition = "TRUE"
        empty_sql = f"""
            INSERT INTO V_string (oid, attribute, value)
            SELECT oid, null, null
            FROM {table_name}
            WHERE {condition}
            ORDER BY oid;
        """
        cur.execute(empty_sql)
        
        # Erstellen der kombinierten Sicht V_all
        cur.execute("""
            CREATE OR REPLACE VIEW V_all AS
            SELECT oid, attribute, value::VARCHAR(50) AS value FROM V_string
            UNION ALL
            SELECT oid, attribute, value::VARCHAR(50) FROM V_integer
            ORDER BY attribute;
        """)
        conn.commit()
        print("H2V-Operator erfolgreich ausgeführt. Sicht V_all wurde erstellt.")
    except Exception as e:
        print("Fehler im H2V-Operator:", e)
    finally:
        cur.close()

def attribute_sort_key(attr):
    """
    Extrahiert einen Sortierschlüssel, der den alphabetischen Präfix
    und den numerischen Teil (falls vorhanden) enthält.
    """
    match = re.match(r"([A-Za-z]+)(\d+)$", attr)
    if match:
        return (match.group(1), int(match.group(2)))
    else:
        return (attr, 0)

###########################
# V2H-Operator (Vertikal zu Horizontal)
###########################
def v2h(conn, table_name):
    cur = conn.cursor()
    try:
        # Alte Sicht löschen
        cur.execute("DROP VIEW IF EXISTS H_VIEW CASCADE;")
        # Eindeutige Attribute aus der vertikalen Tabelle ermitteln
        cur.execute(f"SELECT DISTINCT attribute FROM {table_name};")
        attributes = [row[0] for row in cur.fetchall()]
        # Aufsteigend sortieren mit unserem benutzerdefinierten Schlüssel
        attributes = sorted(attributes, key=attribute_sort_key)
        
        # Dynamische Erstellung der SELECT-Abfrage für H_VIEW
        create_view_query = "CREATE OR REPLACE VIEW H_VIEW AS SELECT o.oid"
        for index, attribute in enumerate(attributes, start=1):
            create_view_query += f", v{index}.value AS {attribute}"
        create_view_query += f" FROM (SELECT DISTINCT oid FROM {table_name}) o"
        for index, attribute in enumerate(attributes, start=1):
            create_view_query += f" LEFT JOIN {table_name} as v{index} ON o.oid = v{index}.oid AND v{index}.attribute = '{attribute}'"
        if len(attributes) > 1:
            create_view_query += " ORDER BY oid;"
        cur.execute(create_view_query)
        conn.commit()
        print("V2H-Operator erfolgreich ausgeführt. Sicht H_VIEW wurde erstellt mit aufsteigend sortierten Attributen.")
    except Exception as e:
        print("Fehler im V2H-Operator:", e)
    finally:
        cur.close()

###########################
# API-Funktionen erstellen (analog zu create_api.py)
###########################
def create_api_functions(conn):
    cur = conn.cursor()
    try:
        # Attribute aus den vertikalen Tabellen ermitteln
        cur.execute("SELECT DISTINCT attribute FROM V_string;")
        string_attrs = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT attribute FROM V_integer;")
        integer_attrs = [row[0] for row in cur.fetchall()]
        
        # Zusammenführen der Attribute mit zugehörigen Datentypen
        all_attrs = {}
        for attr in string_attrs:
            all_attrs[attr] = "text"
        for attr in integer_attrs:
            if attr not in all_attrs:
                all_attrs[attr] = "integer"
                
        # Aufsteigend sortieren mit benutzerdefiniertem Schlüssel (sofern benötigt)
        sorted_attrs = sorted(all_attrs.keys())
        
        # Dynamisch erzeugte RETURNS-Klausel
        returns_clause = "RETURNS TABLE (oid integer"
        for attr in sorted_attrs:
            returns_clause += f", {attr} {all_attrs[attr]}"
        returns_clause += ")"
        
        # Dynamisch erzeugte SELECT-Spalten (aufsteigend sortiert)
        select_columns = "v.oid"
        for attr in sorted_attrs:
            select_columns += f", MAX(CASE WHEN v.attribute = '{attr}' THEN v.value::{all_attrs[attr]} END) AS {attr}"
        
        # API-Funktion q_i (Abfrage per oid)
        sql_qi = f"""
        DROP FUNCTION IF EXISTS q_i(integer) CASCADE;
        CREATE OR REPLACE FUNCTION q_i(search_oid integer)
        {returns_clause}
        LANGUAGE plpgsql AS $$
        BEGIN
            RETURN QUERY 
            SELECT {select_columns}
            FROM V_all v
            WHERE v.oid = search_oid
            GROUP BY v.oid;
        END;
        $$;
        """
        # API-Funktion q_ii für TEXT-Werte mit case-insensitivem Vergleich
        sql_qii_text = f"""
        DROP FUNCTION IF EXISTS q_ii(text, text) CASCADE;
        CREATE OR REPLACE FUNCTION q_ii(attr_name text, search_value text)
        {returns_clause}
        LANGUAGE plpgsql AS $$
        BEGIN
            RETURN QUERY 
            SELECT {select_columns}
            FROM V_all v
            WHERE v.oid IN (
                SELECT v2.oid FROM V_all v2
                WHERE LOWER(v2.attribute) = LOWER(attr_name)
                  AND LOWER(v2.value) = LOWER(search_value)
            )
            GROUP BY v.oid;
        END;
        $$;
        """
        # API-Funktion q_ii für INTEGER-Werte (case-insensitive ist hier nicht nötig)
        sql_qii_int = f"""
        DROP FUNCTION IF EXISTS q_ii(text, integer) CASCADE;
        CREATE OR REPLACE FUNCTION q_ii(attr_name text, search_value integer)
        {returns_clause}
        LANGUAGE plpgsql AS $$
        BEGIN
            RETURN QUERY 
            SELECT {select_columns}
            FROM V_all v
            WHERE v.oid IN (
                SELECT v2.oid FROM V_all v2
                WHERE v2.attribute = attr_name
                  AND v2.value::integer = search_value
            )
            GROUP BY v.oid;
        END;
        $$;
        """
        cur.execute(sql_qi)
        print("Funktion q_i(search_oid INTEGER) erstellt.")
        cur.execute(sql_qii_text)
        print("Funktion q_ii(attr_name TEXT, search_value TEXT) erstellt.")
        cur.execute(sql_qii_int)
        print("Funktion q_ii(attr_name TEXT, search_value INTEGER) erstellt.")
        conn.commit()
    except Exception as e:
        print("Fehler beim Erstellen der API-Funktionen:", e)
    finally:
        cur.close()

###########################
# Speicherauslastung messen
###########################
def measure_storage_size(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT pg_total_relation_size('{table_name}');")
    size = cur.fetchone()[0]
    cur.close()
    return size

###########################
# EXPLAIN ANALYZE ausführen
###########################
def run_explain_analyze(conn, query, params):
    cur = conn.cursor()
    cur.execute("EXPLAIN ANALYZE " + query, params)
    results = cur.fetchall()
    print("EXPLAIN ANALYZE Ergebnisse:")
    for line in results:
        print(line[0])
    cur.close()

###########################
# API-Funktionen aufrufen
###########################
def call_q_i(conn, oid):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM q_i(%s);", (oid,))
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=headers)
        print(f"\nErgebnisse von q_i (oid={oid}):")
        print(df.to_string(index=False))
    except Exception as e:
        print("Fehler beim Aufruf von q_i:", e)
    finally:
        cur.close()

def call_q_ii(conn, attr, value):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM q_ii(%s, %s);", (attr, value))
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=headers)
        print(f"\nErgebnisse von q_ii (Attribut {attr} = {value}):")
        print(df.to_string(index=False))
    except Exception as e:
        print("Fehler beim Aufruf von q_ii:", e)
    finally:
        cur.close()

###########################
# MAIN-Funktion: Ablauf der Operationen
###########################
def main():
    # Parameter für die Datengenerierung
    num_tuples = 100
    sparsity = 0.5
    num_attributes = 10

    conn = connect_db()
    
    print("\n--- Generierung der Tabelle H ---")
    generate_data(conn, num_tuples, sparsity, num_attributes)
    
    # Ausgabe der Inhalte der Tabelle H formatiert
    print_table_contents(conn, "H", title="Inhalt der Tabelle H")
    
    print("\n--- Ausführung des H2V-Operators ---")
    h2v(conn, "H")
    print_table_contents(conn, "V_all", title="Erste 10 Zeilen der Sicht V_all", limit=10)
    
    print("\n--- Ausführung des V2H-Operators ---")
    v2h(conn, "V_all")
    print_table_contents(conn, "H_VIEW", title="Inhalt der Sicht H_VIEW")
    
    print("\n--- Erstellen der API-Funktionen ---")
    create_api_functions(conn)
    
    print("\n--- Speicherauslastung ermitteln ---")
    size_H = measure_storage_size(conn, "H")
    size_V_all = measure_storage_size(conn, "V_all")
    size_H_VIEW = measure_storage_size(conn, "H_VIEW")
    print(f"Speichergröße von H: {size_H} Bytes")
    print(f"Speichergröße von V_all: {size_V_all} Bytes")
    print(f"Speichergröße von H_VIEW: {size_H_VIEW} Bytes")
    
    print("\n--- EXPLAIN ANALYZE ausführen ---")
    random_oid = random.randint(1, num_tuples)
    print(f"\nEXPLAIN ANALYZE für q_i mit oid {random_oid}:")
    run_explain_analyze(conn, "SELECT * FROM q_i(%s);", (random_oid,))
    
    # Für q_ii: Verwende das Attribut A1 und den ersten nicht-NULL-Wert
    cur = conn.cursor()
    cur.execute("SELECT A1 FROM H WHERE A1 IS NOT NULL LIMIT 1;")
    result = cur.fetchone()
    cur.close()
    if result:
        attr_value = result[0]
        print(f"\nEXPLAIN ANALYZE für q_ii mit Attribut A1 und Wert {attr_value}:")
        run_explain_analyze(conn, "SELECT * FROM q_ii(%s, %s);", ("A1", attr_value))
    
    print("\n--- API-Funktionen aufrufen ---")
    print(f"\nAufruf von q_i mit oid {random_oid}:")
    call_q_i(conn, random_oid)
    if result:
        print(f"\nAufruf von q_ii mit Attribut A1 und Wert {attr_value}:")
        call_q_ii(conn, "A1", attr_value)
    
    conn.close()
    print("\nAlle Operationen abgeschlossen.")

if __name__ == "__main__":
    main()
