import psycopg
import argparse
import config
# import psycopg2
#
# create or replace view h2v_toy as
#       select  o.o1,
#               v1.value as a1,
#               v2.value as a2,
#               v3.value as a3
#           from (select distinct o1 from v_toy) o
#           left join v_toy as v1 on o.o1 = v1.o1 and v1.attribute = 'a1'
#           left join v_toy as v2 on o.o1 = v2.o1 and v2.attribute = 'a2'
#           left join v_toy as v3 on o.o1 = v3.o1 and v3.attribute = 'a3'
#           order by o1;

# def get_postgres_connection():
#     try:
#         conn = psycopg2.connect(
#             dbname="postgres",
#             user="postgres",
#             password="zoesair12",
#             host="localhost",
#             port="5432"
#         )
#         print("Verbindung erfolgreich!")
#         return conn
#     except Exception as e:
#         print(f"Fehler bei der Verbindung: {e}")
#         return None




# Horizontal zu Vertikal (H2V) umwandeln
def h2v(table_name):
    try:
        conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
        cur = conn.cursor()

        # Vertikale Tabellen löschen, falls sie existieren
        cur.execute("DROP TABLE IF EXISTS V_string CASCADE;")
        cur.execute("DROP TABLE IF EXISTS V_integer CASCADE;")

        # Erstellen der vertikalen Tabellen für String- und Integer-Werte
        cur.execute("CREATE TABLE V_string (oid INTEGER, attribute TEXT, value TEXT);")
        cur.execute("CREATE TABLE V_integer (oid INTEGER, attribute TEXT, value INTEGER);")

        # Abfragen der Metadaten der horizontalen Tabelle, um die Spaltennamen und Datentypen zu erhalten
        cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name.lower()}';")
        string_columns = []
        integer_columns = []

        for row in cur.fetchall():
            column_name, data_type = row
            if column_name != "oid":  # Die 'oid'-Spalte ausschließen
                if data_type in ["character varying", "text"]:
                    string_columns.append(column_name)
                elif data_type == "integer":
                    integer_columns.append(column_name)

        # String-Werte in die Tabelle V_string einfügen
        for column in string_columns:
            insert_data = f"""
                INSERT INTO V_string (oid, attribute, value)
                SELECT oid, '{column}', {column}
                FROM {table_name}
                WHERE {column} IS NOT NULL
                ORDER BY oid;
            """
            cur.execute(insert_data)

        # Integer-Werte in die Tabelle V_integer einfügen
        for column in integer_columns:
            insert_data = f"""
                INSERT INTO V_integer (oid, attribute, value)
                SELECT oid, '{column}', {column}
                FROM {table_name}
                WHERE {column} IS NOT NULL
                ORDER BY oid;
            """
            cur.execute(insert_data)

        # Eine Sicht erstellen, die die Daten aus V_string und V_integer kombiniert
        cur.execute("""
            CREATE OR REPLACE VIEW V_all AS
            SELECT oid, attribute, value::VARCHAR(50) AS value FROM V_string
            UNION ALL
            SELECT oid, attribute, value::VARCHAR(50) FROM V_integer
            ORDER BY attribute;
        """)

        print("\nH2V-Operator erfolgreich ausgeführt. Tabellen V_string und V_integer wurden erstellt und befüllt.")
        print("Sicht V_all wurde erstellt, um die Daten aus V_string und V_integer zu kombinieren.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Fehler bei der Ausführung des H2V-Operators: {e}")

# Vertikal zu Horizontal (V2H) umwandeln
def v2h(table_name):
    try:
        conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
        cur = conn.cursor()

        # Löschen der Tabelle H_VIEW, falls sie existiert
        # cur.execute("DROP TABLE IF EXISTS H_VIEW CASCADE;")
        # Löschen der Sicht H_VIEW, falls sie existiert
        cur.execute("DROP VIEW IF EXISTS H_VIEW CASCADE;")

        # Abfragen der eindeutigen Attribute in der vertikalen Tabelle
        cur.execute(f"SELECT DISTINCT attribute FROM {table_name};")
        attributes = [row[0] for row in cur.fetchall()]

        # Dynamische Erstellung der SELECT-Abfrage für die Sicht
        create_view_query = f"CREATE OR REPLACE VIEW H_VIEW AS SELECT o.oid"
        for index, attribute in enumerate(attributes, start=1):
            create_view_query += f", v{index}.value AS {attribute}"

        # LEFT JOIN mit der ursprünglichen Tabelle H_toy
        create_view_query += f" FROM (SELECT DISTINCT oid FROM {table_name}) o"
        for index, attribute in enumerate(attributes, start=1):
            create_view_query += f" LEFT JOIN {table_name} as v{index} on o.oid=v{index}.oid and v{index}.attribute='{attribute}'"
        create_view_query += "order by oid;"

        # Erstellen der Sicht H_VIEW
        cur.execute(create_view_query)

        print("\nv2h-Operator erfolgreich ausgeführt. Sicht H_VIEW wurde erstellt.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Fehler bei der Ausführung des V2H-Operators: {e}")

# Überprüft, ob H_toy und H_VIEW identisch sind
def checkCorrectness():
    try:
        conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
        cur = conn.cursor()

        # Abrufen der originalen Daten aus H
        cur.execute("SELECT * FROM H ORDER BY oid;")
        original_data = cur.fetchall()

        # Abrufen der wiederhergestellten Daten aus H_VIEW_ALL
        cur.execute("SELECT * FROM H_VIEW ORDER BY oid;")
        recovered_data = cur.fetchall()

        # Vergleichen der Daten
        if original_data == recovered_data:
            print("Die Daten in H_toy und H_VIEW sind identisch.")
        else:
            print("Die Daten in H_toy und H_VIEW sind NICHT identisch.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Fehler bei der Überprüfung der Daten: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Datenbankoperationen")
    parser.add_argument('operation', choices=['h2v', 'v2h', 'check'], help="Wählen Sie die Operation: h2v, v2h oder check")
    parser.add_argument('table_name', help="Name der Tabelle, auf die die Operation angewendet werden soll")
    args = parser.parse_args()

    if args.operation == 'h2v':
        h2v(args.table_name)
    elif args.operation == 'v2h':
        v2h(args.table_name)
    elif args.operation == 'check':
        checkCorrectness()
