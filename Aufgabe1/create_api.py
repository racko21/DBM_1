import psycopg
import config

def create_api_functions():
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    cur = conn.cursor()

    # Ermittele alle Attribute und deren Typen aus den vertikalen Tabellen
    cur.execute("SELECT DISTINCT attribute FROM V_string;")
    string_attrs = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT DISTINCT attribute FROM V_integer;")
    integer_attrs = [row[0] for row in cur.fetchall()]
    
    # Baue ein Dictionary, das jedem Attribut seinen Typ zuordnet
    all_attrs = {}
    for attr in string_attrs:
        all_attrs[attr] = "text"
    for attr in integer_attrs:
        # Falls ein Attribut schon als Text auftaucht, belasse es dabei; sonst als integer
        if attr not in all_attrs:
            all_attrs[attr] = "integer"

    # Dynamisch erstellen des RETURNS TABLE-Teils
    returns_clause = "RETURNS TABLE (oid integer"
    for attr, typ in all_attrs.items():
        returns_clause += f", {attr} {typ}"
    returns_clause += ")"

    # Dynamisch erstellen der SELECT-Spalten mit CASE-Konstruktion
    select_columns = "v.oid"
    for attr, typ in all_attrs.items():
        select_columns += f", MAX(CASE WHEN v.attribute = '{attr}' THEN v.value::{typ} END) AS {attr}"
    
    # API-Funktion: get_by_id
    sql_get_by_id = f"""
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

    # API-Funktion: get_by_attr für Text-Werte
    sql_get_by_attr_text = f"""
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
            WHERE v2.attribute = attr_name AND v2.value = search_value
        )
        GROUP BY v.oid;
    END;
    $$;
    """

    # API-Funktion: get_by_attr für Integer-Werte
    sql_get_by_attr_int = f"""
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
            WHERE v2.attribute = attr_name AND v2.value::integer = search_value
        )
        GROUP BY v.oid;
    END;
    $$;
    """

    try:
        cur.execute(sql_get_by_id)
        print("Funktion q_i(search_oid INTEGER) wurde erstellt.")
        cur.execute(sql_get_by_attr_text)
        print("Funktion q_ii(attr_name TEXT, search_value TEXT) wurde erstellt.")
        cur.execute(sql_get_by_attr_int)
        print("Funktion q_ii(attr_name TEXT, search_value INTEGER) wurde erstellt.")
    except Exception as e:
        print("Fehler beim Erstellen der API-Funktionen: " + str(e))
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_api_functions()
