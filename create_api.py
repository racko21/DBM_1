import psycopg
import config

def create_api_functions():
    conn = psycopg.connect(f"dbname={config.DB_NAME} user={config.DB_USER}")
    conn.autocommit = True
    cur = conn.cursor()

    # q_i: Liefert das horizontale Tupel per OID aus H_VIEW
    q_i_sql = """
    CREATE OR REPLACE FUNCTION q_i(p_oid INTEGER)
    RETURNS SETOF H_VIEW AS $$
        SELECT * FROM H_VIEW WHERE oid = p_oid;
    $$ LANGUAGE SQL;
    """
    cur.execute(q_i_sql)
    print("Funktion q_i(p_oid INTEGER) wurde erstellt.")

    # q_ii: Dynamische Funktion, die p_attribute als Spaltenname verwendet und p_value als Filterwert.
    # p_value ist polymorph (ANYELEMENT) – somit passt sie sich dem korrekten Typ an.
    q_ii_sql = """
    CREATE OR REPLACE FUNCTION q_ii(p_attribute TEXT, p_value ANYELEMENT)
    RETURNS SETOF H_VIEW AS
    $$
    BEGIN
      RETURN QUERY EXECUTE format('SELECT * FROM H_VIEW WHERE %I = $1', p_attribute)
      USING p_value;
    END;
    $$ LANGUAGE plpgsql;
    """
    cur.execute(q_ii_sql)
    print("Funktion q_ii(p_attribute TEXT, p_value ANYELEMENT) wurde erstellt.")

    cur.close()
    conn.close()

if __name__ == '__main__':
    create_api_functions()
