# phase1_xpath_edge.py
# Phase 1 der Projektaufgabe: Import des Toy-XML in ein EDGE-Modell und Berechnung von XPath-Achsen

import psycopg2
from lxml import etree
from collections import defaultdict
import configparser

# =====================================
# KONFIGURATION LADEN AUS config.ini
# =====================================
config = configparser.ConfigParser()
config.read("config.ini")

DB_NAME = config["postgres"]["dbname"]
DB_USER = config["postgres"]["user"]
DB_PASS = config["postgres"]["password"]
DB_HOST = config["postgres"]["host"]
DB_PORT = config["postgres"]["port"]
TOY_XML_PATH = config["files"]["toy_xml_path"]

# =====================================
# NODE-KLASSE FÜr BAUMSTRUKTUR
# =====================================
class Node:
    _id_counter = 0

    def __init__(self, tag, content=None):
        self.id = Node._id_counter
        Node._id_counter += 1
        self.tag = tag
        self.content = content
        self.children = []

    def add_child(self, node):
        self.children.append(node)

    def to_edge_model(self, cursor):
        cursor.execute("INSERT INTO node (id, s_id, type, content) VALUES (%s, %s, %s, %s)",
                       (self.id, None, self.tag, self.content))
        for child in self.children:
            cursor.execute("INSERT INTO edge (from_id, to_id) VALUES (%s, %s)", (self.id, child.id))
            child.to_edge_model(cursor)

# =====================================
# DATENBANK TABELLEN FÜr EDGE MODELL
# =====================================
def setup_db(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS edge")
    cur.execute("DROP TABLE IF EXISTS node")
    cur.execute("""
        CREATE TABLE node (
            id INTEGER PRIMARY KEY,
            s_id TEXT,
            type TEXT,
            content TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE edge (
            from_id INTEGER,
            to_id INTEGER
        )
    """)
    conn.commit()

# =====================================
# XML PARSEN UND TRANSFORMATION
# =====================================
def parse_and_transform(file_path):
    tree = etree.parse(file_path)
    root = tree.getroot()

    bib_node = Node("bib")
    venues = defaultdict(lambda: defaultdict(list))

    for pub in root:
        key = pub.attrib.get("key", "")
        year = pub.findtext("year")
        if not year:
            continue
        if "vldb" in key:
            venue = "vldb"
        elif "sigmod" in key:
            venue = "sigmod"
        elif "icde" in key:
            venue = "icde"
        else:
            continue
        venues[venue][year].append(pub)

    for venue, years in venues.items():
        venue_node = Node(venue)
        bib_node.add_child(venue_node)
        for year, publications in years.items():
            year_node = Node(f"{venue}_{year}")
            venue_node.add_child(year_node)
            for pub in publications:
                pub_node = Node(pub.tag)
                year_node.add_child(pub_node)
                for elem in pub:
                    if elem.tag in {"author", "title", "pages", "year", "volume", "journal", "number", "ee", "url", "booktitle"}:
                        pub_node.add_child(Node(elem.tag, elem.text))
    return bib_node

# =====================================
# XML-BAUMSTRUKTUR IN DIE DB EINTRAGEN
# =====================================
def import_to_db(root_node, conn):
    cur = conn.cursor()
    root_node.to_edge_model(cur)
    conn.commit()

# =====================================
# XPATH-ACHSEN FUNKTIONEN
# =====================================
def get_ancestors(conn, node_id):
    query = """
    WITH RECURSIVE anc(from_id, to_id) AS (
        SELECT from_id, to_id FROM edge WHERE to_id = %s
        UNION
        SELECT e.from_id, e.to_id FROM edge e JOIN anc ON e.to_id = anc.from_id
    )
    SELECT * FROM node WHERE id IN (SELECT from_id FROM anc)
    """
    cur = conn.cursor()
    cur.execute(query, (node_id,))
    return cur.fetchall()

def get_descendants(conn, node_id):
    query = """
    WITH RECURSIVE descs(from_id, to_id) AS (
        SELECT from_id, to_id FROM edge WHERE from_id = %s
        UNION
        SELECT e.from_id, e.to_id FROM edge e JOIN descs ON e.from_id = descs.to_id
    )
    SELECT * FROM node WHERE id IN (SELECT to_id FROM descs)
    """
    cur = conn.cursor()
    cur.execute(query, (node_id,))
    return cur.fetchall()

def get_siblings(conn, node_id, direction="following"):
    cur = conn.cursor()
    cur.execute("SELECT from_id FROM edge WHERE to_id = %s", (node_id,))
    parent = cur.fetchone()
    if not parent:
        return []

    if direction == "following":
        query = """
        SELECT n.* FROM edge e
        JOIN node n ON e.to_id = n.id
        WHERE e.from_id = %s AND e.to_id > %s
        """
    else:
        query = """
        SELECT n.* FROM edge e
        JOIN node n ON e.to_id = n.id
        WHERE e.from_id = %s AND e.to_id < %s
        """
    cur.execute(query, (parent[0], node_id))
    return cur.fetchall()

# =====================================
# HAUPTFUNKTION
# =====================================
def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

    setup_db(conn)
    print("✅ Tabellen erstellt.")

    root_node = parse_and_transform(TOY_XML_PATH)
    print("✅ XML geparst & transformiert.")

    import_to_db(root_node, conn)
    print("✅ Daten in Datenbank eingefügt.")

    print("\n🔎 Beispiel XPath-Abfragen:")
    for row in get_ancestors(conn, 4):
        print("Ancestor:", row)

    for row in get_descendants(conn, 2):
        print("Descendant:", row)

    for row in get_siblings(conn, 3, "following"):
        print("Following Sibling:", row)

    for row in get_siblings(conn, 20, "preceding"):
        print("Preceding Sibling:", row)

    conn.close()

if __name__ == "__main__":
    main()
