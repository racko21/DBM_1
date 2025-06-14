import psycopg2
from lxml import etree
from collections import defaultdict
import configparser
import html

# =====================================
# KONFIGURATION LADEN
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
# VEREINHEITLICHTE NODE-KLASSE
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

    def __repr__(self, level=0):
        indent = "  " * level
        content = f": {self.content}" if self.content else ""
        result = f"{indent}Node({self.tag}{content})\n"
        for child in self.children:
            result += child.__repr__(level + 1)
        return result

# =====================================
# DB-TABELLEN ERSTELLEN
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
# XML PARSEN UND BAUM AUFBAUEN
# =====================================

def parse_and_transform(file_path):
    # Datei einlesen und Entities wie &auml ersetzen
    with open(file_path, "r", encoding="utf-8") as f:
        raw_xml = f.read()

    replacements = {
        "&auml;": "ä", "&Auml;": "Ä",
        "&ouml;": "ö", "&Ouml;": "Ö",
        "&uuml;": "ü", "&Uuml;": "Ü",
        "&szlig;": "ß",
        "&nbsp;": " ",
        "&amp;": "&",
        "&quot;": "\"",
        "&lt;": "<",
        "&gt;": ">",
    }
    for entity, char in replacements.items():
        raw_xml = raw_xml.replace(entity, char)
                  
    decoded_xml = html.unescape(raw_xml)
    parser = etree.XMLParser(recover=True, encoding='utf-8')
    root = etree.fromstring(decoded_xml.encode("utf-8"), parser)

    bib_node = Node("bib")
    venues = defaultdict(lambda: defaultdict(list))

    for pub in root:
        key = pub.attrib.get("key", "").lower()
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
                    if elem.tag in {
                        "author", "title", "pages", "year", "volume", "journal",
                        "number", "ee", "url", "booktitle"
                    }:
                        if elem.text:
                            decoded_text = html.unescape(elem.text.strip())
                            pub_node.add_child(Node(elem.tag, decoded_text))
    return bib_node

# =====================================
# IN DIE DATENBANK IMPORTIEREN
# =====================================
def import_to_db(root_node, conn):
    cur = conn.cursor()
    root_node.to_edge_model(cur)
    conn.commit()

# =====================================
# XPATH-ACHSEN-FUNKTIONEN
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
# HAUPTPROGRAMM
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
