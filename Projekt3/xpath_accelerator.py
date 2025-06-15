import os
from lxml import etree
from collections import defaultdict
import psycopg
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

DBLP_XML_PATH = "./dblp.xml"
MY_SMALL_BIB_PATH = "./my_small_bib.xml"

def is_relevant_venue(key):
    key = key.lower()
    if key.startswith("journals/pvldb/") or key.startswith("conf/vldb/"):
        return "vldb"
    elif key.startswith("journals/pacmmod/") or key.startswith("conf/sigmod/"):
        return "sigmod"
    elif key.startswith("conf/icde/"):
        return "icde"
    return None

def extract_venues_from_dblp():
    venues = defaultdict(lambda: defaultdict(list))

    for event, elem in etree.iterparse(DBLP_XML_PATH, load_dtd=True, dtd_validation=False, encoding='utf-8'):
        if elem.tag in {"article", "inproceedings"}:
            key = elem.get("key", "").lower()
            venue = is_relevant_venue(key)
            if venue:
                year_elem = elem.find("year")
                if year_elem is not None and year_elem.text and year_elem.text.isdigit():
                    year = year_elem.text.strip()
                    venues[venue][year].append(etree.tostring(elem, encoding='unicode'))
        elem.clear()
    return venues

def build_my_small_bib_xml(venues):
    root = etree.Element("bib")
    for venue, year_dict in venues.items():
        venue_el = etree.SubElement(root, venue)
        for year, entries in sorted(year_dict.items()):
            year_el = etree.SubElement(venue_el, f"{venue}_{year}")
            for entry in entries:
                try:
                    pub = etree.fromstring(entry.encode("utf-8"))
                    year_el.append(pub)
                except Exception:
                    continue
    tree = etree.ElementTree(root)
    tree.write(MY_SMALL_BIB_PATH, pretty_print=True, xml_declaration=True, encoding='utf-8')

def create_accel_schema(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS attribute")
        cur.execute("DROP TABLE IF EXISTS content")
        cur.execute("DROP TABLE IF EXISTS accel")
        cur.execute("""
            CREATE TABLE accel (
                pre INTEGER PRIMARY KEY,
                post INTEGER,
                parent INTEGER,
                kind TEXT,
                name TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE content (
                pre INTEGER PRIMARY KEY REFERENCES accel(pre),
                text TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE attribute (
                pre INTEGER PRIMARY KEY REFERENCES accel(pre),
                text TEXT
            )
        """)
    conn.commit()

# Run all steps
def main():
    print("📦 Extracting DBLP entries...")
    venues = extract_venues_from_dblp()
    print(f"✅ Extracted venues: {', '.join(venues.keys())}")

    print("📄 Building my_small_bib.xml...")
    build_my_small_bib_xml(venues)
    print("✅ my_small_bib.xml created at", MY_SMALL_BIB_PATH)

    print("🗄️ Setting up accel schema...")
    with psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT) as conn:
        create_accel_schema(conn)
    print("✅ Database schema ready.")

if __name__ == "__main__":
    main()
