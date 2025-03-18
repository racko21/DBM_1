import argparse
import psycopg
import random
import math
import itertools
import string

def generate(num_tuples, sparsity, num_attributes):
    conn = psycopg.connect("dbname=racko21 user=racko21")
    cur  = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS H;")

    attributes = []
    att_types  = []
    for i in range(1, num_attributes+1):
        if random.choice([0,1]) == 1:
            att_type = "INTEGER"
        else:
            att_type = "TEXT"
        att_types.append(att_type)
        attributes.append(f"A{i} {att_type}")

    create_table = f"CREATE TABLE H (id SERIAL PRIMARY KEY, {','.join(attributes)});"
    cur.execute(create_table)

    col_data = [None] * num_attributes
    for col in range(num_attributes):
        col_values   = [None] * num_tuples
        non_null_idx = [] 

        for idx in range(num_tuples):
            if random.random() >= sparsity:
                non_null_idx.append(idx)

        distinct_attributes = math.ceil(len(non_null_idx)/5)

        if att_types[col] == "INTEGER":
            pool = list(range(1, distinct_attributes+1))
        else:
            pool = generate_letter_pool(distinct_attributes)

        value_pool = []
        for val in pool:
            value_pool.extend([val]*5)
        random.shuffle(value_pool)

        for j, pos in enumerate(non_null_idx):
            col_values[pos] = value_pool[j]

        col_data[col] = col_values

    for i in range(num_tuples):
        row = []
        for col in range(num_attributes):
            row.append(col_data[col][i])
        x = ", ".join(["%s"] * num_attributes)
        col_names = ", ".join([f"A{j}" for j in range(1, num_attributes+1)])
        insert = f"INSERT INTO H ({col_names}) VALUES ({x});"
        cur.execute(insert, row)

    conn.commit()
    cur.close()
    conn.close()


def generate_letter_pool(n):
    pool    = []
    length  = 1

    while len(pool) < n:
        for p in itertools.product(string.ascii_lowercase, repeat=length):
            pool.append(''.join(p))
            if len(pool) == n:
                break
        length += 1

    return pool


if __name__ == '__main__':
    parser = argparse.ArgumentParser("generate")
    parser.add_argument("num_tuples", help = "spezifiziert die Anzahl der Tupel in H", type = int)
    parser.add_argument("sparsity", help = "spezifiziert den (durchschnittlichen) Anteil der Tupel pro Attribut mit dem Attributwert null", type = float)
    parser.add_argument("num_attributes", help = "spezifiziert die Anzahl an Attributen pro Tupel", type = int)
    args = parser.parse_args()

    generate(args.num_tuples, args.sparsity, args.num_attributes)
