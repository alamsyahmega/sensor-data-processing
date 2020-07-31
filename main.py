import mysql.connector
import json


with open('config.json') as fp:
    config = json.load(fp)
    fp.close()


def sql_connection():
    db = config['database']
    try:
        conn = mysql.connector.connect(
            host=db['host'],
            port=db['port'],
            user=db['user'],
            password=db['password'],
            database=db['database']
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as error:
        print(error)


def insert_to_db(database, *args):
    conn, cur = sql_connection()
    cur.execute(f"INSERT INTO {database} (inserted_at, temperature, humidity, co, co2) VALUES \
        ({args[0]},{args[1]},{args[2]},{args[3]},{args[4]})")
    conn.commit()
    conn.close()


def query_db(*args):
    conn, cur = sql_connection()
    cur.execute(f"SELECT {args[0]} from {args[1]} {args[2]}")
    row = cur.fetchone()
    conn.close()
    return row


def process_fusi(data, key):
    get_data = [i['key'] for i in data]
    max_data = max(get_data) # Tergantung matematik untuk fuzzy logic yang diinginkan
    # nilai rata2 => average_data = sum(get_data) / len(get_data)
    return max_data


def main_program():
    while True:
        ref = query_db("reftime", "time_reference", "WHERE is_proccess=0", "limit 1")
        if ref:
            data = [query_db("*", node, f"where inserted_at={ref} limit 1") for node in config['node']['nodes']]
            if len(data > 0):
                humidity = process_fusi(data, "humidity")
                temperature = process_fusi(data, "temperature")
                co = process_fusi(data, "co")
                co2 = process_fusi(data, "co2")
                insert_to_db("DataSet", ref, temperature, humidity, co, co2)


if __name__ == "__main__":
    main_program()
