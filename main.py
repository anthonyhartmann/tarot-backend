import datetime
import json
import random
import hashlib

import pg8000
from flask import Flask
from google.cloud.sql.connector import Connector
from dotenv import dotenv_values

app = Flask(__name__)

tarot = list(range(78))


def serialize_datetime(obj):
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def hash_string(s):
    s_bytes = bytes(s, 'utf-8')
    four_hex = hashlib.shake_128(s_bytes).hexdigest(4)
    decimal = int(four_hex, 16)
    normalized = (2 * ((decimal) / (4294967296))) - 1
    return normalized


def get_db_connection() -> pg8000.dbapi.Connection:
    config = dotenv_values(".env")
    connector1 = Connector()
    conn: pg8000.dbapi.Connection = connector1.connect(
        config["DB_ICS"],
        "pg8000",
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        db=config["DB_NAME"]
    )
    return conn


def rows_to_dict(rows, headers):
    return [{headers[i] : row[i] for i in range(len(headers))} for row in rows]


@app.route('/data')
def choose_tarot():
    conn = get_db_connection()
    cur = conn.cursor()
    today_date = datetime.date.today()
    seed = hash_string(today_date.isoformat())
    cur.execute('SELECT setseed(' + str(seed) + ')')
    cur.execute('SELECT * from cards ORDER BY random() LIMIT 10')
    cards = cur.fetchall()
    headers = [x[0] for x in cur.description]
    cards = rows_to_dict(cards, headers)
    for c in cards:
        c["reversed"] = bool(random.getrandbits(1))
    cards = json.dumps(cards, default=serialize_datetime)
    cur.close()
    conn.close()
    return cards


if __name__ == '__main__':
    #initialize()
    app.run(debug=True)
