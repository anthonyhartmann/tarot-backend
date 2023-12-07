import csv
from google.cloud.sql.connector import Connector, connector
import pg8000
from dotenv import dotenv_values


def initialize():
    config = dotenv_values(".env")
    connector1 = Connector()

    def get_conn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector1.connect(
            config["DB_ICS"],
            "pg8000",
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
            db=config["DB_NAME"]
        )
        return conn

    conn = get_conn()

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a command: this creates a new table
    cur.execute('DROP TABLE IF EXISTS cards;')
    cur.execute('DROP TYPE IF EXISTS suit;')
    cur.execute('CREATE TYPE suit AS ENUM (\'major\', \'cups\', \'wands\', \'swords\', \'pentacles\');')

    cur.execute('CREATE TABLE cards (id serial PRIMARY KEY,'
                'name varchar (50) NOT NULL,'
                'reversed_description text NOT NULL,'
                'upright_description text NOT NULL,'
                'suit suit NOT NULL,'
                'upright_card_tags text[] NOT NULL,'
                'reverse_card_tags text[] NOT NULL,'
                'file_path varchar (1024),'
                'date_added date DEFAULT CURRENT_TIMESTAMP);'
                )

    cur.execute('DROP INDEX IF EXISTS cards_x_upright;')
    cur.execute('CREATE INDEX cards_x_upright ON cards USING GIN (upright_card_tags);')

    cur.execute('DROP INDEX IF EXISTS cards_x_reverse;')
    cur.execute('CREATE INDEX cards_x_reverse ON cards USING GIN (reverse_card_tags);')

    conn.commit()

    def seed_db(cur, csv_path='tarot_guide.csv'):
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                upright_description = row["upright_description"].replace("’", "''")
                reversed_description = row["reversed_description"].replace("’", "''")
                upright_card_tags = [x.lower().strip() for x in row["upright_card_tags"].split(",")]
                reverse_card_tags = [x.lower().strip() for x in row["reverse_card_tags"].split(",")]
                cur.execute('INSERT INTO cards '
                            '(name, reversed_description, '
                            'upright_description, suit, '
                            'upright_card_tags, reverse_card_tags) '
                            'VALUES(\'{}\',\'{}\',\'{}\',\'{}\',ARRAY {},ARRAY {})'.format(row["name"],
                                                                                           reversed_description,
                                                                                           upright_description,
                                                                                           row["suit"],
                                                                                           upright_card_tags,
                                                                                           reverse_card_tags)
                            )

    seed_db(cur)
    conn.commit()

    cur.close()
    conn.close()
