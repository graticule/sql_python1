from pprint import pprint

import psycopg2
import dotenv
from psycopg2 import sql


ENV_PATH = '.env'


def print_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * 
          FROM clients c
               LEFT JOIN phone_numbers pn
               USING (client_id)
        """)
        pprint(cur.fetchall())


def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS clients cascade;
        DROP TABLE IF EXISTS phone_numbers;
        """)
        conn.commit()


def create_tables(conn):
    drop_tables(conn)

    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            PRIMARY KEY (client_id),
            client_id SERIAL,
            first_name VARCHAR(50) NOT NULL,
            surname VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_numbers (
            PRIMARY KEY (phone_id),
            phone_id SERIAL,
            phone_number VARCHAR(50) UNIQUE NOT NULL,
            client_id INTEGER REFERENCES clients(client_id)
        );
        """)
        conn.commit()


def add_client(conn, first_name, surname, email):
    id = None
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients (first_name, surname, email)
        VALUES (%s, %s, %s) RETURNING CLIENT_ID;
        """, (first_name, surname, email))
        id = cur.fetchone()[0]
    return id


def add_phone_number_of_client(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phone_numbers (phone_number, client_id)
            VALUES (%s, %s);
            """, (phone_number, client_id))
        conn.commit()


def change_client(conn, client_id, *, first_name=None, surname=None, email=None):
    params = {'first_name': first_name,
              'surname': surname,
              'email': email}
    with conn.cursor() as cur:
        for k, value in params.items():
            if value is not None:
                cur.execute(sql.SQL("""
                    UPDATE clients 
                       SET {} = %s 
                     WHERE client_id = %s;
                    """).format(sql.Identifier(k)), (value, client_id))
        conn.commit()


def delete_phone_number_of_client(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone_numbers
             WHERE client_id = %s
               AND phone_number = %s;
            """, (client_id, phone_number))
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone_numbers
             WHERE client_id = %s;
            """, (client_id,))
        cur.execute("""
            DELETE FROM clients
             WHERE client_id = %s;
            """, (client_id,))
        conn.commit()


def find_client_id(conn, *, first_name=None, surname=None, email=None, phone_number=None):
    ids = []
    if first_name is None:
        first_name = '%'
    if surname is None:
        surname = '%'
    if email is None:
        email = '%'
    with conn.cursor() as cur:
        if phone_number is None:
            query = """
                SELECT client_id 
                  FROM clients
                 WHERE first_name LIKE %(first_name)s
                   AND surname LIKE %(surname)s
                   AND email LIKE %(email)s;"""
            cur.execute(query, {'first_name': first_name,
                                'surname': surname,
                                'email': email})
        else:
            query = """
                SELECT c.client_id 
                  FROM clients c
                       LEFT JOIN phone_numbers pn
                       ON pn.client_id = c.client_id
                 WHERE c.first_name LIKE %(first_name)s
                   AND c.surname LIKE %(surname)s
                   AND c.email LIKE %(email)s
                   AND pn.phone_number LIKE %(phone_number)s;"""
            cur.execute(query,
                        {'first_name': first_name,
                         'surname': surname,
                         'email': email,
                         'phone_number': phone_number})
        result = cur.fetchall()
        ids = [d[0] for d in result]
    return ids


if __name__ == "__main__":

    with psycopg2.connect(database='client_db', user='postgres', password=dotenv.get_key(ENV_PATH, 'PASSWORD')) as conn:
        create_tables(conn)

        id = add_client(conn, 'Michael', 'Scott', 'm.scott@gmail.com')
        add_phone_number_of_client(conn, id, '+1000011')

        id = add_client(conn, 'Dwight', 'Schrute', 'd.schrute@yahoo.com')
        add_phone_number_of_client(conn, id, '+1000012')
        add_phone_number_of_client(conn, id, '+10025647')

        add_client(conn, 'Jim', 'Halpert', 'j.halpert@aol.com')

        id = add_client(conn, 'Pam', 'Beesly', 'p.beesly@gmail.com')
        add_phone_number_of_client(conn, id, '+1089089872')
        add_phone_number_of_client(conn, id, '+71828182')
        ids = find_client_id(conn, phone_number='+71828182')
        if len(ids) == 1:
            delete_phone_number_of_client(conn, ids[0], '+71828182')

        delete_client(conn, 1)

        change_client(conn, 2, email='d.rute@gmail.com', surname='Rute')
        
    conn.close()
