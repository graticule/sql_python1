"""
Создайте программу для управления клиентами на python.

Требуется хранить персональную информацию о клиентах:

имя
фамилия
email
телефон
Сложность в том, что телефон у клиента может быть не один, а два, три и даже больше. А может и вообще не быть телефона (например, он не захотел его оставлять).

Вам необходимо разработать структуру БД для хранения информации и несколько функций на python для управления данными:

Функция, создающая структуру БД (таблицы)
Функция, позволяющая добавить нового клиента
Функция, позволяющая добавить телефон для существующего клиента
Функция, позволяющая изменить данные о клиенте
Функция, позволяющая удалить телефон для существующего клиента
Функция, позволяющая удалить существующего клиента
Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
Функции выше являются обязательными, но это не значит что должны быть только они. При необходимости можете создавать дополнительные функции и классы.

Также предоставьте код, демонстрирующий работу всех написанных функций.
"""
import os
from pprint import pprint

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql as psysql


def get_connection():
    return psycopg2.connect(database='client_db', user='postgres', password=os.environ.get('PASSWORD'))


def print_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * 
              FROM clients c
                   LEFT JOIN phone_numbers pn
                   USING (client_id)
            """)
            pprint(cur.fetchall())


def drop_tables():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            DROP TABLE IF EXISTS clients cascade;
            DROP TABLE IF EXISTS phone_numbers;
            """)
            conn.commit()


def create_tables():
    drop_tables()

    with get_connection() as conn:
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


def add_client(first_name, surname, email):
    id = None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO clients (first_name, surname, email)
            VALUES (%s, %s, %s) RETURNING CLIENT_ID;
            """, (first_name, surname, email))
            id = cur.fetchone()[0]
    return id


def add_phone_number_of_client(client_id, phone_number):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO phone_numbers (phone_number, client_id)
                VALUES (%s, %s);
                """, (phone_number, client_id))
            conn.commit()


def change_client(client_id, *, first_name=None, surname=None, email=None):
    params = {'first_name': first_name,
              'surname': surname,
              'email': email}
    with get_connection() as conn:
        with conn.cursor() as cur:
            for k, value in params.items():
                if value is not None:
                    cur.execute(psysql.SQL("""
                        UPDATE clients 
                           SET {} = %s 
                         WHERE client_id = %s;
                        """).format(psysql.Identifier(k)), (value, client_id))
            conn.commit()


def delete_phone_number_of_client(client_id, phone_number):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM phone_numbers
                 WHERE client_id = %s
                   AND phone_number = %s;
                """, (client_id, phone_number))
            conn.commit()


def delete_client(client_id):
    with get_connection() as conn:
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


def find_client_id(*, first_name=None, surname=None, email=None, phone_number=None):
    ids = []
    if first_name is None:
        first_name = '%'
    if surname is None:
        surname = '%'
    if email is None:
        email = '%'
    with get_connection() as conn:
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
    load_dotenv('.env')

    create_tables()

    id = add_client('Michael', 'Scott', 'm.scott@gmail.com')
    add_phone_number_of_client(id, '+1000011')

    id = add_client('Dwight', 'Schrute', 'd.schrute@yahoo.com')
    add_phone_number_of_client(id, '+1000012')
    add_phone_number_of_client(id, '+10025647')

    add_client('Jim', 'Halpert', 'j.halpert@aol.com')

    id = add_client('Pam', 'Beesly', 'p.beesly@gmail.com')
    add_phone_number_of_client(id, '+1089089872')
    add_phone_number_of_client(id, '+71828182')
    ids = find_client_id(phone_number='+71828182')
    if len(ids) == 1:
        delete_phone_number_of_client(ids[0], '+71828182')

    delete_client(1)

    change_client(2, email='d.rute@gmail.com', surname='Rute')
