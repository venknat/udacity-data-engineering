import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops the tables, using the queries specified in drop_table_queries (see sql_queries.py)
    :param cur: db cursor
    :param conn: db connection
    :return: nothing
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the tables, using the queries specified in create_table_queries (see sql_queries.py)
    :param cur: db cursor
    :param conn: db connection
    :return: nothing
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Drops and recreates tables for sparkify db, using the parameters in dwh.config
    :return: Nothing
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = None
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            config.get('CLUSTER', 'HOST'),
            config.get('CLUSTER', 'DB_NAME'),
            config.get('CLUSTER', 'DB_USER'),
            config.get('CLUSTER', 'DB_PASSWORD'),
            config.get('CLUSTER', 'DB_PORT')))
        cur = conn.cursor()
        drop_tables(cur, conn)
        create_tables(cur, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()