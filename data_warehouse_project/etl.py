import configparser

import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries, create_table_queries

def execute_queries(cur, conn, queries, debug=False):
    for query in queries:
        try:
            if debug:
                print(query)
            cur.execute(query)
            conn.commit()
        except Exception:
            print("Error processing {}".format(query))
            raise

def main():
    dwh_config = configparser.ConfigParser()
    dwh_config.read('dwh.cfg')

    cluster_endpoint = dwh_config.get('CLUSTER', 'HOST')
    cluster_port = dwh_config.get('CLUSTER', 'DB_PORT')

    conn = None
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            cluster_endpoint, dwh_config.get('CLUSTER', 'DB_NAME'), dwh_config.get('CLUSTER', 'DB_USER'),
            dwh_config.get('CLUSTER', 'DB_PASSWORD'), cluster_port))
        print('Connected!')
        cur = conn.cursor()
        # execute_queries(cur, conn, drop_table_queries)
        # execute_queries(cur, conn, create_table_queries)
        # TODO: This requires a manual step to add in a sec group ingress, see if this can be done
        # programatically
        execute_queries(cur, conn, copy_table_queries)
        execute_queries(cur, conn, insert_table_queries)
        print("data loaded!")
    finally:
        conn.close() # make sure to close


if __name__ == "__main__":
    main()