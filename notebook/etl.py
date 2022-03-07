import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('\nLoading staging tables ...')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done')


def insert_tables(cur, conn):
    print('\nInserting into final tables ...')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("user={} password={} host={} port={} dbname={}".format(*config['DATABASE'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()