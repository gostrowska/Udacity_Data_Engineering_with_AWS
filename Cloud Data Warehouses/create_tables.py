import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop tables based on code drop_table_queries defined in sql_queries.py
    cur - cursor
    conn - connection to database
    """
    for query in drop_table_queries:
        print('\n'.join(('\nDropping table:', query)))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create tables based on code create_table_queries defined in sql_queries.py
    cur - cursor
    conn - connection to database
    """
    for query in create_table_queries:
        print('\n'.join(('\nCreating table:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to Redshift, drop existing tables and create new tables defined in sql_queries.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to Redshift')
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('Connection to Redshift closed')


if __name__ == "__main__":
    main()