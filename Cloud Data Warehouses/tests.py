import configparser
import psycopg2
from sql_queries import test_queries


def create_tables(cur, conn):
    """
    count rows based on code test_queries defined in sql_queries.py
    cur - cursor
    conn - connection to database
    """
    for query in test_queries:
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)
#         conn.commit()


def main():
    """
    run test queries defined in sql_queries.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to Redshift')
    cur = conn.cursor()

    create_tables(cur, conn)

    conn.close()
    print('Connection to Redshift closed')


if __name__ == "__main__":
    main()