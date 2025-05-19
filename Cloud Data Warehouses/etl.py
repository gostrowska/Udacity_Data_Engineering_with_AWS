import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    staging data from s3 to redshift on AWS
    cur - database cursor
    conn - database connection
    """
    for query in copy_table_queries:
        print('\n'.join(('\nLoading staging_table:', query)))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert data from staging tables to fact table and dimension tables 
    cur - database cursor
    conn - database connection
    """
    for query in insert_table_queries:
        print('\n'.join(('\nInserting table:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
    create staging tables insert data into the fact table and dimension tables
    cur - database cursor
    conn - database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to Redshift')
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print('Connection to Redshift closed')


if __name__ == "__main__":
    main()