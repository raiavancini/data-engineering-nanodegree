import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data extracted from S3 bucket into staging tables using the queries in `copy_table_queries` list.
    
    Parameters:
        cur (Cursor): Cursor of the database
        conn (Connection): Connection of the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populate fact and dimension tables using the queries in `insert_table_queries` list.
    
    Parameters:
        cur (Cursor): Cursor of the database
        conn (Connection): Connection of the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with Redshift cluster.
    
    - Load data into staging tables.  
    
    - Populate the other tables (fact and dimension). 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()